package held_out_tests;

import com.hazelcast.config.Config;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.TopicOverloadPolicy;
import com.hazelcast.topic.ReliableMessageListener;
import com.hazelcast.topic.impl.reliable.ReliableTopicMessage;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Assume;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hazelcast.test.Accessors.getSerializationService;
import static org.junit.Assert.*;

public class ReliableTopicHeldOutTests extends HazelcastTestSupport {

    private Method maxConcurrentPublishesSetter() {
        try {
            Method m = ReliableTopicConfig.class.getDeclaredMethod("setMaxConcurrentPublishes", int.class);
            m.setAccessible(true);
            return m;
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private String uniqueTopic() {
        return "topic-" + UUID.randomUUID();
    }

    private Config baseConfig(String cluster) {
        Config cfg = smallInstanceConfig();
        cfg.setClusterName(cluster);
        return cfg;
    }

    private RingbufferConfig rbConfig(String topic, int capacity) {
        return new RingbufferConfig("_hz_rb_" + topic).setCapacity(capacity);
    }

    private ReliableTopicConfig rtConfig(String topic, int maxConcurrentPublishes, TopicOverloadPolicy policy) {
        Method setter = maxConcurrentPublishesSetter();
        if (setter == null) {
            throw new org.junit.AssumptionViolatedException("maxConcurrentPublishes not supported");
        }
        ReliableTopicConfig cfg = new ReliableTopicConfig(topic).setTopicOverloadPolicy(policy);
        try {
            setter.invoke(cfg, maxConcurrentPublishes);
        } catch (Exception e) {
            throw new org.junit.AssumptionViolatedException("unable to set maxConcurrentPublishes", e);
        }
        return cfg;
    }

    private ITopic<Integer> getTopic(HazelcastInstance hz, String name) {
        return hz.getReliableTopic(name);
    }

    private Object managerFromProxy(Object proxy) {
        try {
            Field f = proxy.getClass().getDeclaredField("concurrencyManager");
            f.setAccessible(true);
            return f.get(proxy);
        } catch (Exception e) {
            throw new org.junit.AssumptionViolatedException("concurrency manager not available", e);
        }
    }

    private ReliableTopicConfig newConfigWithLimit(int limit) {
        Method setter = maxConcurrentPublishesSetter();
        if (setter == null) {
            throw new org.junit.AssumptionViolatedException("maxConcurrentPublishes not supported");
        }
        ReliableTopicConfig cfg = new ReliableTopicConfig("held-topic").setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK);
        try {
            setter.invoke(cfg, limit);
        } catch (Exception e) {
            throw new org.junit.AssumptionViolatedException("unable to set maxConcurrentPublishes", e);
        }
        return cfg;
    }

    private Object newManager(ReliableTopicConfig cfg) {
        try {
            Class<?> clazz = Class.forName("com.hazelcast.topic.impl.reliable.ReliableTopicPublishManager");
            Constructor<?> ctor = clazz.getDeclaredConstructor(ReliableTopicConfig.class);
            ctor.setAccessible(true);
            return ctor.newInstance(cfg);
        } catch (ClassNotFoundException e) {
            throw new org.junit.AssumptionViolatedException("publish manager not available", e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Method findScheduleMethod(Class<?> clazz) {
        for (Method m : clazz.getDeclaredMethods()) {
            if (m.getParameterCount() == 1) {
                Class<?> p = m.getParameterTypes()[0];
                if (Supplier.class.isAssignableFrom(p) || Callable.class.isAssignableFrom(p)) {
                    m.setAccessible(true);
                    return m;
                }
            }
        }
        return null;
    }

    private boolean awaitQuiescence(Object manager, Duration timeout) throws Exception {
        try {
            Method m = manager.getClass().getDeclaredMethod("awaitQuiescence", Duration.class);
            m.setAccessible(true);
            Object result = m.invoke(manager, timeout);
            return Boolean.TRUE.equals(result);
        } catch (NoSuchMethodException e) {
            Thread.sleep(timeout.toMillis());
            return true;
        }
    }

    @Test
    public void testReadBatchSizePreservedWithConcurrency() throws Exception {
        Assume.assumeNotNull(maxConcurrentPublishesSetter());

        String cluster = "c-readbatch-preserve";
        String topic = uniqueTopic();
        int readBatchSize = 5;
        int maxConcurrentPublishes = 3;
        int totalMessages = 25;

        Config cfg = baseConfig(cluster);
        cfg.addRingBufferConfig(rbConfig(topic, 64));

        ReliableTopicConfig topicConfig = new ReliableTopicConfig(topic)
                .setReadBatchSize(readBatchSize)
                .setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK)
                .setStatisticsEnabled(true);
        maxConcurrentPublishesSetter().invoke(topicConfig, maxConcurrentPublishes);
        cfg.addReliableTopicConfig(topicConfig);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        try {
            ITopic<Integer> reliableTopic = getTopic(hz, topic);

            List<Integer> receivedMessages = Collections.synchronizedList(new ArrayList<>());
            List<Integer> batchSizes = Collections.synchronizedList(new ArrayList<>());
            AtomicInteger currentBatchSize = new AtomicInteger(0);
            CountDownLatch allMessagesReceived = new CountDownLatch(totalMessages);
            AtomicBoolean orderViolation = new AtomicBoolean(false);

            reliableTopic.addMessageListener(new ReliableMessageListener<Integer>() {
                private long lastSequence = -1;
                private int expectedNext = 0;

                @Override
                public long retrieveInitialSequence() {
                    return -1;
                }

                @Override
                public void storeSequence(long sequence) {
                    if (lastSequence != -1 && sequence > lastSequence + 1) {
                        if (currentBatchSize.get() > 0) {
                            batchSizes.add(currentBatchSize.getAndSet(0));
                        }
                    }
                    lastSequence = sequence;
                }

                @Override
                public boolean isLossTolerant() {
                    return false;
                }

                @Override
                public boolean isTerminal(Throwable failure) {
                    return false;
                }

                @Override
                public void onMessage(com.hazelcast.topic.Message<Integer> message) {
                    Integer value = message.getMessageObject();
                    receivedMessages.add(value);
                    if (value != expectedNext) {
                        orderViolation.set(true);
                    }
                    expectedNext++;
                    currentBatchSize.incrementAndGet();
                    allMessagesReceived.countDown();
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });

            ExecutorService publisherPool = Executors.newFixedThreadPool(3);
            List<Future<?>> publishFutures = new ArrayList<>();
            for (int i = 0; i < totalMessages; i++) {
                final int messageValue = i;
                publishFutures.add(publisherPool.submit(() -> reliableTopic.publish(messageValue)));
            }
            for (Future<?> future : publishFutures) {
                future.get(5, TimeUnit.SECONDS);
            }
            publisherPool.shutdown();

            assertTrue("Not all messages received within timeout",
                    allMessagesReceived.await(10, TimeUnit.SECONDS));

            if (currentBatchSize.get() > 0) {
                batchSizes.add(currentBatchSize.get());
            }

            assertEquals(totalMessages, receivedMessages.size());
            assertFalse(orderViolation.get());
            for (int i = 0; i < totalMessages; i++) {
                assertEquals(Integer.valueOf(i), receivedMessages.get(i));
            }
            for (int i = 0; i < batchSizes.size() - 1; i++) {
                assertTrue(batchSizes.get(i) <= readBatchSize);
            }
        } finally {
            Hazelcast.shutdownAll();
        }
    }

    @Test
    public void testRingbufferSequenceMatchesSubmissionOrder() throws Exception {
        Assume.assumeNotNull(maxConcurrentPublishesSetter());

        String cluster = "c-ringbuffer-sequence";
        String topic = uniqueTopic();
        int maxConcurrentPublishes = 4;
        int messageCount = 20;

        Config cfg = baseConfig(cluster);
        cfg.addRingBufferConfig(rbConfig(topic, 256));
        cfg.addReliableTopicConfig(rtConfig(topic, maxConcurrentPublishes, TopicOverloadPolicy.BLOCK));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);

        try {
            ITopic<Integer> reliableTopic = getTopic(hz, topic);
            Ringbuffer<Object> ringbuffer = hz.getRingbuffer("_hz_rb_" + topic);

            List<Long> sequenceNumbers = Collections.synchronizedList(new ArrayList<>());
            List<CompletableFuture<Long>> publishFutures = new ArrayList<>();
            CountDownLatch publishStarted = new CountDownLatch(messageCount);

            for (int i = 0; i < messageCount; i++) {
                final int messageValue = i;
                CompletableFuture<Long> seqFuture = new CompletableFuture<>();
                publishFutures.add(seqFuture);

                new Thread(() -> {
                    try {
                        publishStarted.countDown();
                        reliableTopic.publish(messageValue);
                        long tailSequence = ringbuffer.tailSequence();
                        seqFuture.complete(tailSequence);
                    } catch (Exception e) {
                        seqFuture.completeExceptionally(e);
                    }
                }).start();
            }

            assertTrue(publishStarted.await(5, TimeUnit.SECONDS));

            for (int i = 0; i < messageCount; i++) {
                Long sequence = publishFutures.get(i).get(10, TimeUnit.SECONDS);
                sequenceNumbers.add(sequence);
            }

            List<Integer> messagesInSequenceOrder = new ArrayList<>();
            long headSequence = ringbuffer.headSequence();
            long tailSequence = ringbuffer.tailSequence();
            InternalSerializationService ss = getSerializationService(hz);
            for (long seq = headSequence; seq <= tailSequence; seq++) {
                Object item = ringbuffer.readOne(seq);
                if (item instanceof ReliableTopicMessage msg) {
                    messagesInSequenceOrder.add((Integer) ss.toObject(msg.getPayload()));
                }
            }

            for (int i = 1; i < sequenceNumbers.size(); i++) {
                assertTrue(sequenceNumbers.get(i) > sequenceNumbers.get(i - 1));
            }
            assertEquals(messageCount, messagesInSequenceOrder.size());
            for (int i = 0; i < messageCount; i++) {
                assertEquals(Integer.valueOf(i), messagesInSequenceOrder.get(i));
            }
        } finally {
            Hazelcast.shutdownAll();
        }
    }

    @Test
    public void testPublishOperationsUseConfiguredExecutor() throws Exception {
        Assume.assumeNotNull(maxConcurrentPublishesSetter());

        String executorPrefix = "custom-publish-exec-";
        AtomicInteger threadCounter = new AtomicInteger(0);
        Set<String> publishThreadNames = Collections.synchronizedSet(new HashSet<>());

        ThreadPoolExecutor customExecutor = new ThreadPoolExecutor(
                2, 2, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100),
                r -> {
                    Thread t = new Thread(r);
                    String threadName = executorPrefix + threadCounter.incrementAndGet();
                    t.setName(threadName);
                    t.setDaemon(true);
                    return t;
                }
        );

        int maxConcurrentPublishes = 3;
        ReliableTopicConfig topicConfig = newConfigWithLimit(maxConcurrentPublishes);
        topicConfig.setExecutor(customExecutor);

        Object concurrencyManager = newManager(topicConfig);

        AtomicInteger operationsExecuted = new AtomicInteger(0);
        List<CompletableFuture<String>> threadTrackers = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            CompletableFuture<String> tracker = new CompletableFuture<>();
            threadTrackers.add(tracker);

            Method submitMethod = findScheduleMethod(concurrencyManager.getClass());
            assertNotNull(submitMethod);

            Object operation;
            Class<?> paramType = submitMethod.getParameterTypes()[0];
            if (Supplier.class.isAssignableFrom(paramType)) {
                operation = (Supplier<CompletionStage<?>>) () -> {
                    String threadName = Thread.currentThread().getName();
                    publishThreadNames.add(threadName);
                    operationsExecuted.incrementAndGet();
                    tracker.complete(threadName);
                    CompletableFuture<Void> result = new CompletableFuture<>();
                    try {
                        Thread.sleep(50);
                        result.complete(null);
                    } catch (InterruptedException e) {
                        result.completeExceptionally(e);
                    }
                    return result;
                };
            } else if (Callable.class.isAssignableFrom(paramType)) {
                operation = (Callable<CompletionStage<?>>) () -> {
                    String threadName = Thread.currentThread().getName();
                    publishThreadNames.add(threadName);
                    operationsExecuted.incrementAndGet();
                    tracker.complete(threadName);
                    CompletableFuture<Void> result = new CompletableFuture<>();
                    result.complete(null);
                    return result;
                };
            } else {
                fail("Unsupported parameter type: " + paramType);
                return;
            }

            submitMethod.invoke(concurrencyManager, operation);
        }

        for (CompletableFuture<String> tracker : threadTrackers) {
            tracker.get(5, TimeUnit.SECONDS);
        }

        assertTrue(awaitQuiescence(concurrencyManager, Duration.ofSeconds(5)));

        assertEquals(10, operationsExecuted.get());
        for (String threadName : publishThreadNames) {
            assertTrue(threadName.startsWith(executorPrefix));
        }
        assertTrue(publishThreadNames.size() <= 2);
        Set<String> nonCustomThreads = publishThreadNames.stream()
                .filter(name -> !name.startsWith(executorPrefix))
                .collect(Collectors.toSet());
        assertTrue(nonCustomThreads.isEmpty());

        customExecutor.shutdown();
        assertTrue(customExecutor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    public void testFallbackToSharedPoolWhenNoExecutorConfigured() throws Exception {
        Assume.assumeNotNull(maxConcurrentPublishesSetter());

        int maxConcurrentPublishes = 3;
        ReliableTopicConfig topicConfig = newConfigWithLimit(maxConcurrentPublishes);
        assertNull(topicConfig.getExecutor());

        Object concurrencyManager = newManager(topicConfig);

        Set<String> threadNamesUsed = Collections.synchronizedSet(new HashSet<>());
        AtomicInteger completedOperations = new AtomicInteger(0);
        List<CompletableFuture<Void>> operationFutures = new ArrayList<>();

        for (int i = 0; i < 15; i++) {
            CompletableFuture<Void> opFuture = new CompletableFuture<>();
            operationFutures.add(opFuture);

            Method submitMethod = findScheduleMethod(concurrencyManager.getClass());
            assertNotNull(submitMethod);
            Class<?> paramType = submitMethod.getParameterTypes()[0];
            Object operation;
            if (Supplier.class.isAssignableFrom(paramType)) {
                operation = (Supplier<CompletionStage<?>>) () -> {
                    try {
                        String threadName = Thread.currentThread().getName();
                        threadNamesUsed.add(threadName);
                        Thread.sleep(20);
                        completedOperations.incrementAndGet();
                        opFuture.complete(null);
                        return CompletableFuture.completedFuture(null);
                    } catch (Exception e) {
                        opFuture.completeExceptionally(e);
                        return CompletableFuture.failedFuture(e);
                    }
                };
            } else {
                fail("Unsupported parameter type: " + paramType);
                return;
            }
            submitMethod.invoke(concurrencyManager, operation);
        }

        for (CompletableFuture<Void> future : operationFutures) {
            future.get(5, TimeUnit.SECONDS);
        }

        assertEquals(15, completedOperations.get());
        for (String threadName : threadNamesUsed) {
            assertFalse(threadName.contains("main"));
            assertFalse(threadName.contains("custom-") || threadName.contains("rt-test-exec-"));
        }
        assertTrue(threadNamesUsed.size() <= 10);
        if (maxConcurrentPublishes > 1) {
            assertTrue(threadNamesUsed.size() > 1);
        }

        String cluster = "c-fallback-shared";
        String topic = uniqueTopic();

        Config hzConfig = baseConfig(cluster);
        ReliableTopicConfig hzTopicConfig = new ReliableTopicConfig(topic)
                .setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK);
        maxConcurrentPublishesSetter().invoke(hzTopicConfig, maxConcurrentPublishes);
        hzConfig.addReliableTopicConfig(hzTopicConfig);
        hzConfig.addRingBufferConfig(rbConfig(topic, 64));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(hzConfig);
        try {
            ITopic<String> reliableTopic = hz.getReliableTopic(topic);
            CountDownLatch messagesReceived = new CountDownLatch(5);
            reliableTopic.addMessageListener(msg -> messagesReceived.countDown());
            for (int i = 0; i < 5; i++) {
                reliableTopic.publish("message-" + i);
            }
            assertTrue(messagesReceived.await(5, TimeUnit.SECONDS));
        } finally {
            Hazelcast.shutdownAll();
        }
    }
}
