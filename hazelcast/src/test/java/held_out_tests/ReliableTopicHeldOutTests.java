package held_out_tests;

import com.hazelcast.config.Config;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.TopicOverloadPolicy;
import com.hazelcast.topic.ReliableMessageListener;
import com.hazelcast.topic.impl.reliable.ReliableTopicMessage;
import com.hazelcast.topic.impl.reliable.ReliableTopicProxy;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Assume;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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

            ReliableTopicProxy<Integer> proxy = (ReliableTopicProxy<Integer>) reliableTopic;
            Field rbField = proxy.getClass().getDeclaredField("ringbuffer");
            rbField.setAccessible(true);
            Ringbuffer<ReliableTopicMessage> original = (Ringbuffer<ReliableTopicMessage>) rbField.get(proxy);

            List<Integer> batchSizes = Collections.synchronizedList(new ArrayList<>());
            Ringbuffer<ReliableTopicMessage> spyingRingbuffer = (Ringbuffer<ReliableTopicMessage>) Proxy.newProxyInstance(
                    original.getClass().getClassLoader(),
                    new Class[]{Ringbuffer.class},
                    (p, m, args) -> {
                        Object res = m.invoke(original, args);
                        if ("readManyAsync".equals(m.getName())) {
                            CompletionStage<ReadResultSet<ReliableTopicMessage>> stage =
                                    (CompletionStage<ReadResultSet<ReliableTopicMessage>>) res;
                            stage.whenComplete((result, throwable) -> {
                                if (throwable == null) {
                                    batchSizes.add(result.size());
                                }
                            });
                        }
                        return res;
                    });
            rbField.set(proxy, spyingRingbuffer);

            List<Integer> receivedMessages = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch allMessagesReceived = new CountDownLatch(totalMessages);
            AtomicBoolean orderViolation = new AtomicBoolean(false);

            reliableTopic.addMessageListener(new ReliableMessageListener<Integer>() {
                private int expectedNext = 0;

                @Override
                public long retrieveInitialSequence() {
                    return -1;
                }

                @Override
                public void storeSequence(long sequence) {
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
                    int value = message.getMessageObject();
                    receivedMessages.add(value);
                    if (value != expectedNext) {
                        orderViolation.set(true);
                    }
                    expectedNext++;
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

            assertEquals(totalMessages, receivedMessages.size());
            assertFalse(orderViolation.get());
            for (int i = 0; i < totalMessages; i++) {
                assertEquals(Integer.valueOf(i), receivedMessages.get(i));
            }
            assertEquals(totalMessages, batchSizes.stream().mapToInt(Integer::intValue).sum());
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

            ReliableTopicProxy<Integer> topicProxy = (ReliableTopicProxy<Integer>) reliableTopic;
            Field ringbufferField = ReliableTopicProxy.class.getDeclaredField("ringbuffer");
            ringbufferField.setAccessible(true);
            Ringbuffer<ReliableTopicMessage> originalRingbuffer =
                    (Ringbuffer<ReliableTopicMessage>) ringbufferField.get(topicProxy);

            ConcurrentHashMap<Integer, Long> messageToSequence = new ConcurrentHashMap<>();
            InternalSerializationService ss = Accessors.getSerializationService(hz);

            Ringbuffer<ReliableTopicMessage> proxyRingbuffer =
                    (Ringbuffer<ReliableTopicMessage>) Proxy.newProxyInstance(
                            originalRingbuffer.getClass().getClassLoader(),
                            new Class[]{Ringbuffer.class},
                            (p, m, args) -> {
                                if ("addAsync".equals(m.getName())) {
                                    ReliableTopicMessage msg = (ReliableTopicMessage) args[0];
                                    CompletionStage<Long> future =
                                            (CompletionStage<Long>) m.invoke(originalRingbuffer, args);
                                    future.whenComplete((seq, err) -> {
                                        if (err == null && msg != null) {
                                            Integer value = ss.toObject(msg.getPayload());
                                            messageToSequence.put(value, seq);
                                        }
                                    });
                                    return future;
                                }
                                return m.invoke(originalRingbuffer, args);
                            });
            ringbufferField.set(topicProxy, proxyRingbuffer);

            CyclicBarrier startBarrier = new CyclicBarrier(messageCount + 1);
            List<Integer> submissionOrder = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch allPublished = new CountDownLatch(messageCount);
            List<Thread> publishThreads = new ArrayList<>();
            for (int i = 0; i < messageCount; i++) {
                final int messageValue = i;
                Thread t = new Thread(() -> {
                    try {
                        startBarrier.await();
                        submissionOrder.add(messageValue);
                        reliableTopic.publish(messageValue);
                        allPublished.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }, "publisher-" + i);
                publishThreads.add(t);
            }
            for (int i = 0; i < messageCount; i++) {
                publishThreads.get(i).start();
                Thread.sleep(5);
            }
            startBarrier.await();
            assertTrue("Publishing timed out", allPublished.await(10, TimeUnit.SECONDS));

            long timeout = System.currentTimeMillis() + 5000;
            while (messageToSequence.size() < messageCount && System.currentTimeMillis() < timeout) {
                Thread.sleep(10);
            }
            assertEquals(messageCount, messageToSequence.size());

            List<Long> sequencesInSubmissionOrder = submissionOrder.stream()
                    .map(messageToSequence::get)
                    .collect(Collectors.toList());
            for (int i = 0; i < sequencesInSubmissionOrder.size(); i++) {
                assertNotNull("Missing sequence for message " + submissionOrder.get(i), sequencesInSubmissionOrder.get(i));
            }
            for (int i = 1; i < sequencesInSubmissionOrder.size(); i++) {
                assertTrue(
                        String.format("Sequence for message %d (%d) should be greater than sequence for message %d (%d)",
                                submissionOrder.get(i), sequencesInSubmissionOrder.get(i),
                                submissionOrder.get(i - 1), sequencesInSubmissionOrder.get(i - 1)),
                        sequencesInSubmissionOrder.get(i) > sequencesInSubmissionOrder.get(i - 1));
            }
            Long firstSeq = sequencesInSubmissionOrder.get(0);
            for (int i = 0; i < sequencesInSubmissionOrder.size(); i++) {
                assertEquals(Long.valueOf(firstSeq + i), sequencesInSubmissionOrder.get(i));
            }

            List<Integer> messagesInRingbufferOrder = new ArrayList<>();
            long headSequence = originalRingbuffer.headSequence();
            long tailSequence = originalRingbuffer.tailSequence();
            for (long seq = headSequence; seq <= tailSequence; seq++) {
                ReliableTopicMessage msg = originalRingbuffer.readOne(seq);
                messagesInRingbufferOrder.add(ss.toObject(msg.getPayload()));
            }
            assertEquals(messageCount, messagesInRingbufferOrder.size());
            for (int i = 0; i < messageCount; i++) {
                assertEquals(submissionOrder.get(i), messagesInRingbufferOrder.get(i));
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
            assertTrue("Thread should be a Hazelcast thread: " + threadName,
                    threadName.contains("hz.") || threadName.toLowerCase().contains("hazelcast"));
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
