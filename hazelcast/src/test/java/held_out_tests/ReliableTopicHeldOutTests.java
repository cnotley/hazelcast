package held_out_tests;

import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.test.Accessors;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import com.hazelcast.topic.ReliableMessageListener;
import com.hazelcast.topic.TopicOverloadException;
import com.hazelcast.topic.TopicOverloadPolicy;
import com.hazelcast.topic.impl.reliable.ReliableTopicMessage;
import com.hazelcast.topic.impl.reliable.ReliableTopicProxy;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import org.junit.Assume;
import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.util.Collection;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import static org.junit.Assert.*;

public class ReliableTopicHeldOutTests {
    private static final String MANAGER_FQN = "com.hazelcast.topic.impl.reliable.ReliableTopicConcurrencyManager";
    private static final String PROXY_FQN   = "com.hazelcast.topic.impl.reliable.ReliableTopicProxy";

    private static Class<?> managerClass() throws Exception {
        return Class.forName(MANAGER_FQN);
    }

    private static String uniqueTopic() {
        return "rt-" + System.nanoTime();
    }

    private static com.hazelcast.config.Config baseConfig(String clusterName) {
        com.hazelcast.config.Config c = new com.hazelcast.config.Config();
        c.setClusterName(clusterName);

        com.hazelcast.config.JoinConfig join = c.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig()
                .setEnabled(true)
                .setMembers(Collections.singletonList("127.0.0.1"));

        c.getNetworkConfig().setPort(5701).setPortAutoIncrement(true);
        return c;
    }

    private static ReliableTopicConfig rtConfig(String name, int limit,
                                                com.hazelcast.topic.TopicOverloadPolicy policy) {
        ReliableTopicConfig rtc = new ReliableTopicConfig(name)
                .setTopicOverloadPolicy(policy)
                .setReadBatchSize(1)
                .setStatisticsEnabled(true);
        try {
            Method setter = ReliableTopicConfig.class.getDeclaredMethod("setMaxConcurrentPublishes", int.class);
            setter.setAccessible(true);
            setter.invoke(rtc, limit);
        } catch (Exception ignore) {
        }
        return rtc;
    }

    private static com.hazelcast.config.RingbufferConfig rbConfig(String name, int capacity) {
        return new com.hazelcast.config.RingbufferConfig(name)
                .setCapacity(capacity)
                .setTimeToLiveSeconds(0);
    }

    @SuppressWarnings("unchecked")
    private static <E> com.hazelcast.topic.ITopic<E> getTopic(com.hazelcast.core.HazelcastInstance hz, String name) {
        return (com.hazelcast.topic.ITopic<E>) hz.getReliableTopic(name);
    }

    private static Object managerFromProxy(Object topicProxy) throws Exception {
        Method m = topicProxy.getClass().getDeclaredMethod("concurrencyManager");
        m.setAccessible(true);
        return m.invoke(topicProxy);
    }

    private static int currentLimitFromManager(Object mgr) throws Exception {
        Method m = findMethod(mgr.getClass(), "currentLimit");
        assertNotNull(m);
        return (int) m.invoke(mgr);
    }

    private static Object newManager(ReliableTopicConfig cfg) throws Exception {
        return newManager(cfg, null);
    }

    private static Object newManager(ReliableTopicConfig cfg, Executor explicitExecutor) throws Exception {
        Class<?> mClass = managerClass();
        Constructor<?> chosen = null;
        for (Constructor<?> c : mClass.getDeclaredConstructors()) {
            Class<?>[] p = c.getParameterTypes();
            if (p.length >= 1 && p[0].getName().equals(ReliableTopicConfig.class.getName())) {
                chosen = c;
                break;
            }
        }
        if (chosen == null) {
            fail("No ReliableTopicConcurrencyManager ctor accepting ReliableTopicConfig found.");
        }
        chosen.setAccessible(true);
        if (chosen.getParameterCount() == 1) {
            return chosen.newInstance(cfg);
        } else {
            Object[] args = new Object[chosen.getParameterCount()];
            args[0] = cfg;
            for (int i = 1; i < args.length; i++) {
                Class<?> paramType = chosen.getParameterTypes()[i];
                if (paramType == Executor.class || paramType == ExecutorService.class) {
                    if (explicitExecutor != null) {
                        args[i] = explicitExecutor;
                    } else if (cfg.getExecutor() != null) {
                        args[i] = cfg.getExecutor();
                    } else {
                        args[i] = Executors.newCachedThreadPool();
                    }
                } else {
                    args[i] = null;
                }
            }
            return chosen.newInstance(args);
        }
    }

    private static Method findMethod(Class<?> type, String name, Class<?>... paramTypes) {
        try {
            Method m = type.getDeclaredMethod(name, paramTypes);
            m.setAccessible(true);
            return m;
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private static Object invoke(Object target, String name, Class<?>[] paramTypes, Object... args) throws Exception {
        Method m = findMethod(target.getClass(), name, paramTypes);
        if (m == null) {
            fail("Missing method: " + target.getClass().getName() + "#" + name);
        }
        return m.invoke(target, args);
    }

    private static int currentLimit(Object manager) throws Exception {
        Method m = findMethod(manager.getClass(), "currentLimit");
        assertNotNull("currentLimit() missing", m);
        assertEquals(0, m.getParameterCount());
        assertEquals(int.class, m.getReturnType());
        return (int) m.invoke(manager);
    }

    private static int getInFlight(Object manager) throws Exception {
        Method m = findMethod(manager.getClass(), "getInFlightCount");
        assertNotNull("getInFlightCount() missing (package-private required)", m);
        int mods = m.getModifiers();
        assertFalse("getInFlightCount() must not be public", Modifier.isPublic(mods));
        assertFalse("getInFlightCount() must not be protected", Modifier.isProtected(mods));
        assertFalse("getInFlightCount() must not be private", Modifier.isPrivate(mods));
        assertEquals(0, m.getParameterCount());
        assertTrue("getInFlightCount() must return int", m.getReturnType() == int.class);
        return (int) m.invoke(manager);
    }

    private static boolean awaitQuiescence(Object manager, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();

        Method d = findMethod(manager.getClass(), "awaitQuiescence", Duration.class);
        if (d != null) {
            Class<?> rt = d.getReturnType();
            if (rt == boolean.class || rt == Boolean.class) {
                Object result = d.invoke(manager, timeout);
                return (Boolean) result;
            } else {
                d.invoke(manager, timeout);
                return pollUntilQuiescent(manager, remaining(deadline));
            }
        }
        Method lt = findMethod(manager.getClass(), "awaitQuiescence", long.class, TimeUnit.class);
        if (lt != null) {
            Class<?> rt = lt.getReturnType();
            if (rt == boolean.class || rt == Boolean.class) {
                Object result = lt.invoke(manager, timeout.toMillis(), TimeUnit.MILLISECONDS);
                return (Boolean) result;
            } else {
                lt.invoke(manager, timeout.toMillis(), TimeUnit.MILLISECONDS);
                return pollUntilQuiescent(manager, remaining(deadline));
            }
        }
        return pollUntilQuiescent(manager, timeout);
    }

    private static Duration remaining(long deadlineNanos) {
        long rem = deadlineNanos - System.nanoTime();
        if (rem <= 0) return Duration.ZERO;
        return Duration.ofNanos(rem);
    }

    private static boolean pollUntilQuiescent(Object manager, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (getInFlight(manager) == 0) {
                return true;
            }
            Thread.yield();
        }
        return getInFlight(manager) == 0;
    }

    private static boolean isMonotonicallyIncreasing(List<Integer> values) {
        for (int i = 1; i < values.size(); i++) {
            if (values.get(i - 1) >= values.get(i)) {
                return false;
            }
        }
        return true;
    }

    private static Method findScheduleMethod(Class<?> mClass) {
        Method best = null;
        int bestRank = Integer.MAX_VALUE;
        for (Method method : mClass.getDeclaredMethods()) {
            String n = method.getName();
            if (!(n.equals("schedule") || n.equals("submit") || n.equals("enqueue")))
                continue;
            if (method.getParameterCount() != 1)
                continue;
            Class<?> pt = method.getParameterTypes()[0];
            int rank = -1;
            if (Supplier.class.isAssignableFrom(pt))
                rank = 0;
            else if (java.util.concurrent.Callable.class.isAssignableFrom(pt))
                rank = 1;
            else if (Runnable.class.isAssignableFrom(pt))
                rank = 2;
            else
                continue;
            Class<?> rt = method.getReturnType();
            if (!CompletionStage.class.isAssignableFrom(rt) && !CompletableFuture.class.isAssignableFrom(rt)) {
                continue;
            }
            if (rank < bestRank) {
                bestRank = rank;
                method.setAccessible(true);
                best = method;
            }
        }
        return best;
    }

    private static class ControlledOp {
        final int index;
        final CompletableFuture<Void> started = new CompletableFuture<>();
        final CompletableFuture<Void> done = new CompletableFuture<>();
        final AtomicInteger liveCounter;

        ControlledOp(int index, AtomicInteger liveCounter) {
            this.index = index;
            this.liveCounter = liveCounter;
        }

        Object toParamObject(Class<?> paramType) {
            if (Supplier.class.isAssignableFrom(paramType)) {
                return (Supplier<CompletionStage<?>>) () -> {
                    liveCounter.incrementAndGet();
                    started.complete(null);
                    return done.whenComplete((r, t) -> liveCounter.decrementAndGet());
                };
            } else if (java.util.concurrent.Callable.class.isAssignableFrom(paramType)) {
                return (java.util.concurrent.Callable<CompletionStage<?>>) () -> {
                    liveCounter.incrementAndGet();
                    started.complete(null);
                    return done.whenComplete((r, t) -> liveCounter.decrementAndGet());
                };
            } else if (Runnable.class.isAssignableFrom(paramType)) {
                return (Runnable) () -> {
                    liveCounter.incrementAndGet();
                    started.complete(null);
                    try {
                        done.get(500, TimeUnit.MILLISECONDS);
                    } catch (Exception ignored) {
                    } finally {
                        liveCounter.decrementAndGet();
                    }
                };
            }
            throw new IllegalArgumentException("Unsupported param type: " + paramType);
        }
    }

    private static class RetryingOp extends ControlledOp {
        final AtomicInteger attempts = new AtomicInteger();
        final int failTimes;

        RetryingOp(int index, AtomicInteger liveCounter, int failTimes) {
            super(index, liveCounter);
            this.failTimes = failTimes;
        }

        @Override
        Object toParamObject(Class<?> paramType) {
            if (Supplier.class.isAssignableFrom(paramType)) {
                return (Supplier<CompletionStage<?>>) () -> {
                    liveCounter.incrementAndGet();
                    started.complete(null);
                    int n = attempts.incrementAndGet();
                    CompletableFuture<Void> f = new CompletableFuture<>();
                    if (n <= failTimes) {
                        f.completeExceptionally(new RuntimeException("retry-fail-" + n));
                    } else {
                        f.complete(null);
                    }
                    return f.whenComplete((r, t) -> liveCounter.decrementAndGet());
                };
            }
            return super.toParamObject(paramType);
        }
    }

    private static Method maxConcurrentPublishesSetter() {
        try {
            return ReliableTopicConfig.class.getDeclaredMethod("setMaxConcurrentPublishes", int.class);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private static List<CompletionStage<?>> scheduleN(Object manager, int n, List<ControlledOp> ops) throws Exception {
        Method schedule = findScheduleMethod(manager.getClass());
        assertNotNull("No schedule/submit/enqueue(Functional) method returning CompletionStage found", schedule);
        ArrayList<CompletionStage<?>> futures = new ArrayList<>(n);
        Class<?> paramType = schedule.getParameterTypes()[0];
        for (int i = 0; i < n; i++) {
            ControlledOp op = ops.get(i);
            Object param = op.toParamObject(paramType);
            Object stage = schedule.invoke(manager, param);
            assertTrue("schedule(..) must return CompletionStage", stage instanceof CompletionStage);
            futures.add((CompletionStage<?>) stage);
        }
        return futures;
    }

    private static Object[] buildCtorArgs(Class<?>[] p, int limit) {
        Object[] args = new Object[p.length];
        for (int i = 0; i < p.length; i++) {
            Class<?> t = p[i];
            if (i == p.length - 1 && t == int.class) {
                args[i] = limit;
                continue;
            }
            if (t == String.class) {
                args[i] = "hot";
            } else if (t == ReliableTopicConfig.class) {
                args[i] = new ReliableTopicConfig("hot");
            } else if (t == boolean.class) {
                args[i] = false;
            } else if (t == byte.class) {
                args[i] = (byte) 0;
            } else if (t == short.class) {
                args[i] = (short) 0;
            } else if (t == char.class) {
                args[i] = (char) 0;
            } else if (t == int.class) {
                args[i] = 0;
            } else if (t == long.class) {
                args[i] = 0L;
            } else if (t == float.class) {
                args[i] = 0f;
            } else if (t == double.class) {
                args[i] = 0d;
            } else if (t.isEnum()) {
                Object[] constants = t.getEnumConstants();
                Object val = (constants != null && constants.length > 0) ? constants[0] : null;
                if ("com.hazelcast.topic.TopicOverloadPolicy".equals(t.getName())) {
                    for (Object cst : constants) {
                        if ("BLOCK".equals(String.valueOf(cst))) {
                            val = cst;
                            break;
                        }
                    }
                }
                args[i] = val;
            } else {
                args[i] = null;
            }
        }
        return args;
    }

    private static ReliableTopicConfig tryConstructWithTrailingInt(int limit) {
        Constructor<?>[] ctors = ReliableTopicConfig.class.getDeclaredConstructors();
        Arrays.sort(ctors, (a, b) -> Integer.compare(a.getParameterCount(), b.getParameterCount()));
        for (Constructor<?> c : ctors) {
            Class<?>[] p = c.getParameterTypes();
            if (p.length >= 1 && p[p.length - 1] == int.class) {
                try {
                    c.setAccessible(true);
                    Object[] args = buildCtorArgs(p, limit);
                    return (ReliableTopicConfig) c.newInstance(args);
                } catch (Throwable ignored) {
                }
            }
        }
        return null;
    }

    private static ReliableTopicConfig newConfigWithLimit(int limit) throws Exception {
        ReliableTopicConfig cfg = tryConstructWithTrailingInt(limit);
        if (cfg != null) {
            return cfg;
        }
        cfg = new ReliableTopicConfig("hot");
        Method setter = ReliableTopicConfig.class.getDeclaredMethod("setMaxConcurrentPublishes", int.class);
        setter.setAccessible(true);
        setter.invoke(cfg, limit);
        return cfg;
    }

    @Test
    public void testClassExists() throws Exception {
        assertNotNull(managerClass());
    }

    @Test(timeout = 2000)
    public void testLimitOneFromConfig() throws Exception {
        ReliableTopicConfig cfg = newConfigWithLimit(1);
        Object mgr = newManager(cfg);
        assertEquals(1, currentLimit(mgr));
    }

    @Test
    public void testLimitEightFromConfig() throws Exception {
        ReliableTopicConfig cfg = newConfigWithLimit(8);
        Object mgr = newManager(cfg);
        assertEquals(8, currentLimit(mgr));
    }

    @Test
    public void testGetInFlightCountStartsAtZero() throws Exception {
        ReliableTopicConfig cfg = newConfigWithLimit(3);
        Object mgr = newManager(cfg);
        assertEquals(0, getInFlight(mgr));
    }

    @Test
    public void testAwaitQuiescenceCompletes() throws Exception {
        ReliableTopicConfig cfg = newConfigWithLimit(2);
        Object mgr = newManager(cfg);
        AtomicInteger running = new AtomicInteger();
        List<ControlledOp> ops = new ArrayList<>();
        for (int i = 0; i < 3; i++)
            ops.add(new ControlledOp(i, running));
        scheduleN(mgr, ops.size(), ops);
        for (ControlledOp op : ops) {
            op.started.get(500, TimeUnit.MILLISECONDS);
            op.done.complete(null);
        }
        assertTrue("awaitQuiescence() should return true once all completes",
                awaitQuiescence(mgr, Duration.ofSeconds(1)));
        assertEquals(0, getInFlight(mgr));
    }

    @Test
    public void testConcurrentEnabledForLimitGreaterOne() throws Exception {
        int limit = 3;
        ReliableTopicConfig cfg = newConfigWithLimit(limit);
        Object mgr = newManager(cfg);

        AtomicInteger running = new AtomicInteger();
        List<ControlledOp> ops = new ArrayList<>();
        for (int i = 0; i < 6; i++) ops.add(new ControlledOp(i, running));

        scheduleN(mgr, ops.size(), ops);

        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        int maxInflight = 0;
        while (System.nanoTime() < until) {
            maxInflight = Math.max(maxInflight, getInFlight(mgr));
            if (maxInflight >= 2) break;
            Thread.yield();
        }
        assertTrue("Expected concurrent execution (>1 in-flight) when limit>1, observed=" + maxInflight,
                maxInflight >= 2);

        for (ControlledOp op : ops) op.done.complete(null);
        assertTrue(awaitQuiescence(mgr, Duration.ofSeconds(2)));
    }

    @Test
    public void testSequentialOrderWhenLimitOne() throws Exception {
        int limit = 1;
        ReliableTopicConfig cfg = newConfigWithLimit(limit);
        Object mgr = newManager(cfg);
        AtomicInteger running = new AtomicInteger();
        List<Integer> startOrder = Collections.synchronizedList(new ArrayList<>());
        List<ControlledOp> ops = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            int idx = i;
            ops.add(new ControlledOp(idx, running) {
                @Override
                Object toParamObject(Class<?> paramType) {
                    if (Supplier.class.isAssignableFrom(paramType)) {
                        return (Supplier<CompletionStage<?>>) () -> {
                            running.incrementAndGet();
                            startOrder.add(idx);
                            started.complete(null);
                            return done.whenComplete((r, t) -> running.decrementAndGet());
                        };
                    }
                    return super.toParamObject(paramType);
                }
            });
        }
        scheduleN(mgr, ops.size(), ops);
        for (int i = 0; i < ops.size(); i++) {
            ops.get(i).started.get(500, TimeUnit.MILLISECONDS);
            assertEquals("Only one in-flight allowed at limit=1", 1, running.get());
            ops.get(i).done.complete(null);
        }
        awaitQuiescence(mgr, Duration.ofSeconds(1));
        assertEquals(Arrays.asList(0, 1, 2, 3, 4), startOrder);
    }

    @Test
public void testReadBatchSizePreservedWithConcurrency() throws Exception {
    Assume.assumeNotNull(maxConcurrentPublishesSetter());

    String cluster = "c-readbatch-preserve";
    String topic = uniqueTopic();
    int readBatchSize = 5;
    int maxConcurrentPublishes = 4;
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

        List<Integer> receivedMessages = new ArrayList<>();
        CountDownLatch allMessagesReceived = new CountDownLatch(totalMessages);
        AtomicBoolean orderViolation = new AtomicBoolean(false);

        reliableTopic.addMessageListener(new ReliableMessageListener<Integer>() {
            private final Object lock = new Object();

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
            public void onMessage(Message<Integer> m) {
                synchronized (lock) {
                    Integer msg = m.getMessageObject();
                    receivedMessages.add(msg);
                    if (!receivedMessages.isEmpty() &&
                            msg < receivedMessages.get(receivedMessages.size() - 1)) {
                        orderViolation.set(true);
                    }
                    allMessagesReceived.countDown();
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        ExecutorService publisherPool = Executors.newFixedThreadPool(4);
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
                allMessagesReceived.await(20, TimeUnit.SECONDS));

        assertEquals(totalMessages, receivedMessages.size());
        InternalSerializationService ss = Accessors.getSerializationService(hz);
        List<Integer> ringOrder = new ArrayList<>();
        long head = original.headSequence();
        long tail = original.tailSequence();
        for (long seq = head; seq <= tail; seq++) {
            ReliableTopicMessage m = original.readOne(seq);
            ringOrder.add(ss.toObject(m.getPayload()));
        }
        assertFalse("Message order violated", orderViolation.get());
        assertTrue("Messages should be in order", isMonotonicallyIncreasing(receivedMessages));
        for (int i = 0; i < totalMessages; i++) {
            assertEquals(Integer.valueOf(i), receivedMessages.get(i));
        }
        assertEquals(totalMessages, batchSizes.stream().mapToInt(Integer::intValue).sum());
        for (int j = 0; j < batchSizes.size(); j++) {
            int size = batchSizes.get(j);
            assertTrue("Batch " + j + " size " + size +
                    " exceeds readBatchSize " + readBatchSize, size <= readBatchSize);
        }
        assertTrue("No batching occurred; all singles",
                batchSizes.stream().anyMatch(bs -> bs > 1));
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

        AtomicInteger attemptCounter = new AtomicInteger();
        Ringbuffer<ReliableTopicMessage> retryInducingRingbuffer =
                (Ringbuffer<ReliableTopicMessage>) Proxy.newProxyInstance(
                        proxyRingbuffer.getClass().getClassLoader(),
                        new Class[]{Ringbuffer.class},
                        (p, m, args) -> {
                            String methodName = m.getName();

                            if (("addAsync".equals(methodName) || "addAllAsync".equals(methodName))
                                    && attemptCounter.incrementAndGet() <= 10) {
                                if (Math.random() < 0.4) {
    
                                    CompletableFuture<Long> retryNeeded = new CompletableFuture<>();
                                    retryNeeded.complete(-1L);

                                    CompletableFuture.delayedExecutor(50, TimeUnit.MILLISECONDS)
                                            .execute(() -> {
                                                try {
                                                    m.invoke(proxyRingbuffer, args);
                                                } catch (Exception e) {
                                                }
                                            });
                                    return retryNeeded;
                                }
                            }

                            return m.invoke(proxyRingbuffer, args);
                        });

        ringbufferField.set(topicProxy, retryInducingRingbuffer);

        List<Integer> submissionOrder = Collections.synchronizedList(new ArrayList<>());
        List<Future<?>> publishFutures = new ArrayList<>();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        for (int i = 0; i < messageCount; i++) {
            final int messageValue = i;
            publishFutures.add(executor.submit(() -> {
                submissionOrder.add(messageValue);
                reliableTopic.publish(messageValue);
            }));
        }
        executor.shutdown();
        assertTrue("Executor didn't terminate", executor.awaitTermination(10, TimeUnit.SECONDS));

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
            assertTrue("Sequences should strictly increase",
                    sequencesInSubmissionOrder.get(i) > sequencesInSubmissionOrder.get(i - 1));
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

        long firstSeq = sequencesInSubmissionOrder.get(0);
        for (int i = 0; i < sequencesInSubmissionOrder.size(); i++) {
            long expectedSeq = firstSeq + i;
            long actualSeq = sequencesInSubmissionOrder.get(i);
            assertEquals("Sequence gap detected at position " + i +
                            " (message " + submissionOrder.get(i) + "): " +
                            "expected seq " + expectedSeq + " but got " + actualSeq,
                    expectedSeq, actualSeq);
        }
    } finally {
        Hazelcast.shutdownAll();
    }
}

@Test
public void testPublishOperationsUseConfiguredExecutor() throws Exception {
    Assume.assumeNotNull(maxConcurrentPublishesSetter());

    String topic = uniqueTopic();
    String cluster = "c-publish-configured-exec";

    AtomicInteger threadSeq = new AtomicInteger();
    ThreadPoolExecutor customExecutor = new ThreadPoolExecutor(
            2, 2, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(100),
            r -> {
                Thread t = new Thread(r);
                t.setName("custom-publish-exec-" + threadSeq.incrementAndGet());
                t.setDaemon(true);
                return t;
            });

    Config cfg = baseConfig(cluster);
    cfg.addRingBufferConfig(rbConfig(topic, 3));
    ReliableTopicConfig rtc = new ReliableTopicConfig(topic)
            .setExecutor(customExecutor)
            .setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK);
    maxConcurrentPublishesSetter().invoke(rtc, 3);
    cfg.addReliableTopicConfig(rtc);

    HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
    try {
        ITopic<Integer> reliableTopic = getTopic(hz, topic);

        ReliableTopicProxy<Integer> proxy = (ReliableTopicProxy<Integer>) reliableTopic;
        Field rbField = ReliableTopicProxy.class.getDeclaredField("ringbuffer");
        rbField.setAccessible(true);
        Ringbuffer<ReliableTopicMessage> original = (Ringbuffer<ReliableTopicMessage>) rbField.get(proxy);

        Set<String> observedThreads = Collections.synchronizedSet(new HashSet<>());
        Ringbuffer<ReliableTopicMessage> wrapped = (Ringbuffer<ReliableTopicMessage>) Proxy.newProxyInstance(
                original.getClass().getClassLoader(),
                new Class[]{Ringbuffer.class},
                (p, m, args) -> {
                    if ("addAsync".equals(m.getName()) || "addAllAsync".equals(m.getName())) {
                        observedThreads.add(Thread.currentThread().getName());
                    }
                    return m.invoke(original, args);
                });
        rbField.set(proxy, wrapped);

        ExecutorService pub = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 10; i++) {
            final int msg = i;
            pub.submit(() -> reliableTopic.publish(msg));
        }
        Thread.sleep(500);
        for (int i = 0; i < 10; i++) {
            try {
                original.readOne(original.headSequence());
            } catch (Exception ignored) {
            }
        }
        pub.shutdown();
        assertTrue(pub.awaitTermination(10, TimeUnit.SECONDS));

        Set<String> nonCustom = observedThreads.stream()
                .filter(t -> !t.startsWith("custom-publish-exec-"))
                .filter(t -> !t.equals(Thread.currentThread().getName()))
                .collect(Collectors.toSet());
        assertTrue("Publishing used non-custom executor threads: " + nonCustom, nonCustom.isEmpty());
    } finally {
        customExecutor.shutdown();
        Hazelcast.shutdownAll();
    }
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
        assertFalse("Should not use test thread", threadName.contains("main"));
        assertFalse("Should not use custom executor", threadName.contains("custom-"));
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

    @Test
    public void testOrderPreservedUnderParallelism() throws Exception {
        int limit = 3;
        ReliableTopicConfig cfg = newConfigWithLimit(limit);
        Object mgr = newManager(cfg);
        AtomicInteger running = new AtomicInteger();
        List<Integer> startOrder = Collections.synchronizedList(new ArrayList<>());
        List<Integer> effectOrder = Collections.synchronizedList(new ArrayList<>());
        final int N = 7;
        List<ControlledOp> ops = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            final int idx = i;
            ops.add(new ControlledOp(idx, running) {
                @Override
                Object toParamObject(Class<?> paramType) {
                    if (Supplier.class.isAssignableFrom(paramType)) {
                        return (Supplier<CompletionStage<?>>) () -> {
                            running.incrementAndGet();
                            startOrder.add(idx);
                            effectOrder.add(idx);
                            started.complete(null);
                            return done.whenComplete((r, t) -> running.decrementAndGet());
                        };
                    }
                    return super.toParamObject(paramType);
                }
            });
        }
        List<CompletionStage<?>> stages = scheduleN(mgr, ops.size(), ops);

        for (int i = 0; i < Math.min(limit, N); i++) {
            ops.get(i).started.get(1000, TimeUnit.MILLISECONDS);
        }
        for (int i = limit; i < N; i++) {
            ops.get(i - limit).done.complete(null);
            ops.get(i).started.get(1000, TimeUnit.MILLISECONDS);
        }
        for (int i = Math.max(0, N - limit); i < N; i++) {
            ops.get(i).done.complete(null);
        }

        assertTrue("Timed out before quiescent", awaitQuiescence(mgr, Duration.ofSeconds(2)));

        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < N; i++) expected.add(i);
        assertEquals("Dispatch/start order must preserve submission order under parallelism", expected, startOrder);
        assertEquals("Internal 'effect' (add) order must preserve submission order", expected, effectOrder);

        for (int i = 0; i < N; i++) {
            assertTrue("Stage not complete: " + i, stages.get(i).toCompletableFuture().isDone());
        }
    }

    @Test
    public void testOutOfOrderCompletionsNoViolation() throws Exception {
        Assume.assumeNotNull(maxConcurrentPublishesSetter());

        String cluster = "c-out-of-order-retry";
        String topic = uniqueTopic();
        int messageCount = 8;

        Config cfg = baseConfig(cluster);
        cfg.addRingBufferConfig(new RingbufferConfig(topic)
                .setCapacity(32)
                .setTimeToLiveSeconds(0));

        ReliableTopicConfig topicConfig = new ReliableTopicConfig(topic)
                .setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK);
        maxConcurrentPublishesSetter().invoke(topicConfig, messageCount);
        cfg.addReliableTopicConfig(topicConfig);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try {
            ITopic<Integer> topicProxy = getTopic(hz, topic);

            ReliableTopicProxy<Integer> proxy = (ReliableTopicProxy<Integer>) topicProxy;
            Field rbField = ReliableTopicProxy.class.getDeclaredField("ringbuffer");
            rbField.setAccessible(true);
            Ringbuffer<ReliableTopicMessage> original =
                    (Ringbuffer<ReliableTopicMessage>) rbField.get(proxy);

            InternalSerializationService ss = Accessors.getSerializationService(hz);

            class TrackingHandler implements InvocationHandler {
                final Map<Integer, Integer> failPlan = new HashMap<>();
                final Map<Integer, AtomicInteger> attempts = new ConcurrentHashMap<>();
                final List<Integer> submissionOrder = Collections.synchronizedList(new ArrayList<>());
                final List<Integer> completionOrder = Collections.synchronizedList(new ArrayList<>());

                TrackingHandler() {
                    failPlan.put(1, 1);
                    failPlan.put(4, 2);
                }

                @Override
                public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    if (("addAllAsync".equals(method.getName()) || "addAsync".equals(method.getName()))
                            && args.length >= 2) {
                        ReliableTopicMessage msg;
                        if ("addAllAsync".equals(method.getName())) {
                            Collection<ReliableTopicMessage> coll = (Collection<ReliableTopicMessage>) args[0];
                            msg = coll.iterator().next();
                        } else {
                            msg = (ReliableTopicMessage) args[0];
                        }
                        int value = ss.toObject(msg.getPayload());
                        AtomicInteger attempt = attempts.computeIfAbsent(value, k -> new AtomicInteger());
                        int n = attempt.incrementAndGet();
                        if (n == 1) {
                            submissionOrder.add(value);
                        }
                        int fails = failPlan.getOrDefault(value, 0);
                        if (n <= fails) {
                            return InternalCompletableFuture.newCompletedFuture(-1L);
                        }
                        InternalCompletableFuture<Long> fut =
                                (InternalCompletableFuture<Long>) method.invoke(original, args);
                        fut.whenComplete((seq, t) -> {
                            if (t == null && seq != -1) {
                                completionOrder.add(value);
                            }
                        });
                        return fut;
                    }
                    return method.invoke(original, args);
                }
            }

            TrackingHandler handler = new TrackingHandler();
            Ringbuffer<ReliableTopicMessage> wrapped =
                    (Ringbuffer<ReliableTopicMessage>) Proxy.newProxyInstance(
                            Ringbuffer.class.getClassLoader(), new Class[]{Ringbuffer.class}, handler);
            rbField.set(proxy, wrapped);

            List<CompletionStage<Void>> stages = new ArrayList<>();
            for (int i = 0; i < messageCount; i++) {
                final int value = i;
                CompletionStage<Void> stage = submitter
                        .submit(() -> topicProxy.publishAsync(value))
                        .get();
                stages.add(stage);
            }

            for (CompletionStage<Void> stage : stages) {
                stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
            }

            List<Integer> ringbufferOrder = new ArrayList<>();
            long head = original.headSequence();
            long tail = original.tailSequence();
            for (long seq = head; seq <= tail; seq++) {
                ReliableTopicMessage m = original.readOne(seq);
                ringbufferOrder.add(ss.toObject(m.getPayload()));
            }

            assertEquals(handler.submissionOrder, ringbufferOrder);
            assertEquals(handler.submissionOrder.size(), handler.completionOrder.size());
            assertNotEquals(handler.submissionOrder, handler.completionOrder);
        } finally {
            submitter.shutdownNow();
            Hazelcast.shutdownAll();
        }
    }

    @Test
    public void testFailureIsolationPreserved() throws Exception {
        ReliableTopicConfig cfg = newConfigWithLimit(3);
        Object mgr = newManager(cfg);
        AtomicInteger running = new AtomicInteger();
        List<ControlledOp> ops = new ArrayList<>();
        for (int i = 0; i < 6; i++)
            ops.add(new ControlledOp(i, running));
        List<CompletionStage<?>> stages = scheduleN(mgr, ops.size(), ops);
        ops.get(1).done.completeExceptionally(new RuntimeException("boom-1"));
        ops.get(4).done.completeExceptionally(new RuntimeException("boom-2"));
        for (int i = 0; i < ops.size(); i++) {
            if (i == 1 || i == 4) continue;
            ops.get(i).done.complete(null);
        }
        assertTrue("Manager did not quiesce after outcomes",
                awaitQuiescence(mgr, Duration.ofSeconds(2)));
        assertEquals(0, getInFlight(mgr));
        for (int i = 0; i < stages.size(); i++) {
            CompletableFuture<?> cf = stages.get(i).toCompletableFuture();
            assertTrue("Stage not completed for index " + i, cf.isDone());
            if (i == 1 || i == 4) {
                assertTrue("Failure should propagate for index " + i, cf.isCompletedExceptionally());
            } else {
                assertFalse("Unexpected failure for index " + i, cf.isCompletedExceptionally());
            }
        }
    }

    @Test
    public void testOverloadedConstructorWithIntExists() throws Exception {
        Constructor<?>[] ctors = ReliableTopicConfig.class.getDeclaredConstructors();
        Arrays.sort(ctors, (a, b) -> Integer.compare(b.getParameterCount(), a.getParameterCount()));
        boolean foundTrailingInt = false;
        for (Constructor<?> c : ctors) {
            Class<?>[] p = c.getParameterTypes();
            if (p.length >= 1 && p[p.length - 1] == int.class) {
                foundTrailingInt = true;
                c.setAccessible(true);
                Object[] args = buildCtorArgs(p, 5);
                ReliableTopicConfig cfg = (ReliableTopicConfig) c.newInstance(args);
                Method getter = ReliableTopicConfig.class.getDeclaredMethod("getMaxConcurrentPublishes");
                getter.setAccessible(true);
                assertEquals(5, ((Number) getter.invoke(cfg)).intValue());
            }
        }
        assertTrue("Expected at least one overloaded ReliableTopicConfig ctor with trailing int", foundTrailingInt);
        ReliableTopicConfig legacy = new ReliableTopicConfig("hot");
        Method getter = ReliableTopicConfig.class.getDeclaredMethod("getMaxConcurrentPublishes");
        getter.setAccessible(true);
        assertEquals("Legacy constructors must preserve default sequential (1)", 1,
                ((Number) getter.invoke(legacy)).intValue());
    }

    @Test
    public void testGetMaxConcurrentPublishesExists() throws Exception {
        Method getter = ReliableTopicConfig.class.getDeclaredMethod("getMaxConcurrentPublishes");
        int mods = getter.getModifiers();
        assertTrue("getMaxConcurrentPublishes() should be public", Modifier.isPublic(mods));
        assertEquals("getter should return int", int.class, getter.getReturnType());

        ReliableTopicConfig cfg = new ReliableTopicConfig("x");
        getter.setAccessible(true);
        int v = ((Number) getter.invoke(cfg)).intValue();
        assertTrue("Default maxConcurrentPublishes should be >=1", v >= 1);
    }

    @Test
    public void testSetMaxConcurrentPublishesExists() throws Exception {
        Method setter = ReliableTopicConfig.class.getDeclaredMethod("setMaxConcurrentPublishes", int.class);
        int mods = setter.getModifiers();
        assertTrue("setMaxConcurrentPublishes(int) should be public", Modifier.isPublic(mods));
        Class<?> rt = setter.getReturnType();
        assertTrue("setter should return void or ReliableTopicConfig",
                rt == void.class || ReliableTopicConfig.class.isAssignableFrom(rt));

        ReliableTopicConfig cfg = new ReliableTopicConfig("x");
        setter.setAccessible(true);
        setter.invoke(cfg, 5);

        Method getter = ReliableTopicConfig.class.getDeclaredMethod("getMaxConcurrentPublishes");
        getter.setAccessible(true);
        assertEquals(5, ((Number) getter.invoke(cfg)).intValue());
    }

    @Test
    public void testSetterValidatesRange() throws Exception {
        ReliableTopicConfig cfg = new ReliableTopicConfig("x");
        Method setter = ReliableTopicConfig.class.getDeclaredMethod("setMaxConcurrentPublishes", int.class);
        Method getter = ReliableTopicConfig.class.getDeclaredMethod("getMaxConcurrentPublishes");
        setter.setAccessible(true);
        getter.setAccessible(true);

        setter.invoke(cfg, 1);
        assertEquals(1, ((Number) getter.invoke(cfg)).intValue());
        setter.invoke(cfg, 8);
        assertEquals(8, ((Number) getter.invoke(cfg)).intValue());

        try {
            setter.invoke(cfg, 0);
            fail("Expected exception for value=0");
        } catch (java.lang.reflect.InvocationTargetException ite) {
            assertTrue("Expected IllegalArgumentException for low bound",
                    ite.getCause() instanceof IllegalArgumentException);
        }

        try {
            setter.invoke(cfg, 9);
            fail("Expected exception for value=9");
        } catch (java.lang.reflect.InvocationTargetException ite) {
            assertTrue("Expected IllegalArgumentException for high bound",
                    ite.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testProxyExposesConcurrencyManager() throws Exception {
        Class<?> proxy = Class.forName(PROXY_FQN);
        Method m = null;
        try {
            m = proxy.getDeclaredMethod("concurrencyManager");
        } catch (NoSuchMethodException e) {
            fail("ReliableTopicProxy#concurrencyManager() accessor not found");
        }
        int mods = m.getModifiers();
        assertFalse("concurrencyManager() must not be public", Modifier.isPublic(mods));
        assertFalse("concurrencyManager() must not be protected", Modifier.isProtected(mods));
        assertFalse("concurrencyManager() must not be private", Modifier.isPrivate(mods));
        assertEquals("Accessor must return ReliableTopicConcurrencyManager",
                MANAGER_FQN, m.getReturnType().getName());
        assertFalse("concurrencyManager() must be an instance method", Modifier.isStatic(mods));
    }

    @Test
    public void testMemberRemovedResetsCounters() throws Exception {
        String cluster = "c-member-removed";
        String topic = uniqueTopic();
        int limit = 3;

        com.hazelcast.config.Config cfg = baseConfig(cluster);
        cfg.addRingBufferConfig(rbConfig(topic, 32));
        cfg.addReliableTopicConfig(rtConfig(topic, limit, com.hazelcast.topic.TopicOverloadPolicy.BLOCK));

        com.hazelcast.core.HazelcastInstance a = com.hazelcast.core.Hazelcast.newHazelcastInstance(cfg);
        com.hazelcast.core.HazelcastInstance b = com.hazelcast.core.Hazelcast.newHazelcastInstance(cfg);
        try {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (a.getCluster().getMembers().size() != 2 && System.nanoTime() < until) {
                Thread.sleep(20);
            }
            assertEquals("Cluster did not reach size 2", 2, a.getCluster().getMembers().size());

            com.hazelcast.topic.ITopic<String> t = getTopic(a, topic);
            Object mgr = managerFromProxy(t);
            AtomicInteger running = new AtomicInteger();
            List<ControlledOp> ops = new ArrayList<>();
            for (int i = 0; i < 6; i++)
                ops.add(new ControlledOp(i, running));
            scheduleN(mgr, ops.size(), ops);
            ops.get(0).started.get(500, TimeUnit.MILLISECONDS);
            ops.get(1).started.get(500, TimeUnit.MILLISECONDS);
            assertTrue(getInFlight(mgr) >= 2);

            CountDownLatch removed = new CountDownLatch(1);
            a.getCluster().addMembershipListener(new com.hazelcast.cluster.MembershipListener() {
                @Override public void memberAdded(com.hazelcast.cluster.MembershipEvent event) { }
                @Override public void memberRemoved(com.hazelcast.cluster.MembershipEvent event) { removed.countDown(); }
            });

            b.shutdown();
            assertTrue("Expected MEMBER_REMOVED", removed.await(5, TimeUnit.SECONDS));

            assertTrue("manager did not quiesce", awaitQuiescence(mgr, Duration.ofSeconds(5)));
            assertEquals("in-flight must reset to 0", 0, getInFlight(mgr));

            CountDownLatch dl = new CountDownLatch(3);
            t.addMessageListener((com.hazelcast.topic.MessageListener<String>) m2 -> dl.countDown());
            t.publish("a");
            t.publish("b");
            t.publish("c");
            assertTrue("messages not delivered post-removal", dl.await(3, TimeUnit.SECONDS));
        } finally {
            com.hazelcast.core.Hazelcast.shutdownAll();
        }
    }

    @Test
    public void testMergeResetsCounters() throws Exception {
        String cluster = "c-merge-reset";
        String topic = uniqueTopic();

        com.hazelcast.config.Config cfg = baseConfig(cluster);
        cfg.addRingBufferConfig(rbConfig(topic, 32));
        cfg.addReliableTopicConfig(rtConfig(topic, 2, com.hazelcast.topic.TopicOverloadPolicy.BLOCK));

        com.hazelcast.core.HazelcastInstance a1 = com.hazelcast.core.Hazelcast.newHazelcastInstance(cfg);
        try {
            com.hazelcast.topic.ITopic<String> tA = getTopic(a1, topic);
            Object mgr = managerFromProxy(tA);

            java.util.concurrent.atomic.AtomicInteger running = new java.util.concurrent.atomic.AtomicInteger();
            java.util.List<ControlledOp> ops = java.util.Arrays.asList(
                    new ControlledOp(0, running), new ControlledOp(1, running),
                    new ControlledOp(2, running), new ControlledOp(3, running));
            scheduleN(mgr, ops.size(), ops);
            ops.get(0).started.get(500, java.util.concurrent.TimeUnit.MILLISECONDS);
            ops.get(1).started.get(500, java.util.concurrent.TimeUnit.MILLISECONDS);
            assertTrue("precondition: in-flight should be > 0", getInFlight(mgr) > 0);

            java.util.concurrent.CountDownLatch added = new java.util.concurrent.CountDownLatch(1);
            a1.getCluster().addMembershipListener(new com.hazelcast.cluster.MembershipListener() {
                @Override public void memberAdded(com.hazelcast.cluster.MembershipEvent event) { added.countDown(); }
                @Override public void memberRemoved(com.hazelcast.cluster.MembershipEvent event) { }
            });
            com.hazelcast.core.HazelcastInstance a2 = com.hazelcast.core.Hazelcast.newHazelcastInstance(cfg);
            assertTrue("Expected MEMBER_ADDED to current cluster", added.await(5, java.util.concurrent.TimeUnit.SECONDS));

            assertTrue("manager did not quiesce on merge", awaitQuiescence(mgr, java.time.Duration.ofSeconds(5)));
            assertEquals("in-flight must reset to 0 on merge", 0, getInFlight(mgr));

            java.util.concurrent.CountDownLatch dl = new java.util.concurrent.CountDownLatch(2);
            tA.addMessageListener((com.hazelcast.topic.MessageListener<String>) m -> dl.countDown());
            tA.publish("x");
            tA.publish("y");
            assertTrue("messages not delivered after merge/reset", dl.await(3, java.util.concurrent.TimeUnit.SECONDS));
        } finally {
            com.hazelcast.core.Hazelcast.shutdownAll();
        }
    }


    @Test
    public void testAwaitQuiescenceTimeoutsGracefully() throws Exception {
        ReliableTopicConfig cfg = newConfigWithLimit(2);
        Object mgr = newManager(cfg);
        AtomicInteger running = new AtomicInteger();
        ControlledOp op = new ControlledOp(0, running);
        List<ControlledOp> ops = List.of(op);
        scheduleN(mgr, ops.size(), ops);
        op.started.get(500, TimeUnit.MILLISECONDS);
        boolean ok = awaitQuiescence(mgr, Duration.ofMillis(50));
        assertFalse("awaitQuiescence() must return false on timeout", ok);
        op.done.complete(null);
        awaitQuiescence(mgr, Duration.ofSeconds(1));
    }

    @Test
    public void testGradualFillOneInOneOut() throws Exception {
        int limit = 2;
        ReliableTopicConfig cfg = newConfigWithLimit(limit);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        cfg.setExecutor(executor);
        Object mgr = newManager(cfg, executor);
        AtomicInteger running = new AtomicInteger();
        List<Integer> startOrder = Collections.synchronizedList(new ArrayList<>());
        List<ControlledOp> ops = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final int idx = i;
            ops.add(new ControlledOp(idx, running) {
                @Override
                Object toParamObject(Class<?> paramType) {
                    if (Supplier.class.isAssignableFrom(paramType)) {
                        return (Supplier<CompletionStage<?>>) () -> {
                            running.incrementAndGet();
                            startOrder.add(idx);
                            started.complete(null);
                            return done.whenComplete((r, t) -> running.decrementAndGet());
                        };
                    }
                    return super.toParamObject(paramType);
                }
            });
        }

        scheduleN(mgr, ops.size(), ops);

        for (int i = 0; i < limit; i++) {
            ops.get(i).started.get(2000, TimeUnit.MILLISECONDS);
        }
        assertEquals(limit, getInFlight(mgr));

        for (int i = limit; i < ops.size(); i++) {
            ops.get(i - limit).done.complete(null);
            Thread.sleep(50);
            ops.get(i).started.get(2000, TimeUnit.MILLISECONDS);
            assertEquals(limit, getInFlight(mgr));
            assertEquals("Op " + i + " should start after previous complete", i, (int) startOrder.get(i));
        }

        for (ControlledOp op : ops) {
            op.done.complete(null);
        }
        assertTrue(awaitQuiescence(mgr, Duration.ofSeconds(2)));
        executor.shutdownNow();
    }

    @Test
    public void testInFlightNeverExceedsLimit() throws Exception {
        int limit = 4;
        ReliableTopicConfig cfg = newConfigWithLimit(limit);
        Object mgr = newManager(cfg);

        AtomicInteger running = new AtomicInteger();
        List<ControlledOp> ops = new ArrayList<>();
        for (int i = 0; i < 12; i++) ops.add(new ControlledOp(i, running));
        scheduleN(mgr, ops.size(), ops);

        long window = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        int observedMax = 0;
        while (System.nanoTime() < window) {
            int inFlight = getInFlight(mgr);
            observedMax = Math.max(observedMax, inFlight);
            if (inFlight >= limit) {
                for (ControlledOp op : ops) {
                    if (op.started.isDone() && !op.done.isDone()) {
                        op.done.complete(null);
                        break;
                    }
                }
            }
            Thread.yield();
        }
        for (ControlledOp op : ops) op.done.complete(null);
        assertTrue(awaitQuiescence(mgr, Duration.ofSeconds(2)));
        assertTrue("Observed in-flight (" + observedMax + ") exceeded limit " + limit, observedMax <= limit);
    }

    @Test
    public void testOverloadPolicyBlockUnderConcurrency() throws Exception {
        String topic = uniqueTopic();
        String cluster = "c-overload";
        com.hazelcast.config.Config cfg = baseConfig(cluster);
        cfg.addRingBufferConfig(rbConfig(topic, 4));
        cfg.addReliableTopicConfig(rtConfig(topic, 8, com.hazelcast.topic.TopicOverloadPolicy.BLOCK));

        com.hazelcast.core.HazelcastInstance hz = com.hazelcast.core.Hazelcast.newHazelcastInstance(cfg);
        try {
            com.hazelcast.topic.ITopic<Integer> t = getTopic(hz, topic);

            CountDownLatch firstSeen = new CountDownLatch(1);
            t.addMessageListener(new com.hazelcast.topic.ReliableMessageListener<Integer>() {
                public long retrieveInitialSequence() { return -1; }
                public void storeSequence(long seq) { }
                public boolean isLossTolerant() { return false; }
                public boolean isTerminal(Throwable failure) { return false; }
                public void onMessage(com.hazelcast.topic.Message<Integer> m) {
                    firstSeen.countDown();
                    try { Thread.sleep(200); } catch (InterruptedException ignored) { }
                }
            });

            for (int i = 0; i < 4; i++) t.publish(i);
            assertTrue(firstSeen.await(2, TimeUnit.SECONDS));

            Thread publisher = new Thread(() -> {
                try {
                    t.publish(99);
                } catch (Exception e) {
                }
            });
            publisher.start();

            Thread.sleep(150);
            assertTrue("Publisher thread should still be blocked", publisher.isAlive());
            publisher.interrupt();
        } finally {
            com.hazelcast.core.Hazelcast.shutdownAll();
        }
    }

    @Test
    public void testOverloadPolicyErrorUnderConcurrency() throws Exception {
        String topic = uniqueTopic();
        String cluster = "c-overload-error";
        Config cfg = baseConfig(cluster);

        cfg.addRingBufferConfig(rbConfig(topic, 2));
        cfg.addReliableTopicConfig(rtConfig(topic, 2, TopicOverloadPolicy.ERROR));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        try {
            ITopic<Integer> t = getTopic(hz, topic);

            CountDownLatch anySeen = new CountDownLatch(1);
            t.addMessageListener(new com.hazelcast.topic.ReliableMessageListener<Integer>() {
                public long retrieveInitialSequence() { return -1; }
                public void storeSequence(long seq) { }
                public boolean isLossTolerant() { return false; }
                public boolean isTerminal(Throwable failure) { return false; }
                public void onMessage(com.hazelcast.topic.Message<Integer> m) {
                    anySeen.countDown();
                    try { Thread.sleep(150); } catch (InterruptedException ignored) { }
                }
            });

            t.publish(0);
            t.publish(1);
            try {
                t.publish(99);
                fail("expected immediate TopicOverloadException under ERROR policy");
            } catch (com.hazelcast.topic.TopicOverloadException expected) {
            }
            assertTrue("previous messages should be delivered eventually", anySeen.await(3, TimeUnit.SECONDS));
        } finally {
            Hazelcast.shutdownAll();
        }
    }

    @Test
    public void testOverloadPolicyDiscardNewestUnderConcurrency() throws Exception {
        String topic = uniqueTopic();
        String cluster = "c-overload-discard-newest";
        com.hazelcast.config.Config cfg = baseConfig(cluster);

        cfg.addRingBufferConfig(rbConfig(topic, 4));
        cfg.addReliableTopicConfig(rtConfig(topic, 2, com.hazelcast.topic.TopicOverloadPolicy.DISCARD_NEWEST));

        com.hazelcast.core.HazelcastInstance hz = com.hazelcast.core.Hazelcast.newHazelcastInstance(cfg);
        ExecutorService publisher = Executors.newSingleThreadExecutor();
        try {
            com.hazelcast.topic.ITopic<Integer> t = getTopic(hz, topic);
            CountDownLatch startGate = new CountDownLatch(1);
            CountDownLatch sawAtLeastOneHundred = new CountDownLatch(1);
            ConcurrentLinkedQueue<Integer> received = new ConcurrentLinkedQueue<>();

            t.addMessageListener(new com.hazelcast.topic.ReliableMessageListener<Integer>() {
                public long retrieveInitialSequence() { return -1; }
                public void storeSequence(long seq) { }
                public boolean isLossTolerant() { return true; }
                public boolean isTerminal(Throwable failure) { return false; }
                public void onMessage(com.hazelcast.topic.Message<Integer> m) {
                    try { startGate.await(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) { }
                    Integer v = m.getMessageObject();
                    received.add(v);
                    if (v >= 100) {
                        sawAtLeastOneHundred.countDown();
                    }
                    try { Thread.sleep(50); } catch (InterruptedException ignored) { }
                }
            });

            for (int i = 0; i < 4; i++) t.publish(i);
            t.publish(99);
            startGate.countDown();

            Future<?> producer = publisher.submit(() -> {
                for (int i = 100; i < 200; i++) {
                    t.publish(i);
                    try { Thread.sleep(5); } catch (InterruptedException ignored) { }
                }
            });

            assertTrue("listener should eventually consume beyond the 99 marker",
                    sawAtLeastOneHundred.await(10, TimeUnit.SECONDS));
            for (int i = 0; i < 4; i++) {
                assertTrue("Should have received message " + i, received.contains(i));
            }
            assertFalse("Should not have received 99", received.contains(99));

            producer.get(1, TimeUnit.MINUTES);
        } finally {
            publisher.shutdownNow();
            com.hazelcast.core.Hazelcast.shutdownAll();
        }
    }

    @Test
    public void testOverloadPolicyDiscardOldestUnderConcurrency() throws Exception {
        String topic = uniqueTopic();
        String cluster = "c-overload-discard-oldest";
        com.hazelcast.config.Config cfg = baseConfig(cluster);

        cfg.addRingBufferConfig(rbConfig(topic, 4));
        cfg.addReliableTopicConfig(rtConfig(topic, 2, com.hazelcast.topic.TopicOverloadPolicy.DISCARD_OLDEST));

        com.hazelcast.core.HazelcastInstance hz = com.hazelcast.core.Hazelcast.newHazelcastInstance(cfg);
        ExecutorService publisher = Executors.newSingleThreadExecutor();
        try {
            com.hazelcast.topic.ITopic<Integer> t = getTopic(hz, topic);
            CountDownLatch startGate = new CountDownLatch(1);
            CountDownLatch sawAtLeastOneHundred = new CountDownLatch(1);
            ConcurrentLinkedQueue<Integer> received = new ConcurrentLinkedQueue<>();

            t.addMessageListener(new com.hazelcast.topic.ReliableMessageListener<Integer>() {
                public long retrieveInitialSequence() { return -1; }
                public void storeSequence(long seq) { }
                public boolean isLossTolerant() { return true; }
                public boolean isTerminal(Throwable failure) { return false; }
                public void onMessage(com.hazelcast.topic.Message<Integer> m) {
                    try { startGate.await(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) { }
                    received.add(m.getMessageObject());
                    if (m.getMessageObject() >= 100) sawAtLeastOneHundred.countDown();
                    try { Thread.sleep(50); } catch (InterruptedException ignored) { }
                }
            });

            for (int i = 0; i < 4; i++) t.publish(i);
            t.publish(99);
            startGate.countDown();

            Future<?> producer = publisher.submit(() -> {
                for (int i = 100; i < 160; i++) {
                    t.publish(i);
                    try { Thread.sleep(5); } catch (InterruptedException ignored) { }
                }
            });

            assertTrue(sawAtLeastOneHundred.await(10, TimeUnit.SECONDS));
            assertFalse("DISCARD_OLDEST should evict the oldest entry (0) under overload", received.contains(0));
            assertTrue("Should have kept message 99", received.contains(99));
            assertTrue("Should have kept message 3", received.contains(3));

            producer.get(1, TimeUnit.MINUTES);
        } finally {
            publisher.shutdownNow();
            com.hazelcast.core.Hazelcast.shutdownAll();
        }
    }

    @Test
    public void testSequentialDoesNotBlockCallerThread() throws Exception {
        int limit = 1;
        ReliableTopicConfig cfg = newConfigWithLimit(limit);
        Object mgr = newManager(cfg);

        AtomicInteger running = new AtomicInteger();
        List<ControlledOp> ops = new ArrayList<>();
        for (int i = 0; i < 3; i++) ops.add(new ControlledOp(i, running));

        Method schedule = findScheduleMethod(mgr.getClass());
        assertNotNull(schedule);
        Class<?> paramType = schedule.getParameterTypes()[0];

        long[] durationsMs = new long[ops.size()];
        List<CompletionStage<?>> stages = new ArrayList<>();

        for (int i = 0; i < ops.size(); i++) {
            ControlledOp op = ops.get(i);
            Object param = op.toParamObject(paramType);

            long t0 = System.nanoTime();
            Object stage = schedule.invoke(mgr, param);
            long t1 = System.nanoTime();
            durationsMs[i] = TimeUnit.NANOSECONDS.toMillis(t1 - t0);

            assertTrue(stage instanceof CompletionStage);
            stages.add((CompletionStage<?>) stage);
        }

        for (int i = 0; i < durationsMs.length; i++) {
            assertTrue("schedule() call " + i + " took too long: " + durationsMs[i] + "ms",
                    durationsMs[i] < 100);
        }

        ops.get(0).started.get(500, TimeUnit.MILLISECONDS);
        assertFalse("2nd op should not have started yet", ops.get(1).started.isDone());

        ops.get(0).done.complete(null);
        ops.get(1).started.get(1000, TimeUnit.MILLISECONDS);
        ops.get(1).done.complete(null);
        ops.get(2).started.get(1000, TimeUnit.MILLISECONDS);
        ops.get(2).done.complete(null);

        assertTrue(awaitQuiescence(mgr, Duration.ofSeconds(2)));
        for (CompletionStage<?> s : stages) assertTrue(s.toCompletableFuture().isDone());
    }

    @Test
    public void testUsesConfiguredExecutorNoUnbounded() throws Exception {
        String topic = uniqueTopic();
        String cluster = "c-exec-no-unbounded";
        int poolSize = 2;

        AtomicInteger threadSeq = new AtomicInteger();
        ThreadFactory tf = r -> {
            Thread t = new Thread(r);
            t.setName("rt-test-exec-" + threadSeq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                poolSize, poolSize, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(64), tf);

        Config cfg = baseConfig(cluster);
        cfg.addRingBufferConfig(rbConfig(topic, 2));
        ReliableTopicConfig rtc = new ReliableTopicConfig(topic)
                .setExecutor(pool)
                .setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK);
        maxConcurrentPublishesSetter().invoke(rtc, poolSize);
        cfg.addReliableTopicConfig(rtc);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
        try {
            ITopic<Integer> reliableTopic = getTopic(hz, topic);

            ReliableTopicProxy<Integer> proxy = (ReliableTopicProxy<Integer>) reliableTopic;
            Field rbField = ReliableTopicProxy.class.getDeclaredField("ringbuffer");
            rbField.setAccessible(true);
            Ringbuffer<ReliableTopicMessage> original = (Ringbuffer<ReliableTopicMessage>) rbField.get(proxy);

            Set<String> observedThreads = Collections.synchronizedSet(new HashSet<>());
            Ringbuffer<ReliableTopicMessage> wrapped = (Ringbuffer<ReliableTopicMessage>) Proxy.newProxyInstance(
                    original.getClass().getClassLoader(),
                    new Class[]{Ringbuffer.class},
                    (p, m, args) -> {
                        if ("addAsync".equals(m.getName()) || "addAllAsync".equals(m.getName())) {
                            observedThreads.add(Thread.currentThread().getName());
                        }
                        return m.invoke(original, args);
                    });
            rbField.set(proxy, wrapped);

            int before = ManagementFactory.getThreadMXBean().getThreadCount();

            ExecutorService pub = Executors.newFixedThreadPool(5);
            for (int i = 0; i < 5; i++) {
                final int msg = i;
                pub.submit(() -> reliableTopic.publish(msg));
            }
            Thread.sleep(500);
            for (int i = 0; i < 5; i++) {
                try {
                    original.readOne(original.headSequence());
                } catch (Exception ignored) {
                }
            }
            pub.shutdown();
            assertTrue(pub.awaitTermination(10, TimeUnit.SECONDS));

            int after = ManagementFactory.getThreadMXBean().getThreadCount();
            assertTrue("Thread count increased unexpectedly: " + (after - before),
                    after - before <= poolSize);

            Set<String> nonCustom = observedThreads.stream()
                    .filter(t -> !t.startsWith("rt-test-exec-"))
                    .filter(t -> !t.equals(Thread.currentThread().getName()))
                    .collect(Collectors.toSet());
            assertTrue("Publishing used unexpected threads: " + nonCustom, nonCustom.isEmpty());
        } finally {
            pool.shutdownNow();
            Hazelcast.shutdownAll();
        }
    }

   @Test
public void testGlobalOrderUnderConcurrentOverload() throws Exception {
    int limit = 2;
    ReliableTopicConfig cfg = newConfigWithLimit(limit);
    cfg.setTopicOverloadPolicy(com.hazelcast.topic.TopicOverloadPolicy.BLOCK);
    Object mgr = newManager(cfg);
    AtomicInteger running = new AtomicInteger();
    AtomicInteger maxRunning = new AtomicInteger();
    List<Integer> startOrder = Collections.synchronizedList(new ArrayList<>());

    CountDownLatch allStarted = new CountDownLatch(20);
    
    int n = 20;
    List<ControlledOp> ops = new ArrayList<>();
    for (int i = 0; i < n; i++) {
        final int idx = i;
        ops.add(new ControlledOp(idx, running) {
            @Override
            Object toParamObject(Class<?> paramType) {
                if (Supplier.class.isAssignableFrom(paramType)) {
                    return (Supplier<CompletionStage<?>>) () -> {
                        int now = running.incrementAndGet();
                        maxRunning.updateAndGet(prev -> Math.max(prev, now));
                        startOrder.add(idx);
                        started.complete(null);
                        allStarted.countDown();
                        return done.whenComplete((r, t) -> running.decrementAndGet());
                    };
                }
                return super.toParamObject(paramType);
            }
        });
    }
    
    scheduleN(mgr, n, ops);

    for (int i = 0; i < Math.min(limit, n); i++) {
        ops.get(i).started.get(1000, TimeUnit.MILLISECONDS);
    }
    for (int i = limit; i < n; i++) {
        ops.get(i - limit).done.complete(null);
        ops.get(i).started.get(1000, TimeUnit.MILLISECONDS);
    }
    for (int i = Math.max(0, n - limit); i < n; i++) {
        ops.get(i).done.complete(null);
    }

    assertTrue(awaitQuiescence(mgr, Duration.ofSeconds(2)));
    assertTrue("Not all operations started", allStarted.await(3, TimeUnit.SECONDS));
    assertEquals("All operations should have been processed", n, startOrder.size());
    assertEquals("Each operation should start exactly once", n, new HashSet<>(startOrder).size());
    assertEquals("Concurrency limit should be reached", limit, maxRunning.get());

    int inOrder = 0;
    for (int i = 1; i < startOrder.size(); i++) {
        if (startOrder.get(i) > startOrder.get(i - 1)) {
            inOrder++;
        }
    }
    double orderRatio = (double) inOrder / (startOrder.size() - 1);
    assertTrue("Operations should mostly preserve order, but only " + (orderRatio * 100)
            + "% were in order", orderRatio >= 0.6);
}
    @Test
    public void testExecutorIsolationOnMerge() throws Exception {
        String topic = uniqueTopic();
        String cluster = "c-exec-iso-block";

        Set<String> publishThreadNames = Collections.synchronizedSet(new HashSet<>());

        ThreadPoolExecutor customExecutor = new ThreadPoolExecutor(
                2, 2, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(64),
                r -> {
                    Thread t = new Thread(() -> {
                        publishThreadNames.add(Thread.currentThread().getName());
                        r.run();
                    });
                    t.setName("custom-topic-executor-" + System.nanoTime());
                    t.setDaemon(true);
                    return t;
                });

        Config cfg = baseConfig(cluster);
        cfg.addRingBufferConfig(new RingbufferConfig(topic)
                .setCapacity(2)
                .setTimeToLiveSeconds(0));

        ReliableTopicConfig topicConfig = new ReliableTopicConfig(topic)
                .setTopicOverloadPolicy(TopicOverloadPolicy.BLOCK)
                .setExecutor(customExecutor);

        Method setter = maxConcurrentPublishesSetter();
        if (setter != null) {
            setter.invoke(topicConfig, 3);
        }
        cfg.addReliableTopicConfig(topicConfig);

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(cfg);
        try {
            ITopic<String> reliableTopic = getTopic(hz1, topic);

            ReliableTopicProxy<String> proxy = (ReliableTopicProxy<String>) reliableTopic;
            Field rbField = ReliableTopicProxy.class.getDeclaredField("ringbuffer");
            rbField.setAccessible(true);
            Ringbuffer<ReliableTopicMessage> originalRb =
                    (Ringbuffer<ReliableTopicMessage>) rbField.get(proxy);

            Ringbuffer<ReliableTopicMessage> wrappedRb =
                    (Ringbuffer<ReliableTopicMessage>) Proxy.newProxyInstance(
                            originalRb.getClass().getClassLoader(),
                            new Class[]{Ringbuffer.class},
                            (p, method, args) -> {
                                String currentThread = Thread.currentThread().getName();
                                publishThreadNames.add(currentThread);

                                if ("addAsync".equals(method.getName()) ||
                                        "addAllAsync".equals(method.getName())) {
                                    if (Math.random() < 0.8) {
                                        CompletableFuture<Long> failFuture = new CompletableFuture<>();
                                        failFuture.complete(-1L);
                                        return failFuture;
                                    }
                                }
                                return method.invoke(originalRb, args);
                            });
            rbField.set(proxy, wrappedRb);
            reliableTopic.publish("prefill-1");
            reliableTopic.publish("prefill-2");

            ExecutorService testExecutor = Executors.newFixedThreadPool(5);
            List<Future<?>> publishFutures = new ArrayList<>();

            for (int i = 0; i < 10; i++) {
                final String msg = "message-" + i;
                publishFutures.add(testExecutor.submit(() -> {
                    try {
                        reliableTopic.publish(msg);
                    } catch (Exception e) {
                    }
                }));
            }

            Thread.sleep(1000);

            originalRb.readOne(originalRb.headSequence());

            HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(cfg);

            for (int i = 10; i < 15; i++) {
                final String msg = "post-merge-" + i;
                publishFutures.add(testExecutor.submit(() -> {
                    reliableTopic.publish(msg);
                }));
            }

            for (Future<?> f : publishFutures) {
                try {
                    f.get(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                }
            }

            testExecutor.shutdown();

            Set<String> violatingThreads = new HashSet<>();
            for (String threadName : publishThreadNames) {
                if (!threadName.startsWith("custom-topic-executor-") &&
                        !threadName.equals(Thread.currentThread().getName())) {
                    violatingThreads.add(threadName);
                }
            }

            assertTrue("Publishing operations used non-custom executor threads during " +
                    "retry/backoff. Violating threads: " + violatingThreads +
                    "\nAll threads used: " + publishThreadNames,
                    violatingThreads.isEmpty());

            hz2.shutdown();
        } finally {
            customExecutor.shutdown();
            Hazelcast.shutdownAll();
        }
    }

    @Test
    public void testNoUnboundedThreadCreationForPublishing() throws Exception {
        int limit = 3;
        ReliableTopicConfig cfg = newConfigWithLimit(limit);
        Object mgr = newManager(cfg);

        Set<String> observed = Collections.synchronizedSet(new HashSet<>());
        AtomicInteger running = new AtomicInteger();

        List<ControlledOp> ops = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
            final int idx = i;
            ops.add(new ControlledOp(idx, running) {
                @Override Object toParamObject(Class<?> paramType) {
                    if (Supplier.class.isAssignableFrom(paramType)) {
                        return (Supplier<CompletionStage<?>>) () -> {
                            observed.add(Thread.currentThread().getName());
                            running.incrementAndGet();
                            started.complete(null);
                            return done.whenComplete((r, t) -> running.decrementAndGet());
                        };
                    }
                    return super.toParamObject(paramType);
                }
            });
        }

        scheduleN(mgr, ops.size(), ops);

        for (int i = 0; i < Math.min(limit, ops.size()); i++) {
            ops.get(i).started.get(1000, TimeUnit.MILLISECONDS);
        }

        for (int i = limit; i < ops.size(); i++) {
            ops.get(i - limit).done.complete(null);
            ops.get(i).started.get(1000, TimeUnit.MILLISECONDS);
        }
        for (int i = Math.max(0, ops.size() - limit); i < ops.size(); i++) {
            ops.get(i).done.complete(null);
        }

        assertTrue(awaitQuiescence(mgr, Duration.ofSeconds(2)));

        int unique = observed.size();
        int allowed = Math.max(limit, 1) * 2;
        assertTrue("Observed too many thread names (" + unique + "), expected <= " + allowed, unique <= allowed);
    }

    private static boolean verifyBatchSizes(List<Integer> batchSizes, int expectedBatchSize, int totalMessages) {
        int sum = batchSizes.stream().mapToInt(Integer::intValue).sum();
        if (sum != totalMessages) return false;

        for (int i = 0; i < batchSizes.size() - 1; i++) {
            if (batchSizes.get(i) > expectedBatchSize) return false;
        }
        return true;
    }
}
