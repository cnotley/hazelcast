/*
 * Copyright (c) 2008-2025, Hazelcast, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.hazelcast.map.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapEvictionPolicyComparator;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Modifier;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@RunWith(HazelcastSerialClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class WeightedTimeAwareEvictionPolicyTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Before
    public void setUp() {
        factory = createHazelcastInstanceFactory(3);
    }

    @After
    public void tearDown() {
        if (factory != null) {
            factory.shutdownAll();
            factory = null;
        }
    }

    @Test
    public void isSerializableRuntime() {
        WeightedTimeAwareEvictionPolicy<String, String> p = new WeightedTimeAwareEvictionPolicy<>(0.4, 0.3, 0.3);
        assertTrue("instance must be Serializable", p instanceof Serializable);
    }

    @Test
    public void implementsMapEvictionPolicyComparator_interface() {
        assertTrue("must implement MapEvictionPolicyComparator",
                MapEvictionPolicyComparator.class.isAssignableFrom(
                        com.hazelcast.map.eviction.WeightedTimeAwareEvictionPolicy.class));
    }

    @Test
    public void classIsPublic() {
        int mods = com.hazelcast.map.eviction.WeightedTimeAwareEvictionPolicy.class.getModifiers();
        assertTrue("class must be public", Modifier.isPublic(mods));
    }

    @Test
    public void weightsNonNegativeValidation() {
        expectThrows(IllegalArgumentException.class,
                () -> new WeightedTimeAwareEvictionPolicy<>(-0.1, 0.6, 0.5));

        expectThrows(IllegalArgumentException.class,
                () -> new WeightedTimeAwareEvictionPolicy<>(0.5, -0.1, 0.6));

        expectThrows(IllegalArgumentException.class,
                () -> new WeightedTimeAwareEvictionPolicy<>(0.6, 0.5, -0.1));
    }

    @Test
    public void finiteWeightsRejectNaN() {
        expectThrows(IllegalArgumentException.class,
                () -> new WeightedTimeAwareEvictionPolicy<>(Double.NaN, 0.5, 0.5));

        expectThrows(IllegalArgumentException.class,
                () -> new WeightedTimeAwareEvictionPolicy<>(0.5, Double.NaN, 0.5));

        expectThrows(IllegalArgumentException.class,
                () -> new WeightedTimeAwareEvictionPolicy<>(0.5, 0.5, Double.NaN));
    }

    @Test
    public void finiteWeightsRejectInfinity() {
        expectThrows(IllegalArgumentException.class,
                () -> new WeightedTimeAwareEvictionPolicy<>(0.5, Double.POSITIVE_INFINITY, 0.5));
        expectThrows(IllegalArgumentException.class,
                () -> new WeightedTimeAwareEvictionPolicy<>(0.5, 0.5, Double.NEGATIVE_INFINITY));
    }

    @Test
    public void weightsNotAllZeroValidation() {
        expectThrows(IllegalArgumentException.class,
                () -> new WeightedTimeAwareEvictionPolicy<>(0.0, 0.0, 0.0));
    }

    @Test
    public void weightsSumWithinToleranceValidation_boundary() {
        new WeightedTimeAwareEvictionPolicy<>(0.5, 0.5, 2e-7);

        expectThrows(IllegalArgumentException.class,
                () -> new WeightedTimeAwareEvictionPolicy<>(0.5, 0.500002, 0.0));

        expectThrows(IllegalArgumentException.class,
                () -> new WeightedTimeAwareEvictionPolicy<>(0.5, 0.499998, 0.0));
    }

    @Test
    public void supportsProgrammaticWeightsConstruction() {
        MapEvictionPolicyComparator<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.333333, 0.333333,
                0.333334);

        assertNotNull("must be able to construct programmatically with weights", policy);
    }

    @Test
    public void ageWeightDominance() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.51, 0.245,
                0.245);
        long now = System.currentTimeMillis();
        EV older = new EV("older", now - 10_000, 50, 500);
        EV newer = new EV("newer", now - 1_000, 50, 500);
        int r = policy.compare(older, newer);
        assertTrue("older should sort before newer (higher eviction priority)", r < 0);
    }

    @Test
    public void accessWeightDominance() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.245, 0.51,
                0.245);
        long t = System.currentTimeMillis();
        EV rare = new EV("rare", t - 5_000, 1, 100);
        EV freq = new EV("freq", t - 5_000, 100, 100);
        int r = policy.compare(rare, freq);
        assertTrue("rarely-accessed should sort before frequently-accessed", r < 0);
    }

    @Test
    public void sizeWeightDominance() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.245, 0.245,
                0.51);
        long t = System.currentTimeMillis();
        EV large = new EV("large", t - 5_000, 50, 10_000);
        EV small = new EV("small", t - 5_000, 50, 10);
        int r = policy.compare(large, small);
        assertTrue("larger should sort before smaller", r < 0);
    }

    @Test
    public void neutralAgeWhenFuture() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.80, 0.10,
                0.10);
        long now = System.currentTimeMillis();
        EV future = new EV("future", now + 60_000, 10, 1_000);
        EV normal = new EV("normal", now - 5_000, 10, 1_000);
        int r = policy.compare(future, normal);
        assertEquals("Age factor must be excluded for both when one creationTime is in the future", 0, r);
    }

    @Test
    public void neutralSizeWhenZeroCost() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.10, 0.10,
                0.80);
        long t = System.currentTimeMillis();
        EV zero = new EV("zero", t - 5_000, 7, 0);
        EV big = new EV("big", t - 5_000, 7, 10_000);
        int r = policy.compare(zero, big);
        assertEquals("Size factor must be excluded for both when either cost==0", 0, r);
    }

    @Test
    public void neutralAccessWhenMissing() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.10, 0.80,
                0.10);
        long t = System.currentTimeMillis();
        EV left = new EV("L", t - 5_000, -1, 1_000);
        EV right = new EV("R", t - 5_000, 100, 1_000);
        int r = policy.compare(left, right);
        assertEquals("Access factor must be neutral if one side lacks hits", 0, r);
    }

    @Test
    public void neutralAccessWhenMissing_symmetric() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.10, 0.80,
                0.10);
        long t = System.currentTimeMillis();
        EV left = new EV("L", t - 5_000, 100, 1_000);
        EV right = new EV("R", t - 5_000, -1, 1_000);
        assertEquals(0, policy.compare(left, right));
    }

    @Test
    public void neutralAgeWhenMissing() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.80, 0.10,
                0.10);
        long t = System.currentTimeMillis();
        EV left = new EV("L", -1, 10, 1_000);
        EV right = new EV("R", t - 5_000, 10, 1_000);
        int r = policy.compare(left, right);
        assertEquals("Age factor must be neutral if one side lacks creationTime", 0, r);
    }

    @Test
    public void neutralSizeWhenMissing() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.10, 0.10,
                0.80);
        long t = System.currentTimeMillis();
        EV left = new EV("L", t - 5_000, 10, -1);
        EV right = new EV("R", t - 5_000, 10, 10_000);
        int r = policy.compare(left, right);
        assertEquals("Size factor must be neutral if one side lacks cost", 0, r);
    }

    @Test
    public void ttlMaxIdleIndependence() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.4, 0.3, 0.3);

        for (boolean statsEnabled : new boolean[] { true, false }) {
            Config cfg = getConfig();
            cfg.setProperty("hazelcast.partition.count", "1");

            MapConfig ttlOnly = cfg.getMapConfig("ttlOnly");
            ttlOnly.setStatisticsEnabled(statsEnabled);
            ttlOnly.setPerEntryStatsEnabled(true);
            ttlOnly.setEvictionConfig(new EvictionConfig()
                    .setEvictionPolicy(EvictionPolicy.NONE)
                    .setComparator(policy)
                    .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                    .setSize(5));

            MapConfig idleOnly = cfg.getMapConfig("idleOnly");
            idleOnly.setStatisticsEnabled(statsEnabled);
            idleOnly.setPerEntryStatsEnabled(true);
            idleOnly.setEvictionConfig(new EvictionConfig()
                    .setEvictionPolicy(EvictionPolicy.NONE)
                    .setComparator(policy)
                    .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                    .setSize(5));
            idleOnly.setMaxIdleSeconds(2);

            HazelcastInstance inst = factory.newHazelcastInstance(cfg);
            try {
                IMap<String, String> ttl = inst.getMap("ttlOnly");
                for (int i = 0; i < 3; i++) {
                    ttl.put("nottl-" + i, "v");
                }
                for (int i = 0; i < 3; i++) {
                    ttl.put("ttl-" + i, "v", 3, TimeUnit.SECONDS);
                }
                sleepSeconds(1);
                int pre = 0;
                for (int i = 0; i < 3; i++) {
                    if (ttl.containsKey("ttl-" + i)) {
                        pre++;
                    }
                }
                assertTrue("some TTL entries should exist before expiry", pre > 0);

                assertTrueEventually(() -> {
                    for (int i = 0; i < 3; i++) {
                        assertFalse(ttl.containsKey("ttl-" + i));
                    }
                });

                IMap<String, String> idle = inst.getMap("idleOnly");
                idle.put("k1", "v");
                idle.put("k2", "v");
                sleepSeconds(1);
                idle.get("k1");
                sleepSeconds(2);
                assertNotNull("k1 should remain due to access within idle window", idle.get("k1"));
                assertTrueEventually(() -> assertNull(idle.get("k2")));
            } finally {
                inst.shutdown();
            }
        }
    }

    private void fillMapWithAgedEntries(IMap<String, String> map) {
        for (int i = 0; i < 5; i++) {
            map.put("old-" + i, "val");
        }
        sleepSeconds(2);
        for (int i = 0; i < 10; i++) {
            map.put("new-" + i, "val");
        }
    }

    private <K, V> IMap<K, V> getIMapWithPolicy(String mapName, MapEvictionPolicyComparator<K, V> policy) {
        Config config = getConfig();
        config.setProperty("hazelcast.partition.count", "1");

        EvictionConfig evictionConfig = new EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.NONE)
                .setComparator(policy)
                .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                .setSize(10);

        config.getMapConfig(mapName)
                .setEvictionConfig(evictionConfig)
                .setPerEntryStatsEnabled(true)
                .setStatisticsEnabled(true);

        HazelcastInstance instance = factory.newHazelcastInstance(config);
        return instance.getMap(mapName);
    }

    private void assertBiasTowardEvicting(String prefix, IMap<String, ?> map) {
        HazelcastInstance anyMember = factory.getAllHazelcastInstances().iterator().next();
        int perNodeSize = anyMember.getConfig()
                .getMapConfig(map.getName())
                .getEvictionConfig()
                .getSize();
        int members = anyMember.getCluster().getMembers().size();
        final int clusterCap = perNodeSize * members;

        assertTrueEventually(() -> assertTrue("Map size should be at or below cluster cap (" + clusterCap + ")",
                map.size() <= clusterCap));

        int evictedCount = 0;
        for (int i = 0; i < 5; i++) {
            if (!map.containsKey(prefix + i)) {
                evictedCount++;
            }
        }
        assertTrue("More '" + prefix + "' entries should have been evicted (at least 2 of 5)", evictedCount >= 2);
    }

    @Test
    public void integrationEvictsByAge() {
        MapEvictionPolicyComparator<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.51, 0.245, 0.245);
        IMap<String, String> map = getIMapWithPolicy("age-test", policy);
        fillMapWithAgedEntries(map);
        assertBiasTowardEvicting("old-", map);
    }

    @Test
    public void perPartitionEviction() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.4, 0.3, 0.3);
        Config cfg = getConfig();
        cfg.getMapConfig("pp").setEvictionConfig(new EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.NONE)
                .setComparator(policy)
                .setMaxSizePolicy(MaxSizePolicy.PER_PARTITION)
                .setSize(2));
        HazelcastInstance i1 = factory.newHazelcastInstance(cfg);
        HazelcastInstance i2 = factory.newHazelcastInstance(cfg);
        IMap<String, String> m = i1.getMap("pp");
        for (int k = 0; k < 100; k++) {
            m.put("key-" + k, "v");
        }
        assertTrueEventually(() -> {
            PartitionService ps = i1.getPartitionService();
            Map<Integer, List<String>> byPart = m.keySet().stream()
                    .collect(Collectors.groupingBy(k -> ps.getPartition(k).getPartitionId()));
            for (List<String> ks : byPart.values()) {
                assertTrue("partition size ≤ 2", ks.size() <= 2);
            }
        });
    }

    @Test
    public void comparatorEqualityOnDuplicates() {
        WeightedTimeAwareEvictionPolicy<String, String> p = new WeightedTimeAwareEvictionPolicy<>(0.4, 0.3, 0.3);
        long t = System.currentTimeMillis();
        EV a = new EV("a", t - 10000, 5, 1000);
        EV a2 = new EV("a2", t - 10000, 5, 1000);
        assertEquals(0, p.compare(a, a2));
    }

    @Test
    public void comparatorTransitivityHolds() {
        WeightedTimeAwareEvictionPolicy<String, String> p = new WeightedTimeAwareEvictionPolicy<>(0.4, 0.3, 0.3);
        long t = System.currentTimeMillis();
        EV a = new EV("a", t - 10000, 5, 1000);
        EV b = new EV("b", t - 5000, 50, 500);
        EV c = new EV("c", t - 1000, 100, 100);
        int ab = p.compare(a, b);
        int bc = p.compare(b, c);
        if (ab < 0 && bc < 0) {
            int ac = p.compare(a, c);
            assertTrue("transitivity violated", ac < 0);
        }
    }

    @Test
    public void comparatorSymmetryHolds() {
        WeightedTimeAwareEvictionPolicy<String, String> p = new WeightedTimeAwareEvictionPolicy<>(0.4, 0.3, 0.3);
        long t = System.currentTimeMillis();
        EV a = new EV("a", t - 9000, 5, 1000);
        EV b = new EV("b", t - 5000, 50, 500);
        int ab = p.compare(a, b);
        int ba = p.compare(b, a);
        assertEquals("symmetry violated", -ab, ba);
    }

    @Test
    public void balancedWeights() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.333, 0.333,
                0.334);
        long t = System.currentTimeMillis();
        EV hi = new EV("hi", t - 10_000, 5, 1_000);
        EV lo = new EV("lo", t - 1_000, 100, 100);
        int r = policy.compare(hi, lo);
        assertTrue("higher composite score should sort first", r < 0);
    }

    @Test
    public void threadSafetySmoke() throws Exception {
        final WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.4, 0.3,
                0.3);
        final int threads = 4;
        final int iterations = 500;

        ExecutorService exec = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger errors = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            exec.submit(() -> {
                try {
                    Random rnd = new Random();
                    for (int j = 0; j < iterations; j++) {
                        EV e1 = new EV("k" + j, rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
                        EV e2 = new EV("k" + (j + 1), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());

                        policy.compare(e1, e2);
                    }
                } catch (Throwable t) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue("Latch timed out", latch.await(10, TimeUnit.SECONDS));
        exec.shutdown();
        assertEquals("No comparator calls should throw", 0, errors.get());
    }

    @Test
    public void jdkRoundTripComparator() throws Exception {
        WeightedTimeAwareEvictionPolicy<String, String> original = new WeightedTimeAwareEvictionPolicy<>(0.34, 0.33,
                0.33);

        byte[] bytes;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(original);
            oos.flush();
            bytes = bos.toByteArray();
        }
        WeightedTimeAwareEvictionPolicy<String, String> copy;
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            @SuppressWarnings("unchecked")
            WeightedTimeAwareEvictionPolicy<String, String> tmp = (WeightedTimeAwareEvictionPolicy<String, String>) ois
                    .readObject();
            copy = tmp;
        }

        Random rnd = new Random(1234);
        for (int i = 0; i < 200; i++) {
            EV e1 = new EV("k" + i, rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
            EV e2 = new EV("k" + (i + 1), rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
            int a = original.compare(e1, e2);
            int b = copy.compare(e1, e2);
            assertEquals(Integer.signum(a), Integer.signum(b));
        }
    }

    @Test
    public void clusterEvictionConsistency() {
        Config cfg = getConfig();
        cfg.setProperty("hazelcast.partition.count", "1");

        MapConfig mc = cfg.getMapConfig("evict-test");

        MapEvictionPolicyComparator<String, Integer> cmp = new WeightedTimeAwareEvictionPolicy<>(0.4, 0.3, 0.3);

        EvictionConfig ec = new EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.NONE)
                .setComparator(cmp)
                .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                .setSize(10);

        mc.setEvictionConfig(ec);

        HazelcastInstance h1 = factory.newHazelcastInstance(cfg);
        HazelcastInstance h2 = factory.newHazelcastInstance(cfg);

        IMap<String, Integer> map = h1.getMap("evict-test");
        for (int i = 0; i < 200; i++) {
            map.put("k-" + i, i);
        }

        assertTrueEventually(() -> {
            int global = map.size();
            assertTrue("Global size must not exceed 20 with PER_NODE=10 and 2 members, was=" + global,
                    global <= 20);
        });
    }

    @Test
    public void apiSurfaceUsesOnlyAllowedPackages() {
        Class<?> cls = WeightedTimeAwareEvictionPolicy.class;

        for (Constructor<?> c : cls.getDeclaredConstructors()) {
            for (Class<?> pt : c.getParameterTypes()) {
                assertAllowed("constructor parameter", pt);
            }
        }

        for (Method m : cls.getDeclaredMethods()) {
            Class<?> rt = m.getReturnType();
            if (!rt.isPrimitive())
                assertAllowed("return type of " + m.getName(), rt);

            for (Class<?> pt : m.getParameterTypes()) {
                if (!pt.isPrimitive())
                    assertAllowed("parameter of " + m.getName(), pt);
            }
        }
    }

    @Test
    public void fieldsUseOnlyAllowedTypes() {
        Class<?> cls = WeightedTimeAwareEvictionPolicy.class;
        for (Field f : cls.getDeclaredFields()) {
            if (f.getType().isPrimitive())
                continue;
            String p = f.getType().getName();
            assertTrue("Field uses disallowed type: " + f.getName() + " -> " + p,
                    p.startsWith("java.") || p.startsWith("com.hazelcast."));
        }
    }

    @Test
    public void preservesPartitionsBackups() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.4, 0.3, 0.3);

        Config cfg = getConfig();
        cfg.setProperty("hazelcast.partition.count", "1");

        EvictionConfig ev = new EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.NONE)
                .setComparator(policy)
                .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                .setSize(10);

        cfg.getMapConfig("pb").setEvictionConfig(ev);

        HazelcastInstance n1 = factory.newHazelcastInstance(cfg);
        HazelcastInstance n2 = factory.newHazelcastInstance(cfg);

        IMap<String, String> m = n1.getMap("pb");
        for (int i = 0; i < 25; i++) {
            m.put("k-" + i, "v");
        }

        int perNode = n1.getConfig().getMapConfig("pb").getEvictionConfig().getSize();
        int members = n1.getCluster().getMembers().size();

        assertTrueEventually(() -> assertTrue(m.size() <= perNode * members));

        for (String k : m.keySet()) {
            assertEquals(m.get(k), n2.getMap("pb").get(k));
        }
    }

    private static Object roundTrip(Object obj) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new ObjectOutputStream(baos).writeObject(obj);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        return new ObjectInputStream(bais).readObject();
    }

    private static void expectThrows(Class<? extends Throwable> expected, Runnable r) {
        try {
            r.run();
            fail("Expected " + expected.getSimpleName());
        } catch (AssertionError e) {
            throw e;
        } catch (Throwable t) {
            if (!expected.isInstance(t)) {
                fail("Expected " + expected.getSimpleName() + " but got: " + t.getClass().getName());
            }
        }
    }

    private static void assertAllowed(String where, Class<?> t) {
        String name = t.getName();
        assertTrue("Disallowed " + where + ": " + name,
                name.startsWith("java.") || name.startsWith("com.hazelcast."));
    }

    @Test
    public void timeWeightDominance_lastAccess() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.51, 0.245,
                0.245);
        long now = System.currentTimeMillis();
        EV2 older = new EV2("older", now - 20_000, now - 20_000, 50, 500);
        EV2 newer = new EV2("newer", now - 20_000, now - 1_000, 50, 500);
        int r = policy.compare(older, newer);
        assertTrue("Entry with older lastAccessTime should sort first (evict earlier)", r < 0);
    }

    @Test
    public void accessWeightDominance_exp() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.245, 0.51,
                0.245);
        long t = System.currentTimeMillis();
        EV rare = new EV("rare", t - 5_000, 1, 100);
        EV freq = new EV("freq", t - 5_000, 100, 100);
        int r = policy.compare(rare, freq);
        assertTrue("Fewer hits should sort first under access dominance", r < 0);
    }

    @Test
    public void factorsNormalized() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.70, 0.00,
                0.30);
        long now = System.currentTimeMillis();

        EV2 olderSmall = new EV2("olderSmall",
                now - TimeUnit.DAYS.toMillis(1),
                now - TimeUnit.DAYS.toMillis(1),
                10, 1);

        EV2 newerHuge = new EV2("newerHuge",
                now - TimeUnit.SECONDS.toMillis(5),
                now - TimeUnit.SECONDS.toMillis(5),
                10, 1_000_000_000_000L);

        int r = policy.compare(olderSmall, newerHuge);
        assertTrue("Time factor must dominate despite huge cost if factors are normalized", r < 0);
    }

    @Test
    public void ttlInTimeFactor() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.90, 0.05,
                0.05);
        long now = System.currentTimeMillis();

        EV2 shortTtl = new EV2("short", now, now, 10, 100, TimeUnit.SECONDS.toMillis(1));
        EV2 longTtl = new EV2("long", now, now, 10, 100, TimeUnit.MINUTES.toMillis(1));

        int r = policy.compare(shortTtl, longTtl);
        assertNotEquals("TTL difference should affect time factor when set", 0, r);
    }

    @Test
    public void neutralAccessAsZero() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.10, 0.80,
                0.10);
        long t = System.currentTimeMillis();

        EV left = new EV("L", t - 5_000, -1, 1_000);
        EV right = new EV("R", t - 5_000, 100, 1_000);

        assertEquals("Access factor must be neutral if one side lacks hits", 0, policy.compare(left, right));
        assertEquals("Symmetry for missing hits", 0, policy.compare(right, left));
    }

    @Test
    public void neutralTimeAsZero() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.80, 0.10,
                0.10);
        long now = System.currentTimeMillis();

        EV2 invalidPastLA = new EV2("A", now - 5_000, -1, 10, 1_000);
        EV2 normal = new EV2("B", now - 5_000, now - 5_000, 10, 1_000);

        assertEquals("Time factor neutral when lastAccessTime <= 0", 0, policy.compare(invalidPastLA, normal));

        EV2 invalidFutureLA = new EV2("C", now, now + TimeUnit.MINUTES.toMillis(1), 10, 1_000);
        assertEquals("Time factor neutral when lastAccessTime > now", 0, policy.compare(invalidFutureLA, normal));
    }

    @Test
    public void neutralSizeAsZero() {
        WeightedTimeAwareEvictionPolicy<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.10, 0.10,
                0.80);
        long t = System.currentTimeMillis();

        EV zero = new EV("zero", t - 5_000, 7, 0);
        EV pos = new EV("pos", t - 5_000, 7, 10_000);
        assertEquals("Size factor neutral when either cost == 0", 0, policy.compare(zero, pos));

        EV neg = new EV("neg", t - 5_000, 7, -1);
        assertEquals("Size factor neutral when either cost <= 0", 0, policy.compare(neg, pos));
    }

    @Test
    public void integrationEvictsByTime() {
        MapEvictionPolicyComparator<String, String> policy = new WeightedTimeAwareEvictionPolicy<>(0.51, 0.245, 0.245);
        IMap<String, String> map = getIMapWithPolicy("time-test", policy);

        fillMapWithAgedEntries(map);

        assertBiasTowardEvicting("old-", map);
    }

    private static class EV2 implements EntryView<String, String> {
        final String k;
        final long creationTime;
        final long lastAccessTime;
        final long hits;
        final long cost;
        final long ttl;

        EV2(String k, long creationTime, long lastAccessTime, long hits, long cost) {
            this(k, creationTime, lastAccessTime, hits, cost, -1);
        }

        EV2(String k, long creationTime, long lastAccessTime, long hits, long cost, long ttl) {
            this.k = k;
            this.creationTime = creationTime;
            this.lastAccessTime = lastAccessTime;
            this.hits = hits;
            this.cost = cost;
            this.ttl = ttl;
        }

        @Override
        public String getKey() {
            return k;
        }

        @Override
        public String getValue() {
            return "v";
        }

        @Override
        public long getCost() {
            return cost;
        }

        @Override
        public long getCreationTime() {
            return creationTime;
        }

        @Override
        public long getExpirationTime() {
            return ttl > 0 ? creationTime + ttl : -1;
        }

        @Override
        public long getHits() {
            return hits;
        }

        @Override
        public long getLastAccessTime() {
            return lastAccessTime;
        }

        @Override
        public long getLastStoredTime() {
            return -1;
        }

        @Override
        public long getLastUpdateTime() {
            return -1;
        }

        @Override
        public long getVersion() {
            return 0;
        }

        @Override
        public long getTtl() {
            return ttl;
        }

        @Override
        public long getMaxIdle() {
            return -1;
        }
    }

    private static class EV implements EntryView<String, String> {
        final String k;
        final long creationTime;
        final long hits;
        final long cost;

        EV(String k, long creationTime, long hits, long cost) {
            this.k = k;
            this.creationTime = creationTime;
            this.hits = hits;
            this.cost = cost;
        }

        @Override
        public String getKey() {
            return k;
        }

        @Override
        public String getValue() {
            return "v";
        }

        @Override
        public long getCost() {
            return cost;
        }

        @Override
        public long getCreationTime() {
            return creationTime;
        }

        @Override
        public long getExpirationTime() {
            return -1;
        }

        @Override
        public long getHits() {
            return hits;
        }

        @Override
        public long getLastAccessTime() {
            return -1;
        }

        @Override
        public long getLastStoredTime() {
            return -1;
        }

        @Override
        public long getLastUpdateTime() {
            return -1;
        }

        @Override
        public long getVersion() {
            return 0;
        }

        @Override
        public long getTtl() {
            return -1;
        }

        @Override
        public long getMaxIdle() {
            return -1;
        }
    }
}
