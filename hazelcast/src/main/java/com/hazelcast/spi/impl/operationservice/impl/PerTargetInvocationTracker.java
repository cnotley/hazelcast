package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastOverloadException;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.internal.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.internal.util.concurrent.IdleStrategy;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_INVOCATION_REGISTRY_PER_TARGET_PENDING;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_INVOCATION_REGISTRY_PER_TARGET_REJECTED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_PREFIX_INVOCATIONS;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.spi.properties.ClusterProperty.BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.ClusterProperty.BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_TARGET;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Tracks the number of pending invocations per remote target and enforces
 * a configurable upper bound on them.
 */
public final class PerTargetInvocationTracker implements DynamicMetricsProvider {

    private static final IdleStrategy IDLER =
            new BackoffIdleStrategy(0, 0, MILLISECONDS.toNanos(1), MILLISECONDS.toNanos(500));

    private final ConcurrentMap<Address, AtomicInteger> pending = new ConcurrentHashMap<>();
    private final SwCounter rejected = newSwCounter();
    private final int maxPerTarget;
    private final long backoffTimeoutNanos;

    public PerTargetInvocationTracker(HazelcastProperties properties) {
        this.maxPerTarget = properties.getInteger(BACKPRESSURE_MAX_CONCURRENT_INVOCATIONS_PER_TARGET);
        long backoffTimeoutMs = properties.getMillis(BACKPRESSURE_BACKOFF_TIMEOUT_MILLIS);
        this.backoffTimeoutNanos = MILLISECONDS.toNanos(backoffTimeoutMs);
    }

    /**
     * Tries to acquire a slot for the given target, blocking up to the
     * configured timeout if the per-target limit has been reached.
     *
     * @param target the invocation target
     * @throws HazelcastOverloadException if no slot becomes available before
     *                                    the timeout elapses
     */
    public void tryAcquire(Address target) {
        if (target == null) {
            return;
        }
        AtomicInteger counter = pending.computeIfAbsent(target, t -> new AtomicInteger());
        if (maxPerTarget == 0) {
            counter.incrementAndGet();
            return;
        }
        if (counter.incrementAndGet() <= maxPerTarget) {
            return;
        }
        // exceeded limit, revert increment and block
        counter.decrementAndGet();
        long startNanos = Timer.nanos();
        for (long idleCount = 0; ; idleCount++) {
            if (Timer.nanosElapsed(startNanos) > backoffTimeoutNanos) {
                rejected.inc();
                throw new HazelcastOverloadException("Timed out trying to acquire invocation slot for target " + target);
            }
            IDLER.idle(idleCount);
            if (counter.incrementAndGet() <= maxPerTarget) {
                return;
            }
            counter.decrementAndGet();
        }
    }

    /**
     * Releases a previously acquired slot for the given target.
     */
    public void release(Address target) {
        if (target == null) {
            return;
        }
        AtomicInteger counter = pending.get(target);
        if (counter == null) {
            return;
        }
        int value = counter.decrementAndGet();
        if (value <= 0) {
            pending.remove(target, counter);
        }
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        MetricDescriptor base = descriptor.copy().withPrefix(OPERATION_PREFIX_INVOCATIONS);
        for (Map.Entry<Address, AtomicInteger> entry : pending.entrySet()) {
            int value = entry.getValue().get();
            if (value <= 0) {
                continue;
            }
            context.collect(base.copy()
                    .withMetric(OPERATION_METRIC_INVOCATION_REGISTRY_PER_TARGET_PENDING)
                    .withDiscriminator("target", String.valueOf(entry.getKey())),
                    value);
        }
        context.collect(base.copy()
                .withMetric(OPERATION_METRIC_INVOCATION_REGISTRY_PER_TARGET_REJECTED),
                rejected.get());
    }
}
