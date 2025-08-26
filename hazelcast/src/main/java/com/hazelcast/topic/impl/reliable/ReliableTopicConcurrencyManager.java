package com.hazelcast.topic.impl.reliable;

import com.hazelcast.config.ReliableTopicConfig;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Manages concurrency for reliable topic publish operations.
 *
 * <p>The manager tracks the effective concurrency limit and exposes
 * a semaphore to throttle concurrent publish operations. It also
 * maintains ordering by chaining ringbuffer add operations.</p>
 */
public class ReliableTopicConcurrencyManager {

    private final ReliableTopicConfig config;
    private final Semaphore semaphore;
    private final AtomicInteger limit = new AtomicInteger();
    private final AtomicReference<CompletionStage<?>> lastAddStage
            = new AtomicReference<>(CompletableFuture.completedFuture(null));

    public ReliableTopicConcurrencyManager(ReliableTopicConfig config) {
        this.config = config;
        int initialLimit = config.getMaxConcurrentPublishes();
        this.limit.set(initialLimit);
        this.semaphore = new Semaphore(initialLimit);
    }

    /**
     * Returns the current effective concurrency limit. If the underlying
     * configuration changed dynamically, the semaphore permits are adjusted
     * to reflect the new limit.
     */
    public int currentLimit() {
        int configured = config.getMaxConcurrentPublishes();
        int current = limit.get();
        if (configured != current) {
            synchronized (semaphore) {
                int diff = configured - limit.get();
                if (diff > 0) {
                    semaphore.release(diff);
                } else if (diff < 0) {
                    semaphore.reducePermits(-diff);
                }
                limit.set(configured);
            }
            current = configured;
        }
        return current;
    }

    void acquirePermit() throws InterruptedException {
        semaphore.acquire();
    }

    void releasePermit() {
        semaphore.release();
    }

    int getInFlightCount() {
        return currentLimit() - semaphore.availablePermits();
    }

    boolean awaitQuiescence(Duration timeout) throws InterruptedException {
        int limit = currentLimit();
        boolean acquired = semaphore.tryAcquire(limit, timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (acquired) {
            semaphore.release(limit);
        }
        return acquired;
    }

    <T> CompletionStage<T> ordered(Supplier<CompletionStage<T>> supplier) {
        synchronized (lastAddStage) {
            CompletionStage<T> chained = lastAddStage.get().thenCompose(v -> supplier.get());
            lastAddStage.set(chained);
            return chained;
        }
    }
}

