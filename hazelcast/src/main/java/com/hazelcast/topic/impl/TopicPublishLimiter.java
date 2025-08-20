package com.hazelcast.topic.impl;

import java.util.concurrent.Semaphore;

/**
 * Limits the number of concurrently in-flight publish operations for a topic.
 * If the configured limit is {@code 0}, no limiting is applied.
 */
public class TopicPublishLimiter {

    private final Semaphore semaphore;
    private final int limit;

    public TopicPublishLimiter(int limit) {
        this.limit = Math.max(0, limit);
        this.semaphore = this.limit > 0 ? new Semaphore(this.limit) : null;
    }

    /**
     * Tries to acquire a permit for a publish operation.
     *
     * @return {@code true} if a permit was acquired or no limit is configured,
     *         {@code false} otherwise
     */
    public boolean tryAcquire() {
        return semaphore == null || semaphore.tryAcquire();
    }

    /**
     * Releases a previously acquired permit.
     */
    public void release() {
        if (semaphore != null) {
            semaphore.release();
        }
    }

    /**
     * Returns the number of currently in-flight publish operations.
     */
    public int getInFlight() {
        if (semaphore == null) {
            return 0;
        }
        return limit - semaphore.availablePermits();
    }

    public boolean isEnabled() {
        return semaphore != null;
    }

    /**
     * Releases all acquired permits and returns the number of permits released.
     */
    public int releaseAll() {
        if (semaphore == null) {
            return 0;
        }
        int inFlight = getInFlight();
        if (inFlight > 0) {
            semaphore.release(inFlight);
        }
        return inFlight;
    }
}
