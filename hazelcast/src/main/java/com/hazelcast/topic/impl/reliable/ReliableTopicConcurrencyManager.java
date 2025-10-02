/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.topic.impl.reliable;

import com.hazelcast.config.ReliableTopicConfig;

import java.time.Duration;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Coordinates throttled concurrent publishing for Reliable Topic proxies.
 * <p>
 * This manager is intentionally lightweight and does not create threads.
 * It merely exposes a concurrency limit sourced from {@link ReliableTopicConfig}
 * and provides ordering and backpressure primitives to the publish paths.
 */
public final class ReliableTopicConcurrencyManager {

    private final ReliableTopicConfig config;

    // state used to guard admission and ordering
    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition permitsAvailable = lock.newCondition();
    private final Condition turnCondition = lock.newCondition();

    private int inFlight;                 // number of in-flight publish operations
    private long nextTicket;              // next ticket to hand out
    private long nextTicketToCommit;      // ticket that is allowed to perform the ringbuffer add

    public ReliableTopicConcurrencyManager(ReliableTopicConfig config) {
        this.config = config;
    }

    /**
     * Returns the effective maximum number of concurrent publishes.
     * This value is sourced from {@link ReliableTopicConfig} to allow
     * observing supported dynamic updates.
     */
    public int currentLimit() {
        int configured = config.getMaxConcurrentPublishes();
        // defensive guard: clamp to supported range
        if (configured < 1) {
            configured = 1;
        } else if (configured > 8) {
            configured = 8;
        }
        return configured;
    }

    // -------- package-private helpers used by ReliableTopicProxy --------

    /** Returns a snapshot of the current number of in-flight publishes. */
    int getInFlightCount() {
        lock.lock();
        try {
            return inFlight;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Blocks up to the provided timeout until there are no in-flight publish
     * operations. Returns {@code true} if quiescent, {@code false} otherwise.
     */
    boolean awaitQuiescence(Duration timeout) {
        long nanos = timeout == null ? 0L : timeout.toNanos();
        lock.lock();
        try {
            while (inFlight > 0) {
                if (nanos <= 0L) {
                    return false;
                }
                try {
                    nanos = permitsAvailable.awaitNanos(nanos);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Issues a ticket and acquires an admission slot subject to the configured
     * concurrency limit. Callers must eventually invoke {@link #complete(Ticket)}.
     */
    Ticket begin() {
        lock.lock();
        try {
            long t = nextTicket++;
            int limit = currentLimit();
            while (inFlight >= limit) {
                try {
                    permitsAvailable.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting for publish admission", e);
                }
                limit = currentLimit();
            }
            inFlight++;
            return new Ticket(t);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits until it is the caller's turn to perform the ringbuffer add for
     * the supplied ticket. Ordering is strict: the ticket with number {@code N}
     * is allowed to proceed only after {@code N-1} has completed its add.
     */
    void awaitTurn(Ticket ticket) {
        lock.lock();
        try {
            while (nextTicketToCommit != ticket.number) {
                try {
                    turnCondition.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while awaiting publish order", e);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /** Marks the supplied ticket as complete and releases one admission slot. */
    void complete(Ticket ticket) {
        lock.lock();
        try {
            // advance strict ordering for the next ticket
            nextTicketToCommit++;
            // release admission slot
            inFlight--;
            // wake up waiters
            permitsAvailable.signalAll();
            turnCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Resets transient counters. Intended to be called on topology changes
     * and merges; it does not alter the configured limit.
     */
    void reset() {
        lock.lock();
        try {
            inFlight = 0;
            nextTicket = 0L;
            nextTicketToCommit = 0L;
            // best-effort wake-up for any waiters so they can re-evaluate state
            permitsAvailable.signalAll();
            turnCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Lightweight opaque handle representing a scheduled publish. */
    static final class Ticket {
        final long number;

        Ticket(long number) {
            this.number = number;
        }
    }
}
