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
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Coordinates concurrent publication of reliable topic messages according to the
 * limit configured on {@link ReliableTopicConfig}. The manager keeps track of
 * in-flight operations and ensures that at most the configured number of
 * publications are executing at once. The scheduling API is intentionally
 * minimal to keep the implementation deterministic and easily testable.
 */
public class ReliableTopicConcurrencyManager {

    private final ReliableTopicConfig config;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();
    private final Object monitor = new Object();

    public ReliableTopicConcurrencyManager(ReliableTopicConfig config) {
        this.config = config;
    }

    /**
     * Returns the currently effective concurrency limit. Values are clamped to
     * the supported range of {@code [1,8]} to guard against configuration
     * misuse.
     */
    public int currentLimit() {
        int limit = config.getMaxConcurrentPublishes();
        if (limit < 1) {
            return 1;
        }
        return Math.min(limit, 8);
    }

    int getInFlightCount() {
        return inFlight.get();
    }

    boolean awaitQuiescence(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (monitor) {
            while (inFlight.get() > 0 || !queue.isEmpty()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                try {
                    NANOSECONDS.timedWait(monitor, remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        }
    }

    CompletionStage<?> schedule(Callable<? extends CompletionStage<?>> callable) {
        return schedule((SupplierStage) () -> {
            try {
                return callable.call();
            } catch (Exception e) {
                CompletableFuture<?> f = new CompletableFuture<>();
                f.completeExceptionally(e);
                return f;
            }
        });
    }

    CompletionStage<?> schedule(java.util.function.Supplier<? extends CompletionStage<?>> supplier) {
        return schedule((SupplierStage) supplier::get);
    }

    CompletionStage<?> schedule(Runnable runnable) {
        return schedule((SupplierStage) () -> {
            try {
                runnable.run();
                return CompletableFuture.completedFuture(null);
            } catch (Throwable t) {
                CompletableFuture<Object> f = new CompletableFuture<>();
                f.completeExceptionally(t);
                return f;
            }
        });
    }

    private CompletionStage<?> schedule(SupplierStage supplier) {
        CompletableFuture<Object> result = new CompletableFuture<>();
        queue.offer(() -> executeTask(supplier, result));
        drain();
        return result;
    }

    private void executeTask(SupplierStage supplier, CompletableFuture<Object> result) {
        CompletionStage<?> stage;
        try {
            stage = supplier.get();
        } catch (Throwable t) {
            result.completeExceptionally(t);
            onTaskDone();
            return;
        }

        if (stage == null) {
            result.complete(null);
            onTaskDone();
            return;
        }

        stage.whenComplete((r, t) -> {
            if (t != null) {
                result.completeExceptionally(t);
            } else {
                result.complete(r);
            }
            onTaskDone();
        });
    }

    private void drain() {
        while (inFlight.get() < currentLimit()) {
            Runnable r = queue.poll();
            if (r == null) {
                return;
            }
            if (inFlight.incrementAndGet() > currentLimit()) {
                inFlight.decrementAndGet();
                queue.offer(r);
                return;
            }
            try {
                r.run();
            } catch (Throwable t) {
                // any exception is already delivered to the future inside executeTask
            }
        }
    }

    private void onTaskDone() {
        inFlight.decrementAndGet();
        synchronized (monitor) {
            monitor.notifyAll();
        }
        drain();
    }

    @FunctionalInterface
    private interface SupplierStage {
        CompletionStage<?> get() throws Exception;
    }
}

