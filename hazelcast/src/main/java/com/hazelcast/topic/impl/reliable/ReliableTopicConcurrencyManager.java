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

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Coordinates concurrent publication of messages to a reliable topic by throttling
 * submissions of asynchronous ringbuffer write operations. Maintains a strict submission
 * order for invocation of ringbuffer adds while allowing multiple asynchronous adds to
 * be in flight concurrently up to a configured limit. Invocations beyond the configured
 * concurrency window are queued until earlier operations complete.
 * <p>
 * The concurrency manager uses the same executor configured on the containing topic
 * to run its scheduling loop and invoke ringbuffer writes. All invocations of
 * {@link ReliableTopicConcurrencyManager#submit} return immediately with a Future
 * that will be completed once the underlying ringbuffer add sequence finishes.
 * For a limit of 1, invocations are strictly sequential: each add completes before
 * the next begins. For limits greater than 1, the manager will pipeline ringbuffer
 * adds sequentially to preserve ordering of invocations but will allow multiple
 * asynchronous adds to be outstanding.
 */
public final class ReliableTopicConcurrencyManager {

    /**
     * Default polling period (in milliseconds) to wait for outstanding publishes to drain.
     */
    private static final int QUIESCENCE_POLL_MS = 10;

    private final int concurrencyLimit;
    private final Executor executor;
    private final Queue<PublishTask> pending = new ConcurrentLinkedQueue<>();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicBoolean running = new AtomicBoolean();

    /**
     * Create a new concurrency manager bound to the supplied topic config and executor.
     * The current concurrency limit is captured from {@link ReliableTopicConfig#getMaxConcurrentPublishes()}.
     */
    public ReliableTopicConcurrencyManager(@Nonnull ReliableTopicConfig topicConfig,
                                           @Nonnull Executor executor) {
        this.concurrencyLimit = topicConfig.getMaxConcurrentPublishes();
        this.executor = executor;
    }

    /**
     * Returns the effective concurrency limit.
     */
    public int currentLimit() {
        return concurrencyLimit;
    }

    /**
     * Non-blocking submission of an asynchronous publish invocation. The supplier
     * will be invoked when it is this task's turn to issue its ringbuffer add, maintaining
     * submission order. The returned future will be completed once the ringbuffer add completes.
     */
    public <T> CompletableFuture<T> submit(Supplier<CompletableFuture<T>> invocation) {
        CompletableFuture<T> result = new CompletableFuture<>();
        @SuppressWarnings({"rawtypes", "unchecked"})
        PublishTask task = new PublishTask((Supplier) invocation, (CompletableFuture) result);
        pending.offer(task);
        drain();
        return result;
    }

    /**
     * Returns the current count of outstanding publish operations for this topic.
     */
    int getInFlightCount() {
        return inFlight.get();
    }

    /**
     * Blocks for up to {@code timeout} while waiting for this concurrency
     * manager to become idle (no pending or in-flight publishes). Returns
     * immediately if already idle.
     */
    void awaitQuiescence(Duration timeout) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            if (pending.isEmpty() && inFlight.get() == 0) {
                return;
            }
            // periodically poll until idle
            Thread.sleep(QUIESCENCE_POLL_MS);
        }
    }

    private void drain() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        executor.execute(this::run);
    }

    private void run() {
        try {
            while (inFlight.get() < concurrencyLimit) {
                PublishTask task = pending.poll();
                if (task == null) {
                    break;
                }
                CompletableFuture<?> cf;
                try {
                    cf = task.invocation.get();
                } catch (Throwable t) {
                    task.result.completeExceptionally(t);
                    continue;
                }
                inFlight.incrementAndGet();
                cf.whenComplete((res, ex) -> {
                    if (ex != null) {
                        task.result.completeExceptionally(ex);
                    } else {
                        @SuppressWarnings("unchecked")
                        CompletableFuture<Object> resFut = (CompletableFuture<Object>) task.result;
                        resFut.complete(res);
                    }
                    inFlight.decrementAndGet();
                    drain();
                });
            }
        } finally {
            running.set(false);
            if (!pending.isEmpty()) {
                drain();
            }
        }
    }

    private static final class PublishTask {
        final Supplier<CompletableFuture<?>> invocation;
        final CompletableFuture<?> result;

        PublishTask(Supplier<CompletableFuture<?>> invocation, CompletableFuture<?> result) {
            this.invocation = invocation;
            this.result = result;
        }
    }
}
