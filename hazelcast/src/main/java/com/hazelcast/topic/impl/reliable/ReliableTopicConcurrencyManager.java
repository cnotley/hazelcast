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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static java.lang.Math.max;

/**
 * Coordinates bounded concurrent execution for reliable topic publishes.
 */
public final class ReliableTopicConcurrencyManager {

    private static final int MIN_CONCURRENCY = 1;
    private static final int MAX_CONCURRENCY = 8;

    private final ReliableTopicConfig topicConfig;
    private final Executor executor;

    private final ArrayDeque<PublishTask> queuedTasks = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition idleCondition = lock.newCondition();
    private final Condition startCondition = lock.newCondition();

    private volatile int concurrencyLimit;
    private long sequenceGenerator;
    private long nextSequenceToStart;
    private int inFlight;

    public ReliableTopicConcurrencyManager(@Nonnull ReliableTopicConfig topicConfig) {
        this(topicConfig, topicConfig != null ? topicConfig.getExecutor() : null);
    }

    public ReliableTopicConcurrencyManager(@Nonnull ReliableTopicConfig topicConfig, Executor executor) {
        this.topicConfig = Objects.requireNonNull(topicConfig, "topicConfig");
        this.executor = executor != null ? executor : defaultExecutor();
        this.concurrencyLimit = resolveLimit(topicConfig.getMaxConcurrentPublishes());
    }

    /**
     * Returns the currently effective concurrency limit.
     */
    public int currentLimit() {
        return concurrencyLimit;
    }

    /**
     * Returns a snapshot of the number of tasks that are currently in-flight.
     */
    int getInFlightCount() {
        lock.lock();
        try {
            return inFlight;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits until all queued work has completed or the timeout elapses.
     *
     * @return {@code true} if the manager became idle before timing out, {@code false} otherwise
     */
    boolean awaitQuiescence(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");

        long remainingNanos = max(0L, timeout.toNanos());

        lock.lock();
        try {
            while (!isIdle()) {
                if (remainingNanos <= 0) {
                    return false;
                }
                try {
                    remainingNanos = idleCondition.awaitNanos(remainingNanos);
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
     * Submit a new publish task.
     *
     * @param operation operation supplier that returns a {@link CompletionStage} completing when the publish finishes
     * @return stage completing with the outcome of the supplied publish
     */
    public CompletionStage<Void> submit(Supplier<CompletionStage<?>> operation) {
        Objects.requireNonNull(operation, "operation");

        CompletableFuture<Void> future = new CompletableFuture<>();
        PublishTask task = new PublishTask(nextSequence(), operation, future);

        List<PublishTask> toStart;
        lock.lock();
        try {
            queuedTasks.addLast(task);
            toStart = acquireCapacityAndPollLocked();
        } finally {
            lock.unlock();
        }

        startTasks(toStart);
        return future;
    }

    private List<PublishTask> acquireCapacityAndPollLocked() {
        if (queuedTasks.isEmpty()) {
            return List.of();
        }

        List<PublishTask> toStart = new ArrayList<>();
        while (!queuedTasks.isEmpty() && hasCapacity(toStart.size())) {
            toStart.add(queuedTasks.removeFirst());
        }

        if (!toStart.isEmpty()) {
            inFlight += toStart.size();
        }
        return toStart;
    }

    private boolean hasCapacity(int additional) {
        int limit = concurrencyLimit;
        return (inFlight + additional) < limit;
    }

    private void startTasks(List<PublishTask> tasks) {
        for (PublishTask task : tasks) {
            try {
                executor.execute(() -> executeTask(task));
            } catch (RejectedExecutionException rex) {
                task.future.completeExceptionally(rex);
                onTaskFinished();
            }
        }
    }

    private void executeTask(PublishTask task) {
        CompletionStage<?> stage;
        try {
            stage = invokeWhenTurn(task);
        } catch (Throwable t) {
            task.future.completeExceptionally(t);
            onTaskFinished();
            return;
        }

        if (stage == null) {
            task.future.complete(null);
            onTaskFinished();
            return;
        }

        stage.whenComplete((r, t) -> {
            if (t != null) {
                task.future.completeExceptionally(t);
            } else {
                task.future.complete(null);
            }
            onTaskFinished();
        });
    }

    private CompletionStage<?> invokeWhenTurn(PublishTask task) {
        lock.lock();
        try {
            while (task.sequence != nextSequenceToStart) {
                startCondition.awaitUninterruptibly();
            }
            nextSequenceToStart++;
        } finally {
            lock.unlock();
        }

        try {
            return task.operation.get();
        } finally {
            lock.lock();
            try {
                startCondition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    private void onTaskFinished() {
        List<PublishTask> toStart;
        lock.lock();
        try {
            inFlight--;
            toStart = acquireCapacityAndPollLocked();
            signalIfIdle();
        } finally {
            lock.unlock();
        }

        startTasks(toStart);
    }

    private void signalIfIdle() {
        if (isIdle()) {
            idleCondition.signalAll();
        }
    }

    private boolean isIdle() {
        return inFlight == 0 && queuedTasks.isEmpty();
    }

    private long nextSequence() {
        lock.lock();
        try {
            return sequenceGenerator++;
        } finally {
            lock.unlock();
        }
    }

    private static int resolveLimit(int configured) {
        if (configured < MIN_CONCURRENCY || configured > MAX_CONCURRENCY) {
            throw new IllegalArgumentException("maxConcurrentPublishes must be between "
                    + MIN_CONCURRENCY + " and " + MAX_CONCURRENCY);
        }
        return configured;
    }

    private static Executor defaultExecutor() {
        return ForkJoinPool.commonPool();
    }

    private static final class PublishTask {
        final long sequence;
        final Supplier<CompletionStage<?>> operation;
        final CompletableFuture<Void> future;

        PublishTask(long sequence, Supplier<CompletionStage<?>> operation, CompletableFuture<Void> future) {
            this.sequence = sequence;
            this.operation = operation;
            this.future = future;
        }
    }
}

