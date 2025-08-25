package com.hazelcast.topic.impl.reliable;

import com.hazelcast.config.ReliableTopicConfig;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * Scheduler for throttling concurrent publish operations for reliable topics.
 * <p>
 * The manager limits the number of in-flight publish tasks based on the
 * {@link ReliableTopicConfig} and exposes instrumentation methods required for
 * testing.
 */
public class ReliableTopicConcurrencyManager {

    private final ReliableTopicConfig config;
    private final Queue<Task> queue = new ArrayDeque<>();
    private int inFlight;

    public ReliableTopicConcurrencyManager(ReliableTopicConfig config) {
        this.config = Objects.requireNonNull(config);
    }

    /**
     * Returns the effective concurrency limit.
     */
    public int currentLimit() {
        int limit = config.getMaxConcurrentPublishes();
        if (limit < 1) {
            return 1;
        }
        return Math.min(limit, 8);
    }

    /**
     * Schedules the supplied task subject to the configured concurrency limit.
     * The task is invoked when capacity permits and the returned stage
     * completes when the task has finished.
     */
    CompletionStage<?> schedule(Supplier<? extends CompletionStage<?>> task) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        synchronized (this) {
            queue.add(new Task(task, future));
            drain();
        }
        return future;
    }

    /**
     * Returns the number of publish operations currently in-flight.
     */
    int getInFlightCount() {
        synchronized (this) {
            return inFlight;
        }
    }

    /**
     * Waits until all queued and in-flight tasks complete or the timeout
     * expires.
     */
    void awaitQuiescence(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (this) {
            while ((inFlight > 0 || !queue.isEmpty()) && System.nanoTime() < deadline) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                this.wait(remaining / 1_000_000, (int) (remaining % 1_000_000));
            }
        }
    }

    private void drain() {
        int limit = currentLimit();
        while (inFlight < limit && !queue.isEmpty()) {
            Task t = queue.poll();
            inFlight++;
            CompletionStage<?> stage;
            try {
                stage = t.task.get();
            } catch (Throwable e) {
                inFlight--;
                t.future.completeExceptionally(e);
                continue;
            }
            stage.whenComplete((r, e) -> {
                if (e != null) {
                    t.future.completeExceptionally(e);
                } else {
                    t.future.complete(r);
                }
                synchronized (ReliableTopicConcurrencyManager.this) {
                    inFlight--;
                    ReliableTopicConcurrencyManager.this.notifyAll();
                    drain();
                }
            });
        }
    }

    private static final class Task {
        final Supplier<? extends CompletionStage<?>> task;
        final CompletableFuture<Object> future;

        Task(Supplier<? extends CompletionStage<?>> task, CompletableFuture<Object> future) {
            this.task = task;
            this.future = future;
        }
    }
}
