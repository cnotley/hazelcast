package com.hazelcast.topic.impl.reliable;

import com.hazelcast.config.ReliableTopicConfig;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Manages throttled dispatch of publish operations for reliable topics.
 */
public class ReliableTopicConcurrencyManager {

    /** Package-private probe interface used in tests. */
    interface ConcurrencyProbe {
        void onDispatch();
        void onComplete();
    }

    private final ReliableTopicConfig config;
    private final ConcurrencyProbe probe;
    private final ConcurrentLinkedQueue<Task<?>> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final Object signal = new Object();

    public ReliableTopicConcurrencyManager(ReliableTopicConfig config) {
        this(config, null);
    }

    ReliableTopicConcurrencyManager(ReliableTopicConfig config, ConcurrencyProbe probe) {
        this.config = Objects.requireNonNull(config);
        this.probe = probe;
    }

    /**
     * Returns current effective concurrency limit.
     */
    public int currentLimit() {
        int limit = config.getMaxConcurrentPublishes();
        if (limit < 1) {
            return 1;
        }
        return Math.min(limit, 8);
    }

    /**
     * Schedules the supplied operation respecting concurrency limit.
     */
    <T> CompletionStage<T> schedule(TaskSupplier<T> supplier) {
        Task<T> task = new Task<>(supplier);
        queue.offer(task);
        tryStart();
        return task.future;
    }

    private void tryStart() {
        int limit = currentLimit();
        while (inFlight.get() < limit) {
            Task<?> task = queue.poll();
            if (task == null) {
                return;
            }
            inFlight.incrementAndGet();
            if (probe != null) {
                probe.onDispatch();
            }
            CompletionStage<?> stage;
            try {
                stage = task.supplier.get();
            } catch (Throwable t) {
                task.future.completeExceptionally(t);
                completeOne();
                continue;
            }
            stage.whenComplete((r, t) -> {
                if (probe != null) {
                    probe.onComplete();
                }
                if (t != null) {
                    task.future.completeExceptionally(t);
                } else {
                    task.future.complete((T) r);
                }
                completeOne();
            });
        }
    }

    private void completeOne() {
        inFlight.decrementAndGet();
        synchronized (signal) {
            signal.notifyAll();
        }
        tryStart();
    }

    int getInFlightCount() {
        return inFlight.get();
    }

    void reset() {
        queue.clear();
        inFlight.set(0);
    }

    void awaitQuiescence(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (signal) {
            while (inFlight.get() > 0 || !queue.isEmpty()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                try {
                    signal.wait(Math.min(NANOSECONDS.toMillis(remaining), 100));
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /** Supplier of completion stages. */
    interface TaskSupplier<T> {
        CompletionStage<T> get() throws Exception;
    }

    private static final class Task<T> {
        final TaskSupplier<T> supplier;
        final CompletableFuture<T> future = new CompletableFuture<>();

        Task(TaskSupplier<T> supplier) {
            this.supplier = supplier;
        }
    }
}
