/*
 * Copyright (c) 2008-2025, Hazelcast, Inc.
 * Licensed under the Apache License, Version 2.0
 */

 package com.hazelcast.topic.impl.reliable;

 import com.hazelcast.config.ReliableTopicConfig;
 
 import java.time.Duration;
 import java.util.Objects;
 import java.util.concurrent.*;
 import java.util.concurrent.atomic.AtomicBoolean;
 import java.util.concurrent.atomic.AtomicInteger;
 import java.util.concurrent.atomic.AtomicLong;
 import java.util.function.Supplier;
 
 /**
  * Concurrency manager for reliable topic publishing.
  *
  * Guarantees
  * ----------
  *  • At most currentLimit() tasks are "in flight" (started but not finished).
  *  • Tasks are STARTED in strict submission order (0,1,2,...) without over-dispatch.
  *  • With limit=1, tasks are processed strictly one-by-one (full sequential semantics).
  *  • schedule(...) returns immediately; the returned stage completes with the supplier’s outcome.
  *  • awaitQuiescence(timeout) waits (bounded) for both queue and in-flight to drain.
  *
  * Notes
  * -----
  *  • Actual ordering of ringbuffer effects is enforced in the proxy via a serial commit chain.
  *    The manager focuses on start-throttling and visibility.
  *  • All supplier bodies run on the configured executor (or the internal bounded pool if constructed
  *    with the (config)-only constructor in test harnesses).
  */
 public final class ReliableTopicConcurrencyManager {
 
     private static final int MAX_LIMIT = 8;
 
     private final Executor executor;
     private final int configuredLimit;
 
     /** Capacity bound: at most 'configuredLimit' started tasks. */
     private final Semaphore permits;
 
     /** In-flight counter for observability and quiescence. */
     private final AtomicInteger inFlight = new AtomicInteger();
 
     /** Sequence generators to enforce start order. */
     private final AtomicLong nextSeq = new AtomicLong(0L);
     private final AtomicLong nextToStart = new AtomicLong(0L);
 
     /** Pending tasks keyed by their sequence. */
     private final ConcurrentHashMap<Long, Task> pending = new ConcurrentHashMap<>();
 
     /** Dispatch guard. */
     private final AtomicBoolean dispatchScheduled = new AtomicBoolean();
 
     /** Epoch to invalidate previous state on reset (membership/merge). */
     private final AtomicLong epoch = new AtomicLong(1L);
 
     /** Idle monitor for awaitQuiescence(timeout). */
     private final Object idleMonitor = new Object();
 
     /**
      * Preferred constructor: use the executor selected for the topic plus the config (to read the limit).
      */
     public ReliableTopicConcurrencyManager(Executor executor, ReliableTopicConfig config) {
         this.executor = Objects.requireNonNull(executor, "executor");
         this.configuredLimit = clamp(config != null ? config.getMaxConcurrentPublishes() : 1);
         this.permits = new Semaphore(this.configuredLimit, false);
     }
 
     /**
      * Fallback/test constructor: builds a bounded daemon pool sized by the configured limit (1..8).
      */
     public ReliableTopicConcurrencyManager(ReliableTopicConfig config) {
         int limit = clamp(config != null ? config.getMaxConcurrentPublishes() : 1);
         int threads = limit; // bounded by clamp(..)
 
         final String id = Integer.toHexString(System.identityHashCode(this));
         final AtomicInteger idx = new AtomicInteger(1);
         ThreadFactory tf = r -> {
             Thread t = new Thread(r, "rt-concurrency-" + id + "-t" + idx.getAndIncrement());
             t.setDaemon(true);
             return t;
         };
         this.executor = new ThreadPoolExecutor(threads, threads, 60L, TimeUnit.SECONDS,
                 new LinkedBlockingQueue<>(), tf);
         this.configuredLimit = limit;
         this.permits = new Semaphore(this.configuredLimit, false);
     }
 
     private static int clamp(int v) {
         if (v < 1) {
             return 1;
         }
         return Math.min(v, MAX_LIMIT);
     }
 
     /** Public API: returns the effective concurrency limit. */
     public int currentLimit() {
         return configuredLimit;
     }
 
     /** Package-private: snapshot of in-flight tasks. */
     int getInFlightCount() {
         return inFlight.get();
     }
 
     /**
      * Package-private: bounded wait for the scheduler to go idle.
      * Returns as soon as there is no pending task and in-flight is 0 or when timeout elapses.
      */
     void awaitQuiescence(Duration timeout) {
         long deadline = timeout == null ? 0L : System.nanoTime() + timeout.toNanos();
         synchronized (idleMonitor) {
             for (;;) {
                 if (pending.isEmpty() && inFlight.get() == 0) {
                     return;
                 }
                 if (timeout != null) {
                     long remaining = deadline - System.nanoTime();
                     if (remaining <= 0) {
                         return;
                     }
                     long waitMs = Math.max(1L, Math.min(50L, remaining / 1_000_000L));
                     try {
                         idleMonitor.wait(waitMs);
                     } catch (InterruptedException ie) {
                         Thread.currentThread().interrupt();
                         return;
                     }
                 } else {
                     try {
                         idleMonitor.wait(25L);
                     } catch (InterruptedException ie) {
                         Thread.currentThread().interrupt();
                         return;
                     }
                 }
             }
         }
     }
 
     /**
      * Package-private: schedule a supplier.
      * The supplier will be INVOKED on the configured executor and started in strict submission order,
      * with at most currentLimit() tasks in-flight at once.
      */
     CompletionStage<?> schedule(Supplier<CompletionStage<?>> supplier) {
         Objects.requireNonNull(supplier, "supplier");
         final long seq = nextSeq.getAndIncrement();
         final long snapEpoch = epoch.get();
         final CompletableFuture<Void> user = new CompletableFuture<>();
         pending.put(seq, new Task(seq, snapEpoch, supplier, user));
         scheduleDispatch();
         return user;
     }
 
     /**
      * Resets the scheduler state on membership/merge change.
      * Clears pending, resets counters/permits and wakes any waiters.
      */
     void reset() {
         epoch.incrementAndGet();
         pending.clear();
         nextToStart.set(nextSeq.get());
         inFlight.set(0);
         permits.drainPermits();
         permits.release(configuredLimit);
         synchronized (idleMonitor) {
             idleMonitor.notifyAll();
         }
     }
 
     // ---------------- internal dispatch ----------------
 
     private void scheduleDispatch() {
         if (dispatchScheduled.compareAndSet(false, true)) {
             executor.execute(this::runDispatch);
         }
     }
 
     private void runDispatch() {
         try {
             for (;;) {
                 boolean progressed = false;
 
                 // Try to start as many tasks as allowed (strict sequence).
                 while (permits.tryAcquire()) {
                     final long want = nextToStart.get();
                     final Task task = pending.remove(want);
                     if (task == null) {
                         // Can't start 'want' yet; release and stop expanding.
                         permits.release();
                         break;
                     }
 
                    // We are starting 'want'
                    nextToStart.incrementAndGet();
                    inFlight.incrementAndGet();
                    progressed = true;

                    invokeTask(task);
                 }
 
                 if (!progressed) {
                     break;
                 }
             }
         } finally {
             dispatchScheduled.set(false);
             if (!pending.isEmpty() && permits.availablePermits() > 0) {
                 scheduleDispatch();
             }
             wakeIfIdle();
         }
     }
 
    private void invokeTask(Task task) {
        try {
            executor.execute(() -> runTask(task));
        } catch (RejectedExecutionException rex) {
            runTask(task);
        }
    }

    private void runTask(Task task) {
        try {
            CompletionStage<?> inner = task.supplier.get();
            if (inner == null) {
                task.user.complete(null);
                finishOne(task.epochSnapshot);
                scheduleDispatch();
                return;
            }
            inner.whenComplete((r, t) -> {
                if (t != null) {
                    if (t instanceof CompletionException) {
                        task.user.completeExceptionally(t);
                    } else {
                        task.user.completeExceptionally(new CompletionException(t));
                    }
                } else {
                    task.user.complete(null);
                }
                finishOne(task.epochSnapshot);
                scheduleDispatch();
            });
        } catch (Throwable th) {
            task.user.completeExceptionally(th);
            finishOne(task.epochSnapshot);
            scheduleDispatch();
        }
    }

     private void finishOne(long startEpoch) {
         if (startEpoch == epoch.get()) {
             inFlight.decrementAndGet();
             permits.release();
         }
         wakeIfIdle();
     }
 
     private void wakeIfIdle() {
         if (pending.isEmpty() && inFlight.get() == 0) {
             synchronized (idleMonitor) {
                 idleMonitor.notifyAll();
             }
         }
     }
 
     private static final class Task {
         final long seq;
         final long epochSnapshot;
         final Supplier<CompletionStage<?>> supplier;
         final CompletableFuture<Void> user;
 
         Task(long seq, long epochSnapshot, Supplier<CompletionStage<?>> supplier, CompletableFuture<Void> user) {
             this.seq = seq;
             this.epochSnapshot = epochSnapshot;
             this.supplier = supplier;
             this.user = user;
         }
     }
 }
 