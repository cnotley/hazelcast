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
  * Guarantees:
  *  - Suppliers are INVOKED strictly in submission order.
  *  - At most currentLimit() suppliers are in-flight (started but not yet completed).
  *  - With limit=1, the next supplier doesn't start until the previous completes.
  *  - schedule(...) never blocks; it returns a CompletionStage that completes with the supplier's outcome.
  *  - awaitQuiescence(timeout) waits (bounded) until no queued or in-flight tasks remain.
  *
  * The maximum concurrency is read once at construction from {@link ReliableTopicConfig}.
  * All work executes on the provided executor (or a bounded internal pool in standalone usage).
  */
 public final class ReliableTopicConcurrencyManager {
 
     private static final int MAX_LIMIT = 8;
     private static final int MIN_EXEC_THREADS = 1;
 
     private final Executor executor;
     private final int configuredLimit;
 
     /** Capacity bound: at most 'configuredLimit' tasks in-flight. */
     private final Semaphore permits;
 
     /** In-flight counter for observability and awaitQuiescence(). */
     private final AtomicInteger inFlight = new AtomicInteger();
 
     /** Submission order tracking. */
     private final AtomicLong nextSeq = new AtomicLong();
     private final AtomicLong nextToStart = new AtomicLong();
 
     /** Pending tasks keyed by sequence. */
     private final ConcurrentHashMap<Long, Task> pending = new ConcurrentHashMap<>();
 
     /** Epoch to invalidate old tasks on reset (membership/merge). */
     private final AtomicLong epoch = new AtomicLong(1L);
 
     /** Ensure a single dispatcher run at a time. */
     private final AtomicBoolean dispatchScheduled = new AtomicBoolean();
 
     /** Monitor for awaitQuiescence(). */
     private final Object idleMonitor = new Object();
 
     /**
      * Preferred constructor: proxy passes its effective executor + config.
      */
     public ReliableTopicConcurrencyManager(Executor executor, ReliableTopicConfig config) {
         this.executor = Objects.requireNonNull(executor, "executor");
         this.configuredLimit = clamp(config != null ? config.getMaxConcurrentPublishes() : 1);
         this.permits = new Semaphore(this.configuredLimit, false);
     }
 
     /**
      * Fallback constructor used by tests when no executor is configured:
      * Creates a bounded daemon pool sized by the limit (1..8) with distinct worker names.
      */
     public ReliableTopicConcurrencyManager(ReliableTopicConfig config) {
         int limit = clamp(config != null ? config.getMaxConcurrentPublishes() : 1);
         int threads = Math.max(MIN_EXEC_THREADS, Math.min(limit, MAX_LIMIT));
 
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
         if (v < 1) return 1;
         return Math.min(v, MAX_LIMIT);
     }
 
     /** Returns the effective concurrency limit. */
     public int currentLimit() {
         return configuredLimit;
     }
 
     /** Package-private snapshot of currently in-flight tasks. */
     int getInFlightCount() {
         return inFlight.get();
     }
 
     /**
      * Package-private: waits until scheduler is idle or until timeout elapses.
      * Returns immediately when idle; honors the timeout.
      */
     void awaitQuiescence(Duration timeout) {
         final long deadlineNanos = System.nanoTime() + (timeout == null ? 0L : timeout.toNanos());
         synchronized (idleMonitor) {
             for (;;) {
                 if (pending.isEmpty() && inFlight.get() == 0) {
                     return;
                 }
                 long remaining = deadlineNanos - System.nanoTime();
                 if (timeout != null && remaining <= 0L) {
                     return;
                 }
                 long waitMillis = timeout == null ? 25L : Math.min(50L, Math.max(1L, remaining / 1_000_000L));
                 try {
                     idleMonitor.wait(waitMillis);
                 } catch (InterruptedException ie) {
                     Thread.currentThread().interrupt();
                     return;
                 }
             }
         }
     }
 
     /**
      * Package-private: schedules a unit of work. The supplier is INVOKED in strict submission order.
      * The returned stage completes with the supplier's stage outcome.
      */
     CompletionStage<?> schedule(Supplier<CompletionStage<?>> supplier) {
         Objects.requireNonNull(supplier, "supplier");
         final long seq = nextSeq.getAndIncrement();
         final long snap = epoch.get();
         final CompletableFuture<Void> user = new CompletableFuture<>();
         pending.put(seq, new Task(seq, snap, supplier, user));
         scheduleDispatch();
         return user;
     }
 
     /**
      * Resets the scheduler on membership/merge: clears pending, resets counters and permits,
      * and bumps the epoch so old completions are ignored.
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
 
                 // Start as many as allowed: strict sequence + capacity.
                 for (;;) {
                     if (!permits.tryAcquire()) {
                         break;
                     }
                     final long want = nextToStart.get();
                     final Task task = pending.remove(want);
                     if (task == null) {
                         permits.release();
                         break;
                     }
 
                     nextToStart.incrementAndGet();
                     inFlight.incrementAndGet();
                     progressed = true;
 
                     final long startEpoch = task.epochSnapshot;
                     CompletionStage<?> inner;
                     try {
                         inner = task.supplier.get();
                     } catch (Throwable t) {
                         completeUser(task, t);
                         finishOne(startEpoch);
                         continue;
                     }
 
                     inner.whenComplete((r, t) -> {
                         if (t != null) {
                             completeUser(task, t);
                         } else {
                             task.user.complete(null);
                         }
                         if (startEpoch == epoch.get()) {
                             finishOne(startEpoch);
                         }
                         scheduleDispatch();
                     });
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
 
     private void finishOne(long startEpoch) {
         if (startEpoch == epoch.get()) {
             inFlight.decrementAndGet();
             permits.release();
         }
         wakeIfIdle();
     }
 
     private void completeUser(Task task, Throwable t) {
         task.user.completeExceptionally(t instanceof CompletionException ? t : new CompletionException(t));
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
 