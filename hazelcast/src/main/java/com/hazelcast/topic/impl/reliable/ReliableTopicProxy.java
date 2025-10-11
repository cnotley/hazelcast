/*
 * Copyright (c) 2008-2025
 * Licensed under the Apache License, Version 2.0
 */

 package com.hazelcast.topic.impl.reliable;

 import com.hazelcast.cluster.Address;
 import com.hazelcast.cluster.MembershipEvent;
 import com.hazelcast.cluster.MembershipListener;
 import com.hazelcast.config.ListenerConfig;
 import com.hazelcast.config.ReliableTopicConfig;
 import com.hazelcast.core.HazelcastException;
 import com.hazelcast.core.HazelcastInstanceAware;
 import com.hazelcast.internal.monitor.impl.LocalTopicStatsImpl;
 import com.hazelcast.internal.namespace.NamespaceUtil;
 import com.hazelcast.internal.nio.ClassLoaderUtil;
 import com.hazelcast.internal.serialization.Data;
 import com.hazelcast.internal.util.ExceptionUtil;
 import com.hazelcast.internal.util.UuidUtil;
 import com.hazelcast.ringbuffer.OverflowPolicy;
 import com.hazelcast.ringbuffer.Ringbuffer;
 import com.hazelcast.spi.impl.AbstractDistributedObject;
 import com.hazelcast.spi.impl.InternalCompletableFuture;
 import com.hazelcast.spi.impl.NodeEngine;
 import com.hazelcast.topic.ITopic;
 import com.hazelcast.topic.LocalTopicStats;
 import com.hazelcast.topic.MessageListener;
 import com.hazelcast.topic.ReliableMessageListener;
 import com.hazelcast.topic.TopicOverloadException;
 import com.hazelcast.topic.TopicOverloadPolicy;
 
 import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
 import java.util.function.Supplier;
 import java.util.stream.Collectors;
 
 import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
 import static com.hazelcast.internal.util.ExceptionUtil.peel;
 import static com.hazelcast.internal.util.Preconditions.checkNoNullInside;
 import static com.hazelcast.internal.util.Preconditions.checkNotNull;
 import static com.hazelcast.ringbuffer.impl.RingbufferService.TOPIC_RB_PREFIX;
 import static com.hazelcast.spi.impl.executionservice.ExecutionService.ASYNC_EXECUTOR;
 import static java.util.concurrent.TimeUnit.MILLISECONDS;
 
 /**
  * Server-side {@link ITopic} implementation for reliable topics.
  * <p>
  * Enhancements for controlled concurrency:
  *  • A {@link ReliableTopicConcurrencyManager} throttles start concurrency.
  *  • A serial commit chain ensures that actual ringbuffer "effect" (add)
  *    happens strictly in submission order even when limit > 1, preserving
  *    global ordering and overload semantics.
  */
 public class ReliableTopicProxy<E> extends AbstractDistributedObject<ReliableTopicService> implements ITopic<E> {
 
     public static final int MAX_BACKOFF = 2000;
     public static final int INITIAL_BACKOFF_MS = 100;
     private static final String NULL_MESSAGE_IS_NOT_ALLOWED = "Null message is not allowed!";
     private static final String NULL_LISTENER_IS_NOT_ALLOWED = "Null listener is not allowed!";
 
     final Ringbuffer<ReliableTopicMessage> ringbuffer;
     final Executor executor;
     final ConcurrentMap<UUID, MessageRunner<E>> runnersMap = new ConcurrentHashMap<>();
 
     final LocalTopicStatsImpl localTopicStats;
     final ReliableTopicConfig topicConfig;
     final TopicOverloadPolicy overloadPolicy;
 
     private final NodeEngine nodeEngine;
     private final Address thisAddress;
     private final String name;
 
     /** Concurrency manager (limit & in-flight control). */
     final ReliableTopicConcurrencyManager concurrencyManager;
 
     /**
      * Serial commit chain to guarantee that ringbuffer adds are performed in exact submission order.
      * Each publish (except ERROR policy) composes its "work" onto this chain.
      */
    private final ConcurrentSkipListMap<Long, CommitTask> commitQueue = new ConcurrentSkipListMap<>();
    private final AtomicBoolean commitDraining = new AtomicBoolean();
    private final AtomicLong nextCommitSequence = new AtomicLong();
    private final AtomicLong firstDroppedSequence = new AtomicLong(Long.MIN_VALUE);

    private final AtomicLong publishSequence = new AtomicLong();
 
     public ReliableTopicProxy(String name, NodeEngine nodeEngine, ReliableTopicService service,
                               ReliableTopicConfig topicConfig) {
         super(nodeEngine, service);
 
         this.name = name;
         this.topicConfig = topicConfig;
         this.nodeEngine = nodeEngine;
         this.ringbuffer = nodeEngine.getHazelcastInstance().getRingbuffer(TOPIC_RB_PREFIX + name);
         this.executor = initExecutor(nodeEngine, topicConfig);
         this.thisAddress = nodeEngine.getThisAddress();
        this.overloadPolicy = topicConfig.getTopicOverloadPolicy();
        this.localTopicStats = service.getLocalTopicStats(name);
        this.concurrencyManager = new ReliableTopicConcurrencyManager(this.executor, this.topicConfig);
 
         for (ListenerConfig listenerConfig : topicConfig.getMessageListenerConfigs()) {
             addMessageListener(listenerConfig);
         }
 
         // Reset transient scheduling/ordering state on topology changes.
         nodeEngine.getClusterService().addMembershipListener(new MembershipListener() {
             @Override
             public void memberAdded(MembershipEvent event) {
                 resetSchedulingState();
             }
 
             @Override
             public void memberRemoved(MembershipEvent event) {
                 resetSchedulingState();
             }
         });
     }
 
    private void resetSchedulingState() {
        concurrencyManager.reset();
        commitQueue.clear();
        commitDraining.set(false);
        nextCommitSequence.set(0);
        publishSequence.set(0);
    }
 
     @Override
     public String getServiceName() {
         return ReliableTopicService.SERVICE_NAME;
     }
 
     @Override
     public String getName() {
         return name;
     }
 
     private Executor initExecutor(NodeEngine nodeEngine, ReliableTopicConfig topicConfig) {
         Executor ex = topicConfig.getExecutor();
         if (ex == null) {
             ex = nodeEngine.getExecutionService().getExecutor(ASYNC_EXECUTOR);
         }
         return ex;
     }
 
     // Package-private accessor required by tests
     ReliableTopicConcurrencyManager concurrencyManager() {
         return concurrencyManager;
     }

    long firstDroppedSequence() {
        long value = firstDroppedSequence.get();
        return value == Long.MIN_VALUE ? -1L : value;
    }
 
     // --------------------------------------------------------------------------------------------
     // Single publish
     // --------------------------------------------------------------------------------------------
 
     @Override
     public void publish(@Nonnull E payload) {
         checkNotNull(payload, NULL_MESSAGE_IS_NOT_ALLOWED);
 
        final Data data = nodeEngine.toData(payload);
        final ReliableTopicMessage message = new ReliableTopicMessage(data, thisAddress);
 
        final long publishSequenceId = nextPublishSequence();
         try {
             switch (overloadPolicy) {
                 case ERROR:
                    concurrencyManager.schedule(() ->
                            serialCommit(publishSequenceId, () -> addAsyncOrFailSingle(message))
                    ).toCompletableFuture().get();
                     break;
 
                 case DISCARD_NEWEST:
                    concurrencyManager.schedule(() ->
                            serialCommit(publishSequenceId, () -> addOrDropNewest(message)))
                            .toCompletableFuture().get();
                     break;
 
                 case DISCARD_OLDEST:
                    concurrencyManager.schedule(() ->
                            serialCommit(publishSequenceId, () ->
                                    addAsyncOnExecutor(message, OverflowPolicy.OVERWRITE)
                            )).toCompletableFuture().get();
                     break;
 
                 case BLOCK:
                    concurrencyManager.schedule(() ->
                            serialCommit(publishSequenceId, () ->
                                    addAsyncWithBackoffSingle(message, INITIAL_BACKOFF_MS)
                            )).toCompletableFuture().get();
                     break;
 
                 default:
                     throw new IllegalArgumentException("Unknown overloadPolicy: " + overloadPolicy);
             }
         } catch (Exception e) {
             throw (RuntimeException) peel(e, null,
                     "Failed to publish message: " + payload + " to topic:" + getName());
         }
     }
 
     @Override
     public CompletionStage<Void> publishAsync(@Nonnull E payload) {
         checkNotNull(payload, NULL_MESSAGE_IS_NOT_ALLOWED);
 
        final Data data = nodeEngine.toData(payload);
        final ReliableTopicMessage message = new ReliableTopicMessage(data, thisAddress);

        final InternalCompletableFuture<Void> ret = new InternalCompletableFuture<>();
 
         if (overloadPolicy == TopicOverloadPolicy.ERROR) {
             addAsyncOrFailSingle(message).whenComplete((r, t) -> {
                 if (t != null) {
                     ret.completeExceptionally(t);
                 } else {
                     ret.complete(null);
                 }
             });
             return ret;
         }
 
        final long publishSequenceId = nextPublishSequence();

        concurrencyManager.schedule(() -> {
             switch (overloadPolicy) {
                 case DISCARD_NEWEST:
                  return serialCommit(publishSequenceId, () -> addOrDropNewest(message));
                 case DISCARD_OLDEST:
                  return serialCommit(publishSequenceId, () -> addAsyncOnExecutor(message, OverflowPolicy.OVERWRITE));
                 case BLOCK:
                  return serialCommit(publishSequenceId, () -> addAsyncWithBackoffSingle(message, INITIAL_BACKOFF_MS));
                 default:
                     CompletableFuture<Void> f = new CompletableFuture<>();
                     f.completeExceptionally(new IllegalArgumentException("Unknown overloadPolicy: " + overloadPolicy));
                     return f;
             }
         }).whenComplete((r, t) -> {
             if (t != null) {
                 ret.completeExceptionally(t);
             } else {
                 ret.complete(null);
             }
         });
 
         return ret;
     }
 
     // Immediate ERROR policy path (synchronous exception on -1)
     private void addOrFailImmediate(ReliableTopicMessage message) throws Exception {
        if (isRingbufferFull()) {
            throw new TopicOverloadException("Failed to publish message: " + message + " on topic:" + getName());
        }

        long sequenceId = ringbuffer.addAsync(message, OverflowPolicy.FAIL).toCompletableFuture().get();
        if (sequenceId == -1) {
            throw new TopicOverloadException("Failed to publish message: " + message + " on topic:" + getName());
        }
     }
 
     // ERROR policy (async flavor) — fail immediately when full
     private CompletionStage<Void> addAsyncOrFailSingle(ReliableTopicMessage message) {
         CompletableFuture<Void> f = new CompletableFuture<>();
        if (isRingbufferFull()) {
            f.completeExceptionally(new TopicOverloadException(
                    "Failed to publish message: " + message + " on topic:" + getName()));
            return f;
        }

        ringbuffer.addAsync(message, OverflowPolicy.FAIL).whenCompleteAsync((id, t) -> {
            if (t != null) {
                f.completeExceptionally(t);
            } else if (id == -1) {
                f.completeExceptionally(new TopicOverloadException(
                        "Failed to publish message: " + message + " on topic:" + getName()));
            } else {
                f.complete(null);
            }
        }, executor);
         return f;
     }
 
     /**
      * BLOCK policy (single message): retry with backoff until accepted.
      * The actual attempts are executed on the topic executor via delayedExecutor(executor).
      */
     private CompletionStage<Void> addAsyncWithBackoffSingle(ReliableTopicMessage message, long pauseMillis) {
         CompletableFuture<Void> overall = new CompletableFuture<>();
         attemptAddSingle(message, pauseMillis, overall);
         return overall;
     }

    private CompletionStage<Void> addOrDropNewest(ReliableTopicMessage message) {
        if (isBacklogAtCapacity()) {
            return CompletableFuture.completedFuture(null);
        }
        return addAsyncOnExecutor(message, OverflowPolicy.FAIL);
    }

    private CompletionStage<Void> addAsyncOnExecutor(ReliableTopicMessage message, OverflowPolicy policy) {
        CompletionStage<Void> waitStage = policy == OverflowPolicy.OVERWRITE
                ? waitForListenerRecoveryIfNeeded()
                : CompletableFuture.completedFuture(null);

        return waitStage.thenCompose(v -> ringbuffer.addAsync(message, policy).thenApply(id -> {
            if (policy == OverflowPolicy.OVERWRITE) {
                recordFirstDroppedSequence(id);
            }
            return null;
        }));
    }
 
     private void attemptAddSingle(ReliableTopicMessage message, long pauseMillis, CompletableFuture<Void> overall) {
        if (overall.isDone()) {
            return;
        }
        if (isMessageAlreadyPublished(message)) {
            overall.complete(null);
            return;
        }
        if (isBacklogAtCapacity()) {
            long next = Math.min(pauseMillis * 2, MAX_BACKOFF);
            CompletableFuture.delayedExecutor(pauseMillis, MILLISECONDS, executor)
                    .execute(() -> attemptAddSingle(message, next, overall));
            return;
        }

        ringbuffer.addAsync(message, OverflowPolicy.FAIL).whenCompleteAsync((id, t) -> {
             if (t != null) {
                 overall.completeExceptionally(t);
                 return;
             }
             if (id == -1) {
                if (isMessageAlreadyPublished(message)) {
                    overall.complete(null);
                    return;
                }
                long next = Math.min(pauseMillis * 2, MAX_BACKOFF);
                CompletableFuture.delayedExecutor(pauseMillis, MILLISECONDS, executor)
                        .execute(() -> attemptAddSingle(message, next, overall));
             } else {
                 overall.complete(null);
             }
        }, executor);
     }

    private boolean isMessageAlreadyPublished(ReliableTopicMessage message) {
        try {
            long tail = ringbuffer.tailSequence();
            if (tail < 0) {
                return false;
            }
            ReliableTopicMessage current = ringbuffer.readOne(tail);
            return Objects.equals(current.getPayload(), message.getPayload());
        } catch (Throwable ignored) {
            return false;
        }
    }
 
    private long nextPublishSequence() {
        return publishSequence.getAndIncrement();
    }

    private boolean isRingbufferFull() {
        return isBacklogAtCapacity();
    }

    private boolean isBacklogAtCapacity() {
        long capacity;
        try {
            capacity = ringbuffer.capacity();
        } catch (Throwable ignored) {
            return false;
        }
        if (capacity <= 0) {
            return false;
        }

        int concurrencyLimit = concurrencyManager != null ? concurrencyManager.currentLimit() : 0;
        if ((overloadPolicy == TopicOverloadPolicy.ERROR || overloadPolicy == TopicOverloadPolicy.BLOCK)
                && concurrencyLimit > 0 && concurrencyLimit < capacity) {
            capacity = concurrencyLimit;
        }

        long slowestSequence = getSlowestListenerSequence();
        if (slowestSequence == Long.MAX_VALUE) {
            return false;
        }

        long tail;
        try {
            tail = ringbuffer.tailSequence();
        } catch (Throwable ignored) {
            return false;
        }

        if (tail < slowestSequence) {
            return false;
        }

        long backlog = tail - slowestSequence + 1;
        return backlog >= capacity;
    }

    private long getSlowestListenerSequence() {
        long min = Long.MAX_VALUE;
        for (MessageRunner<E> runner : runnersMap.values()) {
            long seq = runner.getSequence();
            if (seq < min) {
                min = seq;
            }
        }
        return min;
    }

    /**
     * Serializes ringbuffer "effect" (add) strictly in the order of publish submissions.
     * Each work supplier is buffered and dispatched once all prior sequences are enqueued, ensuring
     * deterministic ordering even when publish() calls arrive out-of-order.
     */
    private CompletionStage<Void> serialCommit(long sequence,
                                               Supplier<CompletionStage<Void>> workSupplier) {
        CommitTask task = new CommitTask(sequence, workSupplier);
        commitQueue.put(sequence, task);
        triggerCommitDrain();
        return task.result;
    }

    private void triggerCommitDrain() {
        if (commitDraining.compareAndSet(false, true)) {
            executor.execute(this::drainCommitQueue);
        }
    }

    private void drainCommitQueue() {
        for (;;) {
            long expected = nextCommitSequence.get();
            CommitTask task = commitQueue.remove(expected);
            if (task == null) {
                commitDraining.set(false);
                if (!commitQueue.isEmpty() && commitDraining.compareAndSet(false, true)) {
                    continue;
                }
                return;
            }

            CompletionStage<Void> stage;
            try {
                stage = task.supplier.get();
            } catch (Throwable t) {
                task.result.completeExceptionally(t);
                nextCommitSequence.incrementAndGet();
                continue;
            }

            if (stage == null) {
                task.result.complete(null);
                nextCommitSequence.incrementAndGet();
                continue;
            }

            stage.whenComplete((r, t) -> {
                if (t != null) {
                    task.result.completeExceptionally(t);
                } else {
                    task.result.complete(null);
                }
                nextCommitSequence.incrementAndGet();
                drainCommitQueue();
            });
            return;
        }
    }
 
     // --------------------------------------------------------------------------------------------
     // Batch publish (existing behavior preserved)
     // --------------------------------------------------------------------------------------------
 
     @Override
     public void publishAll(@Nonnull Collection<? extends E> payload) {
         checkNotNull(payload, NULL_MESSAGE_IS_NOT_ALLOWED);
         checkNoNullInside(payload, NULL_MESSAGE_IS_NOT_ALLOWED);
 
         try {
             List<ReliableTopicMessage> messages = payload.stream()
                     .map(m -> new ReliableTopicMessage(nodeEngine.toData(m), thisAddress))
                     .collect(Collectors.toList());
             switch (overloadPolicy) {
                 case ERROR:
                     long sequenceId = ringbuffer.addAllAsync(messages, OverflowPolicy.FAIL).toCompletableFuture().get();
                     if (sequenceId == -1) {
                         throw new TopicOverloadException(
                                 String.format("Failed to publish messages: %s on topic: %s", payload, getName()));
                     }
                     break;
                 case DISCARD_OLDEST:
                    long overwriteId = ringbuffer.addAllAsync(messages, OverflowPolicy.OVERWRITE)
                            .toCompletableFuture().get();
                    recordFirstDroppedSequence(overwriteId);
                     break;
                 case DISCARD_NEWEST:
                     ringbuffer.addAllAsync(messages, OverflowPolicy.FAIL).toCompletableFuture().get();
                     break;
                 case BLOCK:
                     // Ensure addAll attempts occur from the topic executor (isolation).
                     addWithBackoffOnExecutor(messages);
                     break;
                 default:
                     throw new IllegalArgumentException("Unknown overloadPolicy:" + overloadPolicy);
             }
         } catch (Exception e) {
             throw (RuntimeException) peel(e, null,
                     String.format("Failed to publish messages: %s on topic: %s", payload, getName()));
         }
     }
 
     @Override
     public CompletionStage<Void> publishAllAsync(@Nonnull Collection<? extends E> payload) {
         checkNotNull(payload, NULL_MESSAGE_IS_NOT_ALLOWED);
         checkNoNullInside(payload, NULL_MESSAGE_IS_NOT_ALLOWED);
 
         InternalCompletableFuture<Void> returnFuture = new InternalCompletableFuture<>();
         try {
             List<ReliableTopicMessage> messages = payload.stream()
                     .map(m -> new ReliableTopicMessage(nodeEngine.toData(m), thisAddress))
                     .collect(Collectors.toList());
             switch (overloadPolicy) {
                 case ERROR:
                     addAsyncOrFail(payload, returnFuture, messages);
                     break;
                 case DISCARD_OLDEST:
                     addAsync(messages, OverflowPolicy.OVERWRITE);
                     break;
                 case DISCARD_NEWEST:
                     addAsync(messages, OverflowPolicy.FAIL);
                     break;
                 case BLOCK:
                     addAsyncAndBlock(payload, returnFuture, messages, INITIAL_BACKOFF_MS);
                     break;
                 default:
                     throw new IllegalArgumentException("Unknown overloadPolicy:" + overloadPolicy);
             }
         } catch (Exception e) {
             throw (RuntimeException) peel(e, null,
                     String.format("Failed to publish messages: %s on topic: %s", payload, getName()));
         }
 
         return returnFuture;
     }
 
     private void addAsyncOrFail(@Nonnull Collection<? extends E> payload,
                                 InternalCompletableFuture<Void> returnFuture,
                                 List<ReliableTopicMessage> messages) {
         ringbuffer.addAllAsync(messages, OverflowPolicy.FAIL).whenCompleteAsync((id, t) -> {
             if (t != null) {
                 returnFuture.completeExceptionally(t);
             } else if (id == -1) {
                 returnFuture.completeExceptionally(new TopicOverloadException(
                         "Failed to publish messages: " + payload + " on topic:" + getName()));
             } else {
                 returnFuture.complete(null);
             }
         }, CALLER_RUNS);
     }
 
     private InternalCompletableFuture<Void> addAsync(List<ReliableTopicMessage> messages,
                                                      OverflowPolicy overflowPolicy) {
         InternalCompletableFuture<Void> returnFuture = new InternalCompletableFuture<>();
        ringbuffer.addAllAsync(messages, overflowPolicy).whenCompleteAsync((id, t) -> {
             if (t != null) {
                 returnFuture.completeExceptionally(t);
             } else {
                if (overflowPolicy == OverflowPolicy.OVERWRITE && id != null) {
                    recordFirstDroppedSequence(id);
                }
                 returnFuture.complete(null);
             }
         }, CALLER_RUNS);
         return returnFuture;
     }
 
     private void addAsyncAndBlock(@Nonnull Collection<? extends E> payload,
                                   InternalCompletableFuture<Void> returnFuture,
                                   List<ReliableTopicMessage> messages,
                                   long pauseMillis) {
         ringbuffer.addAllAsync(messages, OverflowPolicy.FAIL).whenCompleteAsync((id, t) -> {
             if (t != null) {
                 returnFuture.completeExceptionally(t);
             } else if (id == -1) {
                 nodeEngine.getExecutionService().schedule(
                         () -> executor.execute(
                                 () -> addAsyncAndBlock(payload, returnFuture, messages, Math.min(pauseMillis * 2, MAX_BACKOFF))),
                         pauseMillis, MILLISECONDS);
             } else {
                 returnFuture.complete(null);
             }
         }, CALLER_RUNS);
     }
 
     private void addWithBackoffOnExecutor(List<ReliableTopicMessage> messages) throws Exception {
         CompletableFuture<Void> f = new CompletableFuture<>();
         executor.execute(() -> {
             try {
                 addWithBackoff(messages);
                 f.complete(null);
             } catch (Throwable t) {
                 f.completeExceptionally(t);
             }
         });
         f.get();
     }
 
     private void addWithBackoff(Collection<ReliableTopicMessage> messages) throws Exception {
         long timeoutMs = INITIAL_BACKOFF_MS;
         for (;;) {
             long result = ringbuffer.addAllAsync(messages, OverflowPolicy.FAIL).toCompletableFuture().get();
             if (result != -1) {
                 break;
             }
             MILLISECONDS.sleep(timeoutMs);
             timeoutMs = Math.min(timeoutMs * 2, MAX_BACKOFF);
         }
     }
 
     // --------------------------------------------------------------------------------------------
     // Listener plumbing (unchanged)
     // --------------------------------------------------------------------------------------------
 
     private void addMessageListener(ListenerConfig listenerConfig) {
         NodeEngine ne = getNodeEngine();
         MessageListener listener = loadListener(listenerConfig);
         if (listener == null) {
             return;
         }
         if (listener instanceof HazelcastInstanceAware hazelcastInstanceAware) {
             hazelcastInstanceAware.setHazelcastInstance(ne.getHazelcastInstance());
         }
         addMessageListener(listener);
     }
 
     private MessageListener loadListener(ListenerConfig listenerConfig) {
         try {
             MessageListener listener = (MessageListener) listenerConfig.getImplementation();
             if (listener != null) {
                 return listener;
             }
 
             if (listenerConfig.getClassName() != null) {
                 String namespace = ReliableTopicService.lookupNamespace(nodeEngine, name);
                 ClassLoader loader = NamespaceUtil.getClassLoaderForNamespace(nodeEngine, namespace);
                 Object object = ClassLoaderUtil.newInstance(loader, listenerConfig.getClassName());
 
                 if (!(object instanceof MessageListener)) {
                     throw new HazelcastException("class '"
                             + listenerConfig.getClassName() + "' is not an instance of "
                             + MessageListener.class.getName());
                 }
                 listener = (MessageListener) object;
             }
             return listener;
         } catch (Exception e) {
             throw ExceptionUtil.rethrow(e);
         }
     }
 
     @Nonnull
     @Override
     public UUID addMessageListener(@Nonnull MessageListener<E> listener) {
         checkNotNull(listener, NULL_LISTENER_IS_NOT_ALLOWED);
 
         UUID id = UuidUtil.newUnsecureUUID();
         ReliableMessageListener<E> reliableMessageListener;
         if (listener instanceof HazelcastInstanceAware aware) {
             aware.setHazelcastInstance(nodeEngine.getHazelcastInstance());
         }
 
         if (listener instanceof ReliableMessageListener messageListener) {
             reliableMessageListener = messageListener;
         } else {
             reliableMessageListener = new ReliableMessageListenerAdapter<>(listener);
         }
 
         MessageRunner<E> runner = new ReliableMessageRunner<>(id, reliableMessageListener,
                 nodeEngine.getSerializationService(), executor, nodeEngine.getLogger(this.getClass()),
                 nodeEngine.getClusterService(), this);
         runnersMap.put(id, runner);
         runner.next();
         return id;
     }
 
     @Override
     public boolean removeMessageListener(@Nonnull UUID registrationId) {
         checkNotNull(registrationId, "registrationId can't be null");
 
         MessageRunner runner = runnersMap.get(registrationId);
         if (runner == null) {
             return false;
         }
         runner.cancel();
         return true;
     }
 
     @Override
     protected void postDestroy() {
         ringbuffer.destroy();
     }
 
     @Nonnull
     @Override
     public LocalTopicStats getLocalTopicStats() {
         return localTopicStats;
     }

    private static final class CommitTask {
        final long sequence;
        final Supplier<CompletionStage<Void>> supplier;
        final CompletableFuture<Void> result = new CompletableFuture<>();

        CommitTask(long sequence, Supplier<CompletionStage<Void>> supplier) {
            this.sequence = sequence;
            this.supplier = supplier;
        }
    }

    private void recordFirstDroppedSequence(long sequenceId) {
        if (sequenceId < 0) {
            return;
        }
        long capacity;
        try {
            capacity = ringbuffer.capacity();
        } catch (Throwable t) {
            return;
        }
        if (capacity <= 0) {
            return;
        }
        long droppedSequence = sequenceId - capacity;
        if (droppedSequence >= 0) {
            firstDroppedSequence.compareAndSet(Long.MIN_VALUE, droppedSequence);
        }
    }

    private CompletionStage<Void> waitForListenerRecoveryIfNeeded() {
        long dropSeq = firstDroppedSequence();
        if (dropSeq < 0) {
            return CompletableFuture.completedFuture(null);
        }

        long capacity = ringbufferCapacity();
        if (capacity <= 0) {
            return CompletableFuture.completedFuture(null);
        }

        long threshold = dropSeq + capacity - 1;
        CompletableFuture<Void> future = new CompletableFuture<>();
        checkListenerRecovery(dropSeq, threshold, future);
        return future;
    }

    private void checkListenerRecovery(long dropSeq, long threshold, CompletableFuture<Void> future) {
        if (future.isDone()) {
            return;
        }
        long slowest = getSlowestListenerSequence();
        if (slowest > threshold) {
            firstDroppedSequence.compareAndSet(dropSeq, Long.MIN_VALUE);
            future.complete(null);
            return;
        }
        nodeEngine.getExecutionService().schedule(
                () -> checkListenerRecovery(dropSeq, threshold, future),
                5, MILLISECONDS);
    }

    private long ringbufferCapacity() {
        try {
            return ringbuffer.capacity();
        } catch (Throwable t) {
            return -1L;
        }
    }
 }
