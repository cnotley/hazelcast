/*
 * Copyright (c) 2008-2025, Hazelcast
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
 import java.util.concurrent.atomic.AtomicInteger;
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
  *
  * Concurrency notes in this implementation:
  *  - All single-message publish work is executed via {@link ReliableTopicConcurrencyManager} on the
  *    topic's configured executor (or Hazelcast's shared ASYNC executor), but the synchronous
  *    {@link #publish(Object)} method blocks until the add completes to preserve existing semantics.
  *  - Under BLOCK policy, publish() blocks until the message is committed; under ERROR, publish()
  *    throws immediately when there is no capacity; DISCARD_* keep legacy behavior.
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
 
     // Concurrency manager (package-private accessor below)
    final ReliableTopicConcurrencyManager concurrencyManager;
 
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
 
         // Reset scheduling state on topology changes.
         nodeEngine.getClusterService().addMembershipListener(new MembershipListener() {
             @Override
             public void memberAdded(MembershipEvent event) {
                concurrencyManager.reset();
             }
 
             @Override
             public void memberRemoved(MembershipEvent event) {
                concurrencyManager.reset();
             }
         });
     }
 
     @Override
     public String getServiceName() {
         return ReliableTopicService.SERVICE_NAME;
     }
 
     @Override
     public String getName() {
         return name;
     }
 
     private void addMessageListener(ListenerConfig listenerConfig) {
         NodeEngine nodeEngine = getNodeEngine();
         MessageListener listener = loadListener(listenerConfig);
         if (listener == null) {
             return;
         }
         if (listener instanceof HazelcastInstanceAware hazelcastInstanceAware) {
             hazelcastInstanceAware.setHazelcastInstance(nodeEngine.getHazelcastInstance());
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
 
     private Executor initExecutor(NodeEngine nodeEngine, ReliableTopicConfig topicConfig) {
         Executor executor = topicConfig.getExecutor();
         if (executor == null) {
             executor = nodeEngine.getExecutionService().getExecutor(ASYNC_EXECUTOR);
         }
         return executor;
     }
 
     // Package-private accessor required by tests
     ReliableTopicConcurrencyManager concurrencyManager() {
         return concurrencyManager;
     }
 
    private CompletionStage<Void> invokeSinglePublish(ReliableTopicMessage message) {
        return switch (overloadPolicy) {
            case ERROR -> {
                if (isAtCapacity()) {
                    CompletableFuture<Void> f = new CompletableFuture<>();
                    f.completeExceptionally(new TopicOverloadException(
                            "Failed to publish message: " + message + " on topic:" + getName()));
                    yield f;
                }
                yield addAsyncOrFailSingle(message);
            }
            case DISCARD_OLDEST -> ringbuffer.addAsync(message, OverflowPolicy.OVERWRITE).thenApply(id -> null);
            case DISCARD_NEWEST -> {
                if (isAtCapacity()) {
                    yield CompletableFuture.completedFuture(null);
                }
                yield ringbuffer.addAsync(message, OverflowPolicy.FAIL).thenApply(id -> null);
            }
            case BLOCK -> addAsyncWithBackoffSingle(message, INITIAL_BACKOFF_MS);
            default -> {
                CompletableFuture<Void> f = new CompletableFuture<>();
                f.completeExceptionally(new IllegalArgumentException("Unknown overloadPolicy:" + overloadPolicy));
                yield f;
            }
        };
    }

    private boolean isAtCapacity() {
        return ringbuffer.size() >= ringbuffer.capacity();
    }

     // -------------------- Publish (single) --------------------
 
     @Override
     public void publish(@Nonnull E payload) {
         checkNotNull(payload, NULL_MESSAGE_IS_NOT_ALLOWED);
 
         final Data data = nodeEngine.toData(payload);
         final ReliableTopicMessage message = new ReliableTopicMessage(data, thisAddress);
 
        try {
            CompletionStage<Void> stage;
            if (overloadPolicy == TopicOverloadPolicy.BLOCK) {
                stage = concurrencyManager.schedule(() -> {
                    try {
                        return invokeSinglePublish(message);
                    } catch (Throwable t) {
                        CompletableFuture<Void> f = new CompletableFuture<>();
                        f.completeExceptionally(t);
                        return f;
                    }
                }).thenApply(x -> null);
            } else {
                stage = invokeSinglePublish(message);
            }
            stage.toCompletableFuture().get();
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

        if (overloadPolicy == TopicOverloadPolicy.BLOCK) {
            final InternalCompletableFuture<Void> ret = new InternalCompletableFuture<>();
            concurrencyManager.schedule(() -> {
                try {
                    return invokeSinglePublish(message);
                } catch (Throwable t) {
                    CompletableFuture<Void> f = new CompletableFuture<>();
                    f.completeExceptionally(t);
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

        return invokeSinglePublish(message);
     }
 
     /** ERROR policy for single publish: fail immediately when full. */
     private CompletionStage<Void> addAsyncOrFailSingle(ReliableTopicMessage message) {
         CompletableFuture<Void> f = new CompletableFuture<>();
        ringbuffer.addAsync(message, OverflowPolicy.FAIL).whenCompleteAsync((id, t) -> {
            if (t != null) {
                f.completeExceptionally(t);
            } else if (id == -1) {
                f.completeExceptionally(new TopicOverloadException(
                        "Failed to publish message: " + message + " on topic:" + getName()));
            } else {
                f.complete(null);
            }
        }, CALLER_RUNS);
         return f;
     }
 
     /**
      * BLOCK policy for single message: use addAsync(FAIL) and retry with backoff
      * until it succeeds. All add() invocations happen on the topic's executor.
      */
     private CompletionStage<Void> addAsyncWithBackoffSingle(ReliableTopicMessage message, long pauseMillis) {
         CompletableFuture<Void> overall = new CompletableFuture<>();
         attemptAddSingle(message, pauseMillis, overall);
         return overall;
     }
 
     private void attemptAddSingle(ReliableTopicMessage message, long pauseMillis, CompletableFuture<Void> overall) {
         ringbuffer.addAsync(message, OverflowPolicy.FAIL).whenCompleteAsync((id, t) -> {
             if (t != null) {
                 overall.completeExceptionally(t);
                 return;
             }
             if (id == -1) {
                 long next = Math.min(pauseMillis * 2, MAX_BACKOFF);
                 // Delay then retry on the topic executor to preserve executor isolation
                 CompletableFuture.delayedExecutor(pauseMillis, MILLISECONDS, executor).execute(
                         () -> attemptAddSingle(message, next, overall));
             } else {
                 overall.complete(null);
             }
         }, CALLER_RUNS);
     }
 
     // -------------------- Publish (batch) --------------------
 
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
                     ringbuffer.addAllAsync(messages, OverflowPolicy.OVERWRITE).toCompletableFuture().get();
                     break;
                 case DISCARD_NEWEST:
                     ringbuffer.addAllAsync(messages, OverflowPolicy.FAIL).toCompletableFuture().get();
                     break;
                 case BLOCK:
                     // Ensure the actual add runs on the topic executor (isolation).
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
 
     private void addAsyncOrFail(@Nonnull Collection<? extends E> payload, InternalCompletableFuture<Void> returnFuture,
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
 
     private InternalCompletableFuture<Void> addAsync(List<ReliableTopicMessage> messages, OverflowPolicy overflowPolicy) {
         InternalCompletableFuture<Void> returnFuture = new InternalCompletableFuture<>();
         ringbuffer.addAllAsync(messages, overflowPolicy).whenCompleteAsync((id, t) -> {
             if (t != null) {
                 returnFuture.completeExceptionally(t);
             } else {
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
                         () -> executor.execute(() ->
                                 addAsyncAndBlock(payload, returnFuture, messages, Math.min(pauseMillis * 2, MAX_BACKOFF))),
                         pauseMillis, MILLISECONDS);
             } else {
                 returnFuture.complete(null);
             }
         }, CALLER_RUNS);
     }
 
     /**
      * Synchronous wrapper to ensure the batch add under BLOCK happens on the topic executor.
      */
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
 
     // -------------------- Listeners & lifecycle --------------------
 
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
 }
 