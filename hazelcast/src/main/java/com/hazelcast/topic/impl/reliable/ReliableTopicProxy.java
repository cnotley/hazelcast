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

import com.hazelcast.cluster.Address;
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
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.ExceptionUtil.peel;
import static com.hazelcast.internal.util.Preconditions.checkNoNullInside;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.ringbuffer.impl.RingbufferService.TOPIC_RB_PREFIX;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.ASYNC_EXECUTOR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * The serverside {@link ITopic} implementation for reliable topics.
 *
 * @param <E> type of item contained in the topic
 */
public class ReliableTopicProxy<E> extends AbstractDistributedObject<ReliableTopicService> implements ITopic<E> {

    public static final int MAX_BACKOFF = 2000;
    public static final int INITIAL_BACKOFF_MS = 100;
    private static final String NULL_MESSAGE_IS_NOT_ALLOWED = "Null message is not allowed!";
    private static final String NULL_LISTENER_IS_NOT_ALLOWED = "Null listener is not allowed!";

    final Ringbuffer<ReliableTopicMessage> ringbuffer;
    final Executor executor;
    final ConcurrentMap<UUID, MessageRunner<E>> runnersMap
            = new ConcurrentHashMap<>();

    /**
     * Local statistics for this reliable topic, including
     * messages received on and published through this topic.
     */
    final LocalTopicStatsImpl localTopicStats;
    final ReliableTopicConfig topicConfig;
    final TopicOverloadPolicy overloadPolicy;
    final ReliableTopicConcurrencyManager concurrencyManager;
    private final PublishSequencer publishSequencer;

    private final NodeEngine nodeEngine;
    private final Address thisAddress;
    private final String name;

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
        this.concurrencyManager = new ReliableTopicConcurrencyManager(topicConfig);
        this.publishSequencer = new PublishSequencer();
        this.localTopicStats = service.getLocalTopicStats(name);

        for (ListenerConfig listenerConfig : topicConfig.getMessageListenerConfigs()) {
            addMessageListener(listenerConfig);
        }
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

    @Override
    public void publish(@Nonnull E payload) {
        checkNotNull(payload, NULL_MESSAGE_IS_NOT_ALLOWED);
        try {
            publishAsync(payload).toCompletableFuture().join();
        } catch (Throwable t) {
            throw (RuntimeException) peel(t, null,
                    "Failed to publish message: " + payload + " to topic:" + getName());
        }
    }

    @Override
    public CompletionStage<Void> publishAsync(@Nonnull E payload) {
        checkNotNull(payload, NULL_MESSAGE_IS_NOT_ALLOWED);
        Data data = nodeEngine.toData(payload);
        ReliableTopicMessage message = new ReliableTopicMessage(data, thisAddress);
        return publishSequencer.submitSingle(message,
                () -> new TopicOverloadException("Failed to publish message: " + payload + " on topic:" + getName()));
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
        // this will trigger all listeners to destroy themselves.
        concurrencyManager.reset();
        ringbuffer.destroy();
    }

    @Nonnull
    @Override
    public LocalTopicStats getLocalTopicStats() {
        return localTopicStats;
    }

    @Override
    public void publishAll(@Nonnull Collection<? extends E> payload) {
        checkNotNull(payload, NULL_MESSAGE_IS_NOT_ALLOWED);
        checkNoNullInside(payload, NULL_MESSAGE_IS_NOT_ALLOWED);

        try {
            publishAllAsync(payload).toCompletableFuture().join();
        } catch (Throwable t) {
            throw (RuntimeException) peel(t, null,
                    String.format("Failed to publish messages: %s on topic: %s", payload, getName()));
        }
    }

    @Override
    public CompletionStage<Void> publishAllAsync(@Nonnull Collection<? extends E> payload) {
        checkNotNull(payload, NULL_MESSAGE_IS_NOT_ALLOWED);
        checkNoNullInside(payload, NULL_MESSAGE_IS_NOT_ALLOWED);

        List<ReliableTopicMessage> messages = payload.stream()
                .map(m -> new ReliableTopicMessage(nodeEngine.toData(m), thisAddress))
                .collect(Collectors.toList());

        return publishSequencer.submitBatch(messages,
                () -> new TopicOverloadException(
                        String.format("Failed to publish messages: %s on topic: %s", payload, getName())));
    }


    // package-private accessor for tests and internal integration
    ReliableTopicConcurrencyManager concurrencyManager() {
        return concurrencyManager;
    }

    private final class PublishSequencer {

        private final Object lock = new Object();
        private final ArrayDeque<PublishTask> queue = new ArrayDeque<>();
        private PublishTask active;

        InternalCompletableFuture<Void> submitSingle(ReliableTopicMessage message,
                                                     Supplier<TopicOverloadException> errorSupplier) {
            ReliableTopicConcurrencyManager.Ticket ticket = concurrencyManager.begin();
            PublishTask task = new PublishTask(ticket, message, null, errorSupplier);
            enqueue(task);
            return task.resultFuture;
        }

        InternalCompletableFuture<Void> submitBatch(List<ReliableTopicMessage> messages,
                                                    Supplier<TopicOverloadException> errorSupplier) {
            ReliableTopicConcurrencyManager.Ticket ticket = concurrencyManager.begin();
            PublishTask task = new PublishTask(ticket, null, List.copyOf(messages), errorSupplier);
            enqueue(task);
            return task.resultFuture;
        }

        private void enqueue(PublishTask task) {
            boolean shouldSchedule = false;
            synchronized (lock) {
                queue.addLast(task);
                if (active == null) {
                    active = task;
                    shouldSchedule = true;
                }
            }
            if (shouldSchedule) {
                execute(task);
            }
        }

        private void execute(PublishTask task) {
            try {
                executor.execute(task::start);
            } catch (RuntimeException e) {
                task.failBeforeExecution(e);
            }
        }

        private void onTaskDone(PublishTask task) {
            PublishTask next = null;
            synchronized (lock) {
                PublishTask head = queue.peekFirst();
                if (head == task) {
                    queue.pollFirst();
                } else {
                    queue.remove(task);
                }
                active = null;
                if (!queue.isEmpty()) {
                    next = queue.peekFirst();
                    active = next;
                }
            }
            if (next != null) {
                execute(next);
            }
        }

        private final class PublishTask {
            private final ReliableTopicConcurrencyManager.Ticket ticket;
            private final ReliableTopicMessage singleMessage;
            private final List<ReliableTopicMessage> batchMessages;
            private final Supplier<TopicOverloadException> errorSupplier;
            private final InternalCompletableFuture<Void> resultFuture = new InternalCompletableFuture<>();
            private long backoffMs = INITIAL_BACKOFF_MS;

            private PublishTask(ReliableTopicConcurrencyManager.Ticket ticket,
                                ReliableTopicMessage singleMessage,
                                List<ReliableTopicMessage> batchMessages,
                                Supplier<TopicOverloadException> errorSupplier) {
                this.ticket = ticket;
                this.singleMessage = singleMessage;
                this.batchMessages = batchMessages;
                this.errorSupplier = errorSupplier;
            }

            void start() {
                invokeOnce();
            }

            void failBeforeExecution(RuntimeException e) {
                try {
                    resultFuture.completeExceptionally(e);
                } finally {
                    concurrencyManager.complete(ticket);
                    onTaskDone(this);
                }
            }

            private void invokeOnce() {
                OverflowPolicy policy = overloadPolicy == TopicOverloadPolicy.DISCARD_OLDEST
                        ? OverflowPolicy.OVERWRITE
                        : OverflowPolicy.FAIL;

                InternalCompletableFuture<Long> future = batchMessages == null
                        ? ringbuffer.addAsync(singleMessage, policy)
                        : ringbuffer.addAllAsync(batchMessages, policy);

                future.whenCompleteAsync((sequenceId, throwable) -> {
                    if (throwable != null) {
                        fail(throwable);
                        return;
                    }

                    handleResult(sequenceId);
                }, executor);
            }

            private void handleResult(long sequenceId) {
                switch (overloadPolicy) {
                    case ERROR:
                        if (sequenceId == -1) {
                            fail(errorSupplier.get());
                        } else {
                            succeed();
                        }
                        break;
                    case DISCARD_NEWEST:
                    case DISCARD_OLDEST:
                        succeed();
                        break;
                    case BLOCK:
                        if (sequenceId == -1) {
                            retryWithBackoff();
                        } else {
                            succeed();
                        }
                        break;
                    default:
                        fail(new IllegalArgumentException("Unknown overloadPolicy:" + overloadPolicy));
                        break;
                }
            }

            private void retryWithBackoff() {
                long sleepMs = Math.min(backoffMs, MAX_BACKOFF);
                try {
                    MILLISECONDS.sleep(sleepMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    fail(e);
                    return;
                }

                if (backoffMs < MAX_BACKOFF) {
                    backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF);
                }

                invokeOnce();
            }

            private void succeed() {
                resultFuture.complete(null);
                completeAndScheduleNext();
            }

            private void fail(Throwable throwable) {
                resultFuture.completeExceptionally(throwable);
                completeAndScheduleNext();
            }

            private void completeAndScheduleNext() {
                concurrencyManager.complete(ticket);
                onTaskDone(this);
            }
        }
    }
}
