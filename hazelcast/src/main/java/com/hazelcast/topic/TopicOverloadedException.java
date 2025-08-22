package com.hazelcast.topic;

import com.hazelcast.core.HazelcastException;

/**
 * A {@link HazelcastException} thrown when the maximum number of concurrent
 * publish operations on a topic is exceeded.
 * <p>
 * This exception is retryable; callers are expected to apply an appropriate
 * backoff strategy before retrying the publish operation.
 */
public class TopicOverloadedException extends HazelcastException {

    public TopicOverloadedException(String message) {
        super(message);
    }
}
