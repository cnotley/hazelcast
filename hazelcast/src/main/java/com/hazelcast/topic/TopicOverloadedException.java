package com.hazelcast.topic;

import com.hazelcast.core.HazelcastException;

/**
 * Thrown when a reliable topic rejects a publish because the maximum
 * number of concurrent publish operations has been exceeded.
 * This exception is retryable.
 */
public class TopicOverloadedException extends HazelcastException {

    public TopicOverloadedException(String message) {
        super(message);
    }
}
