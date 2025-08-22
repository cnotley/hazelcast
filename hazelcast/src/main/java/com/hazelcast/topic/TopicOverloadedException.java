package com.hazelcast.topic;

import com.hazelcast.core.HazelcastException;

/**
 * Exception thrown when a publish operation is rejected because the
 * configured maximum number of concurrent publishes on the topic has
 * been exceeded. This exception is retryable; callers are expected to
 * retry the publish with an appropriate backoff policy.
 */
public class TopicOverloadedException extends HazelcastException {

    public TopicOverloadedException(String message) {
        super(message);
    }
}
