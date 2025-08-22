package com.hazelcast.topic;

import com.hazelcast.core.HazelcastException;

/**
 * Exception thrown when a topic cannot accept more publish operations due to
 * configured concurrency limits.
 */
public class TopicOverloadedException extends HazelcastException {

    public TopicOverloadedException(String message) {
        super(message);
    }
}
