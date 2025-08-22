package com.hazelcast.topic;

import com.hazelcast.core.HazelcastException;

/**
 * Exception thrown when the maximum number of concurrent publish operations
 * on a reliable topic is exceeded.
 */
public class TopicOverloadedException extends HazelcastException {

    public TopicOverloadedException(String message) {
        super(message);
    }
}
