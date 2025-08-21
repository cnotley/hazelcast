package com.hazelcast.topic;

import com.hazelcast.core.HazelcastException;

/**
 * Exception thrown when a topic is overloaded with concurrent publish operations.
 * Clients may retry the publish at a later time.
 */
public class TopicOverloadedException extends HazelcastException {
    public TopicOverloadedException(String message) {
        super(message);
    }
}
