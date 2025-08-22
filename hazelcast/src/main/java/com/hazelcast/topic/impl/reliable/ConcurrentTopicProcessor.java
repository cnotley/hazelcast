package com.hazelcast.topic.impl.reliable;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

/**
 * Processor responsible for determining the global concurrent publish limit
 * for reliable topics. The value is fetched from the Hazelcast properties
 * and can be refreshed at runtime.
 */
public class ConcurrentTopicProcessor {

    /**
     * System property that defines the maximum number of concurrent publish
     * operations allowed on a topic. A value of {@code -1} disables
     * limiting. Valid range is {@code -1} to {@code 16}.
     */
    public static final HazelcastProperty MAX_CONCURRENT_PUBLISHES_PROPERTY =
            new HazelcastProperty("hazelcast.topic.max_concurrent_publishes", -1);

    private final HazelcastInstance hazelcastInstance;
    private volatile int currentLimit = -1;

    public ConcurrentTopicProcessor(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        refreshLimit();
    }

    /**
     * Returns the current global concurrent publish limit.
     */
    public int getCurrentLimit() {
        return currentLimit;
    }

    /**
     * Refreshes the limit value by reading the corresponding Hazelcast
     * property. Invalid values fall back to {@code -1} (unrestricted).
     */
    public void refreshLimit() {
        int limit = -1;
        HazelcastProperties properties = null;
        if (hazelcastInstance instanceof HazelcastInstanceImpl) {
            properties = ((HazelcastInstanceImpl) hazelcastInstance).node.getProperties();
        }
        if (properties != null) {
            try {
                limit = properties.getInteger(MAX_CONCURRENT_PUBLISHES_PROPERTY);
            } catch (Exception ignored) {
                limit = -1;
            }
        } else {
            String prop = System.getProperty(MAX_CONCURRENT_PUBLISHES_PROPERTY.getName(), "-1");
            try {
                limit = Integer.parseInt(prop);
            } catch (NumberFormatException ignored) {
                limit = -1;
            }
        }
        if (limit < -1) {
            limit = -1;
        } else if (limit > 16) {
            limit = 16;
        }
        this.currentLimit = limit;
    }
}
