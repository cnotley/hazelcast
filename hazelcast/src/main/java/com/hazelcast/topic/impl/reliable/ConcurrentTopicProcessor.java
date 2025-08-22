package com.hazelcast.topic.impl.reliable;

import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

/**
 * Manages global concurrent publish limit for reliable topics.
 */
public class ConcurrentTopicProcessor {

    /**
     * System property to configure maximum number of concurrent publishes
     * across all reliable topics. Value range [-1,16] where -1 disables
     * limiting. Default is -1.
     */
    public static final HazelcastProperty MAX_CONCURRENT_PUBLISHES_PROPERTY =
            new HazelcastProperty("hazelcast.topic.max_concurrent_publishes", -1);

    private final HazelcastProperties properties;
    private volatile int currentLimit;

    public ConcurrentTopicProcessor(HazelcastProperties properties) {
        this.properties = properties;
        refreshLimit();
    }

    /**
     * Returns the currently configured global publish limit.
     */
    public int getCurrentLimit() {
        return currentLimit;
    }

    /**
     * Refreshes the limit from the {@link HazelcastProperties} allowing
     * dynamic updates of the configuration.
     */
    public void refreshLimit() {
        int limit = properties.getInteger(MAX_CONCURRENT_PUBLISHES_PROPERTY);
        if (limit < -1) {
            limit = -1;
        } else if (limit > 16) {
            limit = 16;
        }
        currentLimit = limit;
    }
}
