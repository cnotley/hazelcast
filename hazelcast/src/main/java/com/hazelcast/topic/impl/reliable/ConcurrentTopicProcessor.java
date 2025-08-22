package com.hazelcast.topic.impl.reliable;

import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

/**
 * Utility processor that keeps track of the global configuration limiting the
 * number of concurrent publish operations for topics.
 */
public class ConcurrentTopicProcessor {

    /**
     * System property controlling the maximum allowed concurrent publishes.
     * A value of {@code -1} disables the limit. Valid range is {@code -1..16}.
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
     * Refreshes the current limit from the {@link HazelcastProperties} to allow
     * dynamic configuration changes.
     */
    public final void refreshLimit() {
        int limit = properties.getInteger(MAX_CONCURRENT_PUBLISHES_PROPERTY);
        if (limit < -1) {
            limit = -1;
        } else if (limit > 16) {
            limit = 16;
        }
        this.currentLimit = limit;
    }

    /**
     * Returns the currently configured global limit. A negative value indicates
     * that no limit is enforced.
     */
    public int getCurrentLimit() {
        return currentLimit;
    }
}
