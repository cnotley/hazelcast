package com.hazelcast.topic.impl.reliable;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

/**
 * Manages global configuration for concurrent topic publishing.
 * <p>
 * The maximum number of concurrent publishes can be configured via the
 * {@value #MAX_CONCURRENT_PUBLISHES_PROPERTY} system property. A value of
 * {@code -1} disables the limit. The allowed range is {@code -1} to
 * {@code 16} and any value outside this range is treated as {@code -1}.
 */
public class ConcurrentTopicProcessor {

    /**
     * System property controlling the maximum number of concurrent publishes
     * per topic.
     */
    public static final HazelcastProperty MAX_CONCURRENT_PUBLISHES_PROPERTY =
            new HazelcastProperty("hazelcast.topic.max_concurrent_publishes", -1);

    private final HazelcastInstance hazelcastInstance;
    private volatile int currentLimit;

    public ConcurrentTopicProcessor(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        refreshLimit();
    }

    /**
     * Returns the currently configured limit. A negative value indicates that
     * no limit is enforced.
     */
    public int getCurrentLimit() {
        return currentLimit;
    }

    /**
     * Refreshes the limit from the {@link HazelcastProperties}. This method
     * can be invoked to pick up configuration changes at runtime.
     */
    public void refreshLimit() {
        HazelcastProperties properties = hazelcastInstance.getProperties();
        int configured = properties.getInteger(MAX_CONCURRENT_PUBLISHES_PROPERTY);
        if (configured < -1) {
            configured = -1;
        } else if (configured > 16) {
            configured = 16;
        }
        currentLimit = configured;
    }
}
