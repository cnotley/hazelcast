package com.hazelcast.topic.impl.reliable;

import com.hazelcast.config.ReliableTopicConfig;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages concurrency limits for reliable topic publish operations.
 * <p>
 * The manager reads its initial configuration from {@link ReliableTopicConfig}
 * and exposes the effective limit to callers. The limit can be updated by
 * invoking {@link #update(ReliableTopicConfig)}.
 */
public class ReliableTopicConcurrencyManager {

    private final AtomicInteger maxConcurrentPublishes = new AtomicInteger(1);

    public ReliableTopicConcurrencyManager(ReliableTopicConfig config) {
        update(config);
    }

    /**
     * Updates the concurrency limit based on the provided configuration.
     *
     * @param config the configuration to read from
     */
    public void update(ReliableTopicConfig config) {
        if (config == null) {
            return;
        }
        int limit = config.getMaxConcurrentPublishes();
        if (limit < 1) {
            limit = 1;
        } else if (limit > 8) {
            limit = 8;
        }
        maxConcurrentPublishes.set(limit);
    }

    /**
     * Returns the current concurrency limit.
     *
     * @return max number of concurrent publish operations
     */
    public int getMaxConcurrentPublishes() {
        return maxConcurrentPublishes.get();
    }
}
