package com.hazelcast.topic.impl.reliable;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages concurrency configuration for reliable topic publish operations.
 * The concurrency level can be updated dynamically and queried by
 * components that need to respect the configured maximum number of
 * concurrent publishes.
 */
public class ReliableTopicConcurrencyManager {

    /**
     * Simple representation of a dynamic configuration setting.
     * This is a minimal placeholder used for wiring the concurrency
     * configuration into the wider system.
     */
    public static final class Setting<T> {
        private final String name;
        private final T defaultValue;

        public Setting(String name, T defaultValue) {
            this.name = name;
            this.defaultValue = defaultValue;
        }

        public String getName() {
            return name;
        }

        public T getDefaultValue() {
            return defaultValue;
        }
    }

    /**
     * Name of the dynamic setting governing maximum concurrent publishes.
     */
    public static final Setting<Integer> MAX_CONCURRENT_PUBLISHES_SETTING =
            new Setting<>("topic.max_concurrent_publishes", 1);

    private final AtomicInteger maxConcurrentPublishes = new AtomicInteger(1);

    /**
     * Updates the concurrency limit. Values are clamped to the range [1,8].
     */
    public void updateConcurrency(int newLimit) {
        int limit = Math.max(1, Math.min(8, newLimit));
        maxConcurrentPublishes.set(limit);
    }

    /**
     * Returns the currently configured maximum number of concurrent publishes.
     */
    public int getMaxConcurrentPublishes() {
        return maxConcurrentPublishes.get();
    }
}
