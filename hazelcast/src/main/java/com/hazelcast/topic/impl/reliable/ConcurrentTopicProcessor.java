package com.hazelcast.topic.impl.reliable;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides access to the configured maximum concurrent publish operations.
 * The setting can be changed dynamically through system properties.
 */
public class ConcurrentTopicProcessor {

    /**
     * Name of the system property controlling the maximum concurrent publishes.
     */
    public static final HazelcastProperty MAX_CONCURRENT_PUBLISHES_PROPERTY =
            new HazelcastProperty("hazelcast.topic.max_concurrent_publishes", -1);

    private final HazelcastProperties properties;
    private final AtomicInteger currentLimit = new AtomicInteger(-1);

    public ConcurrentTopicProcessor(HazelcastInstance instance) {
        HazelcastInstanceImpl impl = (HazelcastInstanceImpl) instance;
        this.properties = impl.node.getNodeEngine().getProperties();
        int initial = properties.getInteger(MAX_CONCURRENT_PUBLISHES_PROPERTY);
        this.currentLimit.set(validate(initial));
    }

    private int validate(int value) {
        if (value < -1 || value > 16) {
            return -1;
        }
        return value;
    }

    /**
     * Returns the currently configured limit. The value is read from the
     * system properties on each invocation to pick up dynamic changes.
     */
    public int getCurrentLimit() {
        int configured = properties.getInteger(MAX_CONCURRENT_PUBLISHES_PROPERTY);
        int validated = validate(configured);
        currentLimit.set(validated);
        return validated;
    }
}
