/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.topic.impl.reliable;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.HazelcastProperty;

/**
 * Handles global configuration for reliable topic concurrency limits.
 */
public class ConcurrentTopicProcessor {

    /**
     * System property controlling maximum concurrent publishes across all topics.
     * Default value {@code -1} disables the limit. Valid range is {@code -1} to {@code 16}.
     */
    public static final HazelcastProperty MAX_CONCURRENT_PUBLISHES_PROPERTY
            = new HazelcastProperty("hazelcast.topic.max_concurrent_publishes", -1);

    private final HazelcastInstance hazelcastInstance;
    private volatile int currentLimit;

    public ConcurrentTopicProcessor(HazelcastInstance instance) {
        this.hazelcastInstance = instance;
        refreshLimit();
    }

    /**
     * Returns current global limit for concurrent publishes.
     */
    public int getCurrentLimit() {
        return currentLimit;
    }

    /**
     * Refreshes the limit from the Hazelcast properties allowing dynamic updates.
     */
    public void refreshLimit() {
        int limit = hazelcastInstance.getConfig().getProperties()
                .getInteger(MAX_CONCURRENT_PUBLISHES_PROPERTY);
        if (limit < -1) {
            limit = -1;
        } else if (limit > 16) {
            limit = 16;
        }
        currentLimit = limit;
    }
}

