/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams.utils;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

/**
 * TTLValueTransformer has a configrable way to allow keys to be deleted from state store on a set ttl. The below works on date level but 
 * can be made to be more granular (on timestamp level). This allows the deletion time to be O(m) where m -> avg deleted messages per day vs
 * a typical ttl based transformer that uses O(N) where N is all messages through time since it requires iterating over the state store. It 
 * achieves the optimisation by using two additional state stores, one for keeping track of the deleted keys and other to track on dates, 
 * and running range queries based on ttl.
 */
public abstract class TTLValueTransformer<K, V> implements ValueTransformerWithKey<K, V, V> {
    protected KeyValueStore<K, V> mainStateStore;
    private KeyValueStore<K, LocalDate> keyToTimestampMapper;
    private KeyValueStore<LocalDate, Set<K>> timestampToKeySet;
    protected ProcessorContext context;
    private final Long ttlDays;
    private final Duration scanFrequency;
    private final String mainStateStoreName;
    private final String keyTimestampStateStoreName;
    private final String timestampKeySetStateStoreName;

    private static final Logger log = LoggerFactory.getLogger(TTLValueTransformer.class);


    public TTLValueTransformer(final Long ttlDays,
                               final Duration scanFrequency,
                               final String mainStateStoreName,
                               final String keyTimestampStateStoreName,
                               final String timestampKeySetStateStoreName) {
        this.ttlDays = ttlDays;
        this.scanFrequency = scanFrequency;
        this.mainStateStoreName = mainStateStoreName;
        this.keyTimestampStateStoreName = keyTimestampStateStoreName;
        this.timestampKeySetStateStoreName = timestampKeySetStateStoreName;
    }

    @Override
    public abstract V transform(K readOnlyKey, V value);

    public void undoCleanupForKey(K key) {
        var lastUpdateForKey = keyToTimestampMapper.get(key);
        if (lastUpdateForKey != null) {
            var keySet = timestampToKeySet.get(lastUpdateForKey);
            if (keySet != null)
                keySet.remove(key);
        }
    }

    public void pushKeyForCleanup(K key) {
        LocalDate updatedLevel = LocalDate.now().plusDays(ttlDays);
        if (timestampToKeySet.get(updatedLevel) == null) {
            var baseAvailabilitySet = new HashSet<K>();
            baseAvailabilitySet.add(key);
            timestampToKeySet.put(updatedLevel, baseAvailabilitySet);
        } else {
            timestampToKeySet.get(updatedLevel).add(key);
        }
        keyToTimestampMapper.put(key, updatedLevel);
    }

    @Override
    public void init(final ProcessorContext context) {
        this.context = context;
        this.mainStateStore = context.getStateStore(this.mainStateStoreName);
        this.timestampToKeySet = context.getStateStore(this.timestampKeySetStateStoreName);
        this.keyToTimestampMapper = context.getStateStore(this.keyTimestampStateStoreName);

        context.schedule(scanFrequency, PunctuationType.STREAM_TIME, timestamp -> {
            var rRange = LocalDate.now();
            var lRange = LocalDate.now().minusYears(4); // can be made configurable
            try {
                var baseAvailabilitySetList = timestampToKeySet.range(lRange, rRange);
                while (baseAvailabilitySetList.hasNext()) {
                    var baseAvailabilitySet = baseAvailabilitySetList.next();
                    baseAvailabilitySet.value.forEach(v -> {
                        keyToTimestampMapper.delete(v);
                        mainStateStore.delete(v);
                    });
                    baseAvailabilitySetList.remove();
                }
            }
            catch (Exception e) {
                log.error("Exception caught during deletion of older data in state store, error : {}", e.getMessage());
            }
        });
    }

    @Override
    public void close() {}

}