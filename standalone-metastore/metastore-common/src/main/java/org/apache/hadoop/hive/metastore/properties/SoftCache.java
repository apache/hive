/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.properties;

import java.lang.ref.SoftReference;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A soft referenced cache.
 * <p>
 * The actual cache is held through a soft reference, allowing it to be GCed under memory pressure.</p>
 * <p>
 * This class is <em>not</em> thread-safe.</p>
 * @param <K> the cache key entry type
 * @param <V> the cache key value type
 */
public class SoftCache<K, V> {
    /** The default cache capacity. */
    private static final int CACHE_CAPACITY = 64;
    /** The default cache load factor. */
    private static final float LOAD_FACTOR = 0.75f;
    /** Synchronized cache. */
    private final boolean synchro;
    /** The cache capacity. */
    private final int capacity;
    /** The cache load factor. */
    private final float loadFactor;
    /** The soft reference to the cache map. */
    private SoftReference<Map<K, V>> ref = null;

    /**
     * Creates a new instance of a soft cache.
     */
    public SoftCache() {
        this(CACHE_CAPACITY);
    }

    /**
     * Creates a new instance of a soft cache.
     * @param theCapacity the cache size
     */
    public SoftCache(int theCapacity) {
        this(theCapacity, LOAD_FACTOR, false);
    }

    /**
     * Creates a new instance of a soft cache.
     * @param theCapacity the cache capacity
     * @param theLoadFactor the cache load actor
     * @param synchronizd whether it is synchronized or not
     */
    public SoftCache(int theCapacity, float theLoadFactor, boolean synchronizd) {
        capacity = theCapacity;
        loadFactor = theLoadFactor;
        synchro = synchronizd;
    }

    /**
     * Returns the cache capacity.
     * @return the cache capacity
     */
    public int capacity() {
        return capacity;
    }

    /**
     * Returns the cache size.
     * @return the cache size
     */
    public int size() {
        final Map<K, V> map = ref != null ? ref.get() : null;
        return map != null? map.size() : 0;
    }

    /**
     * Clears the cache.
     */
    public void clear() {
        ref = null;
    }

    /**
     * Produces the cache entry set.
     * @return the cache entry set
     */
    public Set<Entry<K, V>> entrySet() {
        final Map<K, V> map = ref != null ? ref.get() : null;
        return map != null ? map.entrySet() : Collections.emptySet();
    }

    /**
     * Produces the cache values collection.
     * @return the cache values collection
     */
    public Collection<V> values() {
        final Map<K, V> map = ref != null ? ref.get() : null;
        return map != null ? map.values() : Collections.emptyList();
    }

    public boolean containsKey(K key) {
        final Map<K, V> map = ref != null ? ref.get() : null;
        return map != null && map.containsKey(key);
    }

    /**
     * Gets a value from cache.
     * @param key the cache entry key
     * @return the cache entry value
     */
    public V get(K key) {
        final Map<K, V> map = ref != null ? ref.get() : null;
        return map != null ? map.get(key) : null;
    }

    private Map<K, V> map() {
        Map<K, V> map = ref != null ? ref.get() : null;
        if (map == null) {
            synchronized(this) {
                map = ref != null ? ref.get() : null;
                if (map == null) {
                    map = createCache(capacity, loadFactor, synchro);
                    ref = new SoftReference<>(map);
                }
            }
        }
        return map;
    }

    /**
     * Puts a value in cache.
     * @param key the cache entry key
     * @param value the cache entry value
     * @return the previous value if any
     */
    public V put(K key, V value) {
        return map().put(key, value);
    }


    /**
     * Calls putIfAbsent on underlying map, useful when cache is meant to behave as a pull-thru.
     * @param key tke key
     * @param value the value
     * @return the value associated to the key
     */
    public V putIfAbsent(K key, V value) {
        return map().putIfAbsent(key, value);
    }

    /**
     * Calls computeIfAbsent on underlying map, useful when cache is meant to behave as a pull-thru.
     * @param key tke key
     * @param mappingFunction the function called if key does not associate to a value
     * @return the value associated to the key
     */
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        return map().computeIfAbsent(key, mappingFunction);
    }

    /**
     * Calls compute on underlying map, useful when cache is meant to behave as a pull-thru.
     * @param key tke key
     * @param remappingFunction the function called if key does not associate to a value
     * @return the value associated to the key
     */
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return map().compute(key, remappingFunction);
    }

    /**
     * Removes a value from cache.
     * @param key the cache entry key
     * @return the cache entry value
     */
    public V remove(K key) {
        if (ref != null) {
            Map<K, V> map = ref.get();
            if (map != null) {
                return map.remove(key);
            }
        }
        return null;
    }

    /**
     * Creates the underlying cache map.
     * @param capacity the cache size, must be &gt; 0
     * @param loadFactor the cache load factor
     * @param synchro whether the cache is synchronized or not
     * @return a Map usable as a cache bounded to the given size
     */
    protected Map<K, V> createCache(final int capacity, final float loadFactor, boolean synchro) {
        Map<K, V> cache = new CacheMap<>(capacity, loadFactor);
        if (synchro) {
            return Collections.synchronizedMap(cache);
        } else {
            return cache;
        }
    }

    /**
     * Typical LRU map.
     * @param <K> the key type
     * @param <V> the value type
     */
    private static class CacheMap<K, V> extends LinkedHashMap<K, V> {
        /** Serial version UID. */
        private static final long serialVersionUID = 202304041726L;
        /** The cache capacity, ie max number of elements. */
        private final int capacity;

        public CacheMap(int capacity, float loadFactor) {
            super(capacity, loadFactor, true);
            this.capacity = capacity;
        }

        @Override
        protected boolean removeEldestEntry(Entry<K, V> eldest) {
            return size() > capacity;
        }
    }
}
