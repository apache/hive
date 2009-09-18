/**
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

/**
 * JDBM LICENSE v1.00
 *
 * Redistribution and use of this software and associated documentation
 * ("Software"), with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * 1. Redistributions of source code must retain copyright
 *    statements and notices.  Redistributions must also contain a
 *    copy of this document.
 *
 * 2. Redistributions in binary form must reproduce the
 *    above copyright notice, this list of conditions and the
 *    following disclaimer in the documentation and/or other
 *    materials provided with the distribution.
 *
 * 3. The name "JDBM" must not be used to endorse or promote
 *    products derived from this Software without prior written
 *    permission of Cees de Groot.  For written permission,
 *    please contact cg@cdegroot.com.
 *
 * 4. Products derived from this Software may not be called "JDBM"
 *    nor may "JDBM" appear in their names without prior written
 *    permission of Cees de Groot.
 *
 * 5. Due credit should be given to the JDBM Project
 *    (http://jdbm.sourceforge.net/).
 *
 * THIS SOFTWARE IS PROVIDED BY THE JDBM PROJECT AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT
 * NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL
 * CEES DE GROOT OR ANY CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Copyright 2000 (C) Cees de Groot. All Rights Reserved.
 * Contributions are Copyright (C) 2000 by their associated contributors.
 *
 * $Id
 */
package org.apache.hadoop.hive.ql.util.jdbm.helper;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.Reference;
import java.util.Enumeration;
import java.util.Map;
import java.util.HashMap;

/**
 * Wraps a deterministic cache policy with a <q>Level-2</q> cache based on
 * J2SE's {@link SoftReference soft references}. Soft references allow
 * this cache to keep references to objects until the memory they occupy
 * is required elsewhere.
 * <p>
 * Since the {@link CachePolicy} interface requires an event be fired
 * when an object is evicted, and the event contains the actual object,
 * this class cannot be a stand-alone implementation of
 * <code>CachePolicy</code>. This limitation arises because Java References
 * does not support notification before references are cleared; nor do
 * they support reaching soft referents. Therefore, this wrapper cache
 * aggressively notifies evictions: events are fired when the objects are
 * evicted from the internal cache. Consequently, the soft cache may return
 * a non-null object when <code>get( )</code> is called, even if that
 * object was said to have been evicted.
 * <p>
 * The current implementation uses a hash structure for its internal key
 * to value mappings.
 * <p>
 * Note: this component's publicly exposed methods are not threadsafe;
 * potentially concurrent code should synchronize on the cache instance.
 *
 * @author <a href="mailto:dranatunga@users.sourceforge.net">Dilum Ranatunga</a>
 * @version $Id: SoftCache.java,v 1.1 2003/11/01 13:29:27 dranatunga Exp $
 */
public class SoftCache implements CachePolicy {
    private static final int INITIAL_CAPACITY = 128;
    private static final float DEFAULT_LOAD_FACTOR = 1.5f;

    private final ReferenceQueue _clearQueue = new ReferenceQueue();
    private final CachePolicy _internal;
    private final Map _cacheMap;

    /**
     * Creates a soft-reference based L2 cache with a {@link MRU} cache as
     * the internal (L1) cache. The soft reference cache uses the
     * default load capacity of 1.5f, which is intended to sacrifice some
     * performance for space. This compromise is reasonable, since all
     * {@link #get(Object) get( )s} first try the L1 cache anyway. The
     * internal MRU is given a capacity of 128 elements.
     */
    public SoftCache() {
        this(new MRU(INITIAL_CAPACITY));
    }

    /**
     * Creates a soft-reference based L2 cache wrapping the specified
     * L1 cache.
     *
     * @param internal non null internal cache.
     * @throws NullPointerException if the internal cache is null.
     */
    public SoftCache(CachePolicy internal) throws NullPointerException {
        this(DEFAULT_LOAD_FACTOR, internal);
    }

    /**
     * Creates a soft-reference based L2 cache wrapping the specified
     * L1 cache. This constructor is somewhat implementation-specific,
     * so users are encouraged to use {@link #SoftCache(CachePolicy)}
     * instead.
     *
     * @param loadFactor load factor that the soft cache's hash structure
     *        should use.
     * @param internal non null internal cache.
     * @throws IllegalArgumentException if the load factor is nonpositive.
     * @throws NullPointerException if the internal cache is null.
     */
    public SoftCache(float loadFactor, CachePolicy internal) throws IllegalArgumentException, NullPointerException {
        if (internal == null) {
            throw new NullPointerException("Internal cache cannot be null.");
        }
        _internal = internal;
        _cacheMap = new HashMap(INITIAL_CAPACITY, loadFactor);
    }

    /**
     * Adds the specified value to the cache under the specified key. Note
     * that the object is added to both this and the internal cache.
     * @param key the (non-null) key to store the object under
     * @param value the (non-null) object to place in the cache
     * @throws CacheEvictionException exception that the internal cache
     *         would have experienced while evicting an object it currently
     *         cached.
     */
    public void put(Object key, Object value) throws CacheEvictionException {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null.");
        } else if (value == null) {
            throw new IllegalArgumentException("value cannot be null.");
        }
        _internal.put(key, value);
        removeClearedEntries();
        _cacheMap.put(key, new Entry(key, value, _clearQueue));
    }

    /**
     * Gets the object cached under the specified key.
     * <p>
     * The cache is looked up in the following manner:
     * <ol>
     * <li>The internal (L1) cache is checked. If the object is found, it is
     *     returned.</li>
     * <li>This (L2) cache is checked. If the object is not found, then
     *     the caller is informed that the object is inaccessible.</li>
     * <li>Since the object exists in L2, but not in L1, the object is
     *     readded to L1 using {@link CachePolicy#put(Object, Object)}.</li>
     * <li>If the readding succeeds, the value is returned to caller.</li>
     * <li>If a cache eviction exception is encountered instead, we
     *     remove the object from L2 and behave as if the object was
     *     inaccessible.</li>
     * </ol>
     * @param key the key that the object was stored under.
     * @return the object stored under the key specified; null if the
     *         object is not (nolonger) accessible via this cache.
     */
    public Object get(Object key) {
        // first try the internal cache.
        Object value = _internal.get(key);
        if (value != null) {
            return value;
        }
        // poll and remove cleared references.
        removeClearedEntries();
        Entry entry = (Entry)_cacheMap.get(key);
        if (entry == null) { // object is not in cache.
            return null;
        }
        value = entry.getValue();
        if (value == null) { // object was in cache, but it was cleared.
            return null;
        }
        // we have the object. so we try to re-insert it into internal cache
        try {
            _internal.put(key, value);
        } catch (CacheEvictionException e) {
            // if the internal cache causes a fuss, we kick the object out.
            _cacheMap.remove(key);
            return null;
        }
        return value;
    }

    /**
     * Removes any object stored under the key specified. Note that the
     * object is removed from both this (L2) and the internal (L1)
     * cache.
     * @param key the key whose object should be removed
     */
    public void remove(Object key) {
        _cacheMap.remove(key);
        _internal.remove(key);
    }

    /**
     * Removes all objects in this (L2) and its internal (L1) cache.
     */
    public void removeAll() {
        _cacheMap.clear();
        _internal.removeAll();
    }

    /**
     * Gets all the objects stored by the internal (L1) cache.
     * @return an enumeration of objects in internal cache.
     */
    public Enumeration elements() {
        return _internal.elements();
    }

    /**
     * Adds the specified listener to this cache. Note that the events
     * fired by this correspond to the <em>internal</em> cache's events.
     * @param listener the (non-null) listener to add to this policy
     * @throws IllegalArgumentException if listener is null.
     */
    public void addListener(CachePolicyListener listener)
            throws IllegalArgumentException {
        _internal.addListener(listener);
    }

    /**
     * Removes a listener that was added earlier.
     * @param listener the listener to remove.
     */
    public void removeListener(CachePolicyListener listener) {
        _internal.removeListener(listener);
    }

    /**
     * Cleans the mapping structure of any obsolete entries. This is usually
     * called before insertions and lookups on the mapping structure. The
     * runtime of this is usually very small, but it can be as expensive as
     * n * log(n) if a large number of soft references were recently cleared.
     */
    private final void removeClearedEntries() {
        for (Reference r = _clearQueue.poll(); r != null; r = _clearQueue.poll()) {
            Object key = ((Entry)r).getKey();
            _cacheMap.remove(key);
        }
    }

    /**
     * Value objects we keep in the internal map. This contains the key in
     * addition to the value, because polling for cleared references
     * returns these instances, and having access to their corresponding
     * keys drastically improves the performance of removing the pair
     * from the map (see {@link SoftCache#removeClearedEntries()}.)
     */
    private static class Entry extends SoftReference {
        private final Object _key;

        /**
         * Constructor that uses <code>value</code> as the soft
         * reference's referent.
         */
        public Entry(Object key, Object value, ReferenceQueue queue) {
            super(value, queue);
            _key = key;
        }

        /**
         * Gets the key
         * @return the key associated with this value.
         */
        final Object getKey() {
            return _key;
        }

        /**
         * Gets the value
         * @return the value; null if it is no longer accessible
         */
        final Object getValue() {
            return this.get();
        }
    }
}
