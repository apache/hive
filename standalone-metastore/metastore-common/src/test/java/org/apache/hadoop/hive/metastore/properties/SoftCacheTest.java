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

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class SoftCacheTest {

    @Test
    public void testCache0() {
        SoftCache<Long, String> cache = new SoftCache<>();
        Assert.assertTrue(cache.capacity() > 0);
        Assert.assertEquals(0, cache.size());
        runCacheTest(cache);
    }

    @Test
    public void testCache1() {
        SoftCache<Long, String> cache = new SoftCache<>(32);
        Assert.assertEquals(32, cache.capacity());
        Assert.assertEquals(0, cache.size());
        runCacheTest(cache);
    }

    @Test
    public void testCacheSync2() {
        SoftCache<Long, String> cache = new SoftCache<>(32, 0.75f, true);
        Assert.assertEquals(32, cache.capacity());
        Assert.assertEquals(0, cache.size());
        runCacheTest(cache);
    }

    private void runCacheTest(SoftCache<Long, String> cache) {
        int cpy = cache.capacity();
        // fill at capacity
        for(long i = 0; i < cpy; ++i) {
            String p = cache.put(i, Long.toHexString(i));
            Assert.assertNull(p);
        }
        // size == capacity
        Assert.assertEquals(cpy, cache.size());
        // retrieve all
        for(long i = 0; i < cpy; ++i) {
            String p = cache.get(i);
            Assert.assertEquals(Long.toHexString(i), p);
        }
        // rewrite
        for(long i = 0; i < cpy; ++i) {
            String p = cache.put(i, "---" + Long.toHexString(i));
            Assert.assertEquals(Long.toHexString(i), p);
        }
        // go over capacity, force evictions
        for(long i = 0; i < cpy; ++i) {
            String p = cache.put(100 + i, "***" + Long.toHexString(100 + i));
            Assert.assertNull(p);
        }
        // size == capacity
        Assert.assertEquals(cpy, cache.size());
        for(long i = 0; i < cpy; ++i) {
            String p = cache.get(100 + i);
            Assert.assertEquals("***" + Long.toHexString(100 + i), p);
        }
        // remove all
        for(long i = 0; i < cpy; ++i) {
            String p = cache.remove(100 + i);
            Assert.assertEquals("***" + Long.toHexString(100 + i), p);
        }
        // empty
        Assert.assertEquals(0, cache.size());
        // fill 4
        for(long i = 0; i < 4; ++i) {
            String p = cache.put(i, Long.toHexString(i));
            Assert.assertNull(p);
        }
        // 4 values
        Set<String> values = new TreeSet<>(cache.values());
        Assert.assertEquals(4, values.size());
        for(long i = 0; i < 4; ++i) {
            Assert.assertTrue(values.contains(Long.toHexString(i)));
        }
        // 4 entries
        Map<Long,String> entries = new TreeMap<>();
        for(Map.Entry<Long,String> e : cache.entrySet()) {
            entries.put(e.getKey(), e.getValue());
        }
        Assert.assertEquals(4, entries.size());
        for(long i = 0; i < 4; ++i) {
            String p = entries.get(i);
            Assert.assertEquals(Long.toHexString(i), p);
        }
        // clear
        cache.clear();
        Assert.assertEquals(0, cache.size());
    }
}
