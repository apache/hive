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
package org.apache.hadoop.hive.ql.udf.ptf;

import org.apache.hadoop.hive.ql.exec.PTFValueCache;
import org.junit.Test;

import org.junit.Assert;

public class TestPTFValueCache {

  @Test
  public void testBasicUsage() {
    PTFValueCache cache = new PTFValueCache(3).init(1);
    cache.put(0, new Range(0, 10, null), "0-10");
    Assert.assertEquals(1, cache.size());

    // same element again
    cache.put(0, new Range(0, 10, null), "0-10");
    Assert.assertEquals(1, cache.size());

    // new element again
    cache.put(0, new Range(5, 10, null), "5-10");
    Assert.assertEquals(2, cache.size());

    Assert.assertEquals("0-10", cache.get(0, new Range(0, 10, null)));
    Assert.assertEquals("5-10", cache.get(0, new Range(5, 10, null)));
  }

  @Test
  public void testBiggerRangeIsMoreImportant() {
    PTFValueCache cache = new PTFValueCache(3).init(1);
    cache.put(0, new Range(0, 10, null), "0-10");
    Assert.assertEquals(1, cache.size());

    cache.put(0, new Range(0, 100, null), "0-100");
    Assert.assertEquals(2, cache.size());

    cache.put(0, new Range(0, 1000, null), "0-1000");
    Assert.assertEquals(3, cache.size());

    cache.put(0, new Range(0, 10000, null), "0-10000");
    Assert.assertEquals(3, cache.size());

    // smallest range is evicted
    Assert.assertNull(cache.get(0, new Range(0, 10, null)));
    // others are present
    Assert.assertEquals("0-100", cache.get(0, new Range(0, 100, null)));
    Assert.assertEquals("0-1000", cache.get(0, new Range(0, 1000, null)));
    Assert.assertEquals("0-10000", cache.get(0, new Range(0, 10000, null)));
  }

  @Test
  public void testLargerStartIsMoreImportant() {
    PTFValueCache cache = new PTFValueCache(2).init(1);
    cache.put(0, new Range(0, 10, null), "0-10");
    Assert.assertEquals(1, cache.size());

    cache.put(0, new Range(10, 20, null), "10-20");
    Assert.assertEquals(2, cache.size());

    cache.put(0, new Range(20, 30, null), "20-30");
    Assert.assertEquals(2, cache.size());

    // oldest range is evicted
    Assert.assertNull(cache.get(0, new Range(0, 10, null)));
    // later ranges are present
    Assert.assertEquals("10-20", cache.get(0, new Range(10, 20, null)));
    Assert.assertEquals("20-30", cache.get(0, new Range(20, 30, null)));
  }
}
