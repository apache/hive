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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.hbase.utils;

import org.apache.hadoop.hive.metastore.hbase.utils.BloomFilter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBloomFilter {
  static final int SET_SIZE = 50;
  static final double FALSE_POSITIVE_PROBABILITY = 0.01;
  // Pre-calculated for the above set size and fpp
  static final int FILTER_SIZE = 480;
  static final int NUM_HASH_FUNCTIONS = 7;
  BloomFilter bloomFilter;
  // Items that we'll add to the filter
  String[] items = {"Part1=Val1", "Part2=Val2", "Part3=Val3", "Part4=Val4", "Part5=Val5"};

  @BeforeClass
  public static void beforeTest() {
  }

  @AfterClass
  public static void afterTest() {
  }

  @Before
  public void setUp() {
    bloomFilter = new BloomFilter(SET_SIZE, FALSE_POSITIVE_PROBABILITY);
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testFilterAndHashSize() {
    Assert.assertEquals(bloomFilter.getFilterSize(), FILTER_SIZE);
    Assert.assertEquals(bloomFilter.getNumHashFunctions(), NUM_HASH_FUNCTIONS);
  }

  @Test
  public void testFilterFunctions() {
    // Add all items to the bloom filter
    // (since bloom filter returns false positives, no point testing for negative cases)
    for (String item: items) {
      bloomFilter.addToFilter(item.getBytes());
    }
    // Test for presence
    for (String item: items) {
      Assert.assertTrue(bloomFilter.contains(item.getBytes()));
    }
    // Clear all bits
    bloomFilter.getBitVector().clearAll();
    // Test for presence now - should fail
    for (String item: items) {
      Assert.assertFalse(bloomFilter.contains(item.getBytes()));
    }
  }

}
