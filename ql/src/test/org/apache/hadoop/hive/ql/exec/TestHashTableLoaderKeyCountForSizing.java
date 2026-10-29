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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.exec;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * The rule both hash table loaders size from. Every case is one call, so the two loaders cannot
 * drift apart on it.
 */
public class TestHashTableLoaderKeyCountForSizing {

  /**
   * The counter counts rows, the estimate distinct keys. A table sized from the smaller can at
   * worst rehash; one sized from a larger count can ask for more than the memory threshold and
   * fail the query.
   */
  @Test
  public void aRecordCountDoesNotSizeAKeyIndexedTable() {
    assertEquals(65_000_000L, HashTableLoader.keyCountForSizing(65_000_000L, 320_243_730L));
  }

  /**
   * An estimate can be far too high as well: 21,666,666 keys where the counter reads 18,166.
   * Sizing from the estimate allocates 512 MiB of slot arrays per task, the counter 0.5 MiB.
   */
  @Test
  public void aRecordCountBoundsAnEstimateFromAbove() {
    assertEquals(18_166L, HashTableLoader.keyCountForSizing(21_666_666L, 18_166L));
  }

  /** Without an estimate the counter is all there is, which is what HIVE-23953 added it for. */
  @Test
  public void theCounterIsUsedAloneWhenTheEstimateIsAbsent() {
    assertEquals(320_243_730L, HashTableLoader.keyCountForSizing(-1L, 320_243_730L));
  }

  /**
   * 0 is how an absent signal actually arrives: TezCounters.findCounter materialises a counter at
   * zero when the input never announced, so an empty small table must not pin the size to zero.
   */
  @Test
  public void zeroIsTreatedAsAbsentOnEitherSide() {
    assertEquals(500L, HashTableLoader.keyCountForSizing(0L, 500L));
    assertEquals(500L, HashTableLoader.keyCountForSizing(500L, 0L));
    assertEquals(-1L, HashTableLoader.keyCountForSizing(0L, 0L));
  }
}
