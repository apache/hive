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
package org.apache.hadoop.hive.ql.exec;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * The rule both hash table loaders size from. Every case is one call, so the two loaders cannot
 * drift apart on it.
 */
public class TestHashTableLoaderInitialKeyCount {

  /**
   * Measured on q23 at 10Tb. The small side is customer: 65,000,000 rows, estimated at exactly
   * that many distinct keys, while APPROXIMATE_INPUT_RECORDS read 320,243,730 -- 4.93x the rows
   * the table has. Taking the larger sized for 320M and asked for 8.59GB of slot arrays against a
   * 5.15GB threshold. The estimate was the correct number and the one it discarded.
   */
  @Test
  public void aRecordCountDoesNotSizeAKeyIndexedTable() {
    assertEquals(65_000_000L, HashTableLoader.initialKeyCount(65_000_000L, 320_243_730L));
  }

  /** The smaller wins in the other direction too, when the estimate is the larger signal. */
  @Test
  public void anEstimateLargerThanTheRecordCountIsDiscarded() {
    assertEquals(65_000_000L, HashTableLoader.initialKeyCount(400_000_000L, 65_000_000L));
  }

  @Test
  public void theEstimateIsUsedAloneWhenTheCounterIsAbsent() {
    assertEquals(65_000_000L, HashTableLoader.initialKeyCount(65_000_000L, -1L));
  }

  /** Without an estimate the counter is all there is, which is what HIVE-23953 added it for. */
  @Test
  public void theCounterIsUsedAloneWhenTheEstimateIsAbsent() {
    assertEquals(320_243_730L, HashTableLoader.initialKeyCount(-1L, 320_243_730L));
  }

  /**
   * With no signal the result stays negative rather than becoming 0. A container reads a
   * non-positive key count as "no opinion" and keeps its own default capacity, where 0 reaches
   * VectorMapJoinFastHashTable.validateCapacity and throws AssertionError: Invalid capacity 0.
   */
  @Test
  public void noSignalLeavesTheContainerOnItsOwnDefault() {
    assertEquals(-1L, HashTableLoader.initialKeyCount(-1L, -1L));
  }

  /**
   * 0 is how an absent signal actually arrives: TezCounters.findCounter materialises a counter at
   * zero when the input never announced, so an empty small table must not pin the size to zero.
   */
  @Test
  public void zeroIsTreatedAsAbsentOnEitherSide() {
    assertEquals(500L, HashTableLoader.initialKeyCount(0L, 500L));
    assertEquals(500L, HashTableLoader.initialKeyCount(500L, 0L));
    assertEquals(-1L, HashTableLoader.initialKeyCount(0L, 0L));
    assertEquals(-1L, HashTableLoader.initialKeyCount(-1L, 0L));
  }
}
