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

package org.apache.hadoop.hive.ql.stats.estimator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ColStatistics.Range;
import org.junit.jupiter.api.Test;

class TestPessimisticStatCombiner {

  @Test
  void testSingleStatPreservesNdv() {
    ColStatistics stat = createStat("col1", "int", 100, 10, 5.0);
    stat.setRange(new Range(0, 100));

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat);

    Optional<ColStatistics> result = combiner.getResult();
    assertTrue(result.isPresent());
    ColStatistics combined = result.get();

    assertEquals("col1", combined.getColumnName());
    assertEquals("int", combined.getColumnType());
    assertEquals(100, combined.getCountDistint());
    assertEquals(10, combined.getNumNulls());
    assertEquals(5.0, combined.getAvgColLen());
    assertNull(combined.getRange());
    assertTrue(combined.isEstimated());
  }

  @Test
  void testCombineTakesMaxOfAvgColLen() {
    ColStatistics stat1 = createStat("col1", "string", 50, 5, 10.0);
    ColStatistics stat2 = createStat("col2", "string", 30, 3, 20.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(20.0, combined.getAvgColLen());
  }

  @Test
  void testCombineTakesMaxOfNumNulls() {
    ColStatistics stat1 = createStat("col1", "int", 50, 100, 4.0);
    ColStatistics stat2 = createStat("col2", "int", 30, 200, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(200, combined.getNumNulls());
  }

  @Test
  void testCombineSetsCountDistinctToZero() {
    ColStatistics stat1 = createStat("col1", "int", 100, 10, 4.0);
    ColStatistics stat2 = createStat("col2", "int", 200, 20, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(0, combined.getCountDistint());
  }

  @Test
  void testCombineTakesMaxOfNumTruesAndNumFalses() {
    ColStatistics stat1 = createStat("col1", "boolean", 2, 5, 1.0);
    stat1.setNumTrues(100);
    stat1.setNumFalses(50);

    ColStatistics stat2 = createStat("col2", "boolean", 2, 10, 1.0);
    stat2.setNumTrues(50);
    stat2.setNumFalses(150);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(100, combined.getNumTrues());
    assertEquals(150, combined.getNumFalses());
  }

  @Test
  void testCombinePropagatesFilteredColumnFlag() {
    ColStatistics stat1 = createStat("col1", "int", 50, 5, 4.0);
    ColStatistics stat2 = createStat("col2", "int", 30, 3, 4.0);
    stat2.setFilterColumn();

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertTrue(combined.isFilteredColumn());
  }

  @Test
  void testCombineMultipleStats() {
    ColStatistics stat1 = createStat("col1", "bigint", 1000, 50, 8.0);
    ColStatistics stat2 = createStat("col2", "bigint", 500, 100, 8.0);
    ColStatistics stat3 = createStat("col3", "bigint", 2000, 25, 8.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);
    combiner.add(stat3);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(0, combined.getCountDistint());
    assertEquals(100, combined.getNumNulls());
    assertEquals(8.0, combined.getAvgColLen());
  }

  @Test
  void testCombineSameColumnTwice() {
    ColStatistics stat = createStat("col1", "int", 100, 10, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat);
    combiner.add(stat);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(0, combined.getCountDistint());
    assertEquals(10, combined.getNumNulls());
    assertEquals(4.0, combined.getAvgColLen());
  }

  private ColStatistics createStat(String name, String type, long ndv, long numNulls, double avgColLen) {
    ColStatistics stat = new ColStatistics(name, type);
    stat.setCountDistint(ndv);
    stat.setNumNulls(numNulls);
    stat.setAvgColLen(avgColLen);
    return stat;
  }
}
