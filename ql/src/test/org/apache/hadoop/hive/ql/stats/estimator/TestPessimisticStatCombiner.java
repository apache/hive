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

import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.junit.jupiter.api.Test;

class TestPessimisticStatCombiner {

  @Test
  void testNdvSumWhenBothKnown() {
    ColStatistics stat1 = createStat("col1", "int", 50, 0, 4.0);
    ColStatistics stat2 = createStat("col2", "int", 30, 0, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner(1000);
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics result = combiner.getResult().get();
    assertEquals(80, result.getCountDistint(), "NDV should be summed: 50 + 30 = 80");
  }

  @Test
  void testNdvUnknownPropagatedFromFirst() {
    ColStatistics stat1 = createStat("col1", "int", 0, 0, 4.0);
    ColStatistics stat2 = createStat("col2", "int", 100, 0, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner(1000);
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics result = combiner.getResult().get();
    assertEquals(0, result.getCountDistint(), "Unknown NDV (0) should propagate");
  }

  @Test
  void testNdvUnknownPropagatedFromSecond() {
    ColStatistics stat1 = createStat("col1", "int", 100, 0, 4.0);
    ColStatistics stat2 = createStat("col2", "int", 0, 0, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner(1000);
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics result = combiner.getResult().get();
    assertEquals(0, result.getCountDistint(), "Unknown NDV (0) should propagate");
  }

  @Test
  void testCombinePropagatesUnknownNumNullsFromFirst() {
    ColStatistics stat1 = createStat("col1", "int", 50, -1, 4.0); // unknown numNulls
    ColStatistics stat2 = createStat("col2", "int", 30, 100, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner(1000);
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(-1, combined.getNumNulls(), "Unknown numNulls (-1) should be propagated");
  }

  @Test
  void testCombinePropagatesUnknownNumNullsFromSecond() {
    ColStatistics stat1 = createStat("col1", "int", 50, 100, 4.0);
    ColStatistics stat2 = createStat("col2", "int", 30, -1, 4.0); // unknown numNulls

    PessimisticStatCombiner combiner = new PessimisticStatCombiner(1000);
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(-1, combined.getNumNulls(), "Unknown numNulls (-1) should be propagated");
  }

  @Test
  void testCombinePropagatesUnknownNumTruesFromFirst() {
    ColStatistics stat1 = createStat("col1", "boolean", 2, 5, 1.0);
    stat1.setNumTrues(-1); // unknown
    stat1.setNumFalses(50);

    ColStatistics stat2 = createStat("col2", "boolean", 2, 10, 1.0);
    stat2.setNumTrues(100);
    stat2.setNumFalses(150);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner(1000);
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(-1, combined.getNumTrues(), "Unknown numTrues (-1) should be propagated");
  }

  @Test
  void testCombinePropagatesUnknownNumTruesFromSecond() {
    ColStatistics stat1 = createStat("col1", "boolean", 2, 5, 1.0);
    stat1.setNumTrues(100);
    stat1.setNumFalses(50);

    ColStatistics stat2 = createStat("col2", "boolean", 2, 10, 1.0);
    stat2.setNumTrues(-1); // unknown
    stat2.setNumFalses(150);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner(1000);
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(-1, combined.getNumTrues(), "Unknown numTrues (-1) should be propagated");
  }

  @Test
  void testCombinePropagatesUnknownNumFalsesFromFirst() {
    ColStatistics stat1 = createStat("col1", "boolean", 2, 5, 1.0);
    stat1.setNumTrues(100);
    stat1.setNumFalses(-1); // unknown

    ColStatistics stat2 = createStat("col2", "boolean", 2, 10, 1.0);
    stat2.setNumTrues(50);
    stat2.setNumFalses(150);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner(1000);
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(-1, combined.getNumFalses(), "Unknown numFalses (-1) should be propagated");
  }

  @Test
  void testCombinePropagatesUnknownNumFalsesFromSecond() {
    ColStatistics stat1 = createStat("col1", "boolean", 2, 5, 1.0);
    stat1.setNumTrues(100);
    stat1.setNumFalses(50);

    ColStatistics stat2 = createStat("col2", "boolean", 2, 10, 1.0);
    stat2.setNumTrues(50);
    stat2.setNumFalses(-1); // unknown

    PessimisticStatCombiner combiner = new PessimisticStatCombiner(1000);
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(-1, combined.getNumFalses(), "Unknown numFalses (-1) should be propagated");
  }

  @Test
  void testCombineBothUnknownNumNulls() {
    ColStatistics stat1 = createStat("col1", "int", 50, -1, 4.0);
    ColStatistics stat2 = createStat("col2", "int", 30, -1, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner(1000);
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(-1, combined.getNumNulls(), "Both unknown should result in unknown (-1)");
  }

  @Test
  void testCombineBothUnknownNumTruesAndNumFalses() {
    ColStatistics stat1 = createStat("col1", "boolean", 2, 5, 1.0);
    stat1.setNumTrues(-1);
    stat1.setNumFalses(-1);

    ColStatistics stat2 = createStat("col2", "boolean", 2, 10, 1.0);
    stat2.setNumTrues(-1);
    stat2.setNumFalses(-1);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner(1000);
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(-1, combined.getNumTrues(), "Both unknown should result in unknown (-1)");
    assertEquals(-1, combined.getNumFalses(), "Both unknown should result in unknown (-1)");
  }

  @Test
  void testNullConstantDoesNotContributeToNdv() {
    long numRows = 100;
    ColStatistics nullConstant = createStat("null", "int", 0, numRows, 0.0);
    ColStatistics regularStat = createStat("col", "int", 50, 10, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner(numRows);
    combiner.add(nullConstant);
    combiner.add(regularStat);

    ColStatistics result = combiner.getResult().get();
    assertEquals(50, result.getCountDistint(), "NULL constant should not contribute to NDV");
  }

  @Test
  void testNullConstantAsSecondDoesNotContributeToNdv() {
    long numRows = 100;
    ColStatistics regularStat = createStat("col", "int", 50, 10, 4.0);
    ColStatistics nullConstant = createStat("null", "int", 0, numRows, 0.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner(numRows);
    combiner.add(regularStat);
    combiner.add(nullConstant);

    ColStatistics result = combiner.getResult().get();
    assertEquals(50, result.getCountDistint(), "NULL constant should not contribute to NDV");
  }

  @Test
  void testMultipleNullConstantsResultInZeroNdv() {
    long numRows = 100;
    ColStatistics nullConstant1 = createStat("null1", "int", 0, numRows, 0.0);
    ColStatistics nullConstant2 = createStat("null2", "int", 0, numRows, 0.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner(numRows);
    combiner.add(nullConstant1);
    combiner.add(nullConstant2);

    ColStatistics result = combiner.getResult().get();
    assertEquals(0, result.getCountDistint(), "Multiple NULL constants should result in NDV=0");
    assertEquals(numRows, result.getNumNulls(), "numNulls should be numRows");
  }

  @Test
  void testUnknownNdvNotConfusedWithNullConstant() {
    long numRows = 100;
    ColStatistics unknownNdv = createStat("col", "int", 0, 10, 4.0);
    ColStatistics regularStat = createStat("col2", "int", 50, 5, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner(numRows);
    combiner.add(unknownNdv);
    combiner.add(regularStat);

    ColStatistics result = combiner.getResult().get();
    assertEquals(0, result.getCountDistint(), "Unknown NDV should propagate as 0");
  }

  private ColStatistics createStat(String name, String type, long ndv, long numNulls, double avgColLen) {
    ColStatistics stat = new ColStatistics(name, type);
    stat.setCountDistint(ndv);
    stat.setNumNulls(numNulls);
    stat.setAvgColLen(avgColLen);
    return stat;
  }
}
