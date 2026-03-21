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
  void testCombinePropagatesUnknownNumNullsFromFirst() {
    ColStatistics stat1 = createStat("col1", "int", 50, -1, 4.0); // unknown numNulls
    ColStatistics stat2 = createStat("col2", "int", 30, 100, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(-1, combined.getNumNulls(), "Unknown numNulls (-1) should be propagated");
  }

  @Test
  void testCombinePropagatesUnknownNumNullsFromSecond() {
    ColStatistics stat1 = createStat("col1", "int", 50, 100, 4.0);
    ColStatistics stat2 = createStat("col2", "int", 30, -1, 4.0); // unknown numNulls

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
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

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
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

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
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

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
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

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(-1, combined.getNumFalses(), "Unknown numFalses (-1) should be propagated");
  }

  @Test
  void testCombineBothUnknownNumNulls() {
    ColStatistics stat1 = createStat("col1", "int", 50, -1, 4.0);
    ColStatistics stat2 = createStat("col2", "int", 30, -1, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
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

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(-1, combined.getNumTrues(), "Both unknown should result in unknown (-1)");
    assertEquals(-1, combined.getNumFalses(), "Both unknown should result in unknown (-1)");
  }

  @Test
  void testCombinePropagatesUnknownNdvFromFirst() {
    ColStatistics stat1 = createStat("col1", "int", 0, 10, 4.0); // NDV=0 means unknown
    ColStatistics stat2 = createStat("col2", "int", 100, 20, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(0, combined.getCountDistint(), "Unknown NDV (0) from first should be propagated");
  }

  @Test
  void testCombinePropagatesUnknownNdvFromSecond() {
    ColStatistics stat1 = createStat("col1", "int", 100, 10, 4.0);
    ColStatistics stat2 = createStat("col2", "int", 0, 20, 4.0); // NDV=0 means unknown

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(0, combined.getCountDistint(), "Unknown NDV (0) from second should be propagated");
  }

  @Test
  void testCombineBothUnknownNdv() {
    ColStatistics stat1 = createStat("col1", "int", 0, 10, 4.0);
    ColStatistics stat2 = createStat("col2", "int", 0, 20, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(0, combined.getCountDistint(), "Both unknown NDV should result in unknown (0)");
  }

  @Test
  void testCombineSumsNdvWhenBothKnown() {
    ColStatistics stat1 = createStat("col1", "int", 50, 10, 4.0);
    ColStatistics stat2 = createStat("col2", "int", 30, 20, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(80, combined.getCountDistint(), "Known NDVs should be summed");
  }

  @Test
  void testCombineNdvOverflowProtection() {
    ColStatistics stat1 = createStat("col1", "int", Long.MAX_VALUE - 10, 10, 4.0);
    ColStatistics stat2 = createStat("col2", "int", 100, 20, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(Long.MAX_VALUE, combined.getCountDistint(), "NDV overflow should be capped at Long.MAX_VALUE");
  }

  @Test
  void testCombineThreeStats() {
    ColStatistics stat1 = createStat("col1", "int", 10, 5, 4.0);
    ColStatistics stat2 = createStat("col2", "int", 20, 10, 4.0);
    ColStatistics stat3 = createStat("col3", "int", 30, 15, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);
    combiner.add(stat3);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(60, combined.getCountDistint(), "Three NDVs should be summed");
    assertEquals(15, combined.getNumNulls(), "Should take max numNulls");
  }

  @Test
  void testCombineUnknownNdvInMiddle() {
    ColStatistics stat1 = createStat("col1", "int", 10, 5, 4.0);
    ColStatistics stat2 = createStat("col2", "int", 0, 10, 4.0); // unknown
    ColStatistics stat3 = createStat("col3", "int", 30, 15, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);
    combiner.add(stat3);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(0, combined.getCountDistint(), "Unknown NDV in middle should propagate");
  }

  @Test
  void testConstantWithNdvZeroIsNotTreatedAsUnknown() {
    ColStatistics stat1 = createStat("col1", "string", 1, 0, 5.0);
    ColStatistics stat2 = createConstStat("const", "string", 0, 1000, 5.0); // NULL constant: NDV=0, isConst=true

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(1, combined.getCountDistint(), "Constant with NDV=0 should not propagate as unknown");
  }

  @Test
  void testNullConstantFirstThenOtherConstants() {
    ColStatistics nullConst = createConstStat("null", "string", 0, 1000, 5.0); // NULL constant
    ColStatistics constA = createConstStat("A", "string", 1, 0, 5.0);
    ColStatistics constB = createConstStat("B", "string", 1, 0, 5.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(nullConst);
    combiner.add(constA);
    combiner.add(constB);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(2, combined.getCountDistint(), "NULL(0) + A(1) + B(1) should sum to 2");
  }

  @Test
  void testConstantsWithNullInMiddle() {
    ColStatistics constA = createConstStat("A", "string", 1, 0, 5.0);
    ColStatistics nullConst = createConstStat("null", "string", 0, 1000, 5.0); // NULL constant
    ColStatistics constB = createConstStat("B", "string", 1, 0, 5.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(constA);
    combiner.add(nullConst);
    combiner.add(constB);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(2, combined.getCountDistint(), "A(1) + NULL(0) + B(1) should sum to 2");
  }

  @Test
  void testNonConstantNdvZeroStillPropagatesUnknown() {
    ColStatistics stat1 = createStat("col1", "string", 1, 0, 5.0);
    ColStatistics stat2 = createStat("col2", "string", 0, 10, 5.0); // Column with unknown NDV (isConst=false)

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(0, combined.getCountDistint(), "Non-constant with NDV=0 should still propagate as unknown");
  }

  @Test
  void testMixedConstantAndNonConstantWithNdvZero() {
    ColStatistics constStat = createConstStat("const", "string", 0, 1000, 5.0); // NULL constant
    ColStatistics colStat = createStat("col", "string", 0, 10, 5.0); // Column with unknown NDV

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(constStat);
    combiner.add(colStat);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(0, combined.getCountDistint(), "Non-constant with NDV=0 should propagate unknown even if combined with constant");
  }

  @Test
  void testCombinedResultIsNotConst() {
    ColStatistics constA = createConstStat("A", "string", 1, 0, 5.0);
    ColStatistics constB = createConstStat("B", "string", 1, 0, 5.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(constA);
    combiner.add(constB);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(false, combined.isConst(), "Combined result should not be marked as constant");
  }

  private ColStatistics createStat(String name, String type, long ndv, long numNulls, double avgColLen) {
    ColStatistics stat = new ColStatistics(name, type);
    stat.setCountDistint(ndv);
    stat.setNumNulls(numNulls);
    stat.setAvgColLen(avgColLen);
    return stat;
  }

  private ColStatistics createConstStat(String name, String type, long ndv, long numNulls, double avgColLen) {
    ColStatistics stat = createStat(name, type, ndv, numNulls, avgColLen);
    stat.setConst(true);
    return stat;
  }
}
