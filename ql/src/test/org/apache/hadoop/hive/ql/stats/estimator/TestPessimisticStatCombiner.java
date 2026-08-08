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

import java.util.stream.Stream;

import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

  @ParameterizedTest(name = "{0}")
  @MethodSource("combineCountDistinctCases")
  void testCombineCountDistinctMerge(String scenarioName, long stat1Ndv, long stat2Ndv, long expectedNdv) {
    ColStatistics stat1 = createStat("col1", "int", stat1Ndv, 5, 4.0);
    ColStatistics stat2 = createStat("col2", "int", stat2Ndv, 10, 4.0);

    PessimisticStatCombiner combiner = new PessimisticStatCombiner();
    combiner.add(stat1);
    combiner.add(stat2);

    ColStatistics combined = combiner.getResult().get();
    assertEquals(expectedNdv, combined.getCountDistint(),
        "countDistinct after PROPAGATE combine");
  }

  private static Stream<Arguments> combineCountDistinctCases() {
    return Stream.of(
        Arguments.of("firstUnknownPropagates",      -1L, 50L, -1L),
        Arguments.of("secondUnknownPropagates",     50L, -1L, -1L),
        Arguments.of("bothUnknownStaysUnknown",     -1L, -1L, -1L),
        Arguments.of("picksHigherWhenSecondHigher", 30L, 50L, 50L),
        Arguments.of("keepsHigherWhenFirstHigher",  50L, 30L, 50L)
    );
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

  private ColStatistics createStat(String name, String type, long ndv, long numNulls, double avgColLen) {
    ColStatistics stat = new ColStatistics(name, type);
    stat.setCountDistint(ndv);
    stat.setNumNulls(numNulls);
    stat.setAvgColLen(avgColLen);
    return stat;
  }
}
