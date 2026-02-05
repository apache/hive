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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.junit.jupiter.api.Test;

class TestBranchingStatEstimator {

  @Test
  void testMultipleDistinctConstantsDominates() {
    BranchingStatEstimator estimator = new SimpleBranchingStatEstimator(5);

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats(1),
        createColStats(1),
        createColStats(1),
        createColStats(1),
        createColStats(1),
        createColStats(3)));

    assertTrue(result.isPresent());
    assertEquals(5, result.get().getCountDistint());
  }

  @Test
  void testColumnNdvDominates() {
    BranchingStatEstimator estimator = new SimpleBranchingStatEstimator(2);

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats(1),
        createColStats(1),
        createColStats(100)));

    assertTrue(result.isPresent());
    assertEquals(100, result.get().getCountDistint());
  }

  @Test
  void testSingleConstantNoSyntheticAdded() {
    BranchingStatEstimator estimator = new SimpleBranchingStatEstimator(1);

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats(1),
        createColStats(50)));

    assertTrue(result.isPresent());
    assertEquals(50, result.get().getCountDistint());
  }

  @Test
  void testNoConstantsNoSyntheticAdded() {
    BranchingStatEstimator estimator = new SimpleBranchingStatEstimator(0);

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats(100),
        createColStats(200)));

    assertTrue(result.isPresent());
    assertEquals(200, result.get().getCountDistint());
  }

  @Test
  void testUnknownNdvPropagates() {
    BranchingStatEstimator estimator = new SimpleBranchingStatEstimator(3);

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats(1),
        createColStats(1),
        createColStats(1),
        createColStats(0)));

    assertTrue(result.isPresent());
    assertEquals(0, result.get().getCountDistint());
  }

  @Test
  void testAllConstantsKnown() {
    BranchingStatEstimator estimator = new SimpleBranchingStatEstimator(3);

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(
        createColStats(1),
        createColStats(1),
        createColStats(1)));

    assertTrue(result.isPresent());
    assertEquals(3, result.get().getCountDistint());
  }

  @Test
  void testMaxAvgColLenPreserved() {
    BranchingStatEstimator estimator = new SimpleBranchingStatEstimator(2);

    ColStatistics stat1 = createColStats(1);
    stat1.setAvgColLen(10.0);
    ColStatistics stat2 = createColStats(1);
    stat2.setAvgColLen(25.0);
    ColStatistics stat3 = createColStats(50);
    stat3.setAvgColLen(15.0);

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(stat1, stat2, stat3));

    assertTrue(result.isPresent());
    assertEquals(25.0, result.get().getAvgColLen());
  }

  private ColStatistics createColStats(long ndv) {
    ColStatistics cs = new ColStatistics("col", "string");
    cs.setCountDistint(ndv);
    cs.setNumNulls(0);
    cs.setAvgColLen(10.0);
    return cs;
  }

  static class SimpleBranchingStatEstimator extends BranchingStatEstimator {
    SimpleBranchingStatEstimator(int numberOfDistinctConstants) {
      super(numberOfDistinctConstants);
    }

    @Override
    protected void addBranchStats(PessimisticStatCombiner combiner, List<ColStatistics> argStats) {
      for (ColStatistics stat : argStats) {
        combiner.add(stat);
      }
    }
  }
}
