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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.junit.jupiter.api.Test;

class TestStatEstimator {

  @Test
  void testDefaultEstimateWithEmptyList() {
    StatEstimator estimator = new StatEstimator() {};
    Optional<ColStatistics> result = estimator.estimate(Collections.emptyList());
    assertFalse(result.isPresent(), "Empty list should return empty Optional");
  }

  @Test
  void testDefaultEstimateClonesFirstArg() {
    StatEstimator estimator = new StatEstimator() {};
    ColStatistics stat = createStat("col1", "int", 100, 10, 4.0);

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(stat));

    assertTrue(result.isPresent());
    assertEquals(100, result.get().getCountDistint());
    assertEquals(10, result.get().getNumNulls());
    assertEquals(4.0, result.get().getAvgColLen());
  }

  @Test
  void testDefaultEstimateReturnsCloneNotSameReference() {
    StatEstimator estimator = new StatEstimator() {};
    ColStatistics stat = createStat("col1", "int", 100, 10, 4.0);

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(stat));

    assertTrue(result.isPresent());
    assertNotSame(stat, result.get(), "Should return a clone, not the same reference");
    stat.setCountDistint(999);
    assertEquals(100, result.get().getCountDistint(), "Clone should not be affected by original changes");
  }

  @Test
  void testDefaultEstimateIgnoresSubsequentArgs() {
    StatEstimator estimator = new StatEstimator() {};
    ColStatistics stat1 = createStat("col1", "int", 100, 10, 4.0);
    ColStatistics stat2 = createStat("col2", "int", 200, 20, 8.0);

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(stat1, stat2));

    assertTrue(result.isPresent());
    assertEquals(100, result.get().getCountDistint(), "Should use first arg's NDV");
    assertEquals(10, result.get().getNumNulls(), "Should use first arg's numNulls");
  }

  @Test
  void testDefaultEstimateWithNumRowsCapsNdv() {
    StatEstimator estimator = new StatEstimator() {};
    ColStatistics stat = createStat("col1", "int", 1000, 10, 4.0);

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(stat), 500);

    assertTrue(result.isPresent());
    assertEquals(500, result.get().getCountDistint(), "NDV should be capped at numRows");
  }

  @Test
  void testDefaultEstimateWithNumRowsNoCappingNeeded() {
    StatEstimator estimator = new StatEstimator() {};
    ColStatistics stat = createStat("col1", "int", 100, 10, 4.0);

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(stat), 500);

    assertTrue(result.isPresent());
    assertEquals(100, result.get().getCountDistint(), "NDV should remain unchanged when less than numRows");
  }

  @Test
  void testDefaultEstimateWithNumRowsExactlyEqual() {
    StatEstimator estimator = new StatEstimator() {};
    ColStatistics stat = createStat("col1", "int", 500, 10, 4.0);

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(stat), 500);

    assertTrue(result.isPresent());
    assertEquals(500, result.get().getCountDistint(), "NDV should remain unchanged when equal to numRows");
  }

  @Test
  void testDefaultEstimateWithNumRowsEmptyList() {
    StatEstimator estimator = new StatEstimator() {};

    Optional<ColStatistics> result = estimator.estimate(Collections.emptyList(), 500);

    assertFalse(result.isPresent(), "Empty list should return empty Optional");
  }

  @Test
  void testDefaultEstimateWithNumRowsPreservesOtherStats() {
    StatEstimator estimator = new StatEstimator() {};
    ColStatistics stat = createStat("col1", "int", 1000, 10, 4.0);
    stat.setNumTrues(50);
    stat.setNumFalses(40);

    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(stat), 500);

    assertTrue(result.isPresent());
    assertEquals(500, result.get().getCountDistint(), "NDV should be capped");
    assertEquals(10, result.get().getNumNulls(), "numNulls should be preserved");
    assertEquals(4.0, result.get().getAvgColLen(), "avgColLen should be preserved");
  }

  @Test
  void testStatEstimatorProviderDefaultReturnsWorkingEstimator() {
    StatEstimatorProvider provider = new StatEstimatorProvider() {};
    StatEstimator estimator = provider.getStatEstimator();

    ColStatistics stat = createStat("col1", "int", 100, 10, 4.0);
    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(stat));

    assertTrue(result.isPresent());
    assertEquals(100, result.get().getCountDistint());
  }

  @Test
  void testStatEstimatorProviderDefaultCapsNdv() {
    StatEstimatorProvider provider = new StatEstimatorProvider() {};
    StatEstimator estimator = provider.getStatEstimator();

    ColStatistics stat = createStat("col1", "int", 1000, 10, 4.0);
    Optional<ColStatistics> result = estimator.estimate(Arrays.asList(stat), 500);

    assertTrue(result.isPresent());
    assertEquals(500, result.get().getCountDistint(), "Default provider estimator should cap NDV");
  }

  private ColStatistics createStat(String name, String type, long ndv, long numNulls, double avgColLen) {
    ColStatistics stat = new ColStatistics(name, type);
    stat.setCountDistint(ndv);
    stat.setNumNulls(numNulls);
    stat.setAvgColLen(avgColLen);
    return stat;
  }
}
