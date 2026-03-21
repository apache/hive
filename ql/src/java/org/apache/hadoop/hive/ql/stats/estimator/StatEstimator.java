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

import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hive.ql.plan.ColStatistics;

/**
 * Enables statistics related computation on UDFs.
 *
 * <p>This interface provides two default implementations:
 * <ul>
 *   <li>{@link #estimate(List)} - clones the first argument's statistics (suitable for most UDFs)</li>
 *   <li>{@link #estimate(List, long)} - calls estimate(List) and caps NDV at numRows</li>
 * </ul>
 *
 * <p>UDFs that simply pass through statistics (like LOWER, UPPER) can use the defaults.
 * UDFs that combine statistics (like IF, WHEN, COALESCE) should override {@link #estimate(List)}.
 */
public interface StatEstimator {

  /**
   * Computes the output statistics of the actual UDF.
   *
   * <p>The default implementation clones the first argument's statistics, which is suitable
   * for most UDFs that don't significantly alter the statistical properties of their input.
   *
   * <p>Override this method for UDFs that combine multiple inputs (like IF, WHEN, COALESCE)
   * or significantly transform the data.
   *
   * @param argStats the statistics for every argument of the UDF
   * @return {@link ColStatistics} estimate for the actual UDF, or empty if estimation is not possible
   */
  default Optional<ColStatistics> estimate(List<ColStatistics> argStats) {
    if (argStats.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(argStats.get(0).clone());
  }

  /**
   * Computes the output statistics of the actual UDF, ensuring NDV does not exceed numRows.
   *
   * <p>The default implementation calls {@link #estimate(List)} and caps the NDV at numRows.
   * This ensures that estimators which combine statistics from multiple branches (producing
   * potentially inflated NDV values) are automatically bounded by the number of rows.
   *
   * @param argStats the statistics for every argument of the UDF
   * @param numRows the number of rows, used to cap the NDV
   * @return {@link ColStatistics} estimate for the actual UDF with NDV capped at numRows
   */
  default Optional<ColStatistics> estimate(List<ColStatistics> argStats, long numRows) {
    return estimate(argStats).map(cs -> {
      cs.setCountDistint(Math.min(cs.getCountDistint(), numRows));
      return cs;
    });
  }
}
