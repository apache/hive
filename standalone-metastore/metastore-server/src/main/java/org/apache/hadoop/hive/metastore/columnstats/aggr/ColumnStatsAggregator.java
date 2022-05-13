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

package org.apache.hadoop.hive.metastore.columnstats.aggr;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.ColStatsObjWithSourceInfo;

public abstract class ColumnStatsAggregator {
  public boolean useDensityFunctionForNDVEstimation;
  /**
   * The tuner controls the derivation of the NDV value when aggregating statistics from multiple partitions. It accepts
   * values in the range [0, 1] pushing the aggregated NDV closer to the lower, or upper bound respectively.
   * <p>
   * For example, consider the aggregation of three partitions with NDV values 2, 3, and 4, respectively. The NDV
   * lower bound is 4 (the highest among individual NDVs), and the upper bound is 9 (the sum of individual NDVs). In
   * this case the aggregated NDV will be in the range [4, 9] touching the bounds when the tuner is equal to 0, or 1
   * respectively.
   * </p>
   * <p>
   * It is optional and concrete implementations can choose to ignore it completely.
   * </p>
   */
  public double ndvTuner;

  public abstract ColumnStatisticsObj aggregate(
      List<ColStatsObjWithSourceInfo> colStatsWithSourceInfo, List<String> partNames,
      boolean areAllPartsFound) throws MetaException;

  void checkStatisticsList(List<ColStatsObjWithSourceInfo> colStatsWithSourceInfo) {
    if (colStatsWithSourceInfo.isEmpty()) {
      throw new IllegalArgumentException("Column statistics list must not be empty when aggregating");
    }
  }
}
