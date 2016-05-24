/**
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
package org.apache.hadoop.hive.metastore.hbase.stats;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;

public interface IExtrapolatePartStatus {
  // The following function will extrapolate the stats when the column stats of
  // some partitions are missing.
  /**
   * @param extrapolateData
   *          it will carry back the specific stats, e.g., DOUBLE_STATS or
   *          LONG_STATS
   * @param numParts
   *          the total number of partitions
   * @param numPartsWithStats
   *          the number of partitions that have stats
   * @param adjustedIndexMap
   *          the partition name to index map
   * @param adjustedStatsMap
   *          the partition name to its stats map
   * @param densityAvg
   *          the average of ndv density, which is useful when
   *          useDensityFunctionForNDVEstimation is true.
   */
  public abstract void extrapolate(ColumnStatisticsData extrapolateData, int numParts,
      int numPartsWithStats, Map<String, Double> adjustedIndexMap,
      Map<String, ColumnStatisticsData> adjustedStatsMap, double densityAvg);

}
