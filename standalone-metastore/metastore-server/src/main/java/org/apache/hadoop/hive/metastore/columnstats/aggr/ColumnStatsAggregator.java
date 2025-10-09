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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.hadoop.hive.common.histogram.KllHistogramEstimator;
import org.apache.hadoop.hive.common.histogram.KllHistogramEstimatorFactory;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.ColStatsObjWithSourceInfo;

import static org.apache.hadoop.hive.metastore.columnstats.ColumnsStatsUtils.dateInspectorFromStats;
import static org.apache.hadoop.hive.metastore.columnstats.ColumnsStatsUtils.decimalInspectorFromStats;
import static org.apache.hadoop.hive.metastore.columnstats.ColumnsStatsUtils.doubleInspectorFromStats;
import static org.apache.hadoop.hive.metastore.columnstats.ColumnsStatsUtils.longInspectorFromStats;
import static org.apache.hadoop.hive.metastore.columnstats.ColumnsStatsUtils.timestampInspectorFromStats;

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

  protected abstract ColumnStatisticsData initColumnStatisticsData();

  protected KllHistogramEstimator mergeHistograms(List<ColStatsObjWithSourceInfo> colStatsWithSourceInfo) {
    // invariant: no two elements of this list are merge-compatible
    final List<KllHistogramEstimator> mergedHistogramEstimators = new ArrayList<>();
    final BitSet mergedStats = new BitSet(colStatsWithSourceInfo.size());
    int currIndex = 0;

    while (mergedStats.cardinality() != colStatsWithSourceInfo.size()) {
      currIndex = mergedStats.nextClearBit(currIndex);
      final ColumnStatisticsObj currColStatsObj = colStatsWithSourceInfo.get(currIndex).getColStatsObj();
      final KllHistogramEstimator statsEstimator = getHistogramFromStats(currColStatsObj);
      if (statsEstimator == null) {
        mergedStats.set(currIndex); // so we can move past it
        continue;
      }
      final KllHistogramEstimator currHistogram = KllHistogramEstimatorFactory.getEmptyHistogramEstimator(statsEstimator);
      // we need to create a new estimator to not alter the one from the stats object via merging
      currHistogram.mergeEstimators(statsEstimator);

      // check if the histogram can be merged an existing element in the final list
      for (KllHistogramEstimator candidateHistogram : mergedHistogramEstimators) {
        if (candidateHistogram.canMerge(currHistogram)) {
          candidateHistogram.mergeEstimators(currHistogram);
          mergedStats.set(currIndex);
          break;
        }
      }

      // if it has not been merged, then store it in the final list
      if (!mergedStats.get(currIndex)) {
        mergedHistogramEstimators.add(currHistogram);
        mergedStats.set(currIndex);
      }
    }

    // find the histogram with largest N
    long biggestN = -1;
    KllHistogramEstimator largestHistogramEstimator = null;
    for (KllHistogramEstimator hist : mergedHistogramEstimators) {
      if (hist.getSketch().getN() > biggestN) {
        biggestN = hist.getSketch().getN();
        largestHistogramEstimator = hist;
      }
    }

    // set the histogram with largest N
    return largestHistogramEstimator;
  }

  private KllHistogramEstimator getHistogramFromStats(ColumnStatisticsObj currColStatsObj) {
    ColumnStatisticsData columnStatisticsData = currColStatsObj.getStatsData();

    if (columnStatisticsData.isSetDateStats()) {
      return dateInspectorFromStats(currColStatsObj).getHistogramEstimator();
    }
    if (columnStatisticsData.isSetDecimalStats()) {
      return decimalInspectorFromStats(currColStatsObj).getHistogramEstimator();
    }
    if (columnStatisticsData.isSetDoubleStats()) {
      return doubleInspectorFromStats(currColStatsObj).getHistogramEstimator();
    }
    if (columnStatisticsData.isSetLongStats()) {
      return longInspectorFromStats(currColStatsObj).getHistogramEstimator();
    }
    if (columnStatisticsData.isSetTimestampStats()) {
      return timestampInspectorFromStats(currColStatsObj).getHistogramEstimator();
    }

    throw new IllegalArgumentException(currColStatsObj.getColType() + " is not supported for merging column stats histograms");
  }
}
