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

package org.apache.hadoop.hive.metastore.columnstats.merge;

import org.apache.hadoop.hive.common.histogram.KllHistogramEstimator;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hive.metastore.columnstats.ColumnsStatsUtils.longInspectorFromStats;

public class LongColumnStatsMerger extends ColumnStatsMerger<Long> {

  private static final Logger LOG = LoggerFactory.getLogger(LongColumnStatsMerger.class);

  @Override
  public void merge(ColumnStatisticsObj aggregateColStats, ColumnStatisticsObj newColStats) {
    LOG.debug("Merging statistics: [aggregateColStats:{}, newColStats: {}]", aggregateColStats, newColStats);

    LongColumnStatsDataInspector aggregateData = longInspectorFromStats(aggregateColStats);
    LongColumnStatsDataInspector newData = longInspectorFromStats(newColStats);

    Long lowValue = mergeLowValue(getLowValue(aggregateData), getLowValue(newData));
    if (lowValue != null) {
      aggregateData.setLowValue(lowValue);
    }
    Long highValue = mergeHighValue(getHighValue(aggregateData), getHighValue(newData));
    if (highValue != null) {
      aggregateData.setHighValue(highValue);
    }
    aggregateData.setNumNulls(mergeNumNulls(aggregateData.getNumNulls(), newData.getNumNulls()));

    NumDistinctValueEstimator oldNDVEst = aggregateData.getNdvEstimator();
    NumDistinctValueEstimator newNDVEst = newData.getNdvEstimator();
    List<NumDistinctValueEstimator> ndvEstimatorsList = Arrays.asList(oldNDVEst, newNDVEst);
    aggregateData.setNumDVs(mergeNumDistinctValueEstimator(aggregateColStats.getColName(),
        ndvEstimatorsList, aggregateData.getNumDVs(), newData.getNumDVs()));
    aggregateData.setNdvEstimator(ndvEstimatorsList.get(0));

    KllHistogramEstimator oldKllEst = aggregateData.getHistogramEstimator();
    KllHistogramEstimator newKllEst = newData.getHistogramEstimator();
    aggregateData.setHistogramEstimator(mergeHistogramEstimator(aggregateColStats.getColName(), oldKllEst, newKllEst));

    aggregateColStats.getStatsData().setLongStats(aggregateData);
  }

  public Long getLowValue(LongColumnStatsDataInspector data) {
    return data.isSetLowValue() ? data.getLowValue() : null;
  }

  public Long getHighValue(LongColumnStatsDataInspector data) {
    return data.isSetHighValue() ? data.getHighValue() : null;
  }

  @Override
  public Long mergeLowValue(Long oldValue, Long newValue) {
    if (oldValue != null && newValue != null) {
      return Math.min(oldValue, newValue);
    } else if (oldValue != null) {
      return oldValue;
    }
    // it can be null
    return newValue;
  }

  @Override
  public Long mergeHighValue(Long oldValue, Long newValue) {
    if (oldValue != null && newValue != null) {
      return Math.max(oldValue, newValue);
    } else if (oldValue != null) {
      return oldValue;
    }
    // it can be null
    return newValue;
  }
}
