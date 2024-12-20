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
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;

import com.google.common.base.MoreObjects;

import static org.apache.hadoop.hive.metastore.columnstats.ColumnsStatsUtils.decimalInspectorFromStats;

import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class DecimalColumnStatsMerger extends ColumnStatsMerger<Decimal> {

  private static final Logger LOG = LoggerFactory.getLogger(DecimalColumnStatsMerger.class);

  @Override
  public void merge(ColumnStatisticsObj aggregateColStats, ColumnStatisticsObj newColStats) {
    LOG.debug("Merging statistics: [aggregateColStats:{}, newColStats: {}]", aggregateColStats, newColStats);

    DecimalColumnStatsDataInspector aggregateData = decimalInspectorFromStats(aggregateColStats);
    DecimalColumnStatsDataInspector newData = decimalInspectorFromStats(newColStats);

    Decimal lowValue = mergeLowValue(getLowValue(aggregateData), getLowValue(newData));
    if (lowValue != null) {
      aggregateData.setLowValue(lowValue);
    }
    Decimal highValue = mergeHighValue(getHighValue(aggregateData), getHighValue(newData));
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

    aggregateColStats.getStatsData().setDecimalStats(aggregateData);
  }

  public Decimal getLowValue(DecimalColumnStatsDataInspector data) {
    return data.isSetLowValue() ? data.getLowValue() : null;
  }

  public Decimal getHighValue(DecimalColumnStatsDataInspector data) {
    return data.isSetHighValue() ? data.getHighValue() : null;
  }

  @Override
  public Decimal mergeLowValue(Decimal oldValue, Decimal newValue) {
    if (oldValue != null && newValue != null) {
      return ObjectUtils.min(oldValue, newValue);
    }
    if (oldValue != null || newValue != null) {
      return MoreObjects.firstNonNull(oldValue, newValue);
    }
    return null;
  }

  @Override
  public Decimal mergeHighValue(Decimal oldValue, Decimal newValue) {
    if (oldValue != null && newValue != null) {
      return ObjectUtils.max(oldValue, newValue);
    }
    if (oldValue != null || newValue != null) {
      return MoreObjects.firstNonNull(oldValue, newValue);
    }
    return null;
  }
}
