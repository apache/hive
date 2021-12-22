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

import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;

import com.google.common.base.MoreObjects;

import static org.apache.hadoop.hive.metastore.columnstats.ColumnsStatsUtils.decimalInspectorFromStats;

import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DecimalColumnStatsMerger extends ColumnStatsMerger {

  private static final Logger LOG = LoggerFactory.getLogger(DecimalColumnStatsMerger.class);

  @Override
  public void merge(ColumnStatisticsObj aggregateColStats, ColumnStatisticsObj newColStats) {
    LOG.debug("Merging statistics: [aggregateColStats:{}, newColStats: {}]", aggregateColStats, newColStats);

    DecimalColumnStatsDataInspector aggregateData = decimalInspectorFromStats(aggregateColStats);
    DecimalColumnStatsDataInspector newData = decimalInspectorFromStats(newColStats);

    setLowValue(aggregateData, newData);
    setHighValue(aggregateData, newData);

    aggregateData.setNumNulls(aggregateData.getNumNulls() + newData.getNumNulls());

    if (aggregateData.getNdvEstimator() == null || newData.getNdvEstimator() == null) {
      aggregateData.setNumDVs(Math.max(aggregateData.getNumDVs(), newData.getNumDVs()));
    } else {
      NumDistinctValueEstimator oldEst = aggregateData.getNdvEstimator();
      NumDistinctValueEstimator newEst = newData.getNdvEstimator();
      final long ndv;
      if (oldEst.canMerge(newEst)) {
        oldEst.mergeEstimators(newEst);
        ndv = oldEst.estimateNumDistinctValues();
        aggregateData.setNdvEstimator(oldEst);
      } else {
        ndv = Math.max(aggregateData.getNumDVs(), newData.getNumDVs());
      }
      LOG.debug("Use bitvector to merge column {}'s ndvs of {} and {} to be {}", aggregateColStats.getColName(),
          aggregateData.getNumDVs(), newData.getNumDVs(), ndv);
      aggregateData.setNumDVs(ndv);
    }

    aggregateColStats.getStatsData().setDecimalStats(aggregateData);
  }

  public void setLowValue(DecimalColumnStatsDataInspector aggregateData, DecimalColumnStatsDataInspector newData) {
    final Decimal aggregateLowValue = aggregateData.getLowValue();
    final Decimal newLowValue = newData.getLowValue();

    final Decimal mergedLowValue;
    if (!aggregateData.isSetLowValue() && !newData.isSetLowValue()) {
      return;
    } else if (aggregateData.isSetLowValue() && newData.isSetLowValue()) {
      mergedLowValue = ObjectUtils.min(newLowValue, aggregateLowValue);
    } else {
      mergedLowValue = MoreObjects.firstNonNull(aggregateLowValue, newLowValue);
    }

    aggregateData.setLowValue(mergedLowValue);
  }

  public void setHighValue(DecimalColumnStatsDataInspector aggregateData, DecimalColumnStatsDataInspector newData) {
    final Decimal aggregateHighValue = aggregateData.getHighValue();
    final Decimal newHighValue = newData.getHighValue();

    final Decimal mergedHighValue;
    if (!aggregateData.isSetHighValue() && !newData.isSetHighValue()) {
      return;
    } else if (aggregateData.isSetHighValue() && newData.isSetHighValue()) {
      mergedHighValue = ObjectUtils.max(aggregateHighValue, newHighValue);
    } else {
      mergedHighValue = MoreObjects.firstNonNull(aggregateHighValue, newHighValue);
    }

    aggregateData.setHighValue(mergedHighValue);
  }
}
