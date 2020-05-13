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

import static org.apache.hadoop.hive.metastore.columnstats.ColumnsStatsUtils.dateInspectorFromStats;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.columnstats.cache.DateColumnStatsDataInspector;

import com.google.common.base.MoreObjects;

public class DateColumnStatsMerger extends ColumnStatsMerger {

  @Override
  protected void doMerge(ColumnStatisticsObj aggregateColStats, ColumnStatisticsObj newColStats) {
    DateColumnStatsDataInspector aggregateData = dateInspectorFromStats(aggregateColStats);
    DateColumnStatsDataInspector newData = dateInspectorFromStats(newColStats);

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
      log.debug("Use bitvector to merge column {}'s ndvs of {} and {} to be {}", aggregateColStats.getColName(),
          aggregateData.getNumDVs(), newData.getNumDVs(), ndv);
      aggregateData.setNumDVs(ndv);
    }

    aggregateColStats.getStatsData().setDateStats(aggregateData);
  }

  public void setLowValue(DateColumnStatsDataInspector aggregateData, DateColumnStatsDataInspector newData) {
    final Date aggregateLowValue = aggregateData.getLowValue();
    final Date newLowValue = newData.getLowValue();

    final Date mergedLowValue;
    if (!aggregateData.isSetLowValue() && !newData.isSetLowValue()) {
      return;
    } else if (aggregateData.isSetLowValue() && newData.isSetLowValue()) {
      mergedLowValue = ObjectUtils.min(aggregateLowValue, newLowValue);
    } else {
      mergedLowValue = MoreObjects.firstNonNull(aggregateLowValue, newLowValue);
    }

    aggregateData.setLowValue(mergedLowValue);
  }

  public void setHighValue(DateColumnStatsDataInspector aggregateData, DateColumnStatsDataInspector newData) {
    final Date aggregateHighValue = aggregateData.getHighValue();
    final Date newHighValue = newData.getHighValue();

    final Date mergedHighValue;
    if (!aggregateData.isSetHighValue() && !newData.isSetHighValue()) {
      return;
    } else if (aggregateData.isSetHighValue() && newData.isSetHighValue()) {
      mergedHighValue = ObjectUtils.max(newHighValue, aggregateHighValue);
    } else {
      mergedHighValue = MoreObjects.firstNonNull(aggregateHighValue, newHighValue);
    }

    aggregateData.setHighValue(mergedHighValue);
  }
}
