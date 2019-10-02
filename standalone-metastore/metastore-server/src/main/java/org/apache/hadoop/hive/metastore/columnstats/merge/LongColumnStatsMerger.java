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
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;

import static org.apache.hadoop.hive.metastore.columnstats.ColumnsStatsUtils.longInspectorFromStats;

public class LongColumnStatsMerger extends ColumnStatsMerger {
  @Override
  public void merge(ColumnStatisticsObj aggregateColStats, ColumnStatisticsObj newColStats) {
    LongColumnStatsDataInspector aggregateData = longInspectorFromStats(aggregateColStats);
    LongColumnStatsDataInspector newData = longInspectorFromStats(newColStats);
    setLowValue(aggregateData, newData);
    setHighValue(aggregateData, newData);
    aggregateData.setNumNulls(aggregateData.getNumNulls() + newData.getNumNulls());
    if (aggregateData.getNdvEstimator() == null || newData.getNdvEstimator() == null) {
      aggregateData.setNumDVs(Math.max(aggregateData.getNumDVs(), newData.getNumDVs()));
    } else {
      NumDistinctValueEstimator oldEst = aggregateData.getNdvEstimator();
      NumDistinctValueEstimator newEst = newData.getNdvEstimator();
      long ndv = -1;
      if (oldEst.canMerge(newEst)) {
        oldEst.mergeEstimators(newEst);
        ndv = oldEst.estimateNumDistinctValues();
        aggregateData.setNdvEstimator(oldEst);
      } else {
        ndv = Math.max(aggregateData.getNumDVs(), newData.getNumDVs());
      }
      LOG.debug("Use bitvector to merge column " + aggregateColStats.getColName() + "'s ndvs of "
          + aggregateData.getNumDVs() + " and " + newData.getNumDVs() + " to be " + ndv);
      aggregateData.setNumDVs(ndv);
    }

    aggregateColStats.getStatsData().setLongStats(aggregateData);
  }

  private void setLowValue(LongColumnStatsDataInspector aggregateData, LongColumnStatsDataInspector newData) {
    if (!aggregateData.isSetLowValue() && !newData.isSetLowValue()) {
      return;
    }
    long lowValue = Math.min(
        aggregateData.isSetLowValue() ? aggregateData.getLowValue() : Long.MAX_VALUE,
        newData.isSetLowValue() ? newData.getLowValue() : Long.MAX_VALUE);
    aggregateData.setLowValue(lowValue);
  }

  private void setHighValue(LongColumnStatsDataInspector aggregateData, LongColumnStatsDataInspector newData) {
    if (!aggregateData.isSetHighValue() && !newData.isSetHighValue()) {
      return;
    }
    long highValue = Math.max(
        aggregateData.isSetHighValue() ? aggregateData.getHighValue() : Long.MIN_VALUE,
        newData.isSetHighValue() ? newData.getHighValue() : Long.MIN_VALUE);
    aggregateData.setHighValue(highValue);
  }
}
