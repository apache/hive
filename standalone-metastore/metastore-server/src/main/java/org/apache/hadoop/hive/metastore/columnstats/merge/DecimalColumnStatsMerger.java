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

import com.google.common.annotations.VisibleForTesting;

import static org.apache.hadoop.hive.metastore.columnstats.ColumnsStatsUtils.decimalInspectorFromStats;

public class DecimalColumnStatsMerger extends ColumnStatsMerger {
  @Override
  public void merge(ColumnStatisticsObj aggregateColStats, ColumnStatisticsObj newColStats) {
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

    aggregateColStats.getStatsData().setDecimalStats(aggregateData);
  }

  @VisibleForTesting
  void setLowValue(DecimalColumnStatsDataInspector aggregateData, DecimalColumnStatsDataInspector newData) {
    if (!aggregateData.isSetLowValue() && !newData.isSetLowValue()) {
      return;
    }

    Decimal aggregateLowValue = aggregateData.getLowValue();
    Decimal newLowValue = newData.getLowValue();

    Decimal mergedLowValue = null;
    if (aggregateData.isSetLowValue() && newData.isSetLowValue()) {
      mergedLowValue = aggregateLowValue.compareTo(newLowValue) > 0 ? newLowValue : aggregateLowValue;
    } else {
      mergedLowValue = aggregateLowValue == null ? newLowValue : aggregateLowValue;
    }

    aggregateData.setLowValue(mergedLowValue);
  }

  @VisibleForTesting
  void setHighValue(DecimalColumnStatsDataInspector aggregateData, DecimalColumnStatsDataInspector newData) {
    if (!aggregateData.isSetHighValue() && !newData.isSetHighValue()) {
      return;
    }

    Decimal aggregateHighValue = aggregateData.getHighValue();
    Decimal newHighValue = newData.getHighValue();

    Decimal mergedHighValue = null;
    if (aggregateData.isSetHighValue() && newData.isSetHighValue()) {
      mergedHighValue = aggregateHighValue.compareTo(newHighValue) > 0 ? aggregateHighValue : newHighValue;
    } else {
      mergedHighValue = aggregateHighValue == null ? newHighValue : aggregateHighValue;
    }

    aggregateData.setHighValue(mergedHighValue);
  }
}
