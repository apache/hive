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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.columnstats.merge;

import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimatorFactory;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;

public class DecimalColumnStatsMerger extends ColumnStatsMerger {
  @Override
  public void merge(ColumnStatisticsObj aggregateColStats, ColumnStatisticsObj newColStats) {
    DecimalColumnStatsData aggregateData = aggregateColStats.getStatsData().getDecimalStats();
    DecimalColumnStatsData newData = newColStats.getStatsData().getDecimalStats();
    Decimal lowValue = aggregateData.getLowValue() != null
        && (aggregateData.getLowValue().compareTo(newData.getLowValue()) > 0) ? aggregateData
        .getLowValue() : newData.getLowValue();
    aggregateData.setLowValue(lowValue);
    Decimal highValue = aggregateData.getHighValue() != null
        && (aggregateData.getHighValue().compareTo(newData.getHighValue()) > 0) ? aggregateData
        .getHighValue() : newData.getHighValue();
    aggregateData.setHighValue(highValue);
    aggregateData.setNumNulls(aggregateData.getNumNulls() + newData.getNumNulls());
    if (!aggregateData.isSetBitVectors() || aggregateData.getBitVectors().length() == 0
        || !newData.isSetBitVectors() || newData.getBitVectors().length() == 0) {
      aggregateData.setNumDVs(Math.max(aggregateData.getNumDVs(), newData.getNumDVs()));
    } else {
      NumDistinctValueEstimator oldEst = NumDistinctValueEstimatorFactory
          .getNumDistinctValueEstimator(aggregateData.getBitVectors());
      NumDistinctValueEstimator newEst = NumDistinctValueEstimatorFactory
          .getNumDistinctValueEstimator(newData.getBitVectors());
      long ndv = -1;
      if (oldEst.canMerge(newEst)) {
        oldEst.mergeEstimators(newEst);
        ndv = oldEst.estimateNumDistinctValues();
        aggregateData.setBitVectors(oldEst.serialize());
      } else {
        ndv = Math.max(aggregateData.getNumDVs(), newData.getNumDVs());
      }
      LOG.debug("Use bitvector to merge column " + aggregateColStats.getColName() + "'s ndvs of "
          + aggregateData.getNumDVs() + " and " + newData.getNumDVs() + " to be " + ndv);
      aggregateData.setNumDVs(ndv);
    }
  }
}
