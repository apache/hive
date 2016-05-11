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

package org.apache.hadoop.hive.metastore.hbase.stats.merge;

import org.apache.hadoop.hive.metastore.NumDistinctValueEstimator;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;

public class LongColumnStatsMerger extends ColumnStatsMerger {
  @Override
  public void merge(ColumnStatisticsObj aggregateColStats, ColumnStatisticsObj newColStats) {
    LongColumnStatsData aggregateData = aggregateColStats.getStatsData().getLongStats();
    LongColumnStatsData newData = newColStats.getStatsData().getLongStats();
    aggregateData.setLowValue(Math.min(aggregateData.getLowValue(), newData.getLowValue()));
    aggregateData.setHighValue(Math.max(aggregateData.getHighValue(), newData.getHighValue()));
    aggregateData.setNumNulls(aggregateData.getNumNulls() + newData.getNumNulls());
    if (ndvEstimator == null || !newData.isSetBitVectors() || newData.getBitVectors().length() == 0) {
      aggregateData.setNumDVs(Math.max(aggregateData.getNumDVs(), newData.getNumDVs()));
    } else {
      ndvEstimator.mergeEstimators(new NumDistinctValueEstimator(aggregateData.getBitVectors(),
          ndvEstimator.getnumBitVectors()));
      ndvEstimator.mergeEstimators(new NumDistinctValueEstimator(newData.getBitVectors(),
          ndvEstimator.getnumBitVectors()));
      long ndv = ndvEstimator.estimateNumDistinctValues();
      LOG.debug("Use bitvector to merge column " + aggregateColStats.getColName() + "'s ndvs of "
          + aggregateData.getNumDVs() + " and " + newData.getNumDVs() + " to be " + ndv);
      aggregateData.setNumDVs(ndv);
      aggregateData.setBitVectors(ndvEstimator.serialize().toString());
    }
  }
}
