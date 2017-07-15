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

package org.apache.hadoop.hive.metastore.hbase.stats;

import java.util.List;

import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimatorFactory;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;

public class StringColumnStatsAggregator extends ColumnStatsAggregator {

  @Override
  public ColumnStatisticsObj aggregate(String colName, List<String> partNames,
      List<ColumnStatistics> css) throws MetaException {
    ColumnStatisticsObj statsObj = null;

    // check if all the ColumnStatisticsObjs contain stats and all the ndv are
    // bitvectors. Only when both of the conditions are true, we merge bit
    // vectors. Otherwise, just use the maximum function.
    boolean doAllPartitionContainStats = partNames.size() == css.size();
    NumDistinctValueEstimator ndvEstimator = null;
    String colType = null;
    for (ColumnStatistics cs : css) {
      if (cs.getStatsObjSize() != 1) {
        throw new MetaException(
            "The number of columns should be exactly one in aggrStats, but found "
                + cs.getStatsObjSize());
      }
      ColumnStatisticsObj cso = cs.getStatsObjIterator().next();
      if (statsObj == null) {
        colType = cso.getColType();
        statsObj = ColumnStatsAggregatorFactory.newColumnStaticsObj(colName, colType, cso
            .getStatsData().getSetField());
      }
      if (!cso.getStatsData().getStringStats().isSetBitVectors()
          || cso.getStatsData().getStringStats().getBitVectors().length() == 0) {
        ndvEstimator = null;
        break;
      } else {
        // check if all of the bit vectors can merge
        NumDistinctValueEstimator estimator = NumDistinctValueEstimatorFactory
            .getNumDistinctValueEstimator(cso.getStatsData().getStringStats().getBitVectors());
        if (ndvEstimator == null) {
          ndvEstimator = estimator;
        } else {
          if (ndvEstimator.canMerge(estimator)) {
            continue;
          } else {
            ndvEstimator = null;
            break;
          }
        }
      }
    }
    if (ndvEstimator != null) {
      ndvEstimator = NumDistinctValueEstimatorFactory.getEmptyNumDistinctValueEstimator(ndvEstimator);
    }
    ColumnStatisticsData columnStatisticsData = new ColumnStatisticsData();
    if (doAllPartitionContainStats && ndvEstimator!=null) {
      StringColumnStatsData aggregateData = null;
      for (ColumnStatistics cs : css) {
        ColumnStatisticsObj cso = cs.getStatsObjIterator().next();
        StringColumnStatsData newData = cso.getStatsData().getStringStats();
        ndvEstimator.mergeEstimators(NumDistinctValueEstimatorFactory
            .getNumDistinctValueEstimator(newData.getBitVectors()));
        if (aggregateData == null) {
          aggregateData = newData.deepCopy();
        } else {
          aggregateData
              .setMaxColLen(Math.max(aggregateData.getMaxColLen(), newData.getMaxColLen()));
          aggregateData
              .setAvgColLen(Math.max(aggregateData.getAvgColLen(), newData.getAvgColLen()));
          aggregateData.setNumNulls(aggregateData.getNumNulls() + newData.getNumNulls());
        }
      }
      aggregateData.setNumDVs(ndvEstimator.estimateNumDistinctValues());
      columnStatisticsData.setStringStats(aggregateData);
    } else {
      StringColumnStatsData aggregateData = null;
      for (ColumnStatistics cs : css) {
        ColumnStatisticsObj cso = cs.getStatsObjIterator().next();
        StringColumnStatsData newData = cso.getStatsData().getStringStats();
        if (aggregateData == null) {
          aggregateData = newData.deepCopy();
        } else {
          aggregateData
              .setMaxColLen(Math.max(aggregateData.getMaxColLen(), newData.getMaxColLen()));
          aggregateData
              .setAvgColLen(Math.max(aggregateData.getAvgColLen(), newData.getAvgColLen()));
          aggregateData.setNumNulls(aggregateData.getNumNulls() + newData.getNumNulls());
          aggregateData.setNumDVs(Math.max(aggregateData.getNumDVs(), newData.getNumDVs()));
        }
      }
      columnStatisticsData.setStringStats(aggregateData);
    }
    statsObj.setStatsData(columnStatisticsData);
    return statsObj;
  }

}
