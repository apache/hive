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

import java.util.List;

import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.ColStatsObjWithSourceInfo;

public class BooleanColumnStatsAggregator extends ColumnStatsAggregator {

  @Override
  public ColumnStatisticsObj aggregate(List<ColStatsObjWithSourceInfo> colStatsWithSourceInfo,
      List<String> partNames, boolean areAllPartsFound) throws MetaException {
    checkStatisticsList(colStatsWithSourceInfo);

    ColumnStatisticsObj statsObj = null;
    String colType;
    String colName;
    BooleanColumnStatsData aggregateData = null;
    for (ColStatsObjWithSourceInfo csp : colStatsWithSourceInfo) {
      ColumnStatisticsObj cso = csp.getColStatsObj();
      if (statsObj == null) {
        colName = cso.getColName();
        colType = cso.getColType();
        statsObj = ColumnStatsAggregatorFactory.newColumnStaticsObj(colName, colType,
            cso.getStatsData().getSetField());
      }
      BooleanColumnStatsData newData = cso.getStatsData().getBooleanStats();
      if (aggregateData == null) {
        aggregateData = newData.deepCopy();
      } else {
        aggregateData.setNumTrues(aggregateData.getNumTrues() + newData.getNumTrues());
        aggregateData.setNumFalses(aggregateData.getNumFalses() + newData.getNumFalses());
        aggregateData.setNumNulls(aggregateData.getNumNulls() + newData.getNumNulls());
      }
    }
    ColumnStatisticsData columnStatisticsData = initColumnStatisticsData();
    columnStatisticsData.setBooleanStats(aggregateData);
    statsObj.setStatsData(columnStatisticsData);
    return statsObj;
  }

  @Override protected ColumnStatisticsData initColumnStatisticsData() {
    return new ColumnStatisticsData();
  }
}
