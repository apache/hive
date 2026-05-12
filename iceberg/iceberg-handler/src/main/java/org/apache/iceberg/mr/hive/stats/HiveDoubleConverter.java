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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.mr.hive.stats;

import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;

public class HiveDoubleConverter implements HiveTypeStatsConverter<DoubleColumnStatsData, HiveDoubleStats> {
  public HiveDoubleStats fromThrift(DoubleColumnStatsData doubleColumnStatsData) {
    return new HiveDoubleStats(
        doubleColumnStatsData.isSetLowValue() ? doubleColumnStatsData.getLowValue() : null,
        doubleColumnStatsData.isSetHighValue() ? doubleColumnStatsData.getHighValue() : null,
        doubleColumnStatsData.getNumNulls(),
        doubleColumnStatsData.getNumDVs(),
        ColumnStatsConverter.toBytes(doubleColumnStatsData.bufferForBitVectors()),
        ColumnStatsConverter.toBytes(doubleColumnStatsData.bufferForHistogram()));
  }

  public DoubleColumnStatsData toThrift(HiveDoubleStats doubleStats) {
    DoubleColumnStatsData doubleColumnStatsData =
        new DoubleColumnStatsData(doubleStats.numNulls(), doubleStats.numDVs());

    if (doubleStats.lowValue() != null) {
      doubleColumnStatsData.setLowValue(doubleStats.lowValue());
    }
    if (doubleStats.highValue() != null) {
      doubleColumnStatsData.setHighValue(doubleStats.highValue());
    }
    doubleColumnStatsData.setBitVectors(doubleStats.bitVectors());
    doubleColumnStatsData.setHistogram(doubleStats.histogram());

    return doubleColumnStatsData;
  }
}
