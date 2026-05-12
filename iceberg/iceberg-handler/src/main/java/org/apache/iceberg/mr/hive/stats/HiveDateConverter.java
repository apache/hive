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

import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;

public class HiveDateConverter implements HiveTypeStatsConverter<DateColumnStatsData, HiveDateStats> {
  public HiveDateStats fromThrift(DateColumnStatsData dateColumnStatsData) {
    return new HiveDateStats(
        fromThrift(dateColumnStatsData.getLowValue()),
        fromThrift(dateColumnStatsData.getHighValue()),
        dateColumnStatsData.getNumNulls(),
        dateColumnStatsData.getNumDVs(),
        ColumnStatsConverter.toBytes(dateColumnStatsData.bufferForBitVectors()),
        ColumnStatsConverter.toBytes(dateColumnStatsData.bufferForHistogram()));
  }

  private HiveSerializableDate fromThrift(Date date) {
    return (date == null) ? null : new HiveSerializableDate(date.getDaysSinceEpoch());
  }

  public DateColumnStatsData toThrift(HiveDateStats dateStats) {
    DateColumnStatsData dateColumnStatsData = new DateColumnStatsData(dateStats.numNulls(), dateStats.numDVs());
    if (dateStats.lowValue() != null) {
      dateColumnStatsData.setLowValue(toThrift(dateStats.lowValue()));
    }
    if (dateStats.highValue() != null) {
      dateColumnStatsData.setHighValue(toThrift(dateStats.highValue()));
    }
    dateColumnStatsData.setBitVectors(dateStats.bitVectors());
    dateColumnStatsData.setHistogram(dateStats.histogram());
    return dateColumnStatsData;
  }

  private Date toThrift(HiveSerializableDate serializableDate) {
    return new Date(serializableDate.daysSinceEpoch());
  }
}
