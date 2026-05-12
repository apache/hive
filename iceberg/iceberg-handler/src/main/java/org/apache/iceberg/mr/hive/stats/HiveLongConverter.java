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

import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;

public class HiveLongConverter implements HiveTypeStatsConverter<LongColumnStatsData, HiveLongStats> {
  public HiveLongStats fromThrift(LongColumnStatsData longColumnStatsData) {
    return new HiveLongStats(
        longColumnStatsData.isSetLowValue() ? longColumnStatsData.getLowValue() : null,
        longColumnStatsData.isSetHighValue() ? longColumnStatsData.getHighValue() : null,
        longColumnStatsData.getNumNulls(),
        longColumnStatsData.getNumDVs(),
        ColumnStatsConverter.toBytes(longColumnStatsData.bufferForBitVectors()),
        ColumnStatsConverter.toBytes(longColumnStatsData.bufferForHistogram()));
  }

  public LongColumnStatsData toThrift(HiveLongStats longStats) {
    LongColumnStatsData longColumnStatsData = new LongColumnStatsData(longStats.numNulls(), longStats.numDVs());
    if (longStats.lowValue() != null) {
      longColumnStatsData.setLowValue(longStats.lowValue());
    }
    if (longStats.highValue() != null) {
      longColumnStatsData.setHighValue(longStats.highValue());
    }
    longColumnStatsData.setBitVectors(longStats.bitVectors());
    longColumnStatsData.setHistogram(longStats.histogram());
    return longColumnStatsData;
  }
}
