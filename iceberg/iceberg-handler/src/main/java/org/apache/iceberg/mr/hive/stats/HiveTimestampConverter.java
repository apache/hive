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

import org.apache.hadoop.hive.metastore.api.Timestamp;
import org.apache.hadoop.hive.metastore.api.TimestampColumnStatsData;

public class HiveTimestampConverter implements HiveTypeStatsConverter<TimestampColumnStatsData, HiveTimestampStats> {

  public HiveTimestampStats fromThrift(TimestampColumnStatsData timestampColumnStatsData) {
    return new HiveTimestampStats(
        fromThrift(timestampColumnStatsData.getLowValue()),
        fromThrift(timestampColumnStatsData.getHighValue()),
        timestampColumnStatsData.getNumNulls(),
        timestampColumnStatsData.getNumDVs(),
        ColumnStatsConverter.toBytes(timestampColumnStatsData.bufferForBitVectors()),
        ColumnStatsConverter.toBytes(timestampColumnStatsData.bufferForHistogram()));
  }

  private HiveSerializableTimestamp fromThrift(Timestamp timestamp) {
    return (timestamp == null) ? null : new HiveSerializableTimestamp(timestamp.getSecondsSinceEpoch());
  }

  public TimestampColumnStatsData toThrift(HiveTimestampStats timestampStats) {
    TimestampColumnStatsData timestampColumnStatsData =
        new TimestampColumnStatsData(timestampStats.numNulls(), timestampStats.numDVs());

    if (timestampStats.lowValue() != null) {
      timestampColumnStatsData.setLowValue(toThrift(timestampStats.lowValue()));
    }

    if (timestampStats.highValue() != null) {
      timestampColumnStatsData.setHighValue(toThrift(timestampStats.highValue()));
    }

    timestampColumnStatsData.setBitVectors(timestampStats.bitVectors());
    timestampColumnStatsData.setHistogram(timestampStats.histogram());

    return timestampColumnStatsData;
  }

  private Timestamp toThrift(HiveSerializableTimestamp serializableTimestamp) {
    return new Timestamp(serializableTimestamp.secondsSinceEpoch());
  }
}
