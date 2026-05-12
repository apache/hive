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

import java.nio.ByteBuffer;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;

public class HiveDecimalConverter implements HiveTypeStatsConverter<DecimalColumnStatsData, HiveDecimalStats> {

  public HiveDecimalStats fromThrift(DecimalColumnStatsData decimalColumnStatsData) {
    return new HiveDecimalStats(
        fromThrift(decimalColumnStatsData.getLowValue()),
        fromThrift(decimalColumnStatsData.getHighValue()),
        decimalColumnStatsData.getNumNulls(),
        decimalColumnStatsData.getNumDVs(),
        ColumnStatsConverter.toBytes(decimalColumnStatsData.bufferForBitVectors()),
        ColumnStatsConverter.toBytes(decimalColumnStatsData.bufferForHistogram()));
  }

  private HiveSerializableDecimal fromThrift(Decimal decimal) {
    return (decimal == null) ? null : new HiveSerializableDecimal(
        decimal.getScale(), ColumnStatsConverter.toBytes(decimal.bufferForUnscaled()));
  }

  public DecimalColumnStatsData toThrift(HiveDecimalStats decimalStats) {
    DecimalColumnStatsData decimalColumnStatsData =
        new DecimalColumnStatsData(decimalStats.numNulls(), decimalStats.numDVs());

    if (decimalStats.lowValue() != null) {
      decimalColumnStatsData.setLowValue(toThrift(decimalStats.lowValue()));
    }
    if (decimalStats.highValue() != null) {
      decimalColumnStatsData.setHighValue(toThrift(decimalStats.highValue()));
    }
    decimalColumnStatsData.setBitVectors(decimalStats.bitVectors());
    decimalColumnStatsData.setHistogram(decimalStats.histogram());

    return decimalColumnStatsData;
  }

  private Decimal toThrift(HiveSerializableDecimal serializableDecimal) {
    return new Decimal(serializableDecimal.scale(), ByteBuffer.wrap(serializableDecimal.unscaled()));
  }
}
