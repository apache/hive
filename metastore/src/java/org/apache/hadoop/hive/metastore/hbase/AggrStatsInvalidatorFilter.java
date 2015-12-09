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
package org.apache.hadoop.hive.metastore.hbase;

import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hive.common.util.BloomFilter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Filter for scanning aggregates stats table
 */
public class AggrStatsInvalidatorFilter extends FilterBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(AggrStatsInvalidatorFilter.class.getName());
  private final List<HbaseMetastoreProto.AggrStatsInvalidatorFilter.Entry> entries;
  private final long runEvery;
  private final long maxCacheEntryLife;
  // This class is not serializable, so I realize transient doesn't mean anything.  It's just to
  // comunicate that we don't serialize this and ship it across to the filter on the other end.
  // We use the time the filter is actually instantiated in HBase.
  private transient long now;

  public static Filter parseFrom(byte[] serialized) throws DeserializationException {
    try {
      return new AggrStatsInvalidatorFilter(
          HbaseMetastoreProto.AggrStatsInvalidatorFilter.parseFrom(serialized));
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
  }

  /**
   * @param proto Protocol buffer representation of this filter.
   */
  AggrStatsInvalidatorFilter(HbaseMetastoreProto.AggrStatsInvalidatorFilter proto) {
    this.entries = proto.getToInvalidateList();
    this.runEvery = proto.getRunEvery();
    this.maxCacheEntryLife = proto.getMaxCacheEntryLife();
    now = System.currentTimeMillis();
  }

  @Override
  public byte[] toByteArray() throws IOException {
    return HbaseMetastoreProto.AggrStatsInvalidatorFilter.newBuilder()
        .addAllToInvalidate(entries)
        .setRunEvery(runEvery)
        .setMaxCacheEntryLife(maxCacheEntryLife)
        .build()
        .toByteArray();
  }

  @Override
  public boolean filterAllRemaining() throws IOException {
    return false;
  }

  @Override
  public ReturnCode filterKeyValue(Cell cell) throws IOException {
    // Is this the partition we want?
    if (Arrays.equals(CellUtil.cloneQualifier(cell), HBaseReadWrite.AGGR_STATS_BLOOM_COL)) {
      HbaseMetastoreProto.AggrStatsBloomFilter fromCol =
          HbaseMetastoreProto.AggrStatsBloomFilter.parseFrom(CellUtil.cloneValue(cell));
      BloomFilter bloom = null;
      if (now - maxCacheEntryLife > fromCol.getAggregatedAt()) {
        // It's too old, kill it regardless of whether we were asked to or not.
        return ReturnCode.INCLUDE;
      } else if (now - runEvery * 2 <= fromCol.getAggregatedAt()) {
        // It's too new.  We might be stomping on something that was just created.  Skip it.
        return ReturnCode.NEXT_ROW;
      } else {
        // Look through each of our entries and see if any of them match.
        for (HbaseMetastoreProto.AggrStatsInvalidatorFilter.Entry entry : entries) {
          // First check if we match on db and table match
          if (entry.getDbName().equals(fromCol.getDbName()) &&
              entry.getTableName().equals(fromCol.getTableName())) {
            if (bloom == null) {
              // Now, reconstitute the bloom filter and probe it with each of our partition names
              bloom = new BloomFilter(
                  fromCol.getBloomFilter().getBitsList(),
                  fromCol.getBloomFilter().getNumBits(),
                  fromCol.getBloomFilter().getNumFuncs());
            }
            if (bloom.test(entry.getPartName().toByteArray())) {
              // This is most likely a match, so mark it and quit looking.
              return ReturnCode.INCLUDE;
            }
          }
        }
      }
      return ReturnCode.NEXT_ROW;
    } else {
      return ReturnCode.NEXT_COL;
    }
  }
}
