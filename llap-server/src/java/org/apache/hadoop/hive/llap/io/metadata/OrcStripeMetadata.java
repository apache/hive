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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.io.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.llap.IncrementalObjectSizeEstimator;
import org.apache.hadoop.hive.llap.IncrementalObjectSizeEstimator.ObjectEstimator;
import org.apache.hadoop.hive.llap.cache.EvictionDispatcher;
import org.apache.hadoop.hive.llap.cache.LlapCacheableBuffer;
import org.apache.hadoop.hive.llap.io.api.orc.OrcBatchKey;
import org.apache.hadoop.hive.ql.io.orc.MetadataReader;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.BloomFilter;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.BloomFilterIndex;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndex;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndexEntry;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.StripeFooter;
import org.apache.hadoop.hive.ql.io.orc.RecordReaderImpl;
import org.apache.hadoop.hive.ql.io.orc.StripeInformation;

import com.google.common.annotations.VisibleForTesting;

public class OrcStripeMetadata extends LlapCacheableBuffer {
  private final OrcBatchKey stripeKey;
  private final List<ColumnEncoding> encodings;
  private final List<Stream> streams;
  private RecordReaderImpl.Index rowIndex;

  private final int estimatedMemUsage;

  private final static HashMap<Class<?>, ObjectEstimator> SIZE_ESTIMATORS;
  private final static ObjectEstimator SIZE_ESTIMATOR;
  static {
    OrcStripeMetadata osm = createDummy(0);
    SIZE_ESTIMATORS = IncrementalObjectSizeEstimator.createEstimators(osm);
    IncrementalObjectSizeEstimator.addEstimator(
        "com.google.protobuf.LiteralByteString", SIZE_ESTIMATORS);
    SIZE_ESTIMATOR = SIZE_ESTIMATORS.get(OrcStripeMetadata.class);
  }

  public OrcStripeMetadata(OrcBatchKey stripeKey, MetadataReader mr, StripeInformation stripe,
      boolean[] includes, boolean[] sargColumns) throws IOException {
    this.stripeKey = stripeKey;
    StripeFooter footer = mr.readStripeFooter(stripe);
    streams = footer.getStreamsList();
    encodings = footer.getColumnsList();
    rowIndex = mr.readRowIndex(stripe, footer, includes, null, sargColumns, null);

    estimatedMemUsage = SIZE_ESTIMATOR.estimate(this, SIZE_ESTIMATORS);
  }

  public OrcStripeMetadata(long id) {
    stripeKey = new OrcBatchKey(id, 0, 0);
    encodings = new ArrayList<>();
    streams = new ArrayList<>();
    estimatedMemUsage = 0;
  }

  @VisibleForTesting
  public static OrcStripeMetadata createDummy(long id) {
    OrcStripeMetadata dummy = new OrcStripeMetadata(id);
    dummy.encodings.add(ColumnEncoding.getDefaultInstance());
    dummy.streams.add(Stream.getDefaultInstance());
    RowIndex ri = RowIndex.newBuilder().addEntry(
        RowIndexEntry.newBuilder().addPositions(1).setStatistics(
            OrcFileMetadata.createStatsDummy())).build();
    BloomFilterIndex bfi = BloomFilterIndex.newBuilder().addBloomFilter(
        BloomFilter.newBuilder().addBitset(0)).build();
    dummy.rowIndex = new RecordReaderImpl.Index(
        new RowIndex[] { ri }, new BloomFilterIndex[] { bfi });
    return dummy;
  }

  public boolean hasAllIndexes(boolean[] includes) {
    for (int i = 0; i < includes.length; ++i) {
      if (includes[i] && rowIndex.getRowGroupIndex()[i] == null) return false;
    }
    return true;
  }

  public void loadMissingIndexes(MetadataReader mr, StripeInformation stripe, boolean[] includes,
      boolean[] sargColumns) throws IOException {
    // TODO: should we save footer to avoid a read here?
    rowIndex = mr.readRowIndex(stripe, null, includes, rowIndex.getRowGroupIndex(),
        sargColumns, rowIndex.getBloomFilterIndex());
    // TODO: theoretically, we should re-estimate memory usage here and update memory manager
  }

  public int getStripeIx() {
    return stripeKey.stripeIx;
  }

  public RowIndex[] getRowIndexes() {
    return rowIndex.getRowGroupIndex();
  }

  public List<ColumnEncoding> getEncodings() {
    return encodings;
  }

  public List<Stream> getStreams() {
    return streams;
  }

  @Override
  public long getMemoryUsage() {
    return estimatedMemUsage;
  }

  @Override
  public void notifyEvicted(EvictionDispatcher evictionDispatcher) {
    evictionDispatcher.notifyEvicted(this);
  }

  @Override
  protected boolean invalidate() {
    return true;
  }

  @Override
  protected boolean isLocked() {
    return false;
  }

  public OrcBatchKey getKey() {
    return stripeKey;
  }

  @VisibleForTesting
  public void resetRowIndex() {
    rowIndex = null;
  }
}
