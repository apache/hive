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
package org.apache.hadoop.hive.llap.io.metadata;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.encoded.OrcBatchKey;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcProto.RowIndexEntry;
import org.apache.orc.StripeInformation;
import org.apache.orc.impl.OrcIndex;

public class OrcStripeMetadata implements ConsumerStripeMetadata {
  private final OrcBatchKey stripeKey;
  private final List<OrcProto.ColumnEncoding> encodings;
  private final List<OrcProto.Stream> streams;
  private final String writerTimezone;
  private final long rowCount;
  private OrcIndex rowIndex;

  public OrcStripeMetadata(OrcBatchKey stripeKey, OrcProto.StripeFooter footer,
      OrcIndex orcIndex, StripeInformation stripe) throws IOException {
    this.stripeKey = stripeKey;
    streams = footer.getStreamsList();
    encodings = footer.getColumnsList();
    writerTimezone = footer.getWriterTimezone();
    rowCount = stripe.getNumberOfRows();
    rowIndex = orcIndex;
  }

  public int getStripeIx() {
    return stripeKey.stripeIx;
  }

  public OrcProto.RowIndex[] getRowIndexes() {
    return rowIndex.getRowGroupIndex();
  }

  public OrcProto.Stream.Kind[] getBloomFilterKinds() {
    return rowIndex.getBloomFilterKinds();
  }

  public OrcProto.BloomFilterIndex[] getBloomFilterIndexes() {
    return rowIndex.getBloomFilterIndex();
  }

  public List<OrcProto.ColumnEncoding> getEncodings() {
    return encodings;
  }

  public List<OrcProto.Stream> getStreams() {
    return streams;
  }

  public String getWriterTimezone() {
    return writerTimezone;
  }

  public OrcBatchKey getKey() {
    return stripeKey;
  }

  public long getRowCount() {
    return rowCount;
  }

  @VisibleForTesting
  public void resetRowIndex() {
    rowIndex = null;
  }

  @Override
  public RowIndexEntry getRowIndexEntry(int colIx, int rgIx) {
    return rowIndex.getRowGroupIndex()[colIx].getEntry(rgIx);
  }

  @Override
  public boolean supportsRowIndexes() {
    return true;
  }

  public OrcIndex getIndex() {
    return rowIndex;
  }

  @Override
  public String toString() {
    return "OrcStripeMetadata [stripeKey=" + stripeKey + ", rowCount="
        + rowCount + ", writerTimezone=" + writerTimezone + ", encodings="
        + encodings + ", streams=" + streams + ", rowIndex=" + rowIndex + "]";
  }
}
