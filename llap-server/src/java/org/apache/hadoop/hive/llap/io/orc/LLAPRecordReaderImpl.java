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
package org.apache.hadoop.hive.llap.io.orc;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.Consumer;
import org.apache.hadoop.hive.llap.io.api.EncodedColumn;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.io.api.orc.OrcBatchKey;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;

/**
 *
 */
public class LLAPRecordReaderImpl extends RecordReaderImpl implements RecordReader {
  LLAPRecordReaderImpl(List<StripeInformation> stripes,
      FileSystem fileSystem, Path path,
      Reader.Options options,
      List<OrcProto.Type> types, CompressionCodec codec,
      int bufferSize, long strideRate, Configuration conf) throws IOException {
    super(stripes, fileSystem, path, options, types, codec, bufferSize, strideRate, conf);
  }

  @Override
  public OrcProto.RowIndex[] getRowIndexEntries(int stripeIdx) throws IOException {
    return readRowIndex(stripeIdx);
  }

  @Override
  public List<OrcProto.ColumnEncoding> getColumnEncodings(int stripeIdx) throws IOException {
    StripeInformation si = stripes.get(stripeIdx);
    OrcProto.StripeFooter sf = readStripeFooter(si);
    return sf.getColumnsList();
  }

  @Override
  public boolean[] getIncludedRowGroups(int stripeIdx) throws IOException {
    currentStripe = stripeIdx;
    return pickRowGroups();
  }

  @Override
  public boolean hasNext() throws IOException {
    return false;
  }

  @Override
  public Object next(Object previous) throws IOException {
    return null;
  }

  @Override
  public VectorizedRowBatch nextBatch(VectorizedRowBatch previousBatch) throws IOException {
    return null;
  }

  @Override
  public long getRowNumber() {
    return 0;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void seekToRow(long rowCount) throws IOException {

  }

  @Override
  public void readEncodedColumns(long[][] colRgs, int rgCount,
      Consumer<EncodedColumn<OrcBatchKey>> consumer, LowLevelCache cache) {

  }
}
