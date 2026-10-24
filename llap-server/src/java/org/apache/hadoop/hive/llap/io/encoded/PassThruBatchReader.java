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
package org.apache.hadoop.hive.llap.io.encoded;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Batch-shaped {@link PassThruOffsetReader}: the source {@link RecordReader} is
 * a vectorized reader whose {@code createValue()} returns a
 * {@link VectorizedRowBatch}, and each {@code next()} refills that same batch.
 * {@link #getCurrentRow()} returns the batch itself (VRB implements
 * {@link org.apache.hadoop.io.Writable}), and {@link #isBatchShaped()} returns
 * {@code true} so
 * {@link SerDeEncodedDataReader.FileReaderYieldReturn#readNextSlice} routes it
 * to {@link SerDeEncodedDataReader.EncodingWriter#writeBatch(VectorizedRowBatch)}
 * instead of the per-row {@code writeOneRow(Writable)}.
 *
 * <p>Used on the {@code hive.llap.io.encode.vector.parquet.enabled} path
 * (default on for Parquet sources). Setting that flag to {@code false} routes
 * the reader through {@link SerDeEncodedDataReader#buildSourceReaderJobConf(JobConf)}
 * and the base row-shaped {@link PassThruOffsetReader} instead.
 */
class PassThruBatchReader extends PassThruOffsetReader {
  PassThruBatchReader(RecordReader sourceReader, JobConf jobConf, int headerCnt, int footerCnt) {
    super(sourceReader, jobConf, headerCnt, footerCnt);
  }

  @Override
  public boolean isBatchShaped() {
    return true;
  }
}
