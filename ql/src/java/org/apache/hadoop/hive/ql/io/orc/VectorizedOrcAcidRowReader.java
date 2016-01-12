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

package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedBatchUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Implement a RecordReader that stitches together base and delta files to
 * support tables and partitions stored in the ACID format. It works by using
 * the non-vectorized ACID reader and moving the data into a vectorized row
 * batch.
 */
class VectorizedOrcAcidRowReader
    implements org.apache.hadoop.mapred.RecordReader<NullWritable,
                                                     VectorizedRowBatch> {
  private final AcidInputFormat.RowReader<OrcStruct> innerReader;
  private final RecordIdentifier key;
  private final OrcStruct value;
  private VectorizedRowBatchCtx rbCtx;
  private Object[] partitionValues;
  private final ObjectInspector objectInspector;
  private final DataOutputBuffer buffer = new DataOutputBuffer();

  VectorizedOrcAcidRowReader(AcidInputFormat.RowReader<OrcStruct> inner,
                             Configuration conf,
                             VectorizedRowBatchCtx vectorizedRowBatchCtx,
                             FileSplit split) throws IOException {
    this.innerReader = inner;
    this.key = inner.createKey();
    rbCtx = vectorizedRowBatchCtx;
    int partitionColumnCount = rbCtx.getPartitionColumnCount();
    if (partitionColumnCount > 0) {
      partitionValues = new Object[partitionColumnCount];
      rbCtx.getPartitionValues(rbCtx, conf, split, partitionValues);
    }
    this.value = inner.createValue();
    this.objectInspector = inner.getObjectInspector();
  }

  @Override
  public boolean next(NullWritable nullWritable,
                      VectorizedRowBatch vectorizedRowBatch
                      ) throws IOException {
    vectorizedRowBatch.reset();
    buffer.reset();
    if (!innerReader.next(key, value)) {
      return false;
    }
    if (partitionValues != null) {
      rbCtx.addPartitionColsToBatch(vectorizedRowBatch, partitionValues);
    }
    try {
      VectorizedBatchUtil.acidAddRowToBatch(value,
          (StructObjectInspector) objectInspector,
          vectorizedRowBatch.size++, vectorizedRowBatch, rbCtx, buffer);
      while (vectorizedRowBatch.size < vectorizedRowBatch.selected.length &&
          innerReader.next(key, value)) {
        VectorizedBatchUtil.acidAddRowToBatch(value,
            (StructObjectInspector) objectInspector,
            vectorizedRowBatch.size++, vectorizedRowBatch, rbCtx, buffer);
      }
    } catch (Exception e) {
      throw new IOException("error iterating", e);
    }
    return true;
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public VectorizedRowBatch createValue() {
    return rbCtx.createVectorizedRowBatch();
  }

  @Override
  public long getPos() throws IOException {
    return innerReader.getPos();
  }

  @Override
  public void close() throws IOException {
    innerReader.close();
  }

  @Override
  public float getProgress() throws IOException {
    return innerReader.getProgress();
  }
}
