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
import org.apache.hadoop.hive.serde2.SerDeException;
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
  private final VectorizedRowBatchCtx rowBatchCtx;
  private final ObjectInspector objectInspector;
  private final DataOutputBuffer buffer = new DataOutputBuffer();

  VectorizedOrcAcidRowReader(AcidInputFormat.RowReader<OrcStruct> inner,
                             Configuration conf,
                             FileSplit split) throws IOException {
    this.innerReader = inner;
    this.key = inner.createKey();
    this.rowBatchCtx = new VectorizedRowBatchCtx();
    this.value = inner.createValue();
    this.objectInspector = inner.getObjectInspector();
    try {
      rowBatchCtx.init(conf, split);
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed to initialize context", e);
    } catch (SerDeException e) {
      throw new IOException("Failed to initialize context", e);
    } catch (InstantiationException e) {
      throw new IOException("Failed to initialize context", e);
    } catch (IllegalAccessException e) {
      throw new IOException("Failed to initialize context", e);
    } catch (HiveException e) {
      throw new IOException("Failed to initialize context", e);
    }
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
    try {
      rowBatchCtx.addPartitionColsToBatch(vectorizedRowBatch);
    } catch (HiveException e) {
      throw new IOException("Problem adding partition column", e);
    }
    try {
      VectorizedBatchUtil.acidAddRowToBatch(value,
          (StructObjectInspector) objectInspector,
          vectorizedRowBatch.size++, vectorizedRowBatch, rowBatchCtx, buffer);
      while (vectorizedRowBatch.size < vectorizedRowBatch.selected.length &&
          innerReader.next(key, value)) {
        VectorizedBatchUtil.acidAddRowToBatch(value,
            (StructObjectInspector) objectInspector,
            vectorizedRowBatch.size++, vectorizedRowBatch, rowBatchCtx, buffer);
      }
    } catch (HiveException he) {
      throw new IOException("error iterating", he);
    }
    return true;
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public VectorizedRowBatch createValue() {
    try {
      return rowBatchCtx.createVectorizedRowBatch();
    } catch (HiveException e) {
      throw new RuntimeException("Error creating a batch", e);
    }
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
