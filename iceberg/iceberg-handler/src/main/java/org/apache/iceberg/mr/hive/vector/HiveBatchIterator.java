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

package org.apache.iceberg.mr.hive.vector;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.LongStream;
import org.apache.hadoop.hive.llap.LlapHiveUtils;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.RowPositionAwareVectorizedRecordReader;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.mr.hive.IcebergAcidUtil;
import org.apache.iceberg.util.StructProjection;

/**
 * Iterator wrapper around Hive's VectorizedRowBatch producer (MRv1 implementing) record readers.
 */
public final class HiveBatchIterator implements CloseableIterator<HiveBatchContext> {

  private final RecordReader<NullWritable, VectorizedRowBatch> recordReader;
  private final NullWritable key;
  private final VectorizedRowBatch batch;
  private final VectorizedRowBatchCtx vrbCtx;
  private final int[] partitionColIndices;
  private final Object[] partitionValues;
  private boolean advanced = false;
  private long rowOffset = Long.MIN_VALUE;
  private Map<Integer, ?> idToConstant;

  HiveBatchIterator(RecordReader<NullWritable, VectorizedRowBatch> recordReader, JobConf job,
      int[] partitionColIndices, Object[] partitionValues, Map<Integer, ?> idToConstant) {
    this.recordReader = recordReader;
    this.key = recordReader.createKey();
    this.batch = recordReader.createValue();
    this.vrbCtx = LlapHiveUtils.findMapWork(job).getVectorizedRowBatchCtx();
    this.partitionColIndices = partitionColIndices;
    this.partitionValues = partitionValues;
    this.idToConstant = idToConstant;
  }

  @Override
  public void close() throws IOException {
    this.recordReader.close();
  }

  private void advance() {
    if (!advanced) {
      try {

        if (!recordReader.next(key, batch)) {
          batch.size = 0;
        }

        if (batch.size != 0 && recordReader instanceof RowPositionAwareVectorizedRecordReader) {
          rowOffset = ((RowPositionAwareVectorizedRecordReader) recordReader).getRowNumber();
        }

        // Fill partition values
        if (partitionColIndices != null) {
          for (int i = 0; i < partitionColIndices.length; ++i) {
            int colIdx = partitionColIndices[i];
            // The partition column might not be part of the current projection - in which case no CV is inited
            if (batch.cols[colIdx] != null) {
              vrbCtx.addPartitionColsToBatch(batch.cols[colIdx], partitionValues[i], partitionColIndices[i]);
            }
          }
        }
        // Fill virtual columns
        for (VirtualColumn vc : vrbCtx.getNeededVirtualColumns()) {
          Object value;
          int idx = vrbCtx.findVirtualColumnNum(vc);
          switch (vc) {
            case PARTITION_SPEC_ID:
              value = idToConstant.get(MetadataColumns.SPEC_ID.fieldId());
              vrbCtx.addPartitionColsToBatch(batch.cols[idx], value, idx);
              break;
            case PARTITION_HASH:
              value = IcebergAcidUtil.computeHash(
                  (StructProjection) idToConstant.get(MetadataColumns.PARTITION_COLUMN_ID));
              vrbCtx.addPartitionColsToBatch(batch.cols[idx], value, idx);
              break;
            case FILE_PATH:
              value = idToConstant.get(MetadataColumns.FILE_PATH.fieldId());
              BytesColumnVector bcv = (BytesColumnVector) batch.cols[idx];
              if (value == null) {
                bcv.noNulls = false;
                bcv.isNull[0] = true;
                bcv.isRepeating = true;
              } else {
                bcv.fill(((String) value).getBytes());
              }
              break;
            case ROW_POSITION:
              value = LongStream.range(rowOffset, rowOffset + batch.size).toArray();
              LongColumnVector lcv = (LongColumnVector) batch.cols[idx];
              lcv.noNulls = true;
              Arrays.fill(lcv.isNull, false);
              lcv.isRepeating = false;
              System.arraycopy(value, 0, lcv.vector, 0, batch.size);
              break;
          }
        }
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
      advanced = true;
    }
  }

  @Override
  public boolean hasNext() {
    advance();
    return batch.size > 0;
  }

  @Override
  public HiveBatchContext next() {
    advance();
    advanced = false;
    return new HiveBatchContext(batch, vrbCtx, rowOffset);
  }
}
