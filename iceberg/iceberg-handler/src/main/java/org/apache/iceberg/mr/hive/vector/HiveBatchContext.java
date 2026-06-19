/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.vector;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.io.CloseableIterator;

public class HiveBatchContext {

  private final VectorizedRowBatch batch;
  private final VectorizedRowBatchCtx vrbCtx;
  /**
   * File row position of the first row in this batch. Long.MIN_VALUE if unknown.
   */
  private final long fileRowOffset;

  public HiveBatchContext(VectorizedRowBatch batch, VectorizedRowBatchCtx vrbCtx, long fileRowOffset) {
    this.batch = batch;
    this.vrbCtx = vrbCtx;
    this.fileRowOffset = fileRowOffset;
  }

  public VectorizedRowBatch getBatch() {
    return batch;
  }

  public CloseableIterator<HiveRow> rowIterator() throws IOException {
    return new RowIterator();
  }

  class RowIterator implements CloseableIterator<HiveRow> {

    private final VectorExtractRow vectorExtractRow;
    private final int originalSize;
    private final int[] originalIndices;
    private int currentPosition = 0;

    RowIterator() throws IOException {
      try {
        this.vectorExtractRow = new VectorExtractRow();
        this.vectorExtractRow.init(vrbCtx.getRowColumnTypeInfos());
        this.originalSize = batch.size;
        if (batch.isSelectedInUse()) {
          // copy, as further operations working on this batch might change what rows are selected
          originalIndices = Arrays.copyOf(batch.selected, batch.size);
        } else {
          originalIndices = IntStream.range(0, batch.size).toArray();
        }
      } catch (HiveException e) {
        throw new IOException(e);
      }
    }

    @Override
    public boolean hasNext() {
      return currentPosition < originalSize;
    }

    @Override
    public HiveRow next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      // position of the row as this batch is intended to be read (e.g. if batch is already filtered this
      // can be different from the physical position)
      int logicalPosition = currentPosition++;
      // real position of this row within the original (i.e unfiltered) batch
      int physicalPosition = originalIndices[logicalPosition];

      HiveRow row = new HiveRow() {

        @Override
        public Object get(int rowIndex) {

          if (rowIndex == MetadataColumns.ROW_POSITION.fieldId()) {
            if (fileRowOffset == Long.MIN_VALUE) {
              throw new UnsupportedOperationException("Can't provide row position for batch.");
            }
            return fileRowOffset + physicalPosition;
          } else {
            return vectorExtractRow.accessor(batch).apply(physicalPosition).apply(rowIndex);
          }
        }

        @Override
        public int physicalBatchIndex() {
          return physicalPosition;
        }

      };
      return row;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }
}
