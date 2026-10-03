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

import java.util.Arrays;
import java.util.function.LongConsumer;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Removes the rows of a data file's position deletes (delete files or a deletion vector) from its row batches.
 */
class HiveDeleteFilter {

  private final PositionDeleteIndex deletes;
  // the deleted rows of the batch being filtered, by row index
  private boolean[] deletedRows = new boolean[0];
  private long batchOffset;
  private boolean anyDeleted;
  private final LongConsumer markDeleted = pos -> {
    deletedRows[(int) (pos - batchOffset)] = true;
    anyDeleted = true;
  };

  HiveDeleteFilter(PositionDeleteIndex deletes) {
    this.deletes = deletes;
  }

  /**
   * Adjusts the pipeline of incoming VRBs so that the deleted rows of each batch are filtered out.
   * @param batches iterable of HiveBatchContexts i.e. VRBs and their meta information, in file order
   * @return the adjusted iterable of HiveBatchContexts
   */
  CloseableIterable<HiveBatchContext> filterBatch(CloseableIterable<HiveBatchContext> batches) {
    return CloseableIterable.transform(batches, context -> {
      filter(context.getBatch(), context.getFileRowOffset());
      return context;
    });
  }

  /**
   * Drops the deleted rows from the batch's selection, where row {@code i} of the batch is at file position
   * {@code fileRowOffset + i}.
   */
  void filter(VectorizedRowBatch batch, long fileRowOffset) {
    int size = batch.size;
    if (size == 0) {
      return;
    }
    if (fileRowOffset == Long.MIN_VALUE) {
      throw new UnsupportedOperationException("Can't provide row position for batch.");
    }
    boolean selectedInUse = batch.selectedInUse;
    int[] selected = batch.selected;
    int first = selectedInUse ? selected[0] : 0;
    int end = selectedInUse ? selected[size - 1] + 1 : size;
    if (deletedRows.length < end) {
      deletedRows = new boolean[Math.max(end, selected.length)];
    }
    batchOffset = fileRowOffset;
    anyDeleted = false;
    deletes.forEachInRange(fileRowOffset + first, fileRowOffset + end, markDeleted);
    if (!anyDeleted) {
      return;
    }
    int newSize = 0;
    for (int i = 0; i < size; i++) {
      int row = selectedInUse ? selected[i] : i;
      if (!deletedRows[row]) {
        selected[newSize++] = row;
      }
    }
    Arrays.fill(deletedRows, first, end, false);
    batch.size = newSize;
    batch.selectedInUse = true;
  }
}
