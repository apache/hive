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
import java.io.UncheckedIOException;
import java.util.NoSuchElementException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

/**
 * Delete filter implementation which is consuming HiveRow instances.
 */
public class HiveDeleteFilter extends DeleteFilter<HiveRow> {

  private final FileIO io;
  private final HiveStructLike asStructLike;

  public HiveDeleteFilter(FileIO io, FileScanTask task, Schema tableSchema, Schema requestedSchema) {
    super((task.file()).path().toString(), task.deletes(), tableSchema, requestedSchema);
    this.io = io;
    this.asStructLike = new HiveStructLike(this.requiredSchema().asStruct());
  }

  @Override
  protected StructLike asStructLike(HiveRow record) {
    return asStructLike.wrap(record);
  }

  @Override
  protected long pos(HiveRow record) {
    return (long) record.get(MetadataColumns.ROW_POSITION.fieldId());
  }

  @Override
  protected void markRowDeleted(HiveRow row) {
    row.setDeleted(true);
  }

  @Override
  protected InputFile getInputFile(String location) {
    return this.io.newInputFile(location);
  }

  /**
   * Adjusts the pipeline of incoming VRBs so that for each batch every row goes through the delete filter.
   * @param batches iterable of HiveBatchContexts i.e. VRBs and their meta information
   * @return the adjusted iterable of HiveBatchContexts
   */
  public CloseableIterable<HiveBatchContext> filterBatch(CloseableIterable<HiveBatchContext> batches) {

    // Delete filter pipeline setup logic:
    // A HiveRow iterable (deleteInputIterable) is provided as input iterable for the DeleteFilter.
    // The content in deleteInputIterable is provided by row iterators from the incoming VRBs i.e. on the arrival of
    // a new batch the underlying iterator gets swapped.
    SwappableHiveRowIterable deleteInputIterable = new SwappableHiveRowIterable();

    // Output iterable of DeleteFilter, and its iterator
    CloseableIterable<HiveRow> deleteOutputIterable = filter(deleteInputIterable);
    CloseableIterator<HiveRow> deleteOutputIterator = deleteOutputIterable.iterator();

    return new CloseableIterable<HiveBatchContext>() {

      @Override
      public CloseableIterator<HiveBatchContext> iterator() {

        CloseableIterator<HiveBatchContext> srcIterator = batches.iterator();

        return new CloseableIterator<HiveBatchContext>() {

          @Override
          public boolean hasNext() {
            return srcIterator.hasNext();
          }

          @Override
          public HiveBatchContext next() {
            try {
              if (!hasNext()) {
                throw new NoSuchElementException();
              }
              HiveBatchContext currentBatchContext = srcIterator.next();
              deleteInputIterable.currentRowIterator = currentBatchContext.rowIterator();
              VectorizedRowBatch batch = currentBatchContext.getBatch();

              int oldSize = batch.size;
              int newSize = 0;

              // Apply delete filtering and adjust the selected array so that undeleted row indices are filled with it.
              while (deleteOutputIterator.hasNext()) {
                HiveRow row = deleteOutputIterator.next();
                if (!row.isDeleted()) {
                  batch.selected[newSize++] = row.physicalBatchIndex();
                }
              }

              if (newSize < oldSize) {
                batch.size = newSize;
                batch.selectedInUse = true;
              }
              return currentBatchContext;
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }

          @Override
          public void close() throws IOException {
            srcIterator.close();
          }
        };
      }

      @Override
      public void close() throws IOException {
        batches.close();
      }
    };
  }

  // HiveRow iterable that wraps an interchangeable source HiveRow iterable
  static class SwappableHiveRowIterable implements CloseableIterable<HiveRow> {

    private CloseableIterator<HiveRow> currentRowIterator;

    @Override
    public CloseableIterator<HiveRow> iterator() {

      return new CloseableIterator<HiveRow>() {

        @Override
        public boolean hasNext() {
          return currentRowIterator.hasNext();
        }

        @Override
        public HiveRow next() {
          return currentRowIterator.next();
        }

        @Override
        public void close() throws IOException {
          currentRowIterator.close();
        }
      };
    }

    @Override
    public void close() throws IOException {
      currentRowIterator.close();
    }
  }
}
