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

package org.apache.hadoop.hive.ql.io.parquet.vector.probe;

/**
 * Row-level selection state produced by the Parquet ProbeDecode path.
 *
 * <p>After the join-key column has been decoded and probed against the small-table hash table
 * (see {@link ParquetProbeHashTable}), the surviving row positions within the current batch are
 * recorded here so that the remaining non-key columns can be read via
 * {@code VectorizedColumnReader.readBatch(total, column, type, ParquetProbeFilter)} and skip
 * decode / conversion work for rows that will be filtered out anyway.
 *
 * <p>The filter is <em>advisory</em> for correctness: a reader is free to decode every row and
 * ignore the filter (the default interface method does exactly that). Slots that are marked
 * filtered-out must still be advanced in the underlying page state so subsequent reads stay
 * aligned; concrete readers achieve that by calling {@code skip()} on the {@code ParquetDataColumnReader}.
 *
 * <p>Lifecycle within a batch: {@link ParquetProbeHashTable#probe} returns a filter in bitmap form
 * (one boolean per row); per-row column readers query it via {@link #isSelected(int)} while
 * decoding; once all columns are read, {@link VectorizedParquetRecordReader} calls
 * {@link #compact(int)} to materialize a {@code selected[]} that
 * {@link org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch} consumes downstream. The
 * compacted form is not queried per row -- it flows directly into
 * {@code VectorizedRowBatch.selected}.
 */
public final class ParquetProbeFilter {

  private final boolean[] bitmap;
  private int[] selected;
  private int selectedSize;

  private ParquetProbeFilter(boolean[] bitmap) {
    this.bitmap = bitmap;
  }

  public static ParquetProbeFilter newBitmap(boolean[] bitmap) {
    if (bitmap == null) {
      throw new IllegalArgumentException("bitmap must be non-null");
    }
    return new ParquetProbeFilter(bitmap);
  }

  /**
   * Test whether the given row index survives the filter.
   *
   * @param rowId 0-based row index within the current batch
   * @return {@code true} if the row should be decoded fully, {@code false} if the reader may skip
   *         decode / conversion for that row
   */
  public boolean isSelected(int rowId) {
    return rowId >= 0 && rowId < bitmap.length && bitmap[rowId];
  }

  /**
   * Materialize a compact {@code selected[]} representation over the first {@code batchSize} rows
   * from the underlying bitmap. Idempotent: repeated calls are cheap no-ops after the first.
   *
   * @return {@code this} for chaining
   */
  public ParquetProbeFilter compact(int batchSize) {
    if (selected != null) {
      return this;
    }
    int upper = Math.min(batchSize, bitmap.length);
    int[] out = new int[upper];
    int n = 0;
    for (int i = 0; i < upper; i++) {
      if (bitmap[i]) {
        out[n++] = i;
      }
    }
    this.selected = out;
    this.selectedSize = n;
    return this;
  }

  /** @return the compact {@code selected[]} array; {@code null} until {@link #compact(int)} runs. */
  public int[] getSelected() {
    return selected;
  }

  /** @return the number of live entries in {@link #getSelected()}; {@code 0} until {@link #compact(int)} runs. */
  public int getSelectedSize() {
    return selectedSize;
  }
}
