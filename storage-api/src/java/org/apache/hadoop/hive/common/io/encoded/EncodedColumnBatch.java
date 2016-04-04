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

package org.apache.hadoop.hive.common.io.encoded;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A block of data for a given section of a file, similar to VRB but in encoded form.
 * Stores a set of buffers for each encoded stream that is a part of each column.
 */
public class EncodedColumnBatch<BatchKey> {
  /**
   * Slice of the data for a stream for some column, stored inside MemoryBuffer's.
   * ColumnStreamData can be reused for many EncodedColumnBatch-es (e.g. dictionary stream), so
   * it tracks the number of such users via a refcount.
   */
  public static class ColumnStreamData {
    private List<MemoryBuffer> cacheBuffers;
    /** Base offset from the beginning of the indexable unit; for example, for ORC,
     * offset from the CB in a compressed file, from the stream in uncompressed file. */
    private int indexBaseOffset = 0;

    /** Reference count. */
    private AtomicInteger refCount = new AtomicInteger(0);

    public void reset() {
      cacheBuffers.clear();
      refCount.set(0);
      indexBaseOffset = 0;
    }

    public void incRef() {
      refCount.incrementAndGet();
    }

    public int decRef() {
      int i = refCount.decrementAndGet();
      assert i >= 0;
      return i;
    }

    public List<MemoryBuffer> getCacheBuffers() {
      return cacheBuffers;
    }

    public void setCacheBuffers(List<MemoryBuffer> cacheBuffers) {
      this.cacheBuffers = cacheBuffers;
    }

    public int getIndexBaseOffset() {
      return indexBaseOffset;
    }

    public void setIndexBaseOffset(int indexBaseOffset) {
      this.indexBaseOffset = indexBaseOffset;
    }
  }

  /** The key that is used to map this batch to source location. */
  protected BatchKey batchKey;
  /**
   * Stream data for each stream, for each included column.
   * For each column, streams are indexed by kind, with missing elements being null.
   */
  protected ColumnStreamData[][] columnData;
  /** Column indexes included in the batch. Correspond to columnData elements. */
  protected int[] columnIxs;

  public void reset() {
    if (columnData == null) return;
    for (int i = 0; i < columnData.length; ++i) {
      if (columnData[i] == null) continue;
      for (int j = 0; j < columnData[i].length; ++j) {
        columnData[i][j] = null;
      }
    }
  }

  public void initColumn(int colIxMod, int colIx, int streamCount) {
    columnIxs[colIxMod] = colIx;
    if (columnData[colIxMod] == null || columnData[colIxMod].length != streamCount) {
      columnData[colIxMod] = new ColumnStreamData[streamCount];
    }
  }

  public void setStreamData(int colIxMod, int streamKind, ColumnStreamData csd) {
    columnData[colIxMod][streamKind] = csd;
  }

  public void setAllStreamsData(int colIxMod, int colIx, ColumnStreamData[] sbs) {
    columnIxs[colIxMod] = colIx;
    columnData[colIxMod] = sbs;
  }

  public BatchKey getBatchKey() {
    return batchKey;
  }

  public ColumnStreamData[][] getColumnData() {
    return columnData;
  }

  public int[] getColumnIxs() {
    return columnIxs;
  }

  protected void resetColumnArrays(int columnCount) {
    if (columnIxs != null && columnCount == columnIxs.length) return;
    columnIxs = new int[columnCount];
    ColumnStreamData[][] columnData = new ColumnStreamData[columnCount][];
    if (this.columnData != null) {
      for (int i = 0; i < Math.min(columnData.length, this.columnData.length); ++i) {
        columnData[i] = this.columnData[i];
      }
    }
    this.columnData = columnData;
  }
}