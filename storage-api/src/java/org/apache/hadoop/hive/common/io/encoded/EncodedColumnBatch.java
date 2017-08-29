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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    @Override
    public String toString() {
      String bufStr = "";
      if (cacheBuffers != null) {
        for (MemoryBuffer mb : cacheBuffers) {
          bufStr += mb.getClass().getSimpleName() + " with " + mb.getByteBufferRaw().remaining() + " bytes, ";
        }
      }
      return "ColumnStreamData [cacheBuffers=[" + bufStr
          + "], indexBaseOffset=" + indexBaseOffset + "]";
    }

  }

  /** The key that is used to map this batch to source location. */
  protected BatchKey batchKey;
  /**
   * Stream data for each column that has true in the corresponding hasData position.
   * For each column, streams are indexed by kind (for ORC), with missing elements being null.
   */
  protected ColumnStreamData[][] columnData;
  /** Indicates which columns have data. Correspond to columnData elements. */
  protected boolean[] hasData;

  public void reset() {
    if (hasData != null) {
      Arrays.fill(hasData, false);
    }
    if (columnData == null) return;
    for (int i = 0; i < columnData.length; ++i) {
      if (columnData[i] == null) continue;
      for (int j = 0; j < columnData[i].length; ++j) {
        columnData[i][j] = null;
      }
    }
  }


  public void initColumn(int colIx, int streamCount) {
    hasData[colIx] = true;
    if (columnData[colIx] == null || columnData[colIx].length != streamCount) {
      columnData[colIx] = new ColumnStreamData[streamCount];
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(EncodedColumnBatch.class);
  public void setStreamData(int colIx, int streamIx, ColumnStreamData csd) {
    assert hasData[colIx];
    columnData[colIx][streamIx] = csd;
  }

  public BatchKey getBatchKey() {
    return batchKey;
  }

  public ColumnStreamData[] getColumnData(int colIx) {
    if (!hasData[colIx]) throw new AssertionError("No data for column " + colIx);
    return columnData[colIx];
  }

  public int getTotalColCount() {
    return columnData.length; // Includes the columns that have no data
  }

  protected void resetColumnArrays(int columnCount) {
    if (hasData != null && columnCount == hasData.length) {
      Arrays.fill(hasData, false);
      return;
    }
    hasData = new boolean[columnCount];
    ColumnStreamData[][] columnData = new ColumnStreamData[columnCount][];
    if (this.columnData != null) {
      for (int i = 0; i < Math.min(columnData.length, this.columnData.length); ++i) {
        columnData[i] = this.columnData[i];
      }
    }
    this.columnData = columnData;
  }

  public boolean hasData(int colIx) {
    return hasData[colIx];
  }
}
