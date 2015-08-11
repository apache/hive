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
    private int indexBaseOffset;
    /** Stream type; format-specific. */
    private int streamKind;

    /** Reference count. */
    private AtomicInteger refCount = new AtomicInteger(0);

    public void init(int kind) {
      streamKind = kind;
      indexBaseOffset = 0;
    }

    public void reset() {
      cacheBuffers.clear();
      refCount.set(0);
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

    public int getStreamKind() {
      return streamKind;
    }
  }

  /** The key that is used to map this batch to source location. */
  protected BatchKey batchKey;
  /** Stream data for each stream, for each included column. */
  protected ColumnStreamData[][] columnData;
  /** Column indexes included in the batch. Correspond to columnData elements. */
  protected int[] columnIxs;
  // TODO: Maybe remove when solving the pooling issue.
  /** Generation version necessary to sync pooling reuse with the fact that two separate threads
   * operate on batches - the one that decodes them, and potential separate thread w/a "stop" call
   * that cleans them up. We don't want the decode thread to use the ECB that was thrown out and
   * reused, so it remembers the version and checks it after making sure no cleanup thread can ever
   * get to this ECB anymore. All this sync is ONLY needed because of high level cache code. */
  public int version = Integer.MIN_VALUE;

  public void reset() {
    if (columnData != null) {
      for (int i = 0; i < columnData.length; ++i) {
        columnData[i] = null;
      }
    }
  }

  public void initColumn(int colIxMod, int colIx, int streamCount) {
    columnIxs[colIxMod] = colIx;
    columnData[colIxMod] = new ColumnStreamData[streamCount];
  }

  public void setStreamData(int colIxMod, int streamIx, ColumnStreamData sb) {
    columnData[colIxMod][streamIx] = sb;
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
    columnData = new ColumnStreamData[columnCount][];
  }
}