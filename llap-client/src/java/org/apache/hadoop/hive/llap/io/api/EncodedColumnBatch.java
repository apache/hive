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

package org.apache.hadoop.hive.llap.io.api;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.llap.io.api.cache.LlapMemoryBuffer;

public class EncodedColumnBatch<BatchKey> {
  // TODO: temporary class. Will be filled in when reading (ORC) is implemented. Need to balance
  //       generality, and ability to not copy data from underlying low-level cached buffers.
  public static class StreamBuffer {
    // Decoder knows which stream this belongs to, and each buffer is a compression block,
    // so he can figure out the offsets from metadata.
    public List<LlapMemoryBuffer> cacheBuffers;
    public int streamKind;

    // StreamBuffer can be reused for many RGs (e.g. dictionary case). To avoid locking every
    // LlapMemoryBuffer 500 times, have a separate refcount on StreamBuffer itself.
    public AtomicInteger refCount = new AtomicInteger(0);

    public void init(int kind) {
      streamKind = kind;
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
  }

  public BatchKey batchKey;
  public StreamBuffer[][] columnData;
  public int[] columnIxs;
  /** Generation version necessary to sync pooling reuse with the fact that two separate threads
   * operate on batches - the one that decodes them, and potential separate thread w/a "stop" call
   * that cleans them up. We don't want the decode thread to use the ECB that was thrown out and
   * reused, so it remembers the version and checks it after making sure no cleanup thread can ever
   * get to this ECB anymore. All this sync is ONLY needed because of high level cache code (sync
   * in decode thread is for the map that combines columns coming from cache and from file), so
   * if we throw this presently-unused code out, we'd be able to get rid of this. */
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
    columnData[colIxMod] = new StreamBuffer[streamCount];
  }

  public void setStreamData(int colIxMod, int streamIx, StreamBuffer sb) {
    columnData[colIxMod][streamIx] = sb;
  }

  public void setAllStreams(int colIxMod, int colIx, StreamBuffer[] sbs) {
    columnIxs[colIxMod] = colIx;
    columnData[colIxMod] = sbs;
  }
}