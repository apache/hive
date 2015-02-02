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

public class EncodedColumn<BatchKey> {
  // TODO: temporary class. Will be filled in when reading (ORC) is implemented. Need to balance
  //       generality, and ability to not copy data from underlying low-level cached buffers.
  public static class StreamBuffer {
    public StreamBuffer(int firstOffset, int lastLength) {
      this.firstOffset = firstOffset;
      this.lastLength = lastLength;
    }
    // TODO: given how ORC will allocate, it might make sense to share array between all
    //       returned encodedColumn-s, and store index and length in the array.
    public List<LlapMemoryBuffer> cacheBuffers;
    public int firstOffset, lastLength;
    // StreamBuffer can be reused for many RGs (e.g. dictionary case). To avoid locking every
    // LlapMemoryBuffer 500 times, have a separate refcount on StreamBuffer itself.
    public AtomicInteger refCount = new AtomicInteger(0);
    public void incRef() {
      refCount.incrementAndGet();
    }
    public int decRef() {
      return refCount.decrementAndGet();
    }
  }
  public EncodedColumn(BatchKey batchKey, int columnIndex, int streamCount) {
    this.batchKey = batchKey;
    this.columnIndex = columnIndex;
    this.streamData = new StreamBuffer[streamCount];
    this.streamKind = new int[streamCount];
  }

  public BatchKey batchKey;
  public int columnIndex;
  public StreamBuffer[] streamData;
  public int[] streamKind; // TODO: can decoder infer this from metadata?
}