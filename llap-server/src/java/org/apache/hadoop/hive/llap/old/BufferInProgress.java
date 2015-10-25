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

package org.apache.hadoop.hive.llap.old;

import org.apache.hadoop.hive.llap.old.BufferPool.WeakBuffer;
import org.apache.hadoop.hive.llap.old.ChunkPool.Chunk;

/**
 * Helper struct that is used by loaders (e.g. OrcLoader) and chunk writer to write chunks.
 */
public class BufferInProgress {
  /** Buffer that is being written to. */
  public final WeakBuffer buffer;
  /** Offset in buffer where writing can proceed */
  public int offset; // TODO: use WB's position; these have separate lifecycle now, needed?
  private final int bufferLimit;

  /** The chunk that is currently being written. */
  private Chunk chunkInProgress = null;
  /** The row count of the chunk currently being written. */
  private int chunkInProgressRows = 0;

  public BufferInProgress(WeakBuffer buffer) {
    this.buffer = buffer;
    this.bufferLimit = buffer.getContents().limit();
    this.offset = 0;
  }

  public Chunk ensureChunk() {
    if (chunkInProgress == null) {
      chunkInProgress = new Chunk(buffer, offset, 0);
      chunkInProgressRows = 0;
    }
    return chunkInProgress;
  }

  public Chunk extractChunk() {
    Chunk result = chunkInProgress;
    chunkInProgress = null;
    chunkInProgressRows = 0;
    return result;
  }

  public void update(int newOffset, int rowsWritten) {
    if (newOffset > bufferLimit) {
      throw new AssertionError("Offset is beyond buffer limit: " + newOffset + "/" + bufferLimit
         + "; previous offset " + offset + ", chunk " + chunkInProgress);
    }
    chunkInProgress.length += (newOffset - offset);
    this.offset = newOffset;
    this.chunkInProgressRows += rowsWritten;
  }

  public int getChunkInProgressRows() {
    return chunkInProgressRows;
  }

  public int getSpaceLeft() {
    return getSpaceLeft(-1);
  }

  public int getSpaceLeft(int offset) {
    offset = (offset >= 0) ? offset : this.offset;
    return buffer.getContents().limit() - offset;
  }
}
