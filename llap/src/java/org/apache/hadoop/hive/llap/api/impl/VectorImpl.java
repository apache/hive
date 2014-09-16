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


package org.apache.hadoop.hive.llap.api.impl;

import java.util.Collection;

import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.api.Llap;
import org.apache.hadoop.hive.llap.api.Vector;
import org.apache.hadoop.hive.llap.cache.BufferPool;
import org.apache.hadoop.hive.llap.cache.BufferPool.WeakBuffer;
import org.apache.hadoop.hive.llap.chunk.ChunkReader;
import org.apache.hadoop.hive.llap.loader.ChunkPool.Chunk;

/**
 * Implementation of Vector. Handles mapping of Vector API to chunks.
 */
// TODO: write unit tests if this class becomes less primitive.
public class VectorImpl implements Vector {
  private Collection<BufferPool.WeakBuffer> buffers;
  // TODO: we store columns by plain index, with nulls for the unneeded ones.
  //       Better representation may be added.
  private final Chunk[] chunksPerCol;
  private final Type[] types;
  private final ChunkReader[] readers;
  private transient int cachedNumRows = -1;
  private int numColumnsSet = 0;

  public VectorImpl(Collection<WeakBuffer> buffers, int colCount) {
    chunksPerCol = new Chunk[colCount];
    types = new Type[colCount];
    readers = new ChunkReader[colCount];
    this.buffers = buffers;
  }

  public Collection<BufferPool.WeakBuffer> getCacheBuffers() {
    return buffers;
  }

  /**
   * Adds chunk for a column to vector. Can only be called once per column;
   * caller has to take care of chaining if there are multiple chunks.
   */
  public void addChunk(int colIx, Chunk colChunk, Type type) {
    assert chunksPerCol[colIx] == null;
    chunksPerCol[colIx] = colChunk;
    types[colIx] = type; // not necessary, but helps simplify reading (see prepare).
    ++numColumnsSet;
  }

  @Override
  public int getNumberOfColumns() {
    return numColumnsSet;
  }

  @Override
  public int getNumberOfRows() {
    if (cachedNumRows < 0) {
      // We assume every chunk has the same number of rows.
      int i = -1;
      while (chunksPerCol[++i] == null);
      cachedNumRows = (new ChunkReader(types[i], chunksPerCol[i])).getNumRowsRemaining();
    }
    return cachedNumRows;
  }

  @Override
  public Vector.ColumnReader next(int colIx, int rowCount) {
    if (DebugUtils.isTraceEnabled()) {
      Llap.LOG.info("Next called for column " + colIx + ", asking for " + rowCount);
    }
    ChunkReader reader = readers[colIx];
    if (reader == null) {
      reader = readers[colIx] = new ChunkReader(types[colIx], chunksPerCol[colIx]);
    }
    reader.next(rowCount);
    return reader;
  }
}
