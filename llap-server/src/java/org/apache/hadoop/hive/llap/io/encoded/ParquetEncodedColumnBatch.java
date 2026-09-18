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

package org.apache.hadoop.hive.llap.io.encoded;

import org.apache.hadoop.hive.common.io.encoded.EncodedColumnBatch;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

/**
 * One row-group's worth of cached Parquet column-chunk buffers, handed from the reader to the
 * consumer. Extends {@link EncodedColumnBatch} only to satisfy the {@code EncodedDataConsumer}
 * generic bound; the inherited ColumnStreamData machinery is ORC-shaped and unused here. The real
 * payload is in the fields exposed through the accessors below.
 */
public final class ParquetEncodedColumnBatch extends EncodedColumnBatch<Object> {

  private int rowGroupIx;
  /** This row group's chunk per projected column; the arrays below are indexed the same way. */
  private ColumnChunkMetaData[] chunks;
  private MemoryBuffer[][] columnBuffers;
  private long[][] bufferOffsets;
  private int[][] bufferLengths;

  public ParquetEncodedColumnBatch() {
    // No-arg constructor for pooling / reflection-based construction; fields are populated later
    // by init(...). Left empty on purpose.
  }

  /** fileKey is the cache key; rowGroupIx is the footer index of the block within the file. */
  public void init(Object fileKey, int rowGroupIx, ColumnChunkMetaData[] chunks) {
    this.batchKey = fileKey;
    this.rowGroupIx = rowGroupIx;
    this.chunks = chunks;
    int n = chunks.length;
    resetColumnArrays(n);
    this.columnBuffers = new MemoryBuffer[n][];
    this.bufferOffsets = new long[n][];
    this.bufferLengths = new int[n][];
  }

  public int rowGroupIx() {
    return rowGroupIx;
  }

  public ColumnChunkMetaData[] chunks() {
    return chunks;
  }

  public MemoryBuffer[][] columnBuffers() {
    return columnBuffers;
  }

  public long[][] bufferOffsets() {
    return bufferOffsets;
  }

  public int[][] bufferLengths() {
    return bufferLengths;
  }
}
