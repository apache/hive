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
package org.apache.hadoop.hive.ql.io.orc.encoded;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;

/**
 * DiskRange containing encoded, uncompressed data from cache. Beyond ORC's own EncodedReaderImpl,
 * it is part of the cache's public read surface: any reader consuming cached chunks — ORC and
 * Parquet alike — receives them typed as CacheChunk from the LowLevelCache.
 */
public class CacheChunk extends DiskRangeList {
  protected MemoryBuffer buffer;

  public CacheChunk(MemoryBuffer buffer, long offset, long end) {
    super(offset, end);
    this.buffer = buffer;
    this.offset = offset;
    this.end = end;
  }

  @Override
  public boolean hasData() {
    return buffer != null;
  }

  @Override
  public ByteBuffer getData() {
    // Callers duplicate the buffer, they have to for BufferChunk; so we don't have to.
    return buffer.getByteBufferRaw();
  }

  @Override
  public String toString() {
    return "start: " + offset + " end: " + end + " cache buffer: " + getBuffer();
  }

  @Override
  public DiskRange sliceAndShift(long offset, long end, long shiftBy) {
    throw new UnsupportedOperationException("Cache chunk cannot be sliced - attempted ["
        + this.offset + ", " + this.end + ") to [" + offset + ", " + end + ") ");
  }

  public MemoryBuffer getBuffer() {
    return buffer;
  }

  public void setBuffer(MemoryBuffer buffer) {
    this.buffer = buffer;
  }

  public void handleCacheCollision(DataCache cache,
      MemoryBuffer replacementBuffer, List<MemoryBuffer> cacheBuffers) {
    throw new UnsupportedOperationException();
  }

  public void adjustEnd(long l) {
    this.end += l;
  }
}
