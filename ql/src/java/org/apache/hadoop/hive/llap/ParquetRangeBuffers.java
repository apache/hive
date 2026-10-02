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

package org.apache.hadoop.hive.llap;

import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.io.ElasticByteBufferPool;

/**
 * Heap buffers for one stream's vectored reads. A stream returning slices of a larger read says
 * so ({@link StreamCapabilities#VECTOREDIO_BUFFERS_SLICED}); those alias each other and are left
 * to the collector. Other streams' buffers are exclusive and pooled for reuse across the file's
 * row groups. The pool is per stream, not per JVM: {@link ElasticByteBufferPool} never evicts
 * and has one lock, so a shared one would pin every executor's peak for the daemon's life.
 */
public final class ParquetRangeBuffers {
  private final ElasticByteBufferPool pool;

  private ParquetRangeBuffers(boolean pooled) {
    this.pool = pooled ? new ElasticByteBufferPool() : null;
  }

  public static ParquetRangeBuffers forStream(FSDataInputStream stream) {
    return new ParquetRangeBuffers(!stream.hasCapability(StreamCapabilities.VECTOREDIO_BUFFERS_SLICED));
  }

  public ByteBuffer allocate(int length) {
    if (pool == null) {
      return ByteBuffer.allocate(length);
    }
    ByteBuffer buffer = pool.getBuffer(false, length);
    // The pool hands back anything large enough; limit it so a reader filling remaining() stops
    // at the range end.
    buffer.limit(length);
    return buffer;
  }

  public void release(ByteBuffer buffer) {
    if (pool != null) {
      pool.putBuffer(buffer);
    }
  }
}
