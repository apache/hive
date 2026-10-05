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

import java.nio.ByteBuffer;

import javax.annotation.Nullable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.io.ElasticByteBufferPool;

/**
 * Heap buffers for one stream's vectored reads. A stream returning slices of a larger read says
 * so ({@link StreamCapabilities#VECTOREDIO_BUFFERS_SLICED}); those alias each other and are left
 * to the collector. Other streams' buffers are exclusive and pooled for reuse across the file's
 * row groups. The pool is per stream, not per JVM: {@link ElasticByteBufferPool} never evicts
 * and has one lock, so a shared one would pin one IO thread's peak for the daemon's life.
 *
 * <p>In Hadoop 3.4.2 the only filesystem that declares {@code VECTOREDIO_BUFFERS_SLICED} is the
 * local checksum one, so S3A and HDFS both take the pooled path and the unpooled one is exercised
 * by tests. It is still the branch that has to exist: a stream handing back slices of one buffer
 * would have them recycled under each other by a pool.
 *
 * <p>Nothing here is Parquet-specific; it is the staging area for
 * {@link org.apache.hadoop.fs.FSDataInputStream#readVectored}, and the Parquet reader happens to be
 * its only caller.
 */
final class VectoredReadBuffers {
  private final ElasticByteBufferPool pool;

  /** Pooled: the stream fills buffers we own, so they can be reused across the split's row groups. */
  static VectoredReadBuffers pooled() {
    return new VectoredReadBuffers(new ElasticByteBufferPool());
  }

  /** Unpooled: the stream may hand back slices of a larger buffer; slices alias, so they go to the GC. */
  static VectoredReadBuffers unpooled() {
    return new VectoredReadBuffers(null);
  }

  static VectoredReadBuffers forStream(FSDataInputStream stream) {
    return stream.hasCapability(StreamCapabilities.VECTOREDIO_BUFFERS_SLICED) ? unpooled() : pooled();
  }

  private VectoredReadBuffers(@Nullable ElasticByteBufferPool pool) {
    this.pool = pool;
  }

  ByteBuffer allocate(int length) {
    if (pool == null) {
      return ByteBuffer.allocate(length);
    }
    ByteBuffer buffer = pool.getBuffer(false, length);
    // The pool hands back anything large enough; limit it so a reader filling remaining() stops
    // at the range end.
    buffer.limit(length);
    return buffer;
  }

  void release(ByteBuffer buffer) {
    if (pool != null) {
      pool.putBuffer(buffer);
    }
  }
}
