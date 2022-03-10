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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.common.io;


import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Predicate;

import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;

public interface FileMetadataCache {
  /**
   * @return Metadata for a given file (ORC or Parquet footer).
   *         The caller must decref this buffer when done.
   */
  MemoryBufferOrBuffers getFileMetadata(Object fileKey);

  @Deprecated
  MemoryBufferOrBuffers putFileMetadata(
      Object fileKey, int length, InputStream is) throws IOException;

  @Deprecated
  MemoryBufferOrBuffers putFileMetadata(Object fileKey, ByteBuffer tailBuffer);

  @Deprecated
  MemoryBufferOrBuffers putFileMetadata(
      Object fileKey, int length, InputStream is, CacheTag tag) throws IOException;

  @Deprecated
  MemoryBufferOrBuffers putFileMetadata(Object fileKey, ByteBuffer tailBuffer, CacheTag tag);

  /**
   * Releases the buffer returned from getFileMetadata or putFileMetadata method.
   * @param buffer The buffer to release.
   */
  void decRefBuffer(MemoryBufferOrBuffers buffer);


  /**
   * Puts the metadata for a given file (e.g. a footer buffer into cache).
   * @param fileKey The file key.
   * @return The buffer or buffers representing the cached footer.
   *         The caller must decref this buffer when done.
   */
  MemoryBufferOrBuffers putFileMetadata(Object fileKey, ByteBuffer tailBuffer,
      CacheTag tag, AtomicBoolean isStopped);

  MemoryBufferOrBuffers putFileMetadata(Object fileKey, int length,
      InputStream is, CacheTag tag, AtomicBoolean isStopped) throws IOException;

  /**
   * Iterates through the file entries of this cache and for those that match the given predicate (aka have a matching
   * CacheTag) will have their buffers marked for (a later) proactive eviction.
   * @param predicate - matching the predicate indicates eligibility for proactive eviction
   * @param isInstantDeallocation - whether to ask allocator to deallocate eligible buffers immediately after marking
   * @return number of bytes marked in the buffers eligible for eviction
   */
  long markBuffersForProactiveEviction(Predicate<CacheTag> predicate, boolean isInstantDeallocation);
}
