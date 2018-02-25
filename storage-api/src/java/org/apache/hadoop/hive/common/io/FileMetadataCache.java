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
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;

public interface FileMetadataCache {
  /**
   * @return Metadata for a given file (ORC or Parquet footer).
   *         The caller must decref this buffer when done.
   */
  MemoryBufferOrBuffers getFileMetadata(Object fileKey);

  /**
   * Puts the metadata for a given file (e.g. a footer buffer into cache).
   * @param fileKey The file key.
   * @param length The footer length.
   * @param is The stream to read the footer from.
   * @return The buffer or buffers representing the cached footer.
   *         The caller must decref this buffer when done.
   */
  MemoryBufferOrBuffers putFileMetadata(
      Object fileKey, int length, InputStream is) throws IOException;

  MemoryBufferOrBuffers putFileMetadata(Object fileKey, ByteBuffer tailBuffer);

  /**
   * Releases the buffer returned from getFileMetadata or putFileMetadata method.
   * @param buffer The buffer to release.
   */
  void decRefBuffer(MemoryBufferOrBuffers buffer);
} 