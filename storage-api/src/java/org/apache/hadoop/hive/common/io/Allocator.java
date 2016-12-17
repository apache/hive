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
package org.apache.hadoop.hive.common.io;

import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;

/** An allocator provided externally to storage classes to allocate MemoryBuffer-s. */
public interface Allocator {
  public static class AllocatorOutOfMemoryException extends RuntimeException {
    public AllocatorOutOfMemoryException(String msg) {
      super(msg);
    }

    private static final long serialVersionUID = 268124648177151761L;
  }

  /**
   * Allocates multiple buffers of a given size.
   * @param dest Array where buffers are placed. Objects are reused if already there
   *             (see createUnallocated), created otherwise.
   * @param size Allocation size.
   * @throws AllocatorOutOfMemoryException Cannot allocate.
   */
  void allocateMultiple(MemoryBuffer[] dest, int size) throws AllocatorOutOfMemoryException;

  /**
   * Creates an unallocated memory buffer object. This object can be passed to allocateMultiple
   * to allocate; this is useful if data structures are created for separate buffers that can
   * later be allocated together.
   * @return a new unallocated memory buffer
   */
  MemoryBuffer createUnallocated();

  /** Deallocates a memory buffer.
   * @param buffer the buffer to deallocate
   */
  void deallocate(MemoryBuffer buffer);

  /** Whether the allocator uses direct buffers.
   * @return are they direct buffers?
   */
  boolean isDirectAlloc();

  /** Maximum allocation size supported by this allocator.
   * @return a number of bytes
   */
  int getMaxAllocation();
}
