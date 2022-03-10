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
package org.apache.hadoop.hive.llap.cache;

import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;

/**
 * An allocator that has additional, internal-only call to deallocate evicted buffer.
 * When we evict buffers, we do not release memory to the system; that is because we want it for
 * ourselves, so we set the value atomically to account for both eviction and the new demand.
 */
public interface EvictionAwareAllocator extends Allocator {
  void deallocateEvicted(MemoryBuffer buffer);
  void deallocateProactivelyEvicted(MemoryBuffer buffer);
}
