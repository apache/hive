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

public final class LlapDataBuffer extends LlapAllocatorBuffer {
  public static final int UNKNOWN_CACHED_LENGTH = -1;

  /** ORC cache uses this to store compressed length; buffer is cached uncompressed, but
   * the lookup is on compressed ranges, so we need to know this. */
  public int declaredCachedLength = UNKNOWN_CACHED_LENGTH;

  @Override
  public void notifyEvicted(EvictionDispatcher evictionDispatcher) {
    evictionDispatcher.notifyEvicted(this);
  }
}
