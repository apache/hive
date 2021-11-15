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
package org.apache.hadoop.hive.upgrade.acid;

import java.util.HashSet;
import java.util.Set;

/**
 * Store result of compaction calls.
 */
public class CompactionMetaInfo {
  /**
   * total number of bytes to be compacted across all compaction commands.
   */
  private long numberOfBytes;
  /**
   * IDs of compactions launched by this utility.
   */
  private final Set<Long> compactionIds;

  public CompactionMetaInfo() {
    compactionIds = new HashSet<>();
    numberOfBytes = 0;
  }

  private CompactionMetaInfo(Set<Long> initialCompactionIds, long initialNumberOfBytes) {
    this.compactionIds = new HashSet<>(initialCompactionIds);
    numberOfBytes = initialNumberOfBytes;
  }

  public CompactionMetaInfo merge(CompactionMetaInfo other) {
    CompactionMetaInfo result = new CompactionMetaInfo(this.compactionIds, this.numberOfBytes);
    result.numberOfBytes += other.numberOfBytes;
    result.compactionIds.addAll(other.compactionIds);
    return result;
  }

  public long getNumberOfBytes() {
    return numberOfBytes;
  }

  public void addBytes(long bytes) {
    numberOfBytes += bytes;
  }

  public Set<Long> getCompactionIds() {
    return compactionIds;
  }

  public void addCompactionId(long compactionId) {
    compactionIds.add(compactionId);
  }
}
