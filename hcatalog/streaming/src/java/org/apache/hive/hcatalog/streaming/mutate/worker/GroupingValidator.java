/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate.worker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Tracks the (partition, bucket) combinations that have been encountered, checking that a group is not revisited.
 * Potentially memory intensive.
 */
class GroupingValidator {

  private final Map<String, Set<Integer>> visited;
  private final StringBuffer partitionKeyBuilder;
  private long groups;
  private String lastPartitionKey;
  private int lastBucketId = -1;

  GroupingValidator() {
    visited = new HashMap<String, Set<Integer>>();
    partitionKeyBuilder = new StringBuffer(64);
  }

  /**
   * Checks that this group is either the same as the last or is a new group.
   */
  boolean isInSequence(List<String> partitionValues, int bucketId) {
    String partitionKey = getPartitionKey(partitionValues);
    if (Objects.equals(lastPartitionKey, partitionKey) && lastBucketId == bucketId) {
      return true;
    }
    lastPartitionKey = partitionKey;
    lastBucketId = bucketId;

    Set<Integer> bucketIdSet = visited.get(partitionKey);
    if (bucketIdSet == null) {
      // If the bucket id set component of this data structure proves to be too large there is the
      // option of moving it to Trove or HPPC in an effort to reduce size.
      bucketIdSet = new HashSet<>();
      visited.put(partitionKey, bucketIdSet);
    }

    boolean newGroup = bucketIdSet.add(bucketId);
    if (newGroup) {
      groups++;
    }
    return newGroup;
  }

  private String getPartitionKey(List<String> partitionValues) {
    partitionKeyBuilder.setLength(0);
    boolean first = true;
    for (String element : partitionValues) {
      if (first) {
        first = false;
      } else {
        partitionKeyBuilder.append('/');
      }
      partitionKeyBuilder.append(element);
    }
    String partitionKey = partitionKeyBuilder.toString();
    return partitionKey;
  }

  @Override
  public String toString() {
    return "GroupingValidator [groups=" + groups + ",lastPartitionKey=" + lastPartitionKey + ",lastBucketId="
        + lastBucketId + "]";
  }

}
