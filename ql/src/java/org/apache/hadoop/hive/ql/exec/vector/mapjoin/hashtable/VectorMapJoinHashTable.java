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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable;

import java.io.IOException;

import org.apache.hadoop.hive.common.MemoryEstimate;
import org.apache.hadoop.hive.ql.exec.persistence.MatchTracker;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.BytesWritable;

/*
 * Root interface for a vector map join hash table (which could be a hash map, hash multi-set, or
 * hash set).
 */
public interface VectorMapJoinHashTable extends MemoryEstimate {

  /**
   * @param hashCode current HashCode to avoid re-computation
   * @param currentKey The current Key in bytes
   * @param currentValue The current Value in bytes
   */
  void putRow(long hashCode, BytesWritable currentKey, BytesWritable currentValue)
      throws SerDeException, HiveException, IOException;

  /**
   *
   * @param currentKey
   *          The key to check for existence.
   * @return true
   *          If HashTable contains the given key.
   */
  boolean containsLongKey(long currentKey);

  /**
   * Get hash table size
   */
  int size();

  MatchTracker createMatchTracker();

  VectorMapJoinNonMatchedIterator createNonMatchedIterator(MatchTracker matchTracker);

  int spillPartitionId();
}
