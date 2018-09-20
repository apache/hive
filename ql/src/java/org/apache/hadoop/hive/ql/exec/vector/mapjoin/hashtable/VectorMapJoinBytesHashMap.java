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

import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.persistence.MatchTracker;

/*
 * The interface for a single byte array key hash map lookup method.
 */
public interface VectorMapJoinBytesHashMap
        extends VectorMapJoinBytesHashTable, VectorMapJoinHashMap {

  /*
   * Lookup a byte array key in the hash map.
   *
   * @param keyBytes
   *         A byte array containing the key within a range.
   * @param keyStart
   *         The offset the beginning of the key.
   * @param keyLength
   *         The length of the key.
   * @param hashMapResult
   *         The object to receive small table value(s) information on a MATCH.
   *         Or, for SPILL, it has information on where to spill the big table row.
   *
   *         NOTE: Since the hash table can be shared, the hashMapResult serves as the non-shared
   *         private object for our accessing the hash table lookup values, etc.
   *
   * @return
   *         Whether the lookup was a match, no match, or spill (the partition with the key
   *         is currently spilled).
   */
  JoinUtil.JoinResult lookup(byte[] keyBytes, int keyStart, int keyLength,
          VectorMapJoinHashMapResult hashMapResult) throws IOException;

  /*
   * A version of lookup with match tracking.
   * ...
   *   * @param matchTracker
   *        Optional key match tracking.
   *
   *        NOTE: Since the hash table can be shared, the matchTracker serves as the non-shared
   *        private object for tracking our key matches in the hash table.
   * ...
   */
  JoinUtil.JoinResult lookup(byte[] keyBytes, int keyStart, int keyLength,
      VectorMapJoinHashMapResult hashMapResult, MatchTracker matchTracker) throws IOException;
}