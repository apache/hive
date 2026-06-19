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

/*
 * The interface adds the single long key hash multi-set contains method.
 */
public interface VectorMapJoinLongHashSet
          extends VectorMapJoinLongHashTable, VectorMapJoinHashSet {

  /*
   * Lookup an long in the hash set.
   *
   * @param key
   *         The long key.
   * @param hashSetResult
   *         The object to receive small table value(s) information on a MATCH.
   *         Or, for SPILL, it has information on where to spill the big table row.
   *
   * @return
   *         Whether the lookup was a match, no match, or spilled (the partition with the key
   *         is currently spilled).
   */
  JoinUtil.JoinResult contains(long key, VectorMapJoinHashSetResult hashSetResult) throws IOException;

}
