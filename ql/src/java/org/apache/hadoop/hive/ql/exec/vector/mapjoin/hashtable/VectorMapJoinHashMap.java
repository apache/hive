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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable;

/*
 * The root interface for a vector map join hash map.
 */
public interface VectorMapJoinHashMap extends VectorMapJoinHashTable {

  /*
   * @return A new hash map result implementation specific object.
   *
   * The object can be used to access the values when there is a match, or
   * access spill information when the partition with the key is currently spilled.
   */
  VectorMapJoinHashMapResult createHashMapResult();

}
