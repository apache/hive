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

import org.apache.hadoop.hive.serde2.WriteBuffers.ByteSegmentRef;

/*
 * Abstract class for a hash map result.  For reading the values, one-by-one.
 */
public abstract class VectorMapJoinHashMapResult extends VectorMapJoinHashTableResult {

  /**
   * @return Whether there are any rows (i.e. true for match).
   */
  public abstract boolean hasRows();

  /**
   * @return Whether there is 1 value row.
   */
  public abstract boolean isSingleRow();

  /**
   * @return Whether there is a capped count available from cappedCount.
   */
  public abstract boolean isCappedCountAvailable();

  /**
   * @return The count of values, up to a arbitrary cap limit.  When available, the capped
   *         count can be used to make decisions on how to optimally generate join results.
   */
  public abstract int cappedCount();

  /**
   * @return A reference to the first value, or null if there are no values.
   */
  public abstract ByteSegmentRef first();

  /**
   * @return The next value, or null if there are no more values to be read.
   */
  public abstract ByteSegmentRef next();

  /**
   * @return Whether reading is at the end.
   */
  public abstract boolean isEof();
}
