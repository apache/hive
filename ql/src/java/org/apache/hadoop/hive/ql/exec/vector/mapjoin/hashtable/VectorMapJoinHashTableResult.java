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

import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.serde2.WriteBuffers;

/*
 * Root abstract class for a hash table result.
 */
public abstract class VectorMapJoinHashTableResult {

  private JoinUtil.JoinResult joinResult;

  private int spillPartitionId;

  private final WriteBuffers.Position readPos;

  public VectorMapJoinHashTableResult() {
    joinResult = JoinUtil.JoinResult.NOMATCH;
    spillPartitionId = -1;
    readPos = new WriteBuffers.Position();
  }

  /**
   * @return The join result from the most recent hash map match, or hash multi-set / set contains
   *         call.
   */
  public JoinUtil.JoinResult joinResult() {
    return joinResult;
  }

  /**
   * Set the current join result.
   * @param joinResult
   *               The new join result.
   */
  public void setJoinResult(JoinUtil.JoinResult joinResult) {
    this.joinResult = joinResult;
  }

  /**
   * Forget about the most recent hash table lookup or contains call.
   */
  public void forget() {
    joinResult = JoinUtil.JoinResult.NOMATCH;
  }

  /**
   * Set the spill partition id.
   */
  public void setSpillPartitionId(int spillPartitionId) {
    this.spillPartitionId = spillPartitionId;
  }

  /**
   * @return The Hybrid Grace spill partition id.
   */
  public int spillPartitionId() {
    return spillPartitionId;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("joinResult " + joinResult.name());
    return sb.toString();
  }

  public WriteBuffers.Position getReadPos() {
    return readPos;
  }
}
