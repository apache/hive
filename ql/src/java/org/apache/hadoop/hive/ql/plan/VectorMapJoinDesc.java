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

package org.apache.hadoop.hive.ql.plan;

/**
 * VectorGroupByDesc.
 *
 * Extra parameters beyond MapJoinDesc just for the vector map join operators.
 *
 * We don't extend MapJoinDesc because the base OperatorDesc doesn't support
 * clone and adding it is a lot work for little gain.
 */
public class VectorMapJoinDesc extends AbstractVectorDesc  {

  private static long serialVersionUID = 1L;

  public static enum HashTableImplementationType {
    NONE,
    OPTIMIZED,
    FAST
  }

  public static enum HashTableKind {
    NONE,
    HASH_SET,
    HASH_MULTISET,
    HASH_MAP
  }

  public static enum HashTableKeyType {
    NONE,
    BOOLEAN,
    BYTE,
    SHORT,
    INT,
    LONG,
    STRING,
    MULTI_KEY
  }

  private HashTableImplementationType hashTableImplementationType;
  private HashTableKind hashTableKind;
  private HashTableKeyType hashTableKeyType;
  private boolean minMaxEnabled;

  public VectorMapJoinDesc() {
    hashTableImplementationType = HashTableImplementationType.NONE;
    hashTableKind = HashTableKind.NONE;
    hashTableKeyType = HashTableKeyType.NONE;
    minMaxEnabled = false;
  }

  public VectorMapJoinDesc(VectorMapJoinDesc clone) {
    this.hashTableImplementationType = clone.hashTableImplementationType;
    this.hashTableKind = clone.hashTableKind;
    this.hashTableKeyType = clone.hashTableKeyType;
    this.minMaxEnabled = clone.minMaxEnabled;
  }

  public HashTableImplementationType hashTableImplementationType() {
    return hashTableImplementationType;
  }

  public void setHashTableImplementationType(HashTableImplementationType hashTableImplementationType) {
    this.hashTableImplementationType = hashTableImplementationType;
  }

  public HashTableKind hashTableKind() {
    return hashTableKind;
  }

  public void setHashTableKind(HashTableKind hashTableKind) {
    this.hashTableKind = hashTableKind;
  }

  public HashTableKeyType hashTableKeyType() {
    return hashTableKeyType;
  }

  public void setHashTableKeyType(HashTableKeyType hashTableKeyType) {
    this.hashTableKeyType = hashTableKeyType;
  }

  public boolean minMaxEnabled() {
    return minMaxEnabled;
  }

  public void setMinMaxEnabled(boolean minMaxEnabled) {
    this.minMaxEnabled = minMaxEnabled;
  }
}
