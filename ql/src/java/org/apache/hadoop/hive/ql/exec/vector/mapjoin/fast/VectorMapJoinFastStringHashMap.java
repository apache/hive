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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;

/*
 * An single STRING key hash map optimized for vector map join.
 *
 * The key will be deserialized and just the bytes will be stored.
 */
public class VectorMapJoinFastStringHashMap extends VectorMapJoinFastBytesHashMap {

  private VectorMapJoinFastStringCommon stringCommon;

  @Override
  public void putRow(BytesWritable currentKey, BytesWritable currentValue) throws HiveException, IOException {
    stringCommon.adaptPutRow(this, currentKey, currentValue);
  }

  public VectorMapJoinFastStringHashMap(
      boolean isOuterJoin,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount) {
    super(initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount);
    stringCommon = new VectorMapJoinFastStringCommon(isOuterJoin);
  }

  @Override
  public long getEstimatedMemorySize() {
    long size = 0;
    // adding 16KB constant memory for stringCommon as the rabit hole is deep to implement
    // MemoryEstimate interface, also it is constant overhead
    size += (16 * 1024L);
    return super.getEstimatedMemorySize() + size;
  }
}