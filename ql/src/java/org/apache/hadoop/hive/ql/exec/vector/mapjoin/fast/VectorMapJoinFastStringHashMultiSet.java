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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.io.BytesWritable;

/*
 * An single STRING key hash multi-set optimized for vector map join.
 *
 * The key will be deserialized and just the bytes will be stored.
 */
public class VectorMapJoinFastStringHashMultiSet extends VectorMapJoinFastBytesHashMultiSet {

  private final VectorMapJoinFastStringCommon stringCommon;

  private long fullOuterNullKeyValueCount;

  @Override
  public void putRow(long hashCode, BytesWritable currentKey, BytesWritable currentValue)
      throws HiveException, IOException {
    if (!stringCommon.adaptPutRow(this, currentKey, currentValue, hashCode)) {

      // Ignore NULL keys, except for FULL OUTER.
      if (isFullOuter) {
        fullOuterNullKeyValueCount++;
      }
    }
  }

  public VectorMapJoinFastStringHashMultiSet(
      boolean isFullOuter,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount, TableDesc tableDesc) {
    super(
        isFullOuter,
        initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount);
    fullOuterNullKeyValueCount = 0;
    stringCommon = new VectorMapJoinFastStringCommon(tableDesc);
  }

  @Override
  public long getEstimatedMemorySize() {
    // adding 16KB constant memory for stringCommon as the rabbit hole is deep to implement
    // MemoryEstimate interface, also it is constant overhead
    long size = (16 * 1024L);
    return super.getEstimatedMemorySize() + size;
  }
}