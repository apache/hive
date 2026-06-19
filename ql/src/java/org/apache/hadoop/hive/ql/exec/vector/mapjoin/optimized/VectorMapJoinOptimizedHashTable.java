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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.optimized;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.persistence.BytesBytesMultiHashMap;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MatchTracker;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerDirectAccess;
import org.apache.hadoop.hive.ql.exec.persistence.ReusableGetAdaptorDirectAccess;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer.ReusableGetAdaptor;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTableResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinNonMatchedIterator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

/**
 * Root interface for a vector map join hash table (which could be a hash map, hash multi-set, or
 * hash set).
 */
public abstract class VectorMapJoinOptimizedHashTable
    implements VectorMapJoinHashTable, VectorMapJoinBytesHashTable {

  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinOptimizedMultiKeyHashMap.class.getName());

  protected final MapJoinTableContainer originalTableContainer;
  protected final MapJoinTableContainerDirectAccess containerDirectAccess;
  protected final ReusableGetAdaptorDirectAccess adapterDirectAccess;

  public static class SerializedBytes {
    byte[] bytes;
    int offset;
    int length;
  }

  @Override
  public VectorMapJoinNonMatchedIterator createNonMatchedIterator(MatchTracker matchTracker) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int spillPartitionId() {
    return adapterDirectAccess.directSpillPartitionId();
  }

  @Override
  public void putRow(long hashCode, BytesWritable currentKey, BytesWritable currentValue)
      throws SerDeException, HiveException, IOException {
    // Method only supported by FAST HashTable implementations
    throw new UnsupportedEncodingException();
  }

  @Override
  public boolean containsLongKey(long currentKey) {
    // Method only supported by FAST HashTable implementations
    throw new UnsupportedOperationException();
  }

  protected void putRowInternal(BytesWritable key, BytesWritable value)
      throws SerDeException, HiveException, IOException {

    containerDirectAccess.put((Writable) key, (Writable) value);
  }

  public JoinUtil.JoinResult doLookup(byte[] keyBytes, int keyOffset, int keyLength,
          BytesBytesMultiHashMap.Result bytesBytesMultiHashMapResult,
          VectorMapJoinHashTableResult hashTableResult, MatchTracker matchTracker) {

    hashTableResult.forget();

    JoinUtil.JoinResult joinResult =
            adapterDirectAccess.setDirect(keyBytes, keyOffset, keyLength,
                bytesBytesMultiHashMapResult, matchTracker);
    if (joinResult == JoinUtil.JoinResult.SPILL) {
      hashTableResult.setSpillPartitionId(adapterDirectAccess.directSpillPartitionId());
    }

    hashTableResult.setJoinResult(joinResult);

    return joinResult;
  }

  public VectorMapJoinOptimizedHashTable(
      MapJoinTableContainer originalTableContainer, ReusableGetAdaptor hashMapRowGetter) {

    this.originalTableContainer = originalTableContainer;
    containerDirectAccess = (MapJoinTableContainerDirectAccess) originalTableContainer;
    adapterDirectAccess = (ReusableGetAdaptorDirectAccess) hashMapRowGetter;
  }

  @Override
  public int size() {
    return originalTableContainer.size();
  }

  @Override
  public long getEstimatedMemorySize() {
    long size = 0;
    size += originalTableContainer == null ? 0 : originalTableContainer.getEstimatedMemorySize();
    size += (2 * JavaDataModel.get().object());
    return size;
  }

  @Override
  public MatchTracker createMatchTracker() {
    return adapterDirectAccess.createMatchTracker();
  }
}
