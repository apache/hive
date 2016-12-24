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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.io.BytesWritable;


public class PTFTopNHash extends TopNHash {
  
  protected float memUsage;
  protected boolean isMapGroupBy;
  private Map<Key, TopNHash> partitionHeaps;
  private TopNHash largestPartition;
  private boolean prevIndexPartIsNull;
  private Set<Integer> indexesWithNullPartKey;
  private OperatorDesc conf;
  private Configuration hconf;
    
  public void initialize(
    int topN, float memUsage, boolean isMapGroupBy, BinaryCollector collector, final OperatorDesc conf,
    final Configuration hconf) {
    super.initialize(topN, memUsage, isMapGroupBy, collector, conf, hconf);
    this.conf = conf;
    this.hconf = hconf;
    this.isMapGroupBy = isMapGroupBy;
    this.memUsage = memUsage;
    partitionHeaps = new HashMap<Key, TopNHash>();
    indexesWithNullPartKey = new HashSet<Integer>();
  }
  
  public int tryStoreKey(HiveKey key, boolean partColsIsNull) throws HiveException, IOException {
    prevIndexPartIsNull = partColsIsNull;
    return _tryStoreKey(key, partColsIsNull, -1);
  }
  
  private void updateLargest(TopNHash p) {
    if ( largestPartition == null || largestPartition.usage < p.usage) {
      largestPartition = p;
    }
  }
  
  private void findLargest() {
    for(TopNHash p : partitionHeaps.values() ) {
      updateLargest(p);
    }
  }
  
  public int _tryStoreKey(HiveKey key, boolean partColsIsNull, int batchIndex) throws HiveException, IOException {
    if (!isEnabled) {
      return FORWARD; // short-circuit quickly - forward all rows
    }
    if (topN == 0) {
      return EXCLUDE; // short-circuit quickly - eat all rows
    }
    Key pk = new Key(partColsIsNull, key.hashCode());
    TopNHash partHeap = partitionHeaps.get(pk);
    if ( partHeap == null ) {
      partHeap = new TopNHash();
      partHeap.initialize(topN, memUsage, isMapGroupBy, collector, conf, hconf);
      if ( batchIndex >= 0 ) {
        partHeap.startVectorizedBatch(batchSize);
      }
      partitionHeaps.put(pk, partHeap);
    }
    usage = usage - partHeap.usage;
    int r = 0;
    if ( batchIndex >= 0 ) {
      partHeap.tryStoreVectorizedKey(key, false, batchIndex);
    } else {
      r = partHeap.tryStoreKey(key, false);
    }
    usage = usage + partHeap.usage;
    updateLargest(partHeap);

    if ( usage > threshold ) {
      usage -= largestPartition.usage;
      largestPartition.flush();
      usage += largestPartition.usage;
      largestPartition = null;
      findLargest();
    }
    return r;
  }
  
  public void storeValue(int index, int hashCode, BytesWritable value, boolean vectorized) {
    Key pk = new Key(prevIndexPartIsNull, hashCode);
    TopNHash partHeap = partitionHeaps.get(pk);
    usage = usage - partHeap.usage;
    partHeap.storeValue(index, hashCode, value, vectorized);
    usage = usage + partHeap.usage;
    updateLargest(partHeap);
  }
  
  public void flush() throws HiveException {
    if (!isEnabled || (topN == 0)) return;
    for(TopNHash partHash : partitionHeaps.values()) {
      partHash.flush();
    }
  }
  
  public int startVectorizedBatch(int size) throws IOException, HiveException {
    if (!isEnabled) {
      return FORWARD; // short-circuit quickly - forward all rows
    } else if (topN == 0) {
      return EXCLUDE; // short-circuit quickly - eat all rows
    }
    for(TopNHash partHash : partitionHeaps.values()) {
      usage = usage - partHash.usage;
      partHash.startVectorizedBatch(size);
      usage = usage + partHash.usage;
      updateLargest(partHash);
    }
    batchSize = size;
    if (batchIndexToResult == null || batchIndexToResult.length < batchSize) {
      batchIndexToResult = new int[Math.max(batchSize, VectorizedRowBatch.DEFAULT_SIZE)];
    }
    indexesWithNullPartKey.clear();
    return 0;
  }
  
  public void tryStoreVectorizedKey(HiveKey key, boolean partColsIsNull, int batchIndex)
      throws HiveException, IOException {
    _tryStoreKey(key, partColsIsNull, batchIndex);
    if ( partColsIsNull ) {
      indexesWithNullPartKey.add(batchIndex);
    }
    batchIndexToResult[batchIndex] = key.hashCode();
  }
  
  public int getVectorizedBatchResult(int batchIndex) {
    prevIndexPartIsNull = indexesWithNullPartKey.contains(batchIndex);
    Key pk = new Key(prevIndexPartIsNull, batchIndexToResult[batchIndex]);
    TopNHash partHeap = partitionHeaps.get(pk);
    return partHeap.getVectorizedBatchResult(batchIndex);
  }
  
  public HiveKey getVectorizedKeyToForward(int batchIndex) {
    prevIndexPartIsNull = indexesWithNullPartKey.contains(batchIndex);
    Key pk = new Key(prevIndexPartIsNull, batchIndexToResult[batchIndex]);
    TopNHash partHeap = partitionHeaps.get(pk);
    return partHeap.getVectorizedKeyToForward(batchIndex);
  }
  
  public int getVectorizedKeyDistLength(int batchIndex) {
    prevIndexPartIsNull = indexesWithNullPartKey.contains(batchIndex);
    Key pk = new Key(prevIndexPartIsNull, batchIndexToResult[batchIndex]);
    TopNHash partHeap = partitionHeaps.get(pk);
    return partHeap.getVectorizedKeyDistLength(batchIndex);
  }
  
  public int getVectorizedKeyHashCode(int batchIndex) {
    prevIndexPartIsNull = indexesWithNullPartKey.contains(batchIndex);
    Key pk = new Key(prevIndexPartIsNull, batchIndexToResult[batchIndex]);
    TopNHash partHeap = partitionHeaps.get(pk);
    return partHeap.getVectorizedKeyHashCode(batchIndex);
  }
  
  static class Key {
    boolean isNull;
    int hashCode;
    
    public Key(boolean isNull, int hashCode) {
      super();
      this.isNull = isNull;
      this.hashCode = hashCode;
    }

    @Override
    public int hashCode() {
     return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Key other = (Key) obj;
      if (hashCode != other.hashCode)
        return false;
      if (isNull != other.isNull)
        return false;
      return true;
    }
    
    @Override
    public String toString() {
      return "" + hashCode + "," + isNull;
    }
    
  }
}
