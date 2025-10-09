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

import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinNonMatchedIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectSerDeContext;
import org.apache.hadoop.hive.ql.exec.persistence.MatchTracker;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinTableContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKind;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

/**
 * HashTableLoader for Tez constructs the hashtable from records read from
 * a broadcast edge.
 */
public class VectorMapJoinFastTableContainer implements VectorMapJoinTableContainer, VectorMapJoinHashTable {

  private final MapJoinDesc desc;
  private final Configuration hconf;

  private final float keyCountAdj;
  private final int threshold;
  private final float loadFactor;

  private final VectorMapJoinFastHashTableContainerBase INSTANCE;
  private final long estimatedKeyCount;

  private String key;
  private int numHTs;

  public VectorMapJoinFastTableContainer(MapJoinDesc desc, Configuration hconf,
      long estimatedKeys, int numHTs) throws SerDeException {

    this.desc = desc;
    this.hconf = hconf;

    keyCountAdj = HiveConf.getFloatVar(hconf, HiveConf.ConfVars.HIVE_HASHTABLE_KEY_COUNT_ADJUSTMENT);
    threshold = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVE_HASHTABLE_THRESHOLD);
    loadFactor = HiveConf.getFloatVar(hconf, HiveConf.ConfVars.HIVE_HASHTABLE_LOAD_FACTOR);

    this.numHTs = numHTs;
    this.estimatedKeyCount = estimatedKeys > numHTs ? (estimatedKeys/ numHTs) : estimatedKeys;

    int newThreshold = HashMapWrapper.calculateTableSize(keyCountAdj, threshold, loadFactor, estimatedKeyCount);

    // LOG.debug("VectorMapJoinFastTableContainer load newThreshold " + newThreshold);
    this.INSTANCE = createHashTables(newThreshold);
  }

  @Override
  public VectorMapJoinHashTable vectorMapJoinHashTable() {
    return this.INSTANCE;
  }

  @Override
  public void setKey(String key) {
    this.key = key;
  }

  @Override
  public String getKey() {
    return key;
  }

  private VectorMapJoinFastHashTableContainerBase createHashTables(int newThreshold) {

    VectorMapJoinDesc vectorDesc = (VectorMapJoinDesc) desc.getVectorDesc();
    HashTableKind hashTableKind = vectorDesc.getHashTableKind();
    HashTableKeyType hashTableKeyType = vectorDesc.getHashTableKeyType();
    boolean isFullOuter = vectorDesc.getIsFullOuter();
    boolean minMaxEnabled = vectorDesc.getMinMaxEnabled();

    int writeBufferSize = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVE_HASHTABLE_WB_SIZE);

    VectorMapJoinFastHashTableContainerBase htWrapper = null;
    switch (hashTableKeyType) {
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case DATE:
    case LONG:
      switch (hashTableKind) {
      case HASH_MAP:
        htWrapper = new VectorMapJoinFastLongHashMapContainer(isFullOuter, minMaxEnabled,
            hashTableKeyType, newThreshold, loadFactor, writeBufferSize, estimatedKeyCount, desc.getKeyTblDesc(), numHTs);
        break;
      case HASH_MULTISET:
        htWrapper = new VectorMapJoinFastLongHashMultiSetContainer(isFullOuter, minMaxEnabled,
            hashTableKeyType, newThreshold, loadFactor, writeBufferSize, estimatedKeyCount, desc.getKeyTblDesc(), numHTs);
        break;
      case HASH_SET:
        htWrapper = new VectorMapJoinFastLongHashSetContainer(isFullOuter, minMaxEnabled,
            hashTableKeyType, newThreshold, loadFactor, writeBufferSize, estimatedKeyCount, desc.getKeyTblDesc(), numHTs);
        break;
      }
      break;

    case STRING:
      switch (hashTableKind) {
      case HASH_MAP:
        htWrapper = new VectorMapJoinFastStringHashMapContainer(isFullOuter, newThreshold,
            loadFactor, writeBufferSize, estimatedKeyCount, desc.getKeyTblDesc(), numHTs);
        break;
      case HASH_MULTISET:
        htWrapper = new VectorMapJoinFastStringHashMultiSetContainer(isFullOuter, newThreshold,
            loadFactor, writeBufferSize, estimatedKeyCount, desc.getKeyTblDesc(), numHTs);
        break;
      case HASH_SET:
        htWrapper = new VectorMapJoinFastStringHashSetContainer(isFullOuter, newThreshold,
            loadFactor, writeBufferSize, estimatedKeyCount, desc.getKeyTblDesc(), numHTs);
        break;
      }
      break;

    case MULTI_KEY:
      switch (hashTableKind) {
      case HASH_MAP:
        htWrapper = new VectorMapJoinFastMultiKeyHashMapContainer(isFullOuter, newThreshold,
            loadFactor, writeBufferSize, estimatedKeyCount, numHTs);
        break;
      case HASH_MULTISET:
        htWrapper = new VectorMapJoinFastMultiKeyHashMultiSetContainer(isFullOuter, newThreshold,
            loadFactor, writeBufferSize, estimatedKeyCount, numHTs);
        break;
      case HASH_SET:
        htWrapper = new VectorMapJoinFastMultiKeyHashSetContainer(isFullOuter, newThreshold,
            loadFactor, writeBufferSize, estimatedKeyCount, numHTs);
        break;
      }
      break;
    }
    return htWrapper;
  }

  public long getHashCode(BytesWritable currentKey) throws HiveException, IOException {
    return INSTANCE.getHashCode(currentKey);
  }

  @Override
  public MapJoinKey putRow(Writable currentKey, Writable currentValue)
      throws SerDeException, HiveException, IOException {
    long hashCode = INSTANCE.getHashCode((BytesWritable) currentKey);
    INSTANCE.putRow(hashCode, (BytesWritable) currentKey, (BytesWritable) currentValue);
    return null;
  }

  @Override
  public void putRow(long hashCode, BytesWritable currentKey, BytesWritable currentValue)
      throws SerDeException, HiveException, IOException {
    // We are not using the key and value contexts, nor do we support a MapJoinKey.
    INSTANCE.putRow(hashCode, currentKey, currentValue);
  }

  @Override
  public void seal() {
    // Do nothing
  }

  @Override
  public ReusableGetAdaptor createGetter(MapJoinKey keyTypeFromLoader) {
    throw new RuntimeException("Not applicable");
  }

  @Override
  public NonMatchedSmallTableIterator createNonMatchedSmallTableIterator(MatchTracker matchTracker) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    // Do nothing
  }

  @Override
  public MapJoinKey getAnyKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dumpMetrics() {
    // TODO
  }

  @Override
  public boolean hasSpill() {
    return false;
  }

  @Override
  public boolean containsLongKey(long currentKey) {
    return INSTANCE.containsLongKey(currentKey);
  }

  @Override
  public int size() {
    return INSTANCE.size();
  }

  @Override
  public MatchTracker createMatchTracker() {
    throw new UnsupportedOperationException();
  }

  @Override
  public VectorMapJoinNonMatchedIterator createNonMatchedIterator(MatchTracker matchTracker) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int spillPartitionId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getEstimatedMemorySize() {
    return INSTANCE.getEstimatedMemorySize();
  }

  @Override
  public void setSerde(MapJoinObjectSerDeContext keyCtx, MapJoinObjectSerDeContext valCtx)
      throws SerDeException {
    // Do nothing in this case.

  }
}
