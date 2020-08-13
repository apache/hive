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

import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinFastHashTableParallel;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableImplementationType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKeyType;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc.HashTableKind;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

/**
 * HashTableLoader for Tez constructs the hashtable from records read from
 * a broadcast edge.
 */
public class VectorMapJoinFastTableContainer implements VectorMapJoinTableContainer {

  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastTableContainer.class.getName());

  private final MapJoinDesc desc;
  private final Configuration hconf;

  private final float keyCountAdj;
  private final int threshold;
  private final float loadFactor;
  private final int wbSize;

  private final long estimatedKeyCount;


  private final VectorMapJoinFastHashTableParallel vectorMapJoinFastHashTableParallel;
  private String key;

  public VectorMapJoinFastTableContainer(MapJoinDesc desc, Configuration hconf,
      long estimatedKeyCount) throws SerDeException {

    this.desc = desc;
    this.hconf = hconf;

    keyCountAdj = HiveConf.getFloatVar(hconf, HiveConf.ConfVars.HIVEHASHTABLEKEYCOUNTADJUSTMENT);
    threshold = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHASHTABLETHRESHOLD);
    loadFactor = HiveConf.getFloatVar(hconf, HiveConf.ConfVars.HIVEHASHTABLELOADFACTOR);
    wbSize = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHASHTABLEWBSIZE);

    this.estimatedKeyCount = estimatedKeyCount;
    LOG.info("estimated keycount: "+ estimatedKeyCount);

    int newThreshold = HashMapWrapper.calculateTableSize(
        keyCountAdj, threshold, loadFactor, estimatedKeyCount < 4 ? estimatedKeyCount : estimatedKeyCount/4);

    LOG.info("VectorMapJoinFastTableContainer load newThreshold " + newThreshold);

    vectorMapJoinFastHashTableParallel = createHashTables(newThreshold);
  }

  @Override
  public VectorMapJoinHashTable vectorMapJoinHashTable() {
    return vectorMapJoinFastHashTableParallel;
  }

  @Override
  public void setKey(String key) {
    this.key = key;
  }

  @Override
  public String getKey() {
    return key;
  }

  private VectorMapJoinFastHashTableParallel createHashTables(int newThreshold) {

    VectorMapJoinDesc vectorDesc = (VectorMapJoinDesc) desc.getVectorDesc();
    HashTableImplementationType hashTableImplementationType = vectorDesc.getHashTableImplementationType();
    HashTableKind hashTableKind = vectorDesc.getHashTableKind();
    HashTableKeyType hashTableKeyType = vectorDesc.getHashTableKeyType();
    boolean isFullOuter = vectorDesc.getIsFullOuter();
    boolean minMaxEnabled = vectorDesc.getMinMaxEnabled();

    int writeBufferSize = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHASHTABLEWBSIZE);
    
    VectorMapJoinFastHashTableParallel hashTable = new VectorMapJoinFastHashTableParallel(hashTableKeyType,
        hashTableKind, isFullOuter, minMaxEnabled, newThreshold, loadFactor, writeBufferSize, estimatedKeyCount,
        desc.getKeyTblDesc());

    return hashTable;
  }

  public long calculateLongHashCode(long key, BytesWritable currentKey) throws HiveException, IOException {
    return vectorMapJoinFastHashTableParallel.calculateLongHashCode(key, currentKey);
  }

  public long deserializeToKey(BytesWritable currentKey) throws HiveException, IOException {
    return vectorMapJoinFastHashTableParallel.deserializeToKey(currentKey);
  }

  @Override
  public MapJoinKey putRow(Writable currentKey, Writable currentValue)
      throws SerDeException, HiveException, IOException {

    // We are not using the key and value contexts, nor do we support a MapJoinKey.
    vectorMapJoinFastHashTableParallel.putRow((BytesWritable) currentKey, (BytesWritable) currentValue);
    return null;
  }

  public MapJoinKey putRow(Writable currentKey, Writable currentValue, long hashCode, long key)
      throws SerDeException, HiveException, IOException {

    // We are not using the key and value contexts, nor do we support a MapJoinKey.
    vectorMapJoinFastHashTableParallel.putRow((BytesWritable) currentKey, (BytesWritable) currentValue, hashCode, key);
    return null;
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
  public NonMatchedSmallTableIterator createNonMatchedSmallTableIterator(
      MatchTracker matchTracker) {
    throw new RuntimeException("Not applicable");
  }

  @Override
  public void clear() {
    // Do nothing
  }

  @Override
  public MapJoinKey getAnyKey() {
    throw new RuntimeException("Not applicable");
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
  public int size() {
    return vectorMapJoinFastHashTableParallel.size();
  }

  @Override
  public long getEstimatedMemorySize() {
    JavaDataModel jdm = JavaDataModel.get();
    long size = 0;
    size += vectorMapJoinFastHashTableParallel.getEstimatedMemorySize();
    size += (4 * jdm.primitive1());
    size += (2 * jdm.object());
    size += (jdm.primitive2());
    return size;
  }

  @Override
  public void setSerde(MapJoinObjectSerDeContext keyCtx, MapJoinObjectSerDeContext valCtx)
      throws SerDeException {
    // Do nothing in this case.

  }
}
