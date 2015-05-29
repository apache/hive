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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectSerDeContext;
import org.apache.hadoop.hive.ql.exec.tez.HashTableLoader;
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
import org.apache.tez.runtime.library.api.KeyValueReader;

/**
 * HashTableLoader for Tez constructs the hashtable from records read from
 * a broadcast edge.
 */
public class VectorMapJoinFastTableContainer implements VectorMapJoinTableContainer {

  private static final Log LOG = LogFactory.getLog(HashTableLoader.class.getName());

  private MapJoinDesc desc;
  private Configuration hconf;

  private float keyCountAdj;
  private int threshold;
  private float loadFactor;
  private int wbSize;
  private long keyCount;


  private VectorMapJoinFastHashTable VectorMapJoinFastHashTable;

  public VectorMapJoinFastTableContainer(MapJoinDesc desc, Configuration hconf,
      long keyCount) throws SerDeException {

    this.desc = desc;
    this.hconf = hconf;

    keyCountAdj = HiveConf.getFloatVar(hconf, HiveConf.ConfVars.HIVEHASHTABLEKEYCOUNTADJUSTMENT);
    threshold = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHASHTABLETHRESHOLD);
    loadFactor = HiveConf.getFloatVar(hconf, HiveConf.ConfVars.HIVEHASHTABLELOADFACTOR);
    wbSize = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHASHTABLEWBSIZE);

    this.keyCount = keyCount;

    // LOG.info("VectorMapJoinFastTableContainer load keyCountAdj " + keyCountAdj);
    // LOG.info("VectorMapJoinFastTableContainer load threshold " + threshold);
    // LOG.info("VectorMapJoinFastTableContainer load loadFactor " + loadFactor);
    // LOG.info("VectorMapJoinFastTableContainer load wbSize " + wbSize);

    int newThreshold = HashMapWrapper.calculateTableSize(
        keyCountAdj, threshold, loadFactor, keyCount);

    // LOG.debug("VectorMapJoinFastTableContainer load newThreshold " + newThreshold);

    VectorMapJoinFastHashTable = createHashTable(newThreshold);
  }

  @Override
  public VectorMapJoinHashTable vectorMapJoinHashTable() {
    return (VectorMapJoinHashTable) VectorMapJoinFastHashTable;
  }

  private VectorMapJoinFastHashTable createHashTable(int newThreshold) {

    boolean isOuterJoin = !desc.isNoOuterJoin();
    VectorMapJoinDesc vectorDesc = desc.getVectorDesc();
    HashTableImplementationType hashTableImplementationType = vectorDesc.hashTableImplementationType();
    HashTableKind hashTableKind = vectorDesc.hashTableKind();
    HashTableKeyType hashTableKeyType = vectorDesc.hashTableKeyType();
    boolean minMaxEnabled = vectorDesc.minMaxEnabled();

    int writeBufferSize = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHASHTABLEWBSIZE);

    VectorMapJoinFastHashTable hashTable = null;

    switch (hashTableKeyType) {
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      switch (hashTableKind) {
      case HASH_MAP:
        hashTable = new VectorMapJoinFastLongHashMap(
                minMaxEnabled, isOuterJoin, hashTableKeyType,
                newThreshold, loadFactor, writeBufferSize);
        break;
      case HASH_MULTISET:
        hashTable = new VectorMapJoinFastLongHashMultiSet(
                minMaxEnabled, isOuterJoin, hashTableKeyType,
                newThreshold, loadFactor, writeBufferSize);
        break;
      case HASH_SET:
        hashTable = new VectorMapJoinFastLongHashSet(
                minMaxEnabled, isOuterJoin, hashTableKeyType,
                newThreshold, loadFactor, writeBufferSize);
        break;
      }
      break;

    case STRING:
      switch (hashTableKind) {
      case HASH_MAP:
        hashTable = new VectorMapJoinFastStringHashMap(
                isOuterJoin,
                newThreshold, loadFactor, writeBufferSize);
        break;
      case HASH_MULTISET:
        hashTable = new VectorMapJoinFastStringHashMultiSet(
                isOuterJoin,
                newThreshold, loadFactor, writeBufferSize);
        break;
      case HASH_SET:
        hashTable = new VectorMapJoinFastStringHashSet(
                isOuterJoin,
                newThreshold, loadFactor, writeBufferSize);
        break;
      }
      break;

    case MULTI_KEY:
      switch (hashTableKind) {
      case HASH_MAP:
        hashTable = new VectorMapJoinFastMultiKeyHashMap(
            isOuterJoin,
            newThreshold, loadFactor, writeBufferSize);
        break;
      case HASH_MULTISET:
        hashTable = new VectorMapJoinFastMultiKeyHashMultiSet(
                isOuterJoin,
                newThreshold, loadFactor, writeBufferSize);
        break;
      case HASH_SET:
        hashTable = new VectorMapJoinFastMultiKeyHashSet(
                isOuterJoin,
                newThreshold, loadFactor, writeBufferSize);
        break;
      }
      break;
    }

    return hashTable;
  }

  @Override
  public MapJoinKey putRow(MapJoinObjectSerDeContext keyContext,
      Writable currentKey, MapJoinObjectSerDeContext valueContext,
      Writable currentValue) throws SerDeException, HiveException, IOException {

    // We are not using the key and value contexts, nor do we support a MapJoinKey.
    VectorMapJoinFastHashTable.putRow((BytesWritable) currentKey, (BytesWritable) currentValue);
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
  public void clear() {
    throw new RuntimeException("Not applicable");
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

  /*
  @Override
  public com.esotericsoftware.kryo.io.Output getHybridBigTableSpillOutput(int partitionId) {
    throw new RuntimeException("Not applicable");
  }
  */
}