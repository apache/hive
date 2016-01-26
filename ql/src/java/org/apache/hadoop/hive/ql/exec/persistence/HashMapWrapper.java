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

package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorHashKeyWrapper;
import org.apache.hadoop.hive.ql.exec.vector.VectorHashKeyWrapperBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;


/**
 * Simple wrapper for persistent Hashmap implementing only the put/get/remove/clear interface. The
 * main memory hash table acts as a cache and all put/get will operate on it first. If the size of
 * the main memory hash table exceeds a certain threshold, new elements will go into the persistent
 * hash table.
 */

public class HashMapWrapper extends AbstractMapJoinTableContainer implements Serializable {
  private static final long serialVersionUID = 1L;
  protected static final Logger LOG = LoggerFactory.getLogger(HashMapWrapper.class);

  // default threshold for using main memory based HashMap
  private static final int THRESHOLD = 1000000;
  private static final float LOADFACTOR = 0.75f;
  private final HashMap<MapJoinKey, MapJoinRowContainer> mHash; // main memory HashMap
  private final MapJoinKey lastKey = null;
  private final Output output = new Output(0); // Reusable output for serialization
  private MapJoinObjectSerDeContext keyContext;
  private MapJoinObjectSerDeContext valueContext;

  public HashMapWrapper(Map<String, String> metaData) {
    super(metaData);
    int threshold = Integer.parseInt(metaData.get(THESHOLD_NAME));
    float loadFactor = Float.parseFloat(metaData.get(LOAD_NAME));
    mHash = new HashMap<MapJoinKey, MapJoinRowContainer>(threshold, loadFactor);
  }

  public HashMapWrapper() {
    this(HiveConf.ConfVars.HIVEHASHTABLEKEYCOUNTADJUSTMENT.defaultFloatVal,
        HiveConf.ConfVars.HIVEHASHTABLETHRESHOLD.defaultIntVal,
        HiveConf.ConfVars.HIVEHASHTABLELOADFACTOR.defaultFloatVal, -1);
  }

  public HashMapWrapper(Configuration hconf, long keyCount) {
    this(HiveConf.getFloatVar(hconf, HiveConf.ConfVars.HIVEHASHTABLEKEYCOUNTADJUSTMENT),
        HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHASHTABLETHRESHOLD),
        HiveConf.getFloatVar(hconf, HiveConf.ConfVars.HIVEHASHTABLELOADFACTOR), keyCount);
  }

  private HashMapWrapper(float keyCountAdj, int threshold, float loadFactor, long keyCount) {
    super(createConstructorMetaData(threshold, loadFactor));
    threshold = calculateTableSize(keyCountAdj, threshold, loadFactor, keyCount);
    mHash = new HashMap<MapJoinKey, MapJoinRowContainer>(threshold, loadFactor);
  }

  public static int calculateTableSize(
      float keyCountAdj, int threshold, float loadFactor, long keyCount) {
    if (keyCount >= 0 && keyCountAdj != 0) {
      // We have statistics for the table. Size appropriately.
      threshold = (int)Math.ceil(keyCount / (keyCountAdj * loadFactor));
    }
    LOG.info("Key count from statistics is " + keyCount + "; setting map size to " + threshold);
    return threshold;
  }


  @Override
  public MapJoinRowContainer get(MapJoinKey key) {
    return mHash.get(key);
  }

  @Override
  public void put(MapJoinKey key, MapJoinRowContainer value) {
    mHash.put(key, value);
  }

  @Override
  public int size() {
    return mHash.size();
  }
  @Override
  public Set<Entry<MapJoinKey, MapJoinRowContainer>> entrySet() {
    return mHash.entrySet();
  }
  @Override
  public void clear() {
    mHash.clear();
  }

  @Override
  public MapJoinKey putRow(Writable currentKey, Writable currentValue)
          throws SerDeException, HiveException {
    MapJoinKey key = MapJoinKey.read(output, keyContext, currentKey);
    FlatRowContainer values = (FlatRowContainer)get(key);
    if (values == null) {
      values = new FlatRowContainer();
      put(key, values);
    }
    values.add(valueContext, (BytesWritable)currentValue);
    return key;
  }

  @Override
  public ReusableGetAdaptor createGetter(MapJoinKey keyTypeFromLoader) {
    return new GetAdaptor(keyTypeFromLoader);
  }

  private class GetAdaptor implements ReusableGetAdaptor {

    private Object[] currentKey;
    private List<ObjectInspector> vectorKeyOIs;

    private MapJoinKey key;
    private MapJoinRowContainer currentValue;
    private final Output output = new Output();
    private boolean isFirstKey = true;

    public GetAdaptor(MapJoinKey key) {
      this.key = key;
    }

    @Override
    public JoinUtil.JoinResult setFromVector(VectorHashKeyWrapper kw,
        VectorExpressionWriter[] keyOutputWriters, VectorHashKeyWrapperBatch keyWrapperBatch)
        throws HiveException {
      if (currentKey == null) {
        currentKey = new Object[keyOutputWriters.length];
        vectorKeyOIs = new ArrayList<ObjectInspector>();
        for (int i = 0; i < keyOutputWriters.length; i++) {
          vectorKeyOIs.add(keyOutputWriters[i].getObjectInspector());
        }
      }
      for (int i = 0; i < keyOutputWriters.length; i++) {
        currentKey[i] = keyWrapperBatch.getWritableKeyValue(kw, i, keyOutputWriters[i]);
      }
      key =  MapJoinKey.readFromVector(output, key, currentKey, vectorKeyOIs, !isFirstKey);
      isFirstKey = false;
      this.currentValue = mHash.get(key);
      if (this.currentValue == null) {
        return JoinUtil.JoinResult.NOMATCH;
      }
      else {
        return JoinUtil.JoinResult.MATCH;
      }
    }

    @Override
    public JoinUtil.JoinResult setFromRow(Object row, List<ExprNodeEvaluator> fields,
        List<ObjectInspector> ois) throws HiveException {
      if (currentKey == null) {
        currentKey = new Object[fields.size()];
      }
      for (int keyIndex = 0; keyIndex < fields.size(); ++keyIndex) {
        currentKey[keyIndex] = fields.get(keyIndex).evaluate(row);
      }
      key = MapJoinKey.readFromRow(output, key, currentKey, ois, !isFirstKey);
      isFirstKey = false;
      this.currentValue = mHash.get(key);
      if (this.currentValue == null) {
        return JoinUtil.JoinResult.NOMATCH;
      }
      else {
        return JoinUtil.JoinResult.MATCH;
      }
    }

    @Override
    public JoinUtil.JoinResult setFromOther(ReusableGetAdaptor other) {
      assert other instanceof GetAdaptor;
      GetAdaptor other2 = (GetAdaptor)other;
      this.key = other2.key;
      this.isFirstKey = other2.isFirstKey;
      this.currentValue = mHash.get(key);
      if (this.currentValue == null) {
        return JoinUtil.JoinResult.NOMATCH;
      }
      else {
        return JoinUtil.JoinResult.MATCH;
      }
    }

    @Override
    public boolean hasAnyNulls(int fieldCount, boolean[] nullsafes) {
      return key.hasAnyNulls(fieldCount, nullsafes);
    }

    @Override
    public MapJoinRowContainer getCurrentRows() {
      return currentValue;
    }

    @Override
    public Object[] getCurrentKey() {
      return currentKey;
    }
  }

  @Override
  public void seal() {
    // Nothing to do.
  }

  @Override
  public MapJoinKey getAnyKey() {
    return mHash.isEmpty() ? null : mHash.keySet().iterator().next();
  }

  @Override
  public void dumpMetrics() {
    // Nothing to do.
  }

  @Override
  public boolean hasSpill() {
    return false;
  }

  @Override
  public void setSerde(MapJoinObjectSerDeContext keyCtx, MapJoinObjectSerDeContext valCtx)
      throws SerDeException {
    this.keyContext = keyCtx;
    this.valueContext = valCtx;
  }
}
