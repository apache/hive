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

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectValue;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Map side Join operator implementation.
 */
public class MapJoinOperator extends CommonJoinOperator<MapJoinDesc> implements
    Serializable {
  private static final long serialVersionUID = 1L;
  static final private Log LOG = LogFactory.getLog(MapJoinOperator.class
      .getName());

  /**
   * The expressions for join inputs's join keys.
   */
  transient protected Map<Byte, List<ExprNodeEvaluator>> joinKeys;
  /**
   * The ObjectInspectors for the join inputs's join keys.
   */
  transient protected Map<Byte, List<ObjectInspector>> joinKeysObjectInspectors;
  /**
   * The standard ObjectInspectors for the join inputs's join keys.
   */
  transient protected Map<Byte, List<ObjectInspector>> joinKeysStandardObjectInspectors;

  transient private int posBigTable; // one of the tables that is not in memory
  transient int mapJoinRowsKey; // rows for a given key

  transient protected Map<Byte, HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>> mapJoinTables;

  transient protected RowContainer<ArrayList<Object>> emptyList = null;

  transient static final private String[] fatalErrMsg = {
      null, // counter value 0 means no error
      "Mapside join size exceeds hive.mapjoin.maxsize. Please increase that or remove the mapjoin hint." // counter
                                                                                                         // value
                                                                                                         // 1
  };

  public static class MapJoinObjectCtx {
    ObjectInspector standardOI;
    SerDe serde;
    TableDesc tblDesc;
    Configuration conf;

    /**
     * @param standardOI
     * @param serde
     */
    public MapJoinObjectCtx(ObjectInspector standardOI, SerDe serde,
        TableDesc tblDesc, Configuration conf) {
      this.standardOI = standardOI;
      this.serde = serde;
      this.tblDesc = tblDesc;
      this.conf = conf;
    }

    /**
     * @return the standardOI
     */
    public ObjectInspector getStandardOI() {
      return standardOI;
    }

    /**
     * @return the serde
     */
    public SerDe getSerDe() {
      return serde;
    }

    public TableDesc getTblDesc() {
      return tblDesc;
    }

    public Configuration getConf() {
      return conf;
    }

  }

  transient static Map<Integer, MapJoinObjectCtx> mapMetadata = new HashMap<Integer, MapJoinObjectCtx>();
  transient static int nextVal = 0;

  static public Map<Integer, MapJoinObjectCtx> getMapMetadata() {
    return mapMetadata;
  }

  transient boolean firstRow;

  transient int metadataKeyTag;
  transient int[] metadataValueTag;
  transient List<File> hTables;
  transient int numMapRowsRead;
  transient int heartbeatInterval;
  transient int maxMapJoinSize;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    numMapRowsRead = 0;

    firstRow = true;
    heartbeatInterval = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVESENDHEARTBEAT);
    maxMapJoinSize = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVEMAXMAPJOINSIZE);

    joinKeys = new HashMap<Byte, List<ExprNodeEvaluator>>();

    populateJoinKeyValue(joinKeys, conf.getKeys());
    joinKeysObjectInspectors = getObjectInspectorsFromEvaluators(joinKeys,
        inputObjInspectors);
    joinKeysStandardObjectInspectors = getStandardObjectInspectors(joinKeysObjectInspectors);

    // all other tables are small, and are cached in the hash table
    posBigTable = conf.getPosBigTable();

    metadataValueTag = new int[numAliases];
    for (int pos = 0; pos < numAliases; pos++) {
      metadataValueTag[pos] = -1;
    }

    mapJoinTables = new HashMap<Byte, HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>>();
    hTables = new ArrayList<File>();

    // initialize the hash tables for other tables
    for (int pos = 0; pos < numAliases; pos++) {
      if (pos == posBigTable) {
        continue;
      }

      int cacheSize = HiveConf.getIntVar(hconf,
          HiveConf.ConfVars.HIVEMAPJOINCACHEROWS);
      HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue> hashTable = new HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>(
          cacheSize);

      mapJoinTables.put(Byte.valueOf((byte) pos), hashTable);
    }

    emptyList = new RowContainer<ArrayList<Object>>(1, hconf);
    RowContainer bigPosRC = getRowContainer(hconf, (byte) posBigTable,
        order[posBigTable], joinCacheSize);
    storage.put((byte) posBigTable, bigPosRC);

    mapJoinRowsKey = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVEMAPJOINROWSIZE);

    List<? extends StructField> structFields = ((StructObjectInspector) outputObjInspector)
        .getAllStructFieldRefs();
    if (conf.getOutputColumnNames().size() < structFields.size()) {
      List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
      for (Byte alias : order) {
        int sz = conf.getExprs().get(alias).size();
        List<Integer> retained = conf.getRetainList().get(alias);
        for (int i = 0; i < sz; i++) {
          int pos = retained.get(i);
          structFieldObjectInspectors.add(structFields.get(pos)
              .getFieldObjectInspector());
        }
      }
      outputObjInspector = ObjectInspectorFactory
          .getStandardStructObjectInspector(conf.getOutputColumnNames(),
              structFieldObjectInspectors);
    }
    initializeChildren(hconf);
  }

  @Override
  protected void fatalErrorMessage(StringBuffer errMsg, long counterCode) {
    errMsg.append("Operator " + getOperatorId() + " (id=" + id + "): "
        + fatalErrMsg[(int) counterCode]);
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    try {
      // get alias
      alias = (byte) tag;

      if ((lastAlias == null) || (!lastAlias.equals(alias))) {
        nextSz = joinEmitInterval;
      }

      // compute keys and values as StandardObjects
      ArrayList<Object> key = computeValues(row, joinKeys.get(alias),
          joinKeysObjectInspectors.get(alias));
      ArrayList<Object> value = computeValues(row, joinValues.get(alias),
          joinValuesObjectInspectors.get(alias));

      // does this source need to be stored in the hash map
      if (tag != posBigTable) {
        if (firstRow) {
          metadataKeyTag = nextVal++;

          TableDesc keyTableDesc = conf.getKeyTblDesc();
          SerDe keySerializer = (SerDe) ReflectionUtils.newInstance(
              keyTableDesc.getDeserializerClass(), null);
          keySerializer.initialize(null, keyTableDesc.getProperties());

          mapMetadata.put(Integer.valueOf(metadataKeyTag),
              new MapJoinObjectCtx(
                  ObjectInspectorUtils
                      .getStandardObjectInspector(keySerializer
                          .getObjectInspector(),
                          ObjectInspectorCopyOption.WRITABLE), keySerializer,
                  keyTableDesc, hconf));

          firstRow = false;
        }

        // Send some status periodically
        numMapRowsRead++;
        if (((numMapRowsRead % heartbeatInterval) == 0) && (reporter != null)) {
          reporter.progress();
        }

        if ((numMapRowsRead > maxMapJoinSize) && (reporter != null)
            && (counterNameToEnum != null)) {
          // update counter
          LOG
              .warn("Too many rows in map join tables. Fatal error counter will be incremented!!");
          incrCounter(fatalErrorCntr, 1);
          fatalError = true;
          return;
        }

        HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue> hashTable = mapJoinTables
            .get(alias);
        MapJoinObjectKey keyMap = new MapJoinObjectKey(metadataKeyTag, key);
        MapJoinObjectValue o = hashTable.get(keyMap);
        RowContainer res = null;

        boolean needNewKey = true;
        if (o == null) {
          res = getRowContainer(hconf, (byte) tag, order[tag], joinCacheSize);
          res.add(value);
        } else {
          res = o.getObj();
          res.add(value);
          // If key already exists, HashMapWrapper.get() guarantees it is
          // already in main memory HashMap
          // cache. So just replacing the object value should update the
          // HashMapWrapper. This will save
          // the cost of constructing the new key/object and deleting old one
          // and inserting the new one.
          if (hashTable.cacheSize() > 0) {
            o.setObj(res);
            needNewKey = false;
          }
        }

        if (metadataValueTag[tag] == -1) {
          metadataValueTag[tag] = nextVal++;

          TableDesc valueTableDesc = conf.getValueTblDescs().get(tag);
          SerDe valueSerDe = (SerDe) ReflectionUtils.newInstance(valueTableDesc
              .getDeserializerClass(), null);
          valueSerDe.initialize(null, valueTableDesc.getProperties());

          mapMetadata.put(Integer.valueOf(metadataValueTag[tag]),
              new MapJoinObjectCtx(ObjectInspectorUtils
                  .getStandardObjectInspector(valueSerDe.getObjectInspector(),
                      ObjectInspectorCopyOption.WRITABLE), valueSerDe,
                  valueTableDesc, hconf));
        }

        // Construct externalizable objects for key and value
        if (needNewKey) {
          MapJoinObjectKey keyObj = new MapJoinObjectKey(metadataKeyTag, key);
          MapJoinObjectValue valueObj = new MapJoinObjectValue(
              metadataValueTag[tag], res);
          valueObj.setConf(hconf);
          // This may potentially increase the size of the hashmap on the mapper
          if (res.size() > mapJoinRowsKey) {
            if (res.size() % 100 == 0) {
              LOG.warn("Number of values for a given key " + keyObj + " are "
                  + res.size());
              LOG.warn("used memory " + Runtime.getRuntime().totalMemory());
            }
          }
          hashTable.put(keyObj, valueObj);
        }
        return;
      }

      // Add the value to the ArrayList
      storage.get(alias).add(value);

      for (Byte pos : order) {
        if (pos.intValue() != tag) {
          MapJoinObjectKey keyMap = new MapJoinObjectKey(metadataKeyTag, key);
          MapJoinObjectValue o = mapJoinTables.get(pos).get(keyMap);

          if (o == null) {
            if (noOuterJoin) {
              storage.put(pos, emptyList);
            } else {
              storage.put(pos, dummyObjVectors[pos.intValue()]);
            }
          } else {
            storage.put(pos, o.getObj());
          }
        }
      }

      // generate the output records
      checkAndGenObject();

      // done with the row
      storage.get(alias).clear();

      for (Byte pos : order) {
        if (pos.intValue() != tag) {
          storage.put(pos, null);
        }
      }

    } catch (SerDeException e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    for (HashMapWrapper hashTable : mapJoinTables.values()) {
      hashTable.close();
    }
    super.closeOp(abort);
  }

  /**
   * Implements the getName function for the Node Interface.
   * 
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return "MAPJOIN";
  }

  @Override
  public int getType() {
    return OperatorType.MAPJOIN;
  }
}
