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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Map side Join operator implementation.
 */
public class MapJoinOperator extends AbstractMapJoinOperator<MapJoinDesc> implements
    Serializable {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(MapJoinOperator.class
      .getName());

  protected transient Map<Byte, HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>> mapJoinTables;

  private static final transient String[] FATAL_ERR_MSG = {
      null, // counter value 0 means no error
      "Mapside join size exceeds hive.mapjoin.maxsize. "
          + "Please increase that or remove the mapjoin hint."
      };

  /**
   * MapJoinObjectCtx.
   *
   */
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

  static transient Map<Integer, MapJoinObjectCtx> mapMetadata = new HashMap<Integer, MapJoinObjectCtx>();
  static transient int nextVal = 0;

  public static Map<Integer, MapJoinObjectCtx> getMapMetadata() {
    return mapMetadata;
  }

  transient int metadataKeyTag;
  transient int[] metadataValueTag;
  transient int maxMapJoinSize;

  public MapJoinOperator() {
  }

  public MapJoinOperator(AbstractMapJoinOperator<? extends MapJoinDesc> mjop) {
    super(mjop);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    maxMapJoinSize = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVEMAXMAPJOINSIZE);

    metadataValueTag = new int[numAliases];
    for (int pos = 0; pos < numAliases; pos++) {
      metadataValueTag[pos] = -1;
    }

    mapJoinTables = new HashMap<Byte, HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>>();

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
  }

  @Override
  protected void fatalErrorMessage(StringBuilder errMsg, long counterCode) {
    errMsg.append("Operator " + getOperatorId() + " (id=" + id + "): "
        + FATAL_ERR_MSG[(int) counterCode]);
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {

    if (tag == posBigTable) {
      this.getExecContext().processInputFileChangeForLocalWork();
    }

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

        reportProgress();
        numMapRowsRead++;

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
          int bucketSize = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEMAPJOINBUCKETCACHESIZE);
          res = getRowContainer(hconf, (byte) tag, order[tag], bucketSize);
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

          // there is no join-value or join-key has all null elements
          if (o == null || (hasAnyNulls(key))) {
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
    if(mapJoinTables != null) {
      for (HashMapWrapper hashTable : mapJoinTables.values()) {
        hashTable.close();
      }
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
