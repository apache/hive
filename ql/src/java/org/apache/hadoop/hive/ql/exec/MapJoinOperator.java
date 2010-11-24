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
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.HashTableSinkOperator.HashTableSinkObjectCtx;
import org.apache.hadoop.hive.ql.exec.persistence.AbstractMapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectValue;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinRowContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Map side Join operator implementation.
 */
public class MapJoinOperator extends AbstractMapJoinOperator<MapJoinDesc> implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(MapJoinOperator.class.getName());


  protected transient Map<Byte, HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue>> mapJoinTables;

  private static final transient String[] FATAL_ERR_MSG = {
      null, // counter value 0 means no error
      "Mapside join size exceeds hive.mapjoin.maxsize. "
          + "Please increase that or remove the mapjoin hint."};

  protected transient Map<Byte, MapJoinRowContainer<ArrayList<Object>>> rowContainerMap;
  transient int metadataKeyTag;
  transient int[] metadataValueTag;
  transient int maxMapJoinSize;
  private int bigTableAlias;

  public MapJoinOperator() {
  }

  public MapJoinOperator(AbstractMapJoinOperator<? extends MapJoinDesc> mjop) {
    super(mjop);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    super.initializeOp(hconf);

    maxMapJoinSize = HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEMAXMAPJOINSIZE);

    metadataValueTag = new int[numAliases];
    for (int pos = 0; pos < numAliases; pos++) {
      metadataValueTag[pos] = -1;
    }

    metadataKeyTag = -1;
    bigTableAlias = order[posBigTable];

    mapJoinTables = new HashMap<Byte, HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue>>();
    rowContainerMap = new HashMap<Byte, MapJoinRowContainer<ArrayList<Object>>>();
    // initialize the hash tables for other tables
    for (int pos = 0; pos < numAliases; pos++) {
      if (pos == posBigTable) {
        continue;
      }

      HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue> hashTable = new HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue>();

      mapJoinTables.put(Byte.valueOf((byte) pos), hashTable);
      MapJoinRowContainer<ArrayList<Object>> rowContainer = new MapJoinRowContainer<ArrayList<Object>>();
      rowContainerMap.put(Byte.valueOf((byte) pos), rowContainer);
    }


  }

  @Override
  protected void fatalErrorMessage(StringBuilder errMsg, long counterCode) {
    errMsg.append("Operator " + getOperatorId() + " (id=" + id + "): "
        + FATAL_ERR_MSG[(int) counterCode]);
  }

  public void generateMapMetaData() throws HiveException, SerDeException {
    // generate the meta data for key
    // index for key is -1
    TableDesc keyTableDesc = conf.getKeyTblDesc();
    SerDe keySerializer = (SerDe) ReflectionUtils.newInstance(keyTableDesc.getDeserializerClass(),
        null);
    keySerializer.initialize(null, keyTableDesc.getProperties());
    MapJoinMetaData.put(Integer.valueOf(metadataKeyTag), new HashTableSinkObjectCtx(
        ObjectInspectorUtils.getStandardObjectInspector(keySerializer.getObjectInspector(),
            ObjectInspectorCopyOption.WRITABLE), keySerializer, keyTableDesc, hconf));

    // index for values is just alias
    for (int tag = 0; tag < order.length; tag++) {
      int alias = (int) order[tag];

      if (alias == this.bigTableAlias) {
        continue;
      }


      TableDesc valueTableDesc = conf.getValueTblDescs().get(tag);
      SerDe valueSerDe = (SerDe) ReflectionUtils.newInstance(valueTableDesc.getDeserializerClass(),
          null);
      valueSerDe.initialize(null, valueTableDesc.getProperties());

      MapJoinMetaData.put(Integer.valueOf(alias), new HashTableSinkObjectCtx(ObjectInspectorUtils
          .getStandardObjectInspector(valueSerDe.getObjectInspector(),
              ObjectInspectorCopyOption.WRITABLE), valueSerDe, valueTableDesc, hconf));
    }
  }

  private void loadHashTable() throws HiveException {
    boolean localMode = HiveConf.getVar(hconf, HiveConf.ConfVars.HADOOPJT).equals("local");
    String baseDir = null;
    HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue> hashtable;
    Byte pos;

    String currentInputFile = HiveConf.getVar(hconf, HiveConf.ConfVars.HADOOPMAPFILENAME);
    LOG.info("******* Load from HashTable File: input : " + currentInputFile);

    String currentFileName;

    if (this.getExecContext().getLocalWork().getInputFileChangeSensitive()) {
      currentFileName = this.getFileName(currentInputFile);
    } else {
      currentFileName = "-";
    }

    try {
      if (localMode) {
        baseDir = this.getExecContext().getLocalWork().getTmpFileURI();
      } else {
        Path[] localArchives;
        String stageID = this.getExecContext().getLocalWork().getStageID();
        String suffix = Utilities.generateTarFileName(stageID);
        FileSystem localFs = FileSystem.getLocal(hconf);
        localArchives = DistributedCache.getLocalCacheArchives(this.hconf);
        Path archive;
        for (int j = 0; j < localArchives.length; j++) {
          archive = localArchives[j];
          if (!archive.getName().endsWith(suffix)) {
            continue;
          }
          Path archiveLocalLink = archive.makeQualified(localFs);
          baseDir = archiveLocalLink.toUri().getPath();
        }
      }
      for (Map.Entry<Byte, HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue>> entry : mapJoinTables
          .entrySet()) {
        pos = entry.getKey();
        hashtable = entry.getValue();
        String filePath = Utilities.generatePath(baseDir, pos, currentFileName);
        Path path = new Path(filePath);
        LOG.info("\tLoad back 1 hashtable file from tmp file uri:" + path.toString());
        hashtable.initilizePersistentHash(path.toUri().getPath());
      }
    } catch (Exception e) {
      LOG.error("Load Distributed Cache Error");
      throw new HiveException(e.getMessage());
    }
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {

    try {
      if (firstRow) {
        // generate the map metadata
        generateMapMetaData();
        firstRow = false;
      }
      if (this.getExecContext().inputFileChanged()) {
        loadHashTable();
      }

      // get alias
      alias = order[tag];
      // alias = (byte)tag;

      if ((lastAlias == null) || (!lastAlias.equals(alias))) {
        nextSz = joinEmitInterval;
      }

      // compute keys and values as StandardObjects
      AbstractMapJoinKey key = JoinUtil.computeMapJoinKeys(row, joinKeys.get(alias),
          joinKeysObjectInspectors.get(alias));
      ArrayList<Object> value = JoinUtil.computeValues(row, joinValues.get(alias),
          joinValuesObjectInspectors.get(alias), joinFilters.get(alias), joinFilterObjectInspectors
              .get(alias), noOuterJoin);


      // Add the value to the ArrayList
      storage.get((byte) tag).add(value);

      for (Byte pos : order) {
        if (pos.intValue() != tag) {

          MapJoinObjectValue o = mapJoinTables.get(pos).get(key);
          MapJoinRowContainer<ArrayList<Object>> rowContainer = rowContainerMap.get(pos);

          // there is no join-value or join-key has all null elements
          if (o == null || key.hasAnyNulls()) {
            if (noOuterJoin) {
              storage.put(pos, emptyList);
            } else {
              storage.put(pos, dummyObjVectors[pos.intValue()]);
            }
          } else {
            rowContainer.reset(o.getObj());
            storage.put(pos, rowContainer);
          }
        }
      }

      // generate the output records
      checkAndGenObject();

      // done with the row
      storage.get((byte) tag).clear();

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

  private String getFileName(String path) {
    if (path == null || path.length() == 0) {
      return null;
    }

    int last_separator = path.lastIndexOf(Path.SEPARATOR) + 1;
    String fileName = path.substring(last_separator);
    return fileName;

  }

  @Override
  public void closeOp(boolean abort) throws HiveException {

    if (mapJoinTables != null) {
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
