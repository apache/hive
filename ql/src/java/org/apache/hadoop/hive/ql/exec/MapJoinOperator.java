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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Map side Join operator implementation.
 */
public class MapJoinOperator extends AbstractMapJoinOperator<MapJoinDesc> implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(MapJoinOperator.class.getName());


  protected transient HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue>[] mapJoinTables;

  private static final transient String[] FATAL_ERR_MSG = {
      null, // counter value 0 means no error
      "Mapside join exceeds available memory. "
          + "Please try removing the mapjoin hint."};

  protected transient MapJoinRowContainer<ArrayList<Object>>[] rowContainerMap;
  transient int metadataKeyTag;
  transient int[] metadataValueTag;
  transient boolean hashTblInitedOnce;

  public MapJoinOperator() {
  }

  public MapJoinOperator(AbstractMapJoinOperator<? extends MapJoinDesc> mjop) {
    super(mjop);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void initializeOp(Configuration hconf) throws HiveException {

    super.initializeOp(hconf);

    metadataValueTag = new int[numAliases];
    for (int pos = 0; pos < numAliases; pos++) {
      metadataValueTag[pos] = -1;
    }

    metadataKeyTag = -1;

    int tagLen = conf.getTagLength();

    mapJoinTables = new HashMapWrapper[tagLen];
    rowContainerMap = new MapJoinRowContainer[tagLen];
    // initialize the hash tables for other tables
    for (int pos = 0; pos < numAliases; pos++) {
      if (pos == posBigTable) {
        continue;
      }

      HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue> hashTable = new HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue>();

      mapJoinTables[pos] = hashTable;
      MapJoinRowContainer<ArrayList<Object>> rowContainer = new MapJoinRowContainer<ArrayList<Object>>();
      rowContainerMap[pos] = rowContainer;
    }

    hashTblInitedOnce = false;
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
            ObjectInspectorCopyOption.WRITABLE), keySerializer, keyTableDesc, false, hconf));

    for (int pos = 0; pos < order.length; pos++) {
      if (pos == posBigTable) {
        continue;
      }
      TableDesc valueTableDesc;
      if (conf.getNoOuterJoin()) {
        valueTableDesc = conf.getValueTblDescs().get(pos);
      } else {
        valueTableDesc = conf.getValueFilteredTblDescs().get(pos);
      }
      SerDe valueSerDe = (SerDe) ReflectionUtils.newInstance(valueTableDesc.getDeserializerClass(),
          null);
      valueSerDe.initialize(null, valueTableDesc.getProperties());

      ObjectInspector inspector = valueSerDe.getObjectInspector();
      MapJoinMetaData.put(Integer.valueOf(pos), new HashTableSinkObjectCtx(ObjectInspectorUtils
          .getStandardObjectInspector(inspector, ObjectInspectorCopyOption.WRITABLE),
          valueSerDe, valueTableDesc, hasFilter(pos), hconf));
    }
  }

  private void loadHashTable() throws HiveException {

    if (!this.getExecContext().getLocalWork().getInputFileChangeSensitive()) {
      if (hashTblInitedOnce) {
        return;
      } else {
        hashTblInitedOnce = true;
      }
    }

    String baseDir = null;

    String currentInputFile = getExecContext().getCurrentInputFile();
    LOG.info("******* Load from HashTable File: input : " + currentInputFile);

    String fileName = getExecContext().getLocalWork().getBucketFileName(currentInputFile);

    try {
      if (ShimLoader.getHadoopShims().isLocalMode(hconf)) {
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
      for (byte pos = 0; pos < mapJoinTables.length; pos++) {
        HashMapWrapper<AbstractMapJoinKey, MapJoinObjectValue> hashtable = mapJoinTables[pos];
        if (hashtable == null) {
          continue;
        }
        String filePath = Utilities.generatePath(baseDir, conf.getDumpFilePrefix(), pos, fileName);
        Path path = new Path(filePath);
        LOG.info("\tLoad back 1 hashtable file from tmp file uri:" + path.toString());
        hashtable.initilizePersistentHash(path.toUri().getPath());
      }
    } catch (Exception e) {
      LOG.error("Load Distributed Cache Error");
      throw new HiveException(e.getMessage());
    }
  }

  // Load the hash table
  @Override
  public void cleanUpInputFileChangedOp() throws HiveException {
    try {
      if (firstRow) {
        // generate the map metadata
        generateMapMetaData();
        firstRow = false;
      }

      loadHashTable();
    } catch (SerDeException e) {
      e.printStackTrace();
      throw new HiveException(e);
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

      alias = (byte)tag;

      if ((lastAlias == null) || (!lastAlias.equals(alias))) {
        nextSz = joinEmitInterval;
      }

      // compute keys and values as StandardObjects
      AbstractMapJoinKey key = JoinUtil.computeMapJoinKeys(row, joinKeys[alias],
          joinKeysObjectInspectors[alias]);
      ArrayList<Object> value = getFilteredValue(alias, row);

      // Add the value to the ArrayList
      storage[alias].add(value);

      for (byte pos = 0; pos < order.length; pos++) {
        if (pos != alias) {

          MapJoinObjectValue o = mapJoinTables[pos].get(key);
          MapJoinRowContainer<ArrayList<Object>> rowContainer = rowContainerMap[pos];

          // there is no join-value or join-key has all null elements
          if (o == null || key.hasAnyNulls(nullsafes)) {
            if (noOuterJoin) {
              storage[pos] = emptyList;
            } else {
              storage[pos] = dummyObjVectors[pos];
            }
          } else {
            rowContainer.reset(o.getObj());
            storage[pos] = rowContainer;
            aliasFilterTags[pos] = o.getAliasFilter();
          }
        }
      }

      // generate the output records
      checkAndGenObject();

      // done with the row
      storage[tag].clear();

      for (byte pos = 0; pos < order.length; pos++) {
        if (pos != tag) {
          storage[pos] = null;
        }
      }

    } catch (SerDeException e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {

    if (mapJoinTables != null) {
      for (HashMapWrapper<?, ?> hashTable : mapJoinTables) {
        if (hashTable != null) {
          hashTable.close();
        }
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
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "MAPJOIN";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.MAPJOIN;
  }
}
