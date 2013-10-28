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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectSerDeContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinRowContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainerSerDe;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Map side Join operator implementation.
 */
public class MapJoinOperator extends AbstractMapJoinOperator<MapJoinDesc> implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(MapJoinOperator.class.getName());

  private static final transient String[] FATAL_ERR_MSG = {
      null, // counter value 0 means no error
      "Mapside join exceeds available memory. "
          + "Please try removing the mapjoin hint."};

  protected transient MapJoinTableContainer[] mapJoinTables;
  private transient MapJoinTableContainerSerDe[] mapJoinTableSerdes;
  private transient boolean hashTblInitedOnce;
  private transient MapJoinKey key;

  public MapJoinOperator() {
  }

  public MapJoinOperator(AbstractMapJoinOperator<? extends MapJoinDesc> mjop) {
    super(mjop);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    int tagLen = conf.getTagLength();
    mapJoinTables = new MapJoinTableContainer[tagLen];
    mapJoinTableSerdes = new MapJoinTableContainerSerDe[tagLen];
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
    MapJoinObjectSerDeContext keyContext = new MapJoinObjectSerDeContext(keySerializer, false);
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
      MapJoinObjectSerDeContext valueContext = new MapJoinObjectSerDeContext(valueSerDe, hasFilter(pos));
      mapJoinTableSerdes[pos] = new MapJoinTableContainerSerDe(keyContext, valueContext);
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
    String currentInputFile = getExecContext().getCurrentInputPath().toString();
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
      for (int pos = 0; pos < mapJoinTables.length; pos++) {
        if (pos == posBigTable) {
          continue;
        }
        if(baseDir == null) {
          throw new IllegalStateException("baseDir cannot be null");
        }
        String filePath = Utilities.generatePath(baseDir, conf.getDumpFilePrefix(), (byte)pos, fileName);
        Path path = new Path(filePath);
        LOG.info("\tLoad back 1 hashtable file from tmp file uri:" + path);
        ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(
            new FileInputStream(path.toUri().getPath()), 4096));
        try{
          mapJoinTables[pos] = mapJoinTableSerdes[pos].load(in);
        } finally {
          in.close();
        }
      }
    } catch (Exception e) {
      throw new HiveException(e);
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
      throw new HiveException(e);
    }
  }

  protected MapJoinKey computeMapJoinKey(Object row, byte alias) throws HiveException {
    return JoinUtil.computeMapJoinKeys(key, row, joinKeys[alias],
        joinKeysObjectInspectors[alias]);
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

      // compute keys and values as StandardObjects
      key = computeMapJoinKey(row, alias);
      boolean joinNeeded = false;
      for (byte pos = 0; pos < order.length; pos++) {
        if (pos != alias) {
          MapJoinRowContainer rowContainer = mapJoinTables[pos].get(key);
          // there is no join-value or join-key has all null elements
          if (rowContainer == null || key.hasAnyNulls(nullsafes)) {
            if (!noOuterJoin) {
              joinNeeded = true;
              storage[pos] = dummyObjVectors[pos];
            } else {
              storage[pos] = emptyList;
            }
          } else {
            joinNeeded = true;
            storage[pos] = rowContainer.copy();
            aliasFilterTags[pos] = rowContainer.getAliasFilter();
          }
        }
      }
      if (joinNeeded) {
        List<Object> value = getFilteredValue(alias, row);
        // Add the value to the ArrayList
        storage[alias].add(value);
        // generate the output records
        checkAndGenObject();
      }
      // done with the row
      storage[tag].clear();
      for (byte pos = 0; pos < order.length; pos++) {
        if (pos != tag) {
          storage[pos] = null;
        }
      }
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (mapJoinTables != null) {
      for (MapJoinTableContainer tableContainer : mapJoinTables) {
        if (tableContainer != null) {
          tableContainer.clear();
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
