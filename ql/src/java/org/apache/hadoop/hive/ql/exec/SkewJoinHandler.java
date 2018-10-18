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

package org.apache.hadoop.hive.ql.exec;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * At runtime in Join, we output big keys in one table into one corresponding
 * directories, and all same keys in other tables into different dirs(one for
 * each table). The directories will look like:
 * <ul>
 * <li>
 * dir-T1-bigkeys(containing big keys in T1), dir-T2-keys(containing keys which
 * is big in T1),dir-T3-keys(containing keys which is big in T1), ...
 * <li>
 * dir-T1-keys(containing keys which is big in T2), dir-T2-bigkeys(containing
 * big keys in T2),dir-T3-keys(containing keys which is big in T2), ...
 * <li>
 * dir-T1-keys(containing keys which is big in T3), dir-T2-keys(containing big
 * keys in T3),dir-T3-bigkeys(containing keys which is big in T3), ... .....
 * </ul>
 *
 * <p>
 * For each skew key, we first write all values to a local tmp file. At the time
 * of ending the current group, the local tmp file will be uploaded to hdfs.
 * Right now, we use one file per skew key.
 *
 * <p>
 * For more info, please see https://issues.apache.org/jira/browse/HIVE-964.
 *
 */
public class SkewJoinHandler {

  protected static final Logger LOG = LoggerFactory.getLogger(SkewJoinHandler.class
      .getName());

  public int currBigKeyTag = -1;

  private int rowNumber = 0;
  private int currTag = -1;

  private int skewKeyDefinition = -1;
  private Map<Byte, StructObjectInspector> skewKeysTableObjectInspector = null;
  private Map<Byte, AbstractSerDe> tblSerializers = null;
  private Map<Byte, TableDesc> tblDesc = null;

  private Map<Byte, Boolean> bigKeysExistingMap = null;

  private LongWritable skewjoinFollowupJobs;

  private final boolean noOuterJoin;
  Configuration hconf = null;
  List<Object> dummyKey = null;
  String taskId;

  private final CommonJoinOperator<? extends OperatorDesc> joinOp;
  private final int numAliases;
  private final JoinDesc conf;

  public SkewJoinHandler(CommonJoinOperator<? extends OperatorDesc> joinOp) {
    this.joinOp = joinOp;
    numAliases = joinOp.numAliases;
    conf = joinOp.getConf();
    noOuterJoin = joinOp.noOuterJoin;
  }

  public void initiliaze(Configuration hconf) {
    this.hconf = hconf;
    JoinDesc desc = joinOp.getConf();
    skewKeyDefinition = desc.getSkewKeyDefinition();
    skewKeysTableObjectInspector = new HashMap<Byte, StructObjectInspector>(
        numAliases);
    tblDesc = desc.getSkewKeysValuesTables();
    tblSerializers = new HashMap<Byte, AbstractSerDe>(numAliases);
    bigKeysExistingMap = new HashMap<Byte, Boolean>(numAliases);
    taskId = Utilities.getTaskId(hconf);

    int[][] filterMap = desc.getFilterMap();
    for (int i = 0; i < numAliases; i++) {
      Byte alias = conf.getTagOrder()[i];
      List<ObjectInspector> skewTableKeyInspectors = new ArrayList<ObjectInspector>();
      StructObjectInspector soi = (StructObjectInspector) joinOp.inputObjInspectors[alias];
      StructField sf = soi.getStructFieldRef(Utilities.ReduceField.KEY
          .toString());
      List<? extends StructField> keyFields = ((StructObjectInspector) sf
          .getFieldObjectInspector()).getAllStructFieldRefs();
      int keyFieldSize = keyFields.size();
      for (int k = 0; k < keyFieldSize; k++) {
        skewTableKeyInspectors.add(keyFields.get(k).getFieldObjectInspector());
      }
      TableDesc joinKeyDesc = desc.getKeyTableDesc();
      List<String> keyColNames = Utilities.getColumnNames(joinKeyDesc
          .getProperties());
      StructObjectInspector structTblKeyInpector = ObjectInspectorFactory
          .getStandardStructObjectInspector(keyColNames, skewTableKeyInspectors);

      try {
        AbstractSerDe serializer = (AbstractSerDe) ReflectionUtils.newInstance(tblDesc.get(
            alias).getDeserializerClass(), null);
        SerDeUtils.initializeSerDe(serializer, null, tblDesc.get(alias).getProperties(), null);
        tblSerializers.put((byte) i, serializer);
      } catch (SerDeException e) {
        LOG.error("Skewjoin will be disabled due to " + e.getMessage(), e);
        joinOp.handleSkewJoin = false;
        break;
      }

      boolean hasFilter = filterMap != null && filterMap[i] != null;
      TableDesc valTblDesc = JoinUtil.getSpillTableDesc(alias,
          joinOp.spillTableDesc, conf, !hasFilter);
      List<String> valColNames = new ArrayList<String>();
      if (valTblDesc != null) {
        valColNames = Utilities.getColumnNames(valTblDesc.getProperties());
      }
      StructObjectInspector structTblValInpector = ObjectInspectorFactory
          .getStandardStructObjectInspector(valColNames,
          joinOp.joinValuesStandardObjectInspectors[i]);

      StructObjectInspector structTblInpector = ObjectInspectorFactory
          .getUnionStructObjectInspector(Arrays.asList(structTblValInpector, structTblKeyInpector));
      skewKeysTableObjectInspector.put((byte) i, structTblInpector);
    }

    // reset rowcontainer's serde, objectinspector, and tableDesc.
    for (int i = 0; i < numAliases; i++) {
      Byte alias = conf.getTagOrder()[i];
      RowContainer<ArrayList<Object>> rc = (RowContainer)joinOp.storage[i];
      if (rc != null) {
        rc.setSerDe(tblSerializers.get((byte) i), skewKeysTableObjectInspector
            .get((byte) i));
        rc.setTableDesc(tblDesc.get(alias));
      }
    }
  }

  void endGroup() throws IOException, HiveException {
    if (skewKeyInCurrentGroup) {

      Path specPath = conf.getBigKeysDirMap().get((byte) currBigKeyTag);
      RowContainer<ArrayList<Object>> bigKey = (RowContainer)joinOp.storage[currBigKeyTag];
      Path outputPath = getOperatorOutputPath(specPath);
      FileSystem destFs = outputPath.getFileSystem(hconf);
      bigKey.copyToDFSDirecory(destFs, outputPath);

      for (int i = 0; i < numAliases; i++) {
        if (((byte) i) == currBigKeyTag) {
          continue;
        }
        RowContainer<ArrayList<Object>> values = (RowContainer)joinOp.storage[i];
        if (values != null) {
          specPath = conf.getSmallKeysDirMap().get((byte) currBigKeyTag).get(
              (byte) i);
          values.copyToDFSDirecory(destFs, getOperatorOutputPath(specPath));
        }
      }
    }
    skewKeyInCurrentGroup = false;
  }

  boolean skewKeyInCurrentGroup = false;

  public void handleSkew(int tag) throws HiveException {

    if (joinOp.newGroupStarted || tag != currTag) {
      rowNumber = 0;
      currTag = tag;
    }

    if (joinOp.newGroupStarted) {
      currBigKeyTag = -1;
      joinOp.newGroupStarted = false;
      dummyKey = (List<Object>) joinOp.getGroupKeyObject();
      skewKeyInCurrentGroup = false;

      for (int i = 0; i < numAliases; i++) {
        RowContainer<ArrayList<Object>> rc = (RowContainer)joinOp.storage[i];
        if (rc != null) {
          rc.setKeyObject(dummyKey);
        }
      }
    }

    rowNumber++;
    if (currBigKeyTag == -1 && (tag < numAliases - 1)
        && rowNumber >= skewKeyDefinition) {
      // the first time we see a big key. If this key is not in the last
      // table (the last table can always be streamed), we define that we get
      // a skew key now.
      currBigKeyTag = tag;
      updateSkewJoinJobCounter(tag);
      // right now we assume that the group by is an ArrayList object. It may
      // change in future.
      if (!(dummyKey instanceof List)) {
        throw new RuntimeException("Bug in handle skew key in a separate job.");
      }

      skewKeyInCurrentGroup = true;
      bigKeysExistingMap.put(Byte.valueOf((byte) currBigKeyTag), Boolean.TRUE);
    }
  }

  public void close(boolean abort) throws HiveException {
    if (!abort) {
      try {
        endGroup();
        commit();
      } catch (IOException e) {
        throw new HiveException(e);
      }
    } else {
      for (int bigKeyTbl = 0; bigKeyTbl < numAliases; bigKeyTbl++) {

        // if we did not see a skew key in this table, continue to next
        // table
        if (!bigKeysExistingMap.get((byte) bigKeyTbl)) {
          continue;
        }

        try {
          Path specPath = conf.getBigKeysDirMap().get((byte) bigKeyTbl);
          Path bigKeyPath = getOperatorOutputPath(specPath);
          FileSystem fs = bigKeyPath.getFileSystem(hconf);
          delete(bigKeyPath, fs);
          for (int smallKeyTbl = 0; smallKeyTbl < numAliases; smallKeyTbl++) {
            if (((byte) smallKeyTbl) == bigKeyTbl) {
              continue;
            }
            specPath = conf.getSmallKeysDirMap().get((byte) bigKeyTbl).get(
                (byte) smallKeyTbl);
            delete(getOperatorOutputPath(specPath), fs);
          }
        } catch (IOException e) {
          throw new HiveException(e);
        }
      }
    }
  }

  private void delete(Path operatorOutputPath, FileSystem fs) {
    try {
      fs.delete(operatorOutputPath, true);
    } catch (IOException e) {
      LOG.error("Failed to delete path ", e);
    }
  }

  private void commit() throws IOException {
    for (int bigKeyTbl = 0; bigKeyTbl < numAliases; bigKeyTbl++) {

      // if we did not see a skew key in this table, continue to next table
      // we are trying to avoid an extra call of FileSystem.exists()
      Boolean existing = bigKeysExistingMap.get(Byte.valueOf((byte) bigKeyTbl));
      if (existing == null || !existing) {
        continue;
      }

      Path specPath = conf.getBigKeysDirMap().get(
          Byte.valueOf((byte) bigKeyTbl));
      commitOutputPathToFinalPath(specPath, false);
      for (int smallKeyTbl = 0; smallKeyTbl < numAliases; smallKeyTbl++) {
        if (smallKeyTbl == bigKeyTbl) {
          continue;
        }
        specPath = conf.getSmallKeysDirMap()
            .get(Byte.valueOf((byte) bigKeyTbl)).get(
            Byte.valueOf((byte) smallKeyTbl));
        // the file may not exist, and we just ignore this
        commitOutputPathToFinalPath(specPath, true);
      }
    }
  }

  private void commitOutputPathToFinalPath(Path specPath,
      boolean ignoreNonExisting) throws IOException {
    Path outPath = getOperatorOutputPath(specPath);
    Path finalPath = getOperatorFinalPath(specPath);
    FileSystem fs = outPath.getFileSystem(hconf);
    if (ignoreNonExisting && !fs.exists(outPath)) {
      return;
    }
    if (!fs.rename(outPath, finalPath)) {
      throw new IOException("Unable to rename output to: " + finalPath);
    }
  }

  private Path getOperatorOutputPath(Path specPath) throws IOException {
    return new Path(Utilities.toTempPath(specPath), Utilities.toTempPath(taskId));
  }

  private Path getOperatorFinalPath(Path specPath) throws IOException {
    return new Path(Utilities.toTempPath(specPath), taskId);
  }

  public void setSkewJoinJobCounter(LongWritable skewjoinFollowupJobs) {
    this.skewjoinFollowupJobs = skewjoinFollowupJobs;
  }

  public void updateSkewJoinJobCounter(int tag) {
    this.skewjoinFollowupJobs.set(this.skewjoinFollowupJobs.get() + 1);
  }

}
