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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
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

  protected static final Log LOG = LogFactory.getLog(SkewJoinHandler.class
      .getName());

  public int currBigKeyTag = -1;

  private int rowNumber = 0;
  private int currTag = -1;

  private int skewKeyDefinition = -1;
  private Map<Byte, StructObjectInspector> skewKeysTableObjectInspector = null;
  private Map<Byte, SerDe> tblSerializers = null;
  private Map<Byte, TableDesc> tblDesc = null;

  private Map<Byte, Boolean> bigKeysExistingMap = null;

  private LongWritable skewjoinFollowupJobs;

  Configuration hconf = null;
  List<Object> dummyKey = null;
  String taskId;

  private final CommonJoinOperator<? extends Serializable> joinOp;
  private final int numAliases;
  private final JoinDesc conf;

  public SkewJoinHandler(CommonJoinOperator<? extends Serializable> joinOp) {
    this.joinOp = joinOp;
    numAliases = joinOp.numAliases;
    conf = joinOp.getConf();
  }

  public void initiliaze(Configuration hconf) {
    this.hconf = hconf;
    JoinDesc desc = joinOp.getConf();
    skewKeyDefinition = desc.getSkewKeyDefinition();
    skewKeysTableObjectInspector = new HashMap<Byte, StructObjectInspector>(
        numAliases);
    tblDesc = desc.getSkewKeysValuesTables();
    tblSerializers = new HashMap<Byte, SerDe>(numAliases);
    bigKeysExistingMap = new HashMap<Byte, Boolean>(numAliases);
    taskId = Utilities.getTaskId(hconf);

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
        SerDe serializer = (SerDe) ReflectionUtils.newInstance(tblDesc.get(
            alias).getDeserializerClass(), null);
        serializer.initialize(null, tblDesc.get(alias).getProperties());
        tblSerializers.put((byte) i, serializer);
      } catch (SerDeException e) {
        LOG.error("Skewjoin will be disabled due to " + e.getMessage(), e);
        joinOp.handleSkewJoin = false;
        break;
      }

      TableDesc valTblDesc = joinOp.getSpillTableDesc(alias);
      List<String> valColNames = new ArrayList<String>();
      if (valTblDesc != null) {
        valColNames = Utilities.getColumnNames(valTblDesc.getProperties());
      }
      StructObjectInspector structTblValInpector = ObjectInspectorFactory
          .getStandardStructObjectInspector(valColNames,
          joinOp.joinValuesStandardObjectInspectors.get((byte) i));

      StructObjectInspector structTblInpector = ObjectInspectorFactory
          .getUnionStructObjectInspector(Arrays
          .asList(new StructObjectInspector[] {structTblValInpector, structTblKeyInpector}));
      skewKeysTableObjectInspector.put((byte) i, structTblInpector);
    }

    // reset rowcontainer's serde, objectinspector, and tableDesc.
    for (int i = 0; i < numAliases; i++) {
      Byte alias = conf.getTagOrder()[i];
      RowContainer<ArrayList<Object>> rc = joinOp.storage.get(Byte
          .valueOf((byte) i));
      if (rc != null) {
        rc.setSerDe(tblSerializers.get((byte) i), skewKeysTableObjectInspector
            .get((byte) i));
        rc.setTableDesc(tblDesc.get(alias));
      }
    }
  }

  void endGroup() throws IOException, HiveException {
    if (skewKeyInCurrentGroup) {

      String specPath = conf.getBigKeysDirMap().get((byte) currBigKeyTag);
      RowContainer<ArrayList<Object>> bigKey = joinOp.storage.get(Byte
          .valueOf((byte) currBigKeyTag));
      Path outputPath = getOperatorOutputPath(specPath);
      FileSystem destFs = outputPath.getFileSystem(hconf);
      bigKey.copyToDFSDirecory(destFs, outputPath);

      for (int i = 0; i < numAliases; i++) {
        if (((byte) i) == currBigKeyTag) {
          continue;
        }
        RowContainer<ArrayList<Object>> values = joinOp.storage.get(Byte
            .valueOf((byte) i));
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
        RowContainer<ArrayList<Object>> rc = joinOp.storage.get(Byte
            .valueOf((byte) i));
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
        throw new RuntimeException("Bug in handle skew key in a seperate job.");
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
          String specPath = conf.getBigKeysDirMap().get((byte) bigKeyTbl);
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
      LOG.error(e);
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

      String specPath = conf.getBigKeysDirMap().get(
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

  private void commitOutputPathToFinalPath(String specPath,
      boolean ignoreNonExisting) throws IOException {
    Path outPath = getOperatorOutputPath(specPath);
    Path finalPath = getOperatorFinalPath(specPath);
    FileSystem fs = outPath.getFileSystem(hconf);
    // for local file system in Hadoop-0.17.2.1, it will throw IOException when
    // file not existing.
    try {
      if (!fs.rename(outPath, finalPath)) {
        throw new IOException("Unable to rename output to: " + finalPath);
      }
    } catch (FileNotFoundException e) {
      if (!ignoreNonExisting) {
        throw e;
      }
    } catch (IOException e) {
      if (!fs.exists(outPath) && ignoreNonExisting) {
        return;
      }
      throw e;
    }
  }

  private Path getOperatorOutputPath(String specPath) throws IOException {
    Path tmpPath = Utilities.toTempPath(specPath);
    return new Path(tmpPath, Utilities.toTempPath(taskId));
  }

  private Path getOperatorFinalPath(String specPath) throws IOException {
    Path tmpPath = Utilities.toTempPath(specPath);
    return new Path(tmpPath, taskId);
  }

  public void setSkewJoinJobCounter(LongWritable skewjoinFollowupJobs) {
    this.skewjoinFollowupJobs = skewjoinFollowupJobs;
  }

  public void updateSkewJoinJobCounter(int tag) {
    this.skewjoinFollowupJobs.set(this.skewjoinFollowupJobs.get() + 1);
  }

}
