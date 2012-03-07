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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;

/**
 * Join operator implementation.
 */
public class JoinOperator extends CommonJoinOperator<JoinDesc> implements
    Serializable {
  private static final long serialVersionUID = 1L;

  private transient SkewJoinHandler skewJoinKeyContext = null;

  /**
   * SkewkeyTableCounter.
   *
   */
  public static enum SkewkeyTableCounter {
    SKEWJOINFOLLOWUPJOBS
  }

  private final transient LongWritable skewjoin_followup_jobs = new LongWritable(0);

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    initializeChildren(hconf);
    if (handleSkewJoin) {
      skewJoinKeyContext = new SkewJoinHandler(this);
      skewJoinKeyContext.initiliaze(hconf);
      skewJoinKeyContext.setSkewJoinJobCounter(skewjoin_followup_jobs);
    }
    statsMap.put(SkewkeyTableCounter.SKEWJOINFOLLOWUPJOBS, skewjoin_followup_jobs);
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    try {
      reportProgress();

      // get alias
      alias = (byte) tag;

      if ((lastAlias == null) || (!lastAlias.equals(alias))) {
        nextSz = joinEmitInterval;
      }


      ArrayList<Object> nr = JoinUtil.computeValues(row, joinValues.get(alias),
          joinValuesObjectInspectors.get(alias), joinFilters.get(alias),
          joinFilterObjectInspectors.get(alias), noOuterJoin);


      if (handleSkewJoin) {
        skewJoinKeyContext.handleSkew(tag);
      }

      // number of rows for the key in the given table
      int sz = storage.get(alias).size();
      StructObjectInspector soi = (StructObjectInspector) inputObjInspectors[tag];
      StructField sf = soi.getStructFieldRef(Utilities.ReduceField.KEY
          .toString());
      List keyObject = (List) soi.getStructFieldData(row, sf);
      // Are we consuming too much memory
      if (alias == numAliases - 1 && !(handleSkewJoin && skewJoinKeyContext.currBigKeyTag >= 0)) {
        if (sz == joinEmitInterval) {
          // The input is sorted by alias, so if we are already in the last join
          // operand,
          // we can emit some results now.
          // Note this has to be done before adding the current row to the
          // storage,
          // to preserve the correctness for outer joins.
          checkAndGenObject();
          storage.get(alias).clear();
        }
      } else {
        if (sz == nextSz) {
          // Print a message if we reached at least 1000 rows for a join operand
          // We won't print a message for the last join operand since the size
          // will never goes to joinEmitInterval.
          LOG.info("table " + alias + " has " + sz + " rows for join key "
              + keyObject);
          nextSz = getNextSize(nextSz);
        }
      }

      // Add the value to the vector
      // if join-key is null, process each row in different group.
      StandardStructObjectInspector inspector =
          (StandardStructObjectInspector) sf.getFieldObjectInspector();
      if (SerDeUtils.hasAnyNullObject(keyObject, inspector, nullsafes)) {
        endGroup();
        startGroup();
      }
      storage.get(alias).add(nr);
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  @Override
  public OperatorType getType() {
    return OperatorType.JOIN;
  }

  /**
   * All done.
   *
   */
  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (handleSkewJoin) {
      skewJoinKeyContext.close(abort);
    }
    super.closeOp(abort);
  }

  @Override
  public void jobClose(Configuration hconf, boolean success, JobCloseFeedBack feedBack)
      throws HiveException {
    int numAliases = conf.getExprs().size();
    if (conf.getHandleSkewJoin()) {
      try {
        for (int i = 0; i < numAliases; i++) {
          String specPath = conf.getBigKeysDirMap().get((byte) i);
          mvFileToFinalPath(specPath, hconf, success, LOG);
          for (int j = 0; j < numAliases; j++) {
            if (j == i) {
              continue;
            }
            specPath = getConf().getSmallKeysDirMap().get((byte) i).get(
                (byte) j);
            mvFileToFinalPath(specPath, hconf, success, LOG);
          }
        }

        if (success) {
          // move up files
          for (int i = 0; i < numAliases; i++) {
            String specPath = conf.getBigKeysDirMap().get((byte) i);
            moveUpFiles(specPath, hconf, LOG);
            for (int j = 0; j < numAliases; j++) {
              if (j == i) {
                continue;
              }
              specPath = getConf().getSmallKeysDirMap().get((byte) i).get(
                  (byte) j);
              moveUpFiles(specPath, hconf, LOG);
            }
          }
        }
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }
    super.jobClose(hconf, success, feedBack);
  }

  private void moveUpFiles(String specPath, Configuration hconf, Log log)
      throws IOException, HiveException {
    FileSystem fs = (new Path(specPath)).getFileSystem(hconf);
    Path finalPath = new Path(specPath);

    if (fs.exists(finalPath)) {
      FileStatus[] taskOutputDirs = fs.listStatus(finalPath);
      if (taskOutputDirs != null) {
        for (FileStatus dir : taskOutputDirs) {
          Utilities.renameOrMoveFiles(fs, dir.getPath(), finalPath);
          fs.delete(dir.getPath(), true);
        }
      }
    }
  }

  /**
   * This is a similar implementation of FileSinkOperator.moveFileToFinalPath.
   * @param specPath
   * @param hconf
   * @param success
   * @param log
   * @param dpCtx
   * @throws IOException
   * @throws HiveException
   */
  private void  mvFileToFinalPath(String specPath, Configuration hconf,
      boolean success, Log log) throws IOException, HiveException {

    FileSystem fs = (new Path(specPath)).getFileSystem(hconf);
    Path tmpPath = Utilities.toTempPath(specPath);
    Path intermediatePath = new Path(tmpPath.getParent(), tmpPath.getName()
        + ".intermediate");
    Path finalPath = new Path(specPath);
    ArrayList<String> emptyBuckets = null;
    if (success) {
      if (fs.exists(tmpPath)) {
        // Step1: rename tmp output folder to intermediate path. After this
        // point, updates from speculative tasks still writing to tmpPath
        // will not appear in finalPath.
        log.info("Moving tmp dir: " + tmpPath + " to: " + intermediatePath);
        Utilities.rename(fs, tmpPath, intermediatePath);
        // Step2: remove any tmp file or double-committed output files
        Utilities.removeTempOrDuplicateFiles(fs, intermediatePath);
        // Step3: move to the file destination
        log.info("Moving tmp dir: " + intermediatePath + " to: " + finalPath);
        Utilities.renameOrMoveFiles(fs, intermediatePath, finalPath);
      }
    } else {
      fs.delete(tmpPath, true);
    }
  }

  /**
   * Forward a record of join results.
   *
   * @throws HiveException
   */
  @Override
  public void endGroup() throws HiveException {
    // if this is a skew key, we need to handle it in a separate map reduce job.
    if (handleSkewJoin && skewJoinKeyContext.currBigKeyTag >= 0) {
      try {
        skewJoinKeyContext.endGroup();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        throw new HiveException(e);
      }
      return;
    } else {
      checkAndGenObject();
    }
  }

}
