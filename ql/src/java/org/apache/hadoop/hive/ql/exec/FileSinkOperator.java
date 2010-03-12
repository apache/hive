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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HivePartitioner;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * File Sink operator implementation.
 **/
public class FileSinkOperator extends TerminalOperator<FileSinkDesc> implements
    Serializable {

  /**
   * RecordWriter.
   *
   */
  public static interface RecordWriter {
    void write(Writable w) throws IOException;

    void close(boolean abort) throws IOException;
  }

  private static final long serialVersionUID = 1L;
  protected transient FileSystem fs;
  protected transient Serializer serializer;
  protected transient BytesWritable commonKey = new BytesWritable();
  protected transient TableIdEnum tabIdEnum = null;
  private transient LongWritable row_count;
  private transient boolean isNativeTable = true;

  /**
   * The evaluators for the multiFile sprayer. If the table under consideration has 1000 buckets,
   * it is not a good idea to start so many reducers - if the maximum number of reducers is 100,
   * each reducer can write 10 files - this way we effectively get 1000 files.
   */
  private transient ExprNodeEvaluator[] partitionEval;
  private transient int      totalFiles;
  private transient int      numFiles;
  private transient boolean  multiFileSpray;
  private transient Map<Integer, Integer> bucketMap = new HashMap<Integer, Integer>();

  private transient RecordWriter[] outWriters;
  private transient Path[] outPaths;
  private transient Path[] finalPaths;
  private transient ObjectInspector[] partitionObjectInspectors;
  private transient HivePartitioner<HiveKey, Object> prtner;
  private transient HiveKey key = new HiveKey();

  /**
   * TableIdEnum.
   *
   */
  public static enum TableIdEnum {
    TABLE_ID_1_ROWCOUNT,
    TABLE_ID_2_ROWCOUNT,
    TABLE_ID_3_ROWCOUNT,
    TABLE_ID_4_ROWCOUNT,
    TABLE_ID_5_ROWCOUNT,
    TABLE_ID_6_ROWCOUNT,
    TABLE_ID_7_ROWCOUNT,
    TABLE_ID_8_ROWCOUNT,
    TABLE_ID_9_ROWCOUNT,
    TABLE_ID_10_ROWCOUNT,
    TABLE_ID_11_ROWCOUNT,
    TABLE_ID_12_ROWCOUNT,
    TABLE_ID_13_ROWCOUNT,
    TABLE_ID_14_ROWCOUNT,
    TABLE_ID_15_ROWCOUNT;
  }

  protected transient boolean autoDelete = false;

  private void commit(int idx) throws IOException {
    if (isNativeTable) {
      if (!fs.rename(outPaths[idx], finalPaths[idx])) {
        throw new IOException("Unable to rename output to: " 
          + finalPaths[idx]);
      }
    }
    LOG.info("Committed " + outPaths[idx] + " to output file: " + finalPaths[idx]);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    try {
      serializer = (Serializer) conf.getTableInfo().getDeserializerClass()
          .newInstance();
      serializer.initialize(null, conf.getTableInfo().getProperties());
      isNativeTable = !conf.getTableInfo().isNonNative();

      JobConf jc;
      if (hconf instanceof JobConf) {
        jc = (JobConf) hconf;
      } else {
        // test code path
        jc = new JobConf(hconf, ExecDriver.class);
      }

      multiFileSpray = conf.isMultiFileSpray();
      totalFiles = conf.getTotalFiles();
      numFiles   = conf.getNumFiles();

      if (multiFileSpray) {
        partitionEval = new ExprNodeEvaluator[conf.getPartitionCols().size()];
        int i = 0;
        for (ExprNodeDesc e : conf.getPartitionCols()) {
          partitionEval[i++] = ExprNodeEvaluatorFactory.get(e);
        }

        partitionObjectInspectors = initEvaluators(partitionEval, outputObjInspector);
        prtner = (HivePartitioner<HiveKey, Object>)ReflectionUtils.newInstance(jc.getPartitionerClass(), null);
      }

      outWriters = new RecordWriter[numFiles];
      outPaths   = new Path[numFiles];
      finalPaths = new Path[numFiles];

      String specPath = conf.getDirName();
      Path tmpPath = Utilities.toTempPath(specPath);
      Set<Integer> seenBuckets = new HashSet<Integer>();
      fs = (new Path(specPath)).getFileSystem(hconf);
      HiveOutputFormat<?, ?> hiveOutputFormat = conf.getTableInfo()
        .getOutputFileFormatClass().newInstance();
      boolean isCompressed = conf.getCompressed();
      Path parent = Utilities.toTempPath(specPath);
      final Class<? extends Writable> outputClass = serializer.getSerializedClass();

      // Create all the files - this is required because empty files need to be created for empty buckets
      int filesIdx = 0;
      for (int idx = 0; idx < totalFiles; idx++) {
        String taskId = Utilities.getTaskId(hconf);

        if (multiFileSpray) {
          key.setHashCode(idx);

          // Does this hashcode belong to this reducer
          int numReducers = totalFiles/numFiles;

          if (numReducers > 1) {
            int currReducer = Integer.valueOf(Utilities.getTaskIdFromFilename(Utilities.getTaskId(hconf)));

            int reducerIdx = prtner.getPartition(key, null, numReducers);
            if (currReducer != reducerIdx)
              continue;
          }

          int bucketNum = prtner.getBucket(key, null, totalFiles);
          if (seenBuckets.contains(bucketNum))
            continue;
          seenBuckets.add(bucketNum);

          bucketMap.put(bucketNum, filesIdx);
          taskId = Utilities.replaceTaskIdFromFilename(Utilities.getTaskId(hconf), bucketNum);
        }

        if (isNativeTable) {
          finalPaths[filesIdx] = new Path(tmpPath, taskId);
          LOG.info("Final Path: FS " + finalPaths[filesIdx]);
          outPaths[filesIdx] = new Path(tmpPath, Utilities.toTempPath(taskId));
          LOG.info("Writing to temp file: FS " + outPaths[filesIdx]);
        } else {
          finalPaths[filesIdx] = outPaths[filesIdx] = new Path(specPath);
        }

        // The reason to keep these instead of using
        // OutputFormat.getRecordWriter() is that
        // getRecordWriter does not give us enough control over the file name that
        // we create.
        finalPaths[filesIdx] = HiveFileFormatUtils.getOutputFormatFinalPath(parent, taskId, jc,
                                                                            hiveOutputFormat, isCompressed, finalPaths[filesIdx]);
        LOG.info("New Final Path: FS " + finalPaths[filesIdx]);

        Utilities.copyTableJobPropertiesToConf(conf.getTableInfo(), jc);
        outWriters[filesIdx] = HiveFileFormatUtils.getHiveRecordWriter(jc, conf
                                                                       .getTableInfo(), outputClass, conf, outPaths[filesIdx]);

        filesIdx++;
      }

      assert filesIdx == numFiles;

      // in recent hadoop versions, use deleteOnExit to clean tmp files.
      if (isNativeTable) {
        autoDelete = ShimLoader.getHadoopShims().fileSystemDeleteOnExit(fs,
          outPaths[0]);
      }

      int id = conf.getDestTableId();
      if ((id != 0) && (id <= TableIdEnum.values().length)) {
        String enumName = "TABLE_ID_" + String.valueOf(id) + "_ROWCOUNT";
        tabIdEnum = TableIdEnum.valueOf(enumName);
        row_count = new LongWritable();
        statsMap.put(tabIdEnum, row_count);
      }

      initializeChildren(hconf);
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  Writable recordValue;

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    // Since File Sink is a terminal operator, forward is not called - so,
    // maintain the number of output rows explicitly
    if (counterNameToEnum != null) {
      ++outputRows;
      if (outputRows % 1000 == 0) {
        incrCounter(numOutputRowsCntr, outputRows);
        outputRows = 0;
      }
    }

    try {
      if (reporter != null) {
        reporter.progress();
      }
      // user SerDe to serialize r, and write it out
      recordValue = serializer.serialize(row, inputObjInspectors[tag]);
      if (row_count != null) {
        row_count.set(row_count.get() + 1);
      }

      if (!multiFileSpray) {
        outWriters[0].write(recordValue);
      }
      else {
        int keyHashCode = 0;
        for (int i = 0; i < partitionEval.length; i++) {
          Object o = partitionEval[i].evaluate(row);
          keyHashCode = keyHashCode * 31
              + ObjectInspectorUtils.hashCode(o, partitionObjectInspectors[i]);
        }
        key.setHashCode(keyHashCode);
        int bucketNum = prtner.getBucket(key, null, totalFiles);
        int idx = bucketMap.get(bucketNum);
        outWriters[bucketMap.get(bucketNum)].write(recordValue);
      }
    } catch (IOException e) {
      throw new HiveException(e);
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {

    if (!abort) {
      for (int idx = 0; idx < numFiles; idx++) {
        if (outWriters[idx] != null) {
          try {
            outWriters[idx].close(abort);
            commit(idx);
          } catch (IOException e) {
            throw new HiveException(e);
          }
        }
      }
    } else {
      // Will come here if an Exception was thrown in map() or reduce().
      // Hadoop always call close() even if an Exception was thrown in map() or
      // reduce().
      try {
        for (int idx = 0; idx < numFiles; idx++) {
          outWriters[idx].close(abort);
          if (!autoDelete && isNativeTable) {
            fs.delete(outPaths[idx], true);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return new String("FS");
  }

  @Override
  public void jobClose(Configuration hconf, boolean success) throws HiveException {
    try {
      if ((conf != null) && isNativeTable) {
        String specPath = conf.getDirName();
        FileSinkOperator.mvFileToFinalPath(specPath, hconf, success, LOG);
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
    super.jobClose(hconf, success);
  }

  public static void mvFileToFinalPath(String specPath, Configuration hconf,
      boolean success, Log log) throws IOException, HiveException {
    FileSystem fs = (new Path(specPath)).getFileSystem(hconf);
    Path tmpPath = Utilities.toTempPath(specPath);
    Path intermediatePath = new Path(tmpPath.getParent(), tmpPath.getName()
        + ".intermediate");
    Path finalPath = new Path(specPath);
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

  @Override
  public int getType() {
    return OperatorType.FILESINK;
  }

  @Override
  public void augmentPlan() {
    PlanUtils.configureTableJobPropertiesForStorageHandler(
      getConf().getTableInfo());
  }
}
