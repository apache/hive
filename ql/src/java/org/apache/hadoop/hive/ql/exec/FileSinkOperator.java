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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HivePartitioner;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.ql.stats.StatsSetupConst;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SubStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
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

  protected transient HashMap<String, FSPaths> valToPaths;
  protected transient int numDynParts;
  protected transient List<String> dpColNames;
  protected transient DynamicPartitionCtx dpCtx;
  protected transient boolean isCompressed;
  protected transient Path parent;
  protected transient HiveOutputFormat<?, ?> hiveOutputFormat;
  protected transient Path specPath;
  protected transient int dpStartCol; // start column # for DP columns
  protected transient List<String> dpVals; // array of values corresponding to DP columns
  protected transient List<Object> dpWritables;
  protected transient RecordWriter[] rowOutWriters; // row specific RecordWriters
  protected transient int maxPartitions;

  private static final transient String[] FATAL_ERR_MSG = {
      null, // counter value 0 means no error
      "Number of dynamic partitions exceeded hive.exec.max.dynamic.partitions.pernode."
  };

  /**
   * RecordWriter.
   *
   */
  public static interface RecordWriter {
    void write(Writable w) throws IOException;

    void close(boolean abort) throws IOException;
  }

  public class FSPaths implements Cloneable {
    Path tmpPath;
    Path taskOutputTempPath;
    Path[] outPaths;
    Path[] finalPaths;
    RecordWriter[] outWriters;
    Stat stat;

    public FSPaths() {
    }

    public FSPaths(Path specPath) {
      tmpPath = Utilities.toTempPath(specPath);
      taskOutputTempPath = Utilities.toTaskTempPath(specPath);
      outPaths = new Path[numFiles];
      finalPaths = new Path[numFiles];
      outWriters = new RecordWriter[numFiles];
      stat = new Stat();
    }

    /**
     * Append a subdirectory to the tmp path.
     *
     * @param dp
     *          subdirecgtory name
     */
    public void appendTmpPath(String dp) {
      tmpPath = new Path(tmpPath, dp);
    }

    /**
     * Update OutPath according to tmpPath.
     */
    public Path getTaskOutPath(String taskId) {
      return getOutPath(taskId, this.taskOutputTempPath);
    }


    /**
     * Update OutPath according to tmpPath.
     */
    public Path getOutPath(String taskId) {
      return getOutPath(taskId, this.tmpPath);
    }

    /**
     * Update OutPath according to tmpPath.
     */
    public Path getOutPath(String taskId, Path tmp) {
      return new Path(tmp, Utilities.toTempPath(taskId));
    }

    /**
     * Update the final paths according to tmpPath.
     */
    public Path getFinalPath(String taskId) {
      return getFinalPath(taskId, this.tmpPath, null);
    }

    /**
     * Update the final paths according to tmpPath.
     */
    public Path getFinalPath(String taskId, Path tmpPath, String extension) {
      if (extension != null) {
        return new Path(tmpPath, taskId + extension);
      } else {
        return new Path(tmpPath, taskId);
      }
    }

    public void setOutWriters(RecordWriter[] out) {
      outWriters = out;
    }

    public RecordWriter[] getOutWriters() {
      return outWriters;
    }

    public void closeWriters(boolean abort) throws HiveException {
      for (int idx = 0; idx < outWriters.length; idx++) {
        if (outWriters[idx] != null) {
          try {
            outWriters[idx].close(abort);
            updateProgress();
          } catch (IOException e) {
            throw new HiveException(e);
          }
        }
      }
    }

    private void commit(FileSystem fs) throws HiveException {
      for (int idx = 0; idx < outPaths.length; ++idx) {
        try {
          if (bDynParts && !fs.exists(finalPaths[idx].getParent())) {
            fs.mkdirs(finalPaths[idx].getParent());
          }
          if (!fs.rename(outPaths[idx], finalPaths[idx])) {
            throw new HiveException("Unable to rename output from: " +
                outPaths[idx] + " to: " + finalPaths[idx]);
          }
          updateProgress();
        } catch (IOException e) {
          throw new HiveException("Unable to rename output from: " +
              outPaths[idx] + " to: " + finalPaths[idx], e);
        }
      }
    }

    public void abortWriters(FileSystem fs, boolean abort, boolean delete) throws HiveException {
      for (int idx = 0; idx < outWriters.length; idx++) {
        if (outWriters[idx] != null) {
          try {
            outWriters[idx].close(abort);
            if (delete) {
              fs.delete(outPaths[idx], true);
            }
            updateProgress();
          } catch (IOException e) {
            throw new HiveException(e);
          }
        }
      }
    }
  } // class FSPaths

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
  private transient int totalFiles;
  private transient int numFiles;
  private transient boolean multiFileSpray;
  private transient final Map<Integer, Integer> bucketMap = new HashMap<Integer, Integer>();

  private transient ObjectInspector[] partitionObjectInspectors;
  private transient HivePartitioner<HiveKey, Object> prtner;
  private transient final HiveKey key = new HiveKey();
  private transient Configuration hconf;
  private transient FSPaths fsp;
  private transient boolean bDynParts;
  private transient SubStructObjectInspector subSetOI;
  private transient int timeOut; // JT timeout in msec.
  private transient long lastProgressReport = System.currentTimeMillis();

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
  protected transient JobConf jc;
  Class<? extends Writable> outputClass;
  String taskId;

  private boolean filesCreated = false;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    try {
      this.hconf = hconf;
      filesCreated = false;
      isNativeTable = !conf.getTableInfo().isNonNative();
      multiFileSpray = conf.isMultiFileSpray();
      totalFiles = conf.getTotalFiles();
      numFiles = conf.getNumFiles();
      dpCtx = conf.getDynPartCtx();
      valToPaths = new HashMap<String, FSPaths>();
      taskId = Utilities.getTaskId(hconf);
      specPath = new Path(conf.getDirName());
      fs = specPath.getFileSystem(hconf);
      hiveOutputFormat = conf.getTableInfo().getOutputFileFormatClass().newInstance();
      isCompressed = conf.getCompressed();
      parent = Utilities.toTempPath(conf.getDirName());

      serializer = (Serializer) conf.getTableInfo().getDeserializerClass().newInstance();
      serializer.initialize(null, conf.getTableInfo().getProperties());
      outputClass = serializer.getSerializedClass();

      // Timeout is chosen to make sure that even if one iteration takes more than
      // half of the script.timeout but less than script.timeout, we will still
      // be able to report progress.
      timeOut = hconf.getInt("mapred.healthChecker.script.timeout", 600000) / 2;

      if (hconf instanceof JobConf) {
        jc = (JobConf) hconf;
      } else {
        // test code path
        jc = new JobConf(hconf, ExecDriver.class);
      }

      if (multiFileSpray) {
        partitionEval = new ExprNodeEvaluator[conf.getPartitionCols().size()];
        int i = 0;
        for (ExprNodeDesc e : conf.getPartitionCols()) {
          partitionEval[i++] = ExprNodeEvaluatorFactory.get(e);
        }

        partitionObjectInspectors = initEvaluators(partitionEval, outputObjInspector);
        prtner = (HivePartitioner<HiveKey, Object>) ReflectionUtils.newInstance(
            jc.getPartitionerClass(), null);
      }
      int id = conf.getDestTableId();
      if ((id != 0) && (id <= TableIdEnum.values().length)) {
        String enumName = "TABLE_ID_" + String.valueOf(id) + "_ROWCOUNT";
        tabIdEnum = TableIdEnum.valueOf(enumName);
        row_count = new LongWritable();
        statsMap.put(tabIdEnum, row_count);
      }

      if (dpCtx != null) {
        dpSetup();
      }

      if (!bDynParts) {
        fsp = new FSPaths(specPath);

        // Create all the files - this is required because empty files need to be created for
        // empty buckets
        // createBucketFiles(fsp);
        valToPaths.put("", fsp); // special entry for non-DP case
      }
      initializeChildren(hconf);
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  /**
   * Set up for dynamic partitioning including a new ObjectInspector for the output row.
   */
  private void dpSetup() {

    this.bDynParts = false;
    this.numDynParts = dpCtx.getNumDPCols();
    this.dpColNames = dpCtx.getDPColNames();
    this.maxPartitions = dpCtx.getMaxPartitionsPerNode();

    assert numDynParts == dpColNames.size() : "number of dynamic paritions should be the same as the size of DP mapping";

    if (dpColNames != null && dpColNames.size() > 0) {
      this.bDynParts = true;
      assert inputObjInspectors.length == 1 : "FileSinkOperator should have 1 parent, but it has "
          + inputObjInspectors.length;
      StructObjectInspector soi = (StructObjectInspector) inputObjInspectors[0];
      // remove the last dpMapping.size() columns from the OI
      List<? extends StructField> fieldOI = soi.getAllStructFieldRefs();
      ArrayList<ObjectInspector> newFieldsOI = new ArrayList<ObjectInspector>();
      ArrayList<String> newFieldsName = new ArrayList<String>();
      this.dpStartCol = 0;
      for (StructField sf : fieldOI) {
        String fn = sf.getFieldName();
        if (!dpCtx.getInputToDPCols().containsKey(fn)) {
          newFieldsOI.add(sf.getFieldObjectInspector());
          newFieldsName.add(sf.getFieldName());
          this.dpStartCol++;
        }
      }
      assert newFieldsOI.size() > 0 : "new Fields ObjectInspector is empty";

      this.subSetOI = new SubStructObjectInspector(soi, 0, this.dpStartCol);
      this.dpVals = new ArrayList<String>(numDynParts);
      this.dpWritables = new ArrayList<Object>(numDynParts);
    }
  }

  private void createBucketFiles(FSPaths fsp) throws HiveException {
    try {
      int filesIdx = 0;
      Set<Integer> seenBuckets = new HashSet<Integer>();
      for (int idx = 0; idx < totalFiles; idx++) {
        if (this.getExecContext() != null && this.getExecContext().getFileId() != -1) {
          LOG.info("replace taskId from execContext ");

          taskId = Utilities.replaceTaskIdFromFilename(taskId, this.getExecContext().getFileId());

          LOG.info("new taskId: FS " + taskId);

          assert !multiFileSpray;
          assert totalFiles == 1;
        }

        if (multiFileSpray) {
          key.setHashCode(idx);

          // Does this hashcode belong to this reducer
          int numReducers = totalFiles / numFiles;

          if (numReducers > 1) {
            int currReducer = Integer.valueOf(Utilities.getTaskIdFromFilename(Utilities
                .getTaskId(hconf)));

            int reducerIdx = prtner.getPartition(key, null, numReducers);
            if (currReducer != reducerIdx) {
              continue;
            }
          }

          int bucketNum = prtner.getBucket(key, null, totalFiles);
          if (seenBuckets.contains(bucketNum)) {
            continue;
          }
          seenBuckets.add(bucketNum);

          bucketMap.put(bucketNum, filesIdx);
          taskId = Utilities.replaceTaskIdFromFilename(Utilities.getTaskId(hconf), bucketNum);
        }
        if (isNativeTable) {
          fsp.finalPaths[filesIdx] = fsp.getFinalPath(taskId);
          LOG.info("Final Path: FS " + fsp.finalPaths[filesIdx]);
          fsp.outPaths[filesIdx] = fsp.getTaskOutPath(taskId);
          LOG.info("Writing to temp file: FS " + fsp.outPaths[filesIdx]);
        } else {
          fsp.finalPaths[filesIdx] = fsp.outPaths[filesIdx] = specPath;
        }
        try {
          // The reason to keep these instead of using
          // OutputFormat.getRecordWriter() is that
          // getRecordWriter does not give us enough control over the file name that
          // we create.
          String extension = Utilities.getFileExtension(jc, isCompressed,
              hiveOutputFormat);
          if (!bDynParts) {
            fsp.finalPaths[filesIdx] = fsp.getFinalPath(taskId, parent, extension);
          } else {
            fsp.finalPaths[filesIdx] = fsp.getFinalPath(taskId, fsp.tmpPath, extension);
          }

        } catch (Exception e) {
          e.printStackTrace();
          throw new HiveException(e);
        }
        LOG.info("New Final Path: FS " + fsp.finalPaths[filesIdx]);

        if (isNativeTable) {
          try {
            // in recent hadoop versions, use deleteOnExit to clean tmp files.
            autoDelete = ShimLoader.getHadoopShims().fileSystemDeleteOnExit(
                fs, fsp.outPaths[filesIdx]);
          } catch (IOException e) {
            throw new HiveException(e);
          }
        }

        Utilities.copyTableJobPropertiesToConf(conf.getTableInfo(), jc);
        // only create bucket files only if no dynamic partitions,
        // buckets of dynamic partitions will be created for each newly created partition
        fsp.outWriters[filesIdx] = HiveFileFormatUtils.getHiveRecordWriter(
            jc, conf.getTableInfo(), outputClass, conf, fsp.outPaths[filesIdx]);
        // increment the CREATED_FILES counter
        if (reporter != null) {
          reporter.incrCounter(ProgressCounter.CREATED_FILES, 1);
        }
        filesIdx++;
      }
      assert filesIdx == numFiles;

      // in recent hadoop versions, use deleteOnExit to clean tmp files.
      if (isNativeTable) {
        autoDelete = ShimLoader.getHadoopShims().fileSystemDeleteOnExit(fs, fsp.outPaths[0]);
      }
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }

    filesCreated = true;
  }

  /**
   * Report status to JT so that JT won't kill this task if closing takes too long
   * due to too many files to close and the NN is overloaded.
   *
   * @param lastUpdateTime
   *          the time (msec) that progress update happened.
   * @return true if a new progress update is reported, false otherwise.
   */
  private boolean updateProgress() {
    if (reporter != null &&
        (System.currentTimeMillis() - lastProgressReport) > timeOut) {
      reporter.progress();
      lastProgressReport = System.currentTimeMillis();
      return true;
    } else {
      return false;
    }
  }

  Writable recordValue;

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    if (!bDynParts && !filesCreated) {
      createBucketFiles(fsp);
    }

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
      updateProgress();

      // if DP is enabled, get the final output writers and prepare the real output row
      assert inputObjInspectors[0].getCategory() == ObjectInspector.Category.STRUCT : "input object inspector is not struct";

      FSPaths fpaths;

      if (bDynParts) {
        // copy the DP column values from the input row to dpVals
        dpVals.clear();
        dpWritables.clear();
        ObjectInspectorUtils.partialCopyToStandardObject(dpWritables, row, dpStartCol, numDynParts,
            (StructObjectInspector) inputObjInspectors[0], ObjectInspectorCopyOption.WRITABLE);
        // get a set of RecordWriter based on the DP column values
        // pass the null value along to the escaping process to determine what the dir should be
        for (Object o : dpWritables) {
          if (o == null || o.toString().length() == 0) {
            dpVals.add(dpCtx.getDefaultPartitionName());
          } else {
            dpVals.add(o.toString());
          }
        }
        // use SubStructObjectInspector to serialize the non-partitioning columns in the input row
        recordValue = serializer.serialize(row, subSetOI);
        fpaths = getDynOutPaths(dpVals);

      } else {
        fpaths = fsp;
        // use SerDe to serialize r, and write it out
        recordValue = serializer.serialize(row, inputObjInspectors[0]);
      }

      rowOutWriters = fpaths.outWriters;
      if (conf.isGatherStats()) {
        if (HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVE_STATS_COLLECT_RAWDATASIZE)) {
          SerDeStats stats = serializer.getSerDeStats();
          if (stats != null) {
            fpaths.stat.addToStat(StatsSetupConst.RAW_DATA_SIZE, stats.getRawDataSize());
          }
        }
        fpaths.stat.addToStat(StatsSetupConst.ROW_COUNT, 1);
      }


      if (row_count != null) {
        row_count.set(row_count.get() + 1);
      }

      if (!multiFileSpray) {
        rowOutWriters[0].write(recordValue);
      } else {
        int keyHashCode = 0;
        for (int i = 0; i < partitionEval.length; i++) {
          Object o = partitionEval[i].evaluate(row);
          keyHashCode = keyHashCode * 31
              + ObjectInspectorUtils.hashCode(o, partitionObjectInspectors[i]);
        }
        key.setHashCode(keyHashCode);
        int bucketNum = prtner.getBucket(key, null, totalFiles);
        int idx = bucketMap.get(bucketNum);
        rowOutWriters[idx].write(recordValue);
      }
    } catch (IOException e) {
      throw new HiveException(e);
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
  }

  private FSPaths getDynOutPaths(List<String> row) throws HiveException {

    FSPaths fp;

    // get the path corresponding to the dynamic partition columns,
    String dpDir = getDynPartDirectory(row, dpColNames, numDynParts);

    if (dpDir != null) {
      FSPaths fsp2 = valToPaths.get(dpDir);

      if (fsp2 == null) {
        // check # of dp
        if (valToPaths.size() > maxPartitions) {
          // throw fatal error
          incrCounter(fatalErrorCntr, 1);
          fatalError = true;
          LOG.error("Fatal error was thrown due to exceeding number of dynamic partitions");
        }
        fsp2 = new FSPaths(specPath);
        fsp2.tmpPath = new Path(fsp2.tmpPath, dpDir);
        fsp2.taskOutputTempPath = new Path(fsp2.taskOutputTempPath, dpDir);
        createBucketFiles(fsp2);
        valToPaths.put(dpDir, fsp2);
      }
      fp = fsp2;
    } else {
      fp = fsp;
    }
    return fp;
  }

  // given the current input row, the mapping for input col info to dp columns, and # of dp cols,
  // return the relative path corresponding to the row.
  // e.g., ds=2008-04-08/hr=11
  private String getDynPartDirectory(List<String> row, List<String> dpColNames, int numDynParts) {
    assert row.size() == numDynParts && numDynParts == dpColNames.size() : "data length is different from num of DP columns";
    return FileUtils.makePartName(dpColNames, row);
  }

  @Override
  protected void fatalErrorMessage(StringBuilder errMsg, long counterCode) {
    errMsg.append("Operator ").append(getOperatorId()).append(" (id=").append(id).append("): ");
    errMsg.append(counterCode > FATAL_ERR_MSG.length - 1 ?
        "fatal error." :
          FATAL_ERR_MSG[(int) counterCode]);
    // number of partitions exceeds limit, list all the partition names
    if (counterCode > 0) {
      errMsg.append(lsDir());
    }
  }

  // sample the partitions that are generated so that users have a sense of what's causing the error
  private String lsDir() {
    String specPath = conf.getDirName();
    // need to get a JobConf here because it's not passed through at client side
    JobConf jobConf = new JobConf(ExecDriver.class);
    Path tmpPath = Utilities.toTempPath(specPath);
    StringBuilder sb = new StringBuilder("\n");
    try {
      DynamicPartitionCtx dpCtx = conf.getDynPartCtx();
      int numDP = dpCtx.getNumDPCols();
      FileSystem fs = tmpPath.getFileSystem(jobConf);
      FileStatus[] status = Utilities.getFileStatusRecurse(tmpPath, numDP, fs);
      sb.append("Sample of ")
        .append(Math.min(status.length, 100))
        .append(" partitions created under ")
        .append(tmpPath.toString())
        .append(":\n");
      for (int i = 0; i < status.length; ++i) {
        sb.append("\t.../");
        sb.append(getPartitionSpec(status[i].getPath(), numDP))
          .append("\n");
      }
      sb.append("...\n");
    } catch (Exception e) {
      // cannot get the subdirectories, just return the root directory
      sb.append(tmpPath).append("...\n").append(e.getMessage());
      e.printStackTrace();
    } finally {
      return sb.toString();
    }
  }

  private String getPartitionSpec(Path path, int level) {
    Stack<String> st = new Stack<String>();
    Path p = path;
    for (int i = 0; i < level; ++i) {
      st.push(p.getName());
      p = p.getParent();
    }
    StringBuilder sb = new StringBuilder();
    while (!st.empty()) {
      sb.append(st.pop());
    }
    return sb.toString();
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {

    if (!bDynParts && !filesCreated) {
      createBucketFiles(fsp);
    }

    lastProgressReport = System.currentTimeMillis();
    if (!abort) {
      for (FSPaths fsp : valToPaths.values()) {
        fsp.closeWriters(abort);
        if (isNativeTable) {
          fsp.commit(fs);
        }
      }
      // Only publish stats if this operator's flag was set to gather stats
      if (conf.isGatherStats()) {
        publishStats();
      }
    } else {
      // Will come here if an Exception was thrown in map() or reduce().
      // Hadoop always call close() even if an Exception was thrown in map() or
      // reduce().
      for (FSPaths fsp : valToPaths.values()) {
        fsp.abortWriters(fs, abort, !autoDelete && isNativeTable);
      }
    }
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return "FS";
  }

  @Override
  public void jobClose(Configuration hconf, boolean success, JobCloseFeedBack feedBack)
      throws HiveException {
    try {
      if ((conf != null) && isNativeTable) {
        String specPath = conf.getDirName();
        DynamicPartitionCtx dpCtx = conf.getDynPartCtx();
        Utilities.mvFileToFinalPath(specPath, hconf, success, LOG, dpCtx, conf);
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
    super.jobClose(hconf, success, feedBack);
  }

  @Override
  public OperatorType getType() {
    return OperatorType.FILESINK;
  }

  @Override
  public void augmentPlan() {
    PlanUtils.configureTableJobPropertiesForStorageHandler(
        getConf().getTableInfo());
  }

  private void publishStats() {
    // Initializing a stats publisher
    StatsPublisher statsPublisher = Utilities.getStatsPublisher(jc);

    if (statsPublisher == null) {
      // just return, stats gathering should not block the main query
      LOG.error("StatsPublishing error: StatsPublisher is not initialized.");
      return;
    }
    if (!statsPublisher.connect(hconf)) {
      // just return, stats gathering should not block the main query
      LOG.error("StatsPublishing error: cannot connect to database");
      return;
    }

    String taskID = Utilities.getTaskIdFromFilename(Utilities.getTaskId(hconf));
    String spSpec = conf.getStaticSpec() != null ? conf.getStaticSpec() : "";

    for (String fspKey : valToPaths.keySet()) {
      FSPaths fspValue = valToPaths.get(fspKey);
      String key;

      // construct the key(fileID) to insert into the intermediate stats table
      if (fspKey == "") {
        // for non-partitioned/static partitioned table, the key for temp storage is
        // common key prefix + static partition spec + taskID
        key = conf.getStatsAggPrefix() + spSpec + taskID;
      } else {
        // for partitioned table, the key is
        // common key prefix + static partition spec + DynamicPartSpec + taskID
        key = conf.getStatsAggPrefix() + spSpec + fspKey + Path.SEPARATOR + taskID;
      }
      Map<String, String> statsToPublish = new HashMap<String, String>();
      for (String statType : fspValue.stat.getStoredStats()) {
        statsToPublish.put(statType, Long.toString(fspValue.stat.getStat(statType)));
      }
      statsPublisher.publishStat(key, statsToPublish);
    }
    statsPublisher.closeConnection();
  }
}
