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

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_TEMPORARY_TABLE_STORAGE;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfUtil;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Utilities.MissingBucketsContext;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HivePartitioner;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.ql.io.StatsProvidingRecordWriter;
import org.apache.hadoop.hive.ql.io.StreamingOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveFatalException;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc.DPSortState;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.SkewedColumnPositionPair;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.stats.StatsCollectionContext;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SubStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.shims.HadoopShims.StoragePolicyShim;
import org.apache.hadoop.hive.shims.HadoopShims.StoragePolicyValue;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.Murmur3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiFunction;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_TEMPORARY_TABLE_STORAGE;

/**
 * File Sink operator implementation.
 **/
@SuppressWarnings("deprecation")
public class FileSinkOperator extends TerminalOperator<FileSinkDesc> implements
    Serializable {

  public static final Logger LOG = LoggerFactory.getLogger(FileSinkOperator.class);

  protected transient HashMap<String, FSPaths> valToPaths;
  protected transient int numDynParts;
  protected transient List<String> dpColNames;
  protected transient DynamicPartitionCtx dpCtx;
  protected transient boolean isCompressed;
  protected transient boolean isTemporary;
  protected transient Path parent;
  protected transient HiveOutputFormat<?, ?> hiveOutputFormat;
  protected transient Path specPath;
  protected transient String unionPath;
  protected transient boolean isUnionDp;
  protected transient int dpStartCol; // start column # for DP columns
  protected transient List<String> dpVals; // array of values corresponding to DP columns
  protected transient List<Object> dpWritables;
  protected transient RecordWriter[] rowOutWriters; // row specific RecordWriters
  protected transient int maxPartitions;
  protected transient ListBucketingCtx lbCtx;
  protected transient boolean isSkewedStoredAsSubDirectories;
  protected transient boolean[] statsFromRecordWriter;
  protected transient boolean isCollectRWStats;
  private transient FSPaths prevFsp;
  private transient FSPaths fpaths;
  private StructField recIdField; // field to find record identifier in
  private StructField bucketField; // field bucket is in in record id
  private StructObjectInspector recIdInspector; // OI for inspecting record id
  private IntObjectInspector bucketInspector; // OI for inspecting bucket id
  protected transient long numRows = 0;
  protected transient long cntr = 1;
  protected transient long logEveryNRows = 0;
  protected transient int rowIndex = 0;
  private transient Path destTablePath;
  private transient boolean isInsertOverwrite;
  private transient String counterGroup;
  private transient BiFunction<Object[], ObjectInspector[], Integer> hashFunc;
  /**
   * Counters.
   */
  public static enum Counter {
    RECORDS_OUT
  }

  /**
   * RecordWriter.
   *
   */
  public static interface RecordWriter {
    void write(Writable w) throws IOException;

    void close(boolean abort) throws IOException;
  }

  public class FSPaths implements Cloneable {
    private Path tmpPathRoot;
    private String subdirBeforeTxn, subdirAfterTxn;
    private final String subdirForTxn;
    private Path taskOutputTempPathRoot;
    Path[] outPaths;
    Path[] finalPaths;
    RecordWriter[] outWriters;
    RecordUpdater[] updaters;
    Stat stat;
    int acidLastBucket = -1;
    int acidFileOffset = -1;
    private boolean isMmTable;
    String dpDirForCounters;

    public FSPaths(Path specPath, boolean isMmTable) {
      this.isMmTable = isMmTable;
      if (!isMmTable) {
        tmpPathRoot = Utilities.toTempPath(specPath);
        taskOutputTempPathRoot = Utilities.toTaskTempPath(specPath);
        subdirForTxn = null;
      } else {
        tmpPathRoot = specPath;
        taskOutputTempPathRoot = null; // Should not be used.
        subdirForTxn = AcidUtils.baseOrDeltaSubdir(conf.getInsertOverwrite(),
            conf.getTableWriteId(), conf.getTableWriteId(),  conf.getStatementId());
      }
      if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
        Utilities.FILE_OP_LOGGER.trace("new FSPaths for " + numFiles
            + " files, dynParts = " + bDynParts + " (spec path " + specPath + ")");
      }

      outPaths = new Path[numFiles];
      finalPaths = new Path[numFiles];
      outWriters = new RecordWriter[numFiles];
      updaters = new RecordUpdater[numFiles];
      if (LOG.isDebugEnabled()) {
        LOG.debug("Created slots for  " + numFiles);
      }
      stat = new Stat();
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
      try {
        for (int i = 0; i < updaters.length; i++) {
          if (updaters[i] != null) {
            updaters[i].close(abort);
          }
        }
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }

    private void commit(FileSystem fs, List<Path> commitPaths) throws HiveException {
      for (int idx = 0; idx < outPaths.length; ++idx) {
        try {
          commitOneOutPath(idx, fs, commitPaths);
        } catch (IOException e) {
          throw new HiveException("Unable to commit output from: " +
              outPaths[idx] + " to: " + finalPaths[idx], e);
        }
      }
    }

    private void commitOneOutPath(int idx, FileSystem fs, List<Path> commitPaths)
        throws IOException, HiveException {
      if ((bDynParts || isSkewedStoredAsSubDirectories)
          && !fs.exists(finalPaths[idx].getParent())) {
        if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
          Utilities.FILE_OP_LOGGER.trace("commit making path for dyn/skew: " + finalPaths[idx].getParent());
        }
        FileUtils.mkdir(fs, finalPaths[idx].getParent(), hconf);
      }
      if(outPaths[idx] != null && fs.exists(outPaths[idx])) {
        if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
          Utilities.FILE_OP_LOGGER.trace("committing " + outPaths[idx] + " to "
              + finalPaths[idx] + " (" + isMmTable + ")");
        }
        if (isMmTable) {
          assert outPaths[idx].equals(finalPaths[idx]);
          commitPaths.add(outPaths[idx]);
        } else if (!fs.rename(outPaths[idx], finalPaths[idx])) {
            FileStatus fileStatus = FileUtils.getFileStatusOrNull(fs, finalPaths[idx]);
            if (fileStatus != null) {
              LOG.warn("Target path " + finalPaths[idx] + " with a size " + fileStatus.getLen() + " exists. Trying to delete it.");
              if (!fs.delete(finalPaths[idx], true)) {
                throw new HiveException("Unable to delete existing target output: " + finalPaths[idx]);
              }
            }

            if (!fs.rename(outPaths[idx], finalPaths[idx])) {
              throw new HiveException("Unable to rename output from: "
                + outPaths[idx] + " to: " + finalPaths[idx]);
            }
        }
      }

      updateProgress();
    }

    public void abortWriters(FileSystem fs, boolean abort, boolean delete) throws HiveException {
      //should this close updaters[]?
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

    public void initializeBucketPaths(int filesIdx, String taskId, boolean isNativeTable,
        boolean isSkewedStoredAsSubDirectories) {
      if (isNativeTable) {
        String extension = Utilities.getFileExtension(jc, isCompressed, hiveOutputFormat);
        String taskWithExt = extension == null ? taskId : taskId + extension;
        if (!isMmTable) {
          if (!bDynParts && !isSkewedStoredAsSubDirectories) {
            finalPaths[filesIdx] = new Path(parent, taskWithExt);
          } else {
            finalPaths[filesIdx] =  new Path(buildTmpPath(), taskWithExt);
          }
          outPaths[filesIdx] = new Path(buildTaskOutputTempPath(), Utilities.toTempPath(taskId));
        } else {
          String taskIdPath = taskId;
          if (conf.isMerge()) {
            // Make sure we don't collide with the source files.
            // MM tables don't support concat so we don't expect the merge of merged files.
            taskIdPath += ".merged";
          }
          if (extension != null) {
            taskIdPath += extension;
          }

          Path finalPath = new Path(buildTmpPath(), taskIdPath);

          // In the cases that have multi-stage insert, e.g. a "hive.skewjoin.key"-based skew join,
          // it can happen that we want multiple commits into the same directory from different
          // tasks (not just task instances). In non-MM case, Utilities.renameOrMoveFiles ensures
          // unique names. We could do the same here, but this will still cause the old file to be
          // deleted because it has not been committed in /this/ FSOP. We are going to fail to be
          // safe. Potentially, we could implement some partial commit between stages, if this
          // affects some less obscure scenario.
          try {
            FileSystem fpfs = finalPath.getFileSystem(hconf);
            if (fpfs.exists(finalPath)) throw new RuntimeException(finalPath + " already exists");
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          finalPaths[filesIdx] = finalPath;
          outPaths[filesIdx] = finalPath;
        }
        if (LOG.isInfoEnabled()) {
          LOG.info("Final Path: FS " + finalPaths[filesIdx]);
          if (LOG.isInfoEnabled() && !isMmTable) {
            LOG.info("Writing to temp file: FS " + outPaths[filesIdx]);
          }
        }
      } else {
        finalPaths[filesIdx] = outPaths[filesIdx] = specPath;
      }
    }

    public Path buildTmpPath() {
      String pathStr = tmpPathRoot.toString();
      if (subdirBeforeTxn != null) {
        pathStr += Path.SEPARATOR + subdirBeforeTxn;
      }
      if (subdirForTxn != null) {
        pathStr += Path.SEPARATOR + subdirForTxn;
      }
      if (subdirAfterTxn != null) {
        pathStr += Path.SEPARATOR + subdirAfterTxn;
      }
      return new Path(pathStr);
    }

    public Path buildTaskOutputTempPath() {
      if (taskOutputTempPathRoot == null) return null;
      assert subdirForTxn == null;
      String pathStr = taskOutputTempPathRoot.toString();
      if (subdirBeforeTxn != null) {
        pathStr += Path.SEPARATOR + subdirBeforeTxn;
      }
      if (subdirAfterTxn != null) {
        pathStr += Path.SEPARATOR + subdirAfterTxn;
      }
      return new Path(pathStr);
    }

    public void addToStat(String statType, long amount) {
      stat.addToStat(statType, amount);
    }

    public Collection<String> getStoredStats() {
      return stat.getStoredStats();
    }
  } // class FSPaths

  private static final long serialVersionUID = 1L;
  protected transient FileSystem fs;
  protected transient Serializer serializer;
  protected final transient LongWritable row_count = new LongWritable();

  /**
   * The evaluators for the multiFile sprayer. If the table under consideration has 1000 buckets,
   * it is not a good idea to start so many reducers - if the maximum number of reducers is 100,
   * each reducer can write 10 files - this way we effectively get 1000 files.
   */
  private transient ExprNodeEvaluator[] partitionEval;
  protected transient int totalFiles;
  private transient int numFiles;
  protected transient boolean multiFileSpray;
  protected transient final Map<Integer, Integer> bucketMap = new HashMap<Integer, Integer>();
  private transient boolean isBucketed = false;

  private transient ObjectInspector[] partitionObjectInspectors;
  protected transient HivePartitioner<HiveKey, Object> prtner;
  protected transient final HiveKey key = new HiveKey();
  private transient Configuration hconf;
  protected transient FSPaths fsp;
  protected transient boolean bDynParts;
  private transient SubStructObjectInspector subSetOI;
  private transient int timeOut; // JT timeout in msec.
  private transient long lastProgressReport = System.currentTimeMillis();

  protected transient boolean autoDelete = false;
  protected transient JobConf jc;
  Class<? extends Writable> outputClass;
  String taskId;

  protected boolean filesCreated = false;

  private void initializeSpecPath() {
    // For a query of the type:
    // insert overwrite table T1
    // select * from (subq1 union all subq2)u;
    // subQ1 and subQ2 write to directories Parent/Child_1 and
    // Parent/Child_2 respectively, and union is removed.
    // The movetask that follows subQ1 and subQ2 tasks moves the directory
    // 'Parent'

    // However, if the above query contains dynamic partitions, subQ1 and
    // subQ2 have to write to directories: Parent/DynamicPartition/Child_1
    // and Parent/DynamicPartition/Child_1 respectively.
    // The movetask that follows subQ1 and subQ2 tasks still moves the directory
    // 'Parent'
    boolean isLinked = conf.isLinkedFileSink();
    if (!isLinked) {
      // Simple case - no union. 
      specPath = conf.getDirName();
      unionPath = null;
    } else {
      isUnionDp = (dpCtx != null);
      if (conf.isMmTable() || isUnionDp) {
        // MM tables need custom handling for union suffix; DP tables use parent too.
        specPath = conf.getParentDir();
        unionPath = conf.getDirName().getName();
      } else {
        // For now, keep the old logic for non-MM non-DP union case. Should probably be unified.
        specPath = conf.getDirName();
        unionPath = null;
      }
    }
    if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
      Utilities.FILE_OP_LOGGER.trace("Setting up FSOP " + System.identityHashCode(this) + " ("
        + conf.isLinkedFileSink() + ") with " + taskId + " and " + specPath + " + " + unionPath);
    }
  }

  /** Kryo ctor. */
  protected FileSinkOperator() {
    super();
  }

  public FileSinkOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    try {
      this.hconf = hconf;
      filesCreated = false;
      isTemporary = conf.isTemporary();
      multiFileSpray = conf.isMultiFileSpray();
      this.isBucketed = hconf.getInt(hive_metastoreConstants.BUCKET_COUNT, 0) > 0;
      totalFiles = conf.getTotalFiles();
      numFiles = conf.getNumFiles();
      dpCtx = conf.getDynPartCtx();
      lbCtx = conf.getLbCtx();
      fsp = prevFsp = null;
      valToPaths = new HashMap<String, FSPaths>();
      taskId = Utilities.getTaskId(hconf);
      initializeSpecPath();
      fs = specPath.getFileSystem(hconf);

      if (hconf instanceof JobConf) {
        jc = (JobConf) hconf;
      } else {
        // test code path
        jc = new JobConf(hconf);
      }

      try {
        createHiveOutputFormat(jc);
      } catch (HiveException ex) {
        logOutputFormatError(hconf, ex);
        throw ex;
      }
      isCompressed = conf.getCompressed();
      parent = Utilities.toTempPath(conf.getDirName());
      statsFromRecordWriter = new boolean[numFiles];
      serializer = (Serializer) conf.getTableInfo().getDeserializerClass().newInstance();
      serializer.initialize(unsetNestedColumnPaths(hconf), conf.getTableInfo().getProperties());
      outputClass = serializer.getSerializedClass();
      destTablePath = conf.getDestPath();
      isInsertOverwrite = conf.getInsertOverwrite();
      counterGroup = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVECOUNTERGROUP);
      if (LOG.isInfoEnabled()) {
        LOG.info("Using serializer : " + serializer + " and formatter : " + hiveOutputFormat +
            (isCompressed ? " with compression" : ""));
      }

      // Timeout is chosen to make sure that even if one iteration takes more than
      // half of the script.timeout but less than script.timeout, we will still
      // be able to report progress.
      timeOut = hconf.getInt("mapred.healthChecker.script.timeout", 600000) / 2;

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

      if (dpCtx != null) {
        dpSetup();
      }

      if (lbCtx != null) {
        lbSetup();
      }

      if (!bDynParts) {
        fsp = new FSPaths(specPath, conf.isMmTable());
        fsp.subdirAfterTxn = combinePathFragments(generateListBucketingDirName(null), unionPath);
        if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
          Utilities.FILE_OP_LOGGER.trace("creating new paths " + System.identityHashCode(fsp)
            + " from ctor; childSpec " + unionPath + ": tmpPath " + fsp.buildTmpPath()
            + ", task path " + fsp.buildTaskOutputTempPath());
        }

        // Create all the files - this is required because empty files need to be created for
        // empty buckets
        // createBucketFiles(fsp);
        if (!this.isSkewedStoredAsSubDirectories) {
          valToPaths.put("", fsp); // special entry for non-DP case
        }
      }

      final StoragePolicyValue tmpStorage = StoragePolicyValue.lookup(HiveConf
                                            .getVar(hconf, HIVE_TEMPORARY_TABLE_STORAGE));
      if (isTemporary && fsp != null
          && tmpStorage != StoragePolicyValue.DEFAULT) {
        assert !conf.isMmTable(); // Not supported for temp tables.
        final Path outputPath = fsp.buildTaskOutputTempPath();
        StoragePolicyShim shim = ShimLoader.getHadoopShims()
            .getStoragePolicyShim(fs);
        if (shim != null) {
          // directory creation is otherwise within the writers
          fs.mkdirs(outputPath);
          shim.setStoragePolicy(outputPath, tmpStorage);
        }
      }

      if (conf.getWriteType() == AcidUtils.Operation.UPDATE ||
          conf.getWriteType() == AcidUtils.Operation.DELETE) {
        // ROW__ID is always in the first field
        recIdField = ((StructObjectInspector)outputObjInspector).getAllStructFieldRefs().get(0);
        recIdInspector = (StructObjectInspector)recIdField.getFieldObjectInspector();
        // bucket is the second field in the record id
        bucketField = recIdInspector.getAllStructFieldRefs().get(1);
        bucketInspector = (IntObjectInspector)bucketField.getFieldObjectInspector();
      }

      numRows = 0;
      cntr = 1;
      logEveryNRows = HiveConf.getLongVar(hconf, HiveConf.ConfVars.HIVE_LOG_N_RECORDS);

      statsMap.put(getCounterName(Counter.RECORDS_OUT), row_count);

      // Setup hashcode
      hashFunc = conf.getTableInfo().getBucketingVersion() == 2 ?
          ObjectInspectorUtils::getBucketHashCode :
          ObjectInspectorUtils::getBucketHashCodeOld;
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  public String getCounterName(Counter counter) {
    String suffix = Integer.toString(conf.getDestTableId());
    String fullName = conf.getTableInfo().getTableName();
    if (fullName != null) {
      suffix = suffix + "_" + fullName.toLowerCase();
    }
    return counter + "_" + suffix;
  }

  private void logOutputFormatError(Configuration hconf, HiveException ex) {
    StringBuilder errorWriter = new StringBuilder();
    errorWriter.append("Failed to create output format; configuration: ");
    // redact sensitive information before logging
    HiveConfUtil.dumpConfig(hconf, errorWriter);
    Properties tdp = null;
    if (this.conf.getTableInfo() != null
        && (tdp = this.conf.getTableInfo().getProperties()) != null) {
      errorWriter.append(";\n table properties: { ");
      for (Map.Entry<Object, Object> e : tdp.entrySet()) {
        errorWriter.append(e.getKey() + ": " + e.getValue() + ", ");
      }
      errorWriter.append('}');
    }
    LOG.error(errorWriter.toString(), ex);
  }

  /**
   * Initialize list bucketing information
   */
  private void lbSetup() {
    this.isSkewedStoredAsSubDirectories =  ((lbCtx == null) ? false : lbCtx.isSkewedStoredAsDir());
  }

  /**
   * Set up for dynamic partitioning including a new ObjectInspector for the output row.
   */
  private void dpSetup() {

    this.bDynParts = false;
    this.numDynParts = dpCtx.getNumDPCols();
    this.dpColNames = dpCtx.getDPColNames();
    this.maxPartitions = dpCtx.getMaxPartitionsPerNode();

    assert numDynParts == dpColNames.size()
        : "number of dynamic partitions should be the same as the size of DP mapping";

    if (dpColNames != null && dpColNames.size() > 0) {
      this.bDynParts = true;
      assert inputObjInspectors.length == 1 : "FileSinkOperator should have 1 parent, but it has "
          + inputObjInspectors.length;
      StructObjectInspector soi = (StructObjectInspector) inputObjInspectors[0];
      this.dpStartCol = Utilities.getDPColOffset(conf);
      this.subSetOI = new SubStructObjectInspector(soi, 0, this.dpStartCol);
      this.dpVals = new ArrayList<String>(numDynParts);
      this.dpWritables = new ArrayList<Object>(numDynParts);
    }
  }

  protected void createBucketFiles(FSPaths fsp) throws HiveException {
    try {
      int filesIdx = 0;
      Set<Integer> seenBuckets = new HashSet<Integer>();
      for (int idx = 0; idx < totalFiles; idx++) {
        if (this.getExecContext() != null && this.getExecContext().getFileId() != null) {
          if (LOG.isInfoEnabled()) {
            LOG.info("replace taskId from execContext ");
          }

          taskId = Utilities.replaceTaskIdFromFilename(taskId, this.getExecContext().getFileId());

          if (LOG.isInfoEnabled()) {
            LOG.info("new taskId: FS " + taskId);
          }

          assert !multiFileSpray;
          assert totalFiles == 1;
        }

        int bucketNum = 0;
        if (multiFileSpray) {
          key.setHashCode(idx);

          // Does this hashcode belong to this reducer
          int numReducers = totalFiles / numFiles;

          if (numReducers > 1) {
            int currReducer = Integer.parseInt(Utilities.getTaskIdFromFilename(Utilities
                .getTaskId(hconf)));

            int reducerIdx = prtner.getPartition(key, null, numReducers);
            if (currReducer != reducerIdx) {
              continue;
            }
          }

          bucketNum = prtner.getBucket(key, null, totalFiles);
          if (seenBuckets.contains(bucketNum)) {
            continue;
          }
          seenBuckets.add(bucketNum);

          bucketMap.put(bucketNum, filesIdx);
          taskId = Utilities.replaceTaskIdFromFilename(Utilities.getTaskId(hconf), bucketNum);
        }
        createBucketForFileIdx(fsp, filesIdx);
        filesIdx++;
      }
      assert filesIdx == numFiles;

      // in recent hadoop versions, use deleteOnExit to clean tmp files.
      if (isNativeTable() && fs != null && fsp != null && !conf.isMmTable()) {
        autoDelete = fs.deleteOnExit(fsp.outPaths[0]);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }

    filesCreated = true;
  }

  protected void createBucketForFileIdx(FSPaths fsp, int filesIdx)
      throws HiveException {
    try {
      fsp.initializeBucketPaths(filesIdx, taskId, isNativeTable(), isSkewedStoredAsSubDirectories);
      if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
        Utilities.FILE_OP_LOGGER.trace("createBucketForFileIdx " + filesIdx + ": final path " + fsp.finalPaths[filesIdx]
          + "; out path " + fsp.outPaths[filesIdx] +" (spec path " + specPath + ", tmp path "
          + fsp.buildTmpPath() + ", task " + taskId + ")");
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("New Final Path: FS " + fsp.finalPaths[filesIdx]);
      }

      if (isNativeTable() && !conf.isMmTable()) {
        // in recent hadoop versions, use deleteOnExit to clean tmp files.
        autoDelete = fs.deleteOnExit(fsp.outPaths[filesIdx]);
      }

      updateDPCounters(fsp, filesIdx);

      Utilities.copyTableJobPropertiesToConf(conf.getTableInfo(), jc);
      // only create bucket files only if no dynamic partitions,
      // buckets of dynamic partitions will be created for each newly created partition
      //todo IOW integration. Full Acid uses the else if block to create Acid's RecordUpdater (HiveFileFormatUtils)
      // and that will set writingBase(conf.getInsertOverwrite())
      // If MM wants to create a new base for IOW (instead of delta dir), it should specify it here
      if (conf.getWriteType() == AcidUtils.Operation.NOT_ACID || conf.isMmTable()) {
        Path outPath = fsp.outPaths[filesIdx];
        if (conf.isMmTable()
            && !FileUtils.mkdir(fs, outPath.getParent(), hconf)) {
          LOG.warn("Unable to create directory with inheritPerms: " + outPath);
        }
        fsp.outWriters[filesIdx] = HiveFileFormatUtils.getHiveRecordWriter(jc, conf.getTableInfo(),
            outputClass, conf, outPath, reporter);
        // If the record writer provides stats, get it from there instead of the serde
        statsFromRecordWriter[filesIdx] = fsp.outWriters[filesIdx] instanceof
            StatsProvidingRecordWriter;
        // increment the CREATED_FILES counter
      } else if (conf.getWriteType() == AcidUtils.Operation.INSERT) {
        // Only set up the updater for insert.  For update and delete we don't know unitl we see
        // the row.
        ObjectInspector inspector = bDynParts ? subSetOI : outputObjInspector;
        int acidBucketNum = Integer.parseInt(Utilities.getTaskIdFromFilename(taskId));
        fsp.updaters[filesIdx] = HiveFileFormatUtils.getAcidRecordUpdater(jc, conf.getTableInfo(),
            acidBucketNum, conf, fsp.outPaths[filesIdx], inspector, reporter, -1);
      }

      if (reporter != null) {
        reporter.incrCounter(counterGroup, Operator.HIVE_COUNTER_CREATED_FILES, 1);
      }

    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  private void updateDPCounters(final FSPaths fsp, final int filesIdx) {
    // There are 2 cases where we increment CREATED_DYNAMIC_PARTITIONS counters
    // 1) Insert overwrite (all partitions are newly created)
    // 2) Insert into table which creates new partitions (some new partitions)

    if (bDynParts && destTablePath != null && fsp.dpDirForCounters != null) {
      Path destPartPath = new Path(destTablePath, fsp.dpDirForCounters);
      // For MM tables, directory structure is
      // <table-dir>/<partition-dir>/<delta-dir>/

      // For Non-MM tables, directory structure is
      // <table-dir>/<staging-dir>/<partition-dir>

      // if UNION ALL insert, for non-mm tables subquery creates another subdirectory at the end for each union queries
      // <table-dir>/<staging-dir>/<partition-dir>/<union-dir>

      // for non-MM tables, the final destination partition directory is created during move task via rename
      // for MM tables, the final destination partition directory is created by the tasks themselves
      try {
        if (conf.isMmTable()) {
          createDpDir(destPartPath);
        } else {
          // outPath will be
          // non-union case: <table-dir>/<staging-dir>/<partition-dir>/<taskid>
          // union case: <table-dir>/<staging-dir>/<partition-dir>/<union-dir>/<taskid>
          Path dpStagingDir = fsp.outPaths[filesIdx].getParent();
          if (isUnionDp) {
            dpStagingDir = dpStagingDir.getParent();
          }
          if (isInsertOverwrite) {
            createDpDir(dpStagingDir);
          } else {
            createDpDirCheckSrc(dpStagingDir, destPartPath);
          }
        }
      } catch (IOException e) {
        LOG.warn("Skipping to increment CREATED_DYNAMIC_PARTITIONS counter.Exception: {}", e.getMessage());
      }
    }
  }

  private void createDpDirCheckSrc(final Path dpStagingPath, final Path dpFinalPath) throws IOException {
    if (!fs.exists(dpStagingPath) && !fs.exists(dpFinalPath)) {
      fs.mkdirs(dpStagingPath);
      // move task will create dp final path
      if (reporter != null) {
        reporter.incrCounter(counterGroup, Operator.HIVE_COUNTER_CREATED_DYNAMIC_PARTITIONS, 1);
      }
    }
  }

  private void createDpDir(final Path dpPath) throws IOException {
    if (!fs.exists(dpPath)) {
      fs.mkdirs(dpPath);
      if (reporter != null) {
        reporter.incrCounter(counterGroup, Operator.HIVE_COUNTER_CREATED_DYNAMIC_PARTITIONS, 1);
      }
    }
  }

  /**
   * Report status to JT so that JT won't kill this task if closing takes too long
   * due to too many files to close and the NN is overloaded.
   *
   * @return true if a new progress update is reported, false otherwise.
   */
  protected boolean updateProgress() {
    if (reporter != null &&
        (System.currentTimeMillis() - lastProgressReport) > timeOut) {
      reporter.progress();
      lastProgressReport = System.currentTimeMillis();
      return true;
    } else {
      return false;
    }
  }

  protected Writable recordValue;


  @Override
  public void process(Object row, int tag) throws HiveException {
    runTimeNumRows++;
    /* Create list bucketing sub-directory only if stored-as-directories is on. */
    String lbDirName = null;
    lbDirName = (lbCtx == null) ? null : generateListBucketingDirName(row);

    if (!bDynParts && !filesCreated) {
      if (lbDirName != null) {
        if (valToPaths.get(lbDirName) == null) {
          createNewPaths(null, lbDirName);
        }
      } else {
        createBucketFiles(fsp);
      }
    }

    try {
      updateProgress();

      // if DP is enabled, get the final output writers and prepare the real output row
      assert inputObjInspectors[0].getCategory() == ObjectInspector.Category.STRUCT
          : "input object inspector is not struct";

      if (bDynParts) {

        // we need to read bucket number which is the last column in value (after partition columns)
        if (conf.getDpSortState().equals(DPSortState.PARTITION_BUCKET_SORTED)) {
          numDynParts += 1;
        }

        // copy the DP column values from the input row to dpVals
        dpVals.clear();
        dpWritables.clear();
        ObjectInspectorUtils.partialCopyToStandardObject(dpWritables, row, dpStartCol,numDynParts,
            (StructObjectInspector) inputObjInspectors[0],ObjectInspectorCopyOption.WRITABLE);

        // get a set of RecordWriter based on the DP column values
        // pass the null value along to the escaping process to determine what the dir should be
        for (Object o : dpWritables) {
          if (o == null || o.toString().length() == 0) {
            dpVals.add(dpCtx.getDefaultPartitionName());
          } else {
            dpVals.add(o.toString());
          }
        }

        String invalidPartitionVal;
        if((invalidPartitionVal = HiveStringUtils.getPartitionValWithInvalidCharacter(dpVals, dpCtx.getWhiteListPattern()))!=null) {
          throw new HiveFatalException("Partition value '" + invalidPartitionVal +
              "' contains a character not matched by whitelist pattern '" +
              dpCtx.getWhiteListPattern().toString() + "'.  " + "(configure with " +
              HiveConf.ConfVars.METASTORE_PARTITION_NAME_WHITELIST_PATTERN.varname + ")");
        }
        fpaths = getDynOutPaths(dpVals, lbDirName);

        // use SubStructObjectInspector to serialize the non-partitioning columns in the input row
        recordValue = serializer.serialize(row, subSetOI);
      } else {
        if (lbDirName != null) {
          fpaths = valToPaths.get(lbDirName);
          if (fpaths == null) {
            fpaths = createNewPaths(null, lbDirName);
          }
        } else {
          fpaths = fsp;
        }
        recordValue = serializer.serialize(row, inputObjInspectors[0]);
        // if serializer is ThriftJDBCBinarySerDe, then recordValue is null if the buffer is not full (the size of buffer
        // is kept track of in the SerDe)
        if (recordValue == null) {
          return;
        }
      }

      rowOutWriters = fpaths.outWriters;
      // check if all record writers implement statistics. if atleast one RW
      // doesn't implement stats interface we will fallback to conventional way
      // of gathering stats
      isCollectRWStats = areAllTrue(statsFromRecordWriter);
      if (conf.isGatherStats() && !isCollectRWStats) {
        SerDeStats stats = serializer.getSerDeStats();
        if (stats != null) {
          fpaths.addToStat(StatsSetupConst.RAW_DATA_SIZE, stats.getRawDataSize());
        }
        fpaths.addToStat(StatsSetupConst.ROW_COUNT, 1);
      }

      if ((++numRows == cntr) && LOG.isInfoEnabled()) {
        cntr = logEveryNRows == 0 ? cntr * 10 : numRows + logEveryNRows;
        if (cntr < 0 || numRows < 0) {
          cntr = 0;
          numRows = 1;
        }
        LOG.info(toString() + ": records written - " + numRows);
      }

      int writerOffset;
      // This if/else chain looks ugly in the inner loop, but given that it will be 100% the same
      // for a given operator branch prediction should work quite nicely on it.
      // RecordUpdateer expects to get the actual row, not a serialized version of it.  Thus we
      // pass the row rather than recordValue.
      if (conf.getWriteType() == AcidUtils.Operation.NOT_ACID || conf.isMmTable()) {
        rowOutWriters[findWriterOffset(row)].write(recordValue);
      } else if (conf.getWriteType() == AcidUtils.Operation.INSERT) {
        fpaths.updaters[findWriterOffset(row)].insert(conf.getTableWriteId(), row);
      } else {
        // TODO I suspect we could skip much of the stuff above this in the function in the case
        // of update and delete.  But I don't understand all of the side effects of the above
        // code and don't want to skip over it yet.

        // Find the bucket id, and switch buckets if need to
        ObjectInspector rowInspector = bDynParts ? subSetOI : outputObjInspector;
        Object recId = ((StructObjectInspector)rowInspector).getStructFieldData(row, recIdField);
        int bucketProperty =
            bucketInspector.get(recIdInspector.getStructFieldData(recId, bucketField));
        int bucketNum =
          BucketCodec.determineVersion(bucketProperty).decodeWriterId(bucketProperty);
        writerOffset = 0;
        if (multiFileSpray) {
          //bucket_num_reducers_acid.q, TestTxnCommands.testMoreBucketsThanReducers()
          if (!bucketMap.containsKey(bucketNum)) {
            String extraMsg = "  (no path info/)" + recId;
            if (fpaths != null && fpaths.finalPaths != null && fpaths.finalPaths.length > 0) {
              extraMsg = "  (finalPaths[0]=" + fpaths.finalPaths[0] + ")/" + recId;
            }
            throw new IllegalStateException("Found bucketNum=" + bucketNum +
              " from data but no mapping in 'bucketMap'." + extraMsg);
          }
          writerOffset = bucketMap.get(bucketNum);
        }
        if (fpaths.updaters[writerOffset] == null) {
          /*data for delete commands always have ROW__ID which implies that the bucket ID
          * for each row is known.  RecordUpdater creates bucket_N file based on 'bucketNum' thus
          * delete events always land in the proper bucket_N file.  This could even handle
          * cases where multiple writers are writing bucket_N file for the same N in which case
          * Hive.copyFiles() will make one of them bucket_N_copy_M in the final location.  The
          * reset of acid (read path) doesn't know how to handle copy_N files except for 'original'
          * files (HIVE-16177)*/
          int writerId = -1;
          if(!isBucketed) {
            assert !multiFileSpray;
            assert writerOffset == 0;
            /**For un-bucketed tables, Deletes with ROW__IDs with different 'bucketNum' values can
            * be written to the same bucketN file.
            * N in this case is writerId and there is no relationship
            * between the file name and any property of the data in it.  Inserts will be written
            * to bucketN file such that all {@link RecordIdentifier#getBucketProperty()} indeed
            * contain writerId=N.
            * Since taskId is unique (at least per statementId and thus
            * per [delete_]delta_x_y_stmtId/) there will not be any copy_N files.*/
            writerId = Integer.parseInt(Utilities.getTaskIdFromFilename(taskId));
          }
          fpaths.updaters[writerOffset] = HiveFileFormatUtils.getAcidRecordUpdater(
            jc, conf.getTableInfo(), writerId >= 0 ? writerId : bucketNum, conf,
            fpaths.outPaths[writerOffset], rowInspector, reporter, 0);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Created updater for bucket number " + bucketNum + " using file " +
              fpaths.outPaths[writerOffset]);
          }
        }
        if (conf.getWriteType() == AcidUtils.Operation.UPDATE) {
          fpaths.updaters[writerOffset].update(conf.getTableWriteId(), row);
        } else if (conf.getWriteType() == AcidUtils.Operation.DELETE) {
          fpaths.updaters[writerOffset].delete(conf.getTableWriteId(), row);
        } else {
          throw new HiveException("Unknown write type " + conf.getWriteType().toString());
        }
      }
    } catch (IOException e) {
      throw new HiveException(e);
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
  }

  protected boolean areAllTrue(boolean[] statsFromRW) {
    // If we are doing an acid operation they will always all be true as RecordUpdaters always
    // collect stats
    if (conf.getWriteType() != AcidUtils.Operation.NOT_ACID && !conf.isMmTable()) {
      return true;
    }
    for(boolean b : statsFromRW) {
      if (!b) {
        return false;
      }
    }
    return true;
  }

  private int findWriterOffset(Object row) throws HiveException {
    if (!multiFileSpray) {
      return 0;
    } else {
      assert getConf().getWriteType() != AcidUtils.Operation.DELETE &&
        getConf().getWriteType() != AcidUtils.Operation.UPDATE :
        "Unexpected operation type: " + getConf().getWriteType();
      //this is not used for DELETE commands (partitionEval is not set up correctly
      // (or needed) for that
      Object[] bucketFieldValues = new Object[partitionEval.length];
      for(int i = 0; i < partitionEval.length; i++) {
        bucketFieldValues[i] = partitionEval[i].evaluate(row);
      }
      int keyHashCode = hashFunc.apply(bucketFieldValues, partitionObjectInspectors);
      key.setHashCode(keyHashCode);
      int bucketNum = prtner.getBucket(key, null, totalFiles);
      return bucketMap.get(bucketNum);
    }
  }

  /**
   * create new path.
   *
   * @param dirName
   * @return
   * @throws HiveException
   */
  private FSPaths createNewPaths(String dpDir, String lbDir) throws HiveException {
    FSPaths fsp2 = new FSPaths(specPath, conf.isMmTable());
    fsp2.subdirAfterTxn = combinePathFragments(lbDir, unionPath);
    fsp2.subdirBeforeTxn = dpDir;
    String pathKey = combinePathFragments(dpDir, lbDir);
    if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
      Utilities.FILE_OP_LOGGER.trace("creating new paths {} for {}, childSpec {}: tmpPath {},"
          + " task path {}", System.identityHashCode(fsp2), pathKey, unionPath,
          fsp2.buildTmpPath(), fsp2.buildTaskOutputTempPath());
    }

    if (bDynParts) {
      fsp2.dpDirForCounters = pathKey;
    }
    if(!conf.getDpSortState().equals(DPSortState.PARTITION_BUCKET_SORTED)) {
      createBucketFiles(fsp2);
      valToPaths.put(pathKey, fsp2);
    }
    return fsp2;
  }

  private static String combinePathFragments(String first, String second) {
    return first == null ? second : (second == null ? first : first + Path.SEPARATOR + second);
  }

  /**
   * Generate list bucketing directory name from a row.
   * @param row row to process.
   * @return directory name.
   */
  protected String generateListBucketingDirName(Object row) {
    if (!this.isSkewedStoredAsSubDirectories) {
      return null;
    }

    String lbDirName = null;
    List<String> skewedCols = lbCtx.getSkewedColNames();
    List<List<String>> allSkewedVals = lbCtx.getSkewedColValues();
    Map<List<String>, String> locationMap = lbCtx.getLbLocationMap();

    if (row != null) {
      List<Object> standObjs = new ArrayList<Object>();
      List<String> skewedValsCandidate = null;
      /* Convert input row to standard objects. */
      ObjectInspectorUtils.copyToStandardObject(standObjs, row,
          (StructObjectInspector) inputObjInspectors[0], ObjectInspectorCopyOption.WRITABLE);

      assert (standObjs.size() >= skewedCols.size()) :
        "The row has less number of columns than no. of skewed column.";

      skewedValsCandidate = new ArrayList<String>(skewedCols.size());
      for (SkewedColumnPositionPair posPair : lbCtx.getRowSkewedIndex()) {
        skewedValsCandidate.add(posPair.getSkewColPosition(),
            standObjs.get(posPair.getTblColPosition()).toString());
      }
      /* The row matches skewed column names. */
      if (allSkewedVals.contains(skewedValsCandidate)) {
        /* matches skewed values. */
        lbDirName = FileUtils.makeListBucketingDirName(skewedCols, skewedValsCandidate);
        locationMap.put(skewedValsCandidate, lbDirName);
      } else {
        lbDirName = createDefaultLbDir(skewedCols, locationMap);
      }
    } else {
      lbDirName = createDefaultLbDir(skewedCols, locationMap);
    }
    return lbDirName;
  }

  private String createDefaultLbDir(List<String> skewedCols,
      Map<List<String>, String> locationMap) {
    String lbDirName;
    lbDirName = FileUtils.makeDefaultListBucketingDirName(skewedCols,
          lbCtx.getDefaultDirName());
    List<String> defaultKey = Lists.newArrayList(lbCtx.getDefaultKey());
    if (!locationMap.containsKey(defaultKey)) {
      locationMap.put(defaultKey, lbDirName);
    }
    return lbDirName;
  }

  protected FSPaths getDynOutPaths(List<String> row, String lbDir) throws HiveException {

    FSPaths fp;

    // get the path corresponding to the dynamic partition columns,
    String dpDir = getDynPartDirectory(row, dpColNames);

    String pathKey = null;
    if (dpDir != null) {
      String dpAndLbDir = combinePathFragments(dpDir, lbDir);
      pathKey = dpAndLbDir;
      if (conf.getDpSortState().equals(DPSortState.PARTITION_BUCKET_SORTED)) {
        String buckNum = row.get(row.size() - 1);
        taskId = Utilities.replaceTaskIdFromFilename(taskId, buckNum);
        pathKey = dpAndLbDir + Path.SEPARATOR + taskId;
      }
      FSPaths fsp2 = valToPaths.get(pathKey);

      if (fsp2 == null) {
        // check # of dp
        // TODO: add an option to skip this if number of partitions checks is done by Triggers via
        // CREATED_DYNAMIC_PARTITION counter
        if (valToPaths.size() > maxPartitions) {
          // we cannot proceed and need to tell the hive client that retries won't succeed either
          throw new HiveFatalException(
               ErrorMsg.DYNAMIC_PARTITIONS_TOO_MANY_PER_NODE_ERROR.getErrorCodedMsg()
               + "Maximum was set to " + maxPartitions + " partitions per node"
               + ", number of dynamic partitions on this node: " + valToPaths.size());
        }

        if (!conf.getDpSortState().equals(DPSortState.NONE) && prevFsp != null) {
          // close the previous fsp as it is no longer needed
          prevFsp.closeWriters(false);

          // since we are closing the previous fsp's record writers, we need to see if we can get
          // stats from the record writer and store in the previous fsp that is cached
          if (conf.isGatherStats() && isCollectRWStats) {
            SerDeStats stats = null;
            if (conf.getWriteType() == AcidUtils.Operation.NOT_ACID || conf.isMmTable()) {
              RecordWriter outWriter = prevFsp.outWriters[0];
              if (outWriter != null) {
                stats = ((StatsProvidingRecordWriter) outWriter).getStats();
              }
            } else if (prevFsp.updaters[0] != null) {
              stats = prevFsp.updaters[0].getStats();
            }
            if (stats != null) {
                prevFsp.addToStat(StatsSetupConst.RAW_DATA_SIZE, stats.getRawDataSize());
                prevFsp.addToStat(StatsSetupConst.ROW_COUNT, stats.getRowCount());
            }
          }

          // let writers release the memory for garbage collection
          prevFsp.outWriters[0] = null;

          prevFsp = null;
        }

        fsp2 = createNewPaths(dpDir, lbDir);
        if (prevFsp == null) {
          prevFsp = fsp2;
        }

        if(conf.getDpSortState().equals(DPSortState.PARTITION_BUCKET_SORTED)) {
          createBucketForFileIdx(fsp2, 0);
          valToPaths.put(pathKey, fsp2);
        }

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
  private String getDynPartDirectory(List<String> row, List<String> dpColNames) {
    return FileUtils.makePartName(dpColNames, row);
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {

    row_count.set(numRows);
    LOG.info(toString() + ": records written - " + numRows);

    if (!bDynParts && !filesCreated) {
      boolean skipFiles = "tez".equalsIgnoreCase(
          HiveConf.getVar(hconf, ConfVars.HIVE_EXECUTION_ENGINE));
      if (skipFiles) {
        Class<?> clazz = conf.getTableInfo().getOutputFileFormatClass();
        skipFiles = !StreamingOutputFormat.class.isAssignableFrom(clazz);
      }
      if (!skipFiles) {
        createBucketFiles(fsp);
      }
    }

    lastProgressReport = System.currentTimeMillis();
    if (!abort) {
      // If serializer is ThriftJDBCBinarySerDe, then it buffers rows to a certain limit (hive.server2.thrift.resultset.max.fetch.size)
      // and serializes the whole batch when the buffer is full. The serialize returns null if the buffer is not full
      // (the size of buffer is kept track of in the ThriftJDBCBinarySerDe).
      if (conf.isUsingThriftJDBCBinarySerDe()) {
          try {
            recordValue = serializer.serialize(null, inputObjInspectors[0]);
            if ( null != fpaths ) {
              rowOutWriters = fpaths.outWriters;
              rowOutWriters[0].write(recordValue);
            }
          } catch (SerDeException | IOException e) {
            throw new HiveException(e);
          }
      }
      List<Path> commitPaths = new ArrayList<>();
      for (FSPaths fsp : valToPaths.values()) {
        fsp.closeWriters(abort);
        // before closing the operator check if statistics gathering is requested
        // and is provided by record writer. this is different from the statistics
        // gathering done in processOp(). In processOp(), for each row added
        // serde statistics about the row is gathered and accumulated in hashmap.
        // this adds more overhead to the actual processing of row. But if the
        // record writer already gathers the statistics, it can simply return the
        // accumulated statistics which will be aggregated in case of spray writers
        if (conf.isGatherStats() && isCollectRWStats) {
          if (conf.getWriteType() == AcidUtils.Operation.NOT_ACID || conf.isMmTable()) {
            for (int idx = 0; idx < fsp.outWriters.length; idx++) {
              RecordWriter outWriter = fsp.outWriters[idx];
              if (outWriter != null) {
                SerDeStats stats = ((StatsProvidingRecordWriter) outWriter).getStats();
                if (stats != null) {
                  fsp.addToStat(StatsSetupConst.RAW_DATA_SIZE, stats.getRawDataSize());
                  fsp.addToStat(StatsSetupConst.ROW_COUNT, stats.getRowCount());
                }
              }
            }
          } else {
            for (int i = 0; i < fsp.updaters.length; i++) {
              if (fsp.updaters[i] != null) {
                SerDeStats stats = fsp.updaters[i].getStats();
                if (stats != null) {
                  fsp.addToStat(StatsSetupConst.RAW_DATA_SIZE, stats.getRawDataSize());
                  fsp.addToStat(StatsSetupConst.ROW_COUNT, stats.getRowCount());
                }
              }
            }
          }
        }

        if (isNativeTable()) {
          fsp.commit(fs, commitPaths);
        }
      }
      if (conf.isMmTable()) {
        Utilities.writeMmCommitManifest(commitPaths, specPath, fs, taskId,
                conf.getTableWriteId(), conf.getStatementId(), unionPath, conf.getInsertOverwrite());
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
        fsp.abortWriters(fs, abort, !autoDelete && isNativeTable() && !conf.isMmTable());
      }
    }
    fsp = prevFsp = null;
    super.closeOp(abort);
  }


  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "FS";
  }

  @Override
  public void jobCloseOp(Configuration hconf, boolean success)
      throws HiveException {
    try {
      if ((conf != null) && isNativeTable()) {
        Path specPath = conf.getDirName();
        String unionSuffix = null;
        DynamicPartitionCtx dpCtx = conf.getDynPartCtx();
        ListBucketingCtx lbCtx = conf.getLbCtx();
        if (conf.isLinkedFileSink() && (dpCtx != null || conf.isMmTable())) {
          specPath = conf.getParentDir();
          unionSuffix = conf.getDirName().getName();
        }
        if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
          Utilities.FILE_OP_LOGGER.trace("jobCloseOp using specPath " + specPath);
        }
        if (!conf.isMmTable()) {
          Utilities.mvFileToFinalPath(specPath, hconf, success, LOG, dpCtx, conf, reporter);
        } else {
          int dpLevels = dpCtx == null ? 0 : dpCtx.getNumDPCols(),
              lbLevels = lbCtx == null ? 0 : lbCtx.calculateListBucketingLevel();
          // TODO: why is it stored in both table and dpCtx?
          int numBuckets = (conf.getTable() != null) ? conf.getTable().getNumBuckets()
              : (dpCtx != null ? dpCtx.getNumBuckets() : 0);
          MissingBucketsContext mbc = new MissingBucketsContext(
              conf.getTableInfo(), numBuckets, conf.getCompressed());
          Utilities.handleMmTableFinalPath(specPath, unionSuffix, hconf, success,
              dpLevels, lbLevels, mbc, conf.getTableWriteId(), conf.getStatementId(), reporter,
              conf.isMmTable(), conf.isMmCtas(), conf.getInsertOverwrite());
        }
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
    super.jobCloseOp(hconf, success);
  }

  @Override
  public OperatorType getType() {
    return OperatorType.FILESINK;
  }

  @Override
  public void augmentPlan() {
    PlanUtils.configureOutputJobPropertiesForStorageHandler(
        getConf().getTableInfo());
  }

  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    if (hiveOutputFormat == null) {
      try {
        createHiveOutputFormat(job);
      } catch (HiveException ex) {
        logOutputFormatError(job, ex);
        throw new IOException(ex);
      }
    }
    if (conf.getTableInfo().isNonNative()) {
      //check the ouput specs only if it is a storage handler (native tables's outputformats does
      //not set the job's output properties correctly)
      try {
        hiveOutputFormat.checkOutputSpecs(ignored, job);
      } catch (NoSuchMethodError e) {
        //For BC, ignore this for now, but leave a log message
        LOG.warn("HiveOutputFormat should implement checkOutputSpecs() method`");
      }
    }
  }

  private void createHiveOutputFormat(JobConf job) throws HiveException {
    if (hiveOutputFormat == null) {
      Utilities.copyTableJobPropertiesToConf(conf.getTableInfo(), job);
    }
    try {
      hiveOutputFormat = HiveFileFormatUtils.getHiveOutputFormat(job, getConf().getTableInfo());
    } catch (Throwable t) {
      throw (t instanceof HiveException) ? (HiveException)t : new HiveException(t);
    }
  }

  private void publishStats() throws HiveException {
    boolean isStatsReliable = conf.isStatsReliable();

    // Initializing a stats publisher
    StatsPublisher statsPublisher = Utilities.getStatsPublisher(jc);

    if (statsPublisher == null) {
      // just return, stats gathering should not block the main query
      LOG.error("StatsPublishing error: StatsPublisher is not initialized.");
      if (isStatsReliable) {
        throw new HiveException(ErrorMsg.STATSPUBLISHER_NOT_OBTAINED.getErrorCodedMsg());
      }
      return;
    }

    StatsCollectionContext sContext = new StatsCollectionContext(hconf);
    sContext.setStatsTmpDir(conf.getTmpStatsDir());
    if (!statsPublisher.connect(sContext)) {
      // just return, stats gathering should not block the main query
      LOG.error("StatsPublishing error: cannot connect to database");
      if (isStatsReliable) {
        throw new HiveException(ErrorMsg.STATSPUBLISHER_CONNECTION_ERROR.getErrorCodedMsg());
      }
      return;
    }

    String spSpec = conf.getStaticSpec();

    for (Map.Entry<String, FSPaths> entry : valToPaths.entrySet()) {
      String fspKey = entry.getKey();     // DP/LB
      FSPaths fspValue = entry.getValue();
      if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
        Utilities.FILE_OP_LOGGER.trace("Observing entry for stats " + fspKey
          + " => FSP with tmpPath " + fspValue.buildTmpPath());
      }
      // for bucketed tables, hive.optimize.sort.dynamic.partition optimization
      // adds the taskId to the fspKey.
      if (conf.getDpSortState().equals(DPSortState.PARTITION_BUCKET_SORTED)) {
        String taskID = Utilities.getTaskIdFromFilename(fspKey);
        // if length of (prefix/ds=__HIVE_DEFAULT_PARTITION__/000000_0) is greater than max key prefix
        // and if (prefix/ds=10/000000_0) is less than max key prefix, then former will get hashed
        // to a smaller prefix (MD5hash/000000_0) and later will stored as such in staging stats table.
        // When stats gets aggregated in StatsTask only the keys that starts with "prefix" will be fetched.
        // Now that (prefix/ds=__HIVE_DEFAULT_PARTITION__) is hashed to a smaller prefix it will
        // not be retrieved from staging table and hence not aggregated. To avoid this issue
        // we will remove the taskId from the key which is redundant anyway.
        fspKey = fspKey.split(taskID)[0];
      }

      // split[0] = DP, split[1] = LB
      String[] split = splitKey(fspKey);
      String dpSpec = split[0];
      // key = "database.table/SP/DP/"LB/
      // Hive store lowercase table name in metastore, and Counters is character case sensitive, so we
      // use lowercase table name as prefix here, as StatsTask get table name from metastore to fetch counter.
      String prefix = conf.getTableInfo().getTableName().toLowerCase();
      prefix = Utilities.join(prefix, spSpec, dpSpec);
      prefix = prefix.endsWith(Path.SEPARATOR) ? prefix : prefix + Path.SEPARATOR;
      if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
        Utilities.FILE_OP_LOGGER.trace(
            "Prefix for stats " + prefix + " (from " + spSpec + ", " + dpSpec + ")");
      }

      Map<String, String> statsToPublish = new HashMap<String, String>();
      for (String statType : fspValue.getStoredStats()) {
        statsToPublish.put(statType, Long.toString(fspValue.stat.getStat(statType)));
      }
      if (!statsPublisher.publishStat(prefix, statsToPublish)) {
        LOG.error("Failed to publish stats");
        // The original exception is lost.
        // Not changing the interface to maintain backward compatibility
        if (isStatsReliable) {
          throw new HiveException(ErrorMsg.STATSPUBLISHER_PUBLISHING_ERROR.getErrorCodedMsg());
        }
      }
    }
    sContext.setIndexForTezUnion(this.getIndexForTezUnion());
    if (!statsPublisher.closeConnection(sContext)) {
      LOG.error("Failed to close stats");
      // The original exception is lost.
      // Not changing the interface to maintain backward compatibility
      if (isStatsReliable) {
        throw new HiveException(ErrorMsg.STATSPUBLISHER_CLOSING_ERROR.getErrorCodedMsg());
      }
    }
  }

  /**
   * This is server side code to create key in order to save statistics to stats database.
   * Client side will read it via StatsTask.java aggregateStats().
   * Client side reads it via db query prefix which is based on partition spec.
   * Since store-as-subdir information is not part of partition spec, we have to
   * remove store-as-subdir information from variable "keyPrefix" calculation.
   * But we have to keep store-as-subdir information in variable "key" calculation
   * since each skewed value has a row in stats db and "key" is db key,
   * otherwise later value overwrites previous value.
   * Performance impact due to string handling is minimum since this method is
   * only called once in FileSinkOperator closeOp().
   * For example,
   * create table test skewed by (key, value) on (('484','val_484') stored as DIRECTORIES;
   * skewedValueDirList contains 2 elements:
   * 1. key=484/value=val_484
   * 2. HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME/HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME
   * Case #1: Static partition with store-as-sub-dir
   * spSpec has SP path
   * fspKey has either
   * key=484/value=val_484 or
   * HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME/HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME
   * After filter, fspKey is empty, storedAsDirPostFix has either
   * key=484/value=val_484 or
   * HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME/HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME
   * so, at the end, "keyPrefix" doesnt have subdir information but "key" has
   * Case #2: Dynamic partition with store-as-sub-dir. Assume dp part is hr
   * spSpec has SP path
   * fspKey has either
   * hr=11/key=484/value=val_484 or
   * hr=11/HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME/HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME
   * After filter, fspKey is hr=11, storedAsDirPostFix has either
   * key=484/value=val_484 or
   * HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME/HIVE_LIST_BUCKETING_DEFAULT_DIR_NAME
   * so, at the end, "keyPrefix" doesn't have subdir information from skewed but "key" has
   *
   * In a word, fspKey is consists of DP(dynamic partition spec) + LB(list bucketing spec)
   * In stats publishing, full partition spec consists of prefix part of stat key
   * but list bucketing spec is regarded as a postfix of stat key. So we split it here.
   */
  private String[] splitKey(String fspKey) {
    if (!fspKey.isEmpty() && isSkewedStoredAsSubDirectories) {
      for (String dir : lbCtx.getSkewedValuesDirNames()) {
        int index = fspKey.indexOf(dir);
        if (index >= 0) {
          return new String[] {fspKey.substring(0, index), fspKey.substring(index + 1)};
        }
      }
    }
    return new String[] {fspKey, null};
  }

  /**
   * Check if nested column paths is set for 'conf'.
   * If set, create a copy of 'conf' with this property unset.
   */
  private Configuration unsetNestedColumnPaths(Configuration conf) {
    if (conf.get(ColumnProjectionUtils.READ_NESTED_COLUMN_PATH_CONF_STR) != null) {
      Configuration confCopy = new Configuration(conf);
      confCopy.unset(ColumnProjectionUtils.READ_NESTED_COLUMN_PATH_CONF_STR);
      return confCopy;
    }
    return conf;
  }

  private boolean isNativeTable() {
    return !conf.getTableInfo().isNonNative();
  }

}
