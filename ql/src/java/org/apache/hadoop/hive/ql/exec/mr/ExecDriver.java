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

package org.apache.hadoop.hive.ql.exec.mr;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.security.AccessController;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.AddToClassPathAction;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.log.LogDivertAppenderForTest;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.CompressionUtils;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConfUtil;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.TaskQueue;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FetchOperator;
import org.apache.hadoop.hive.ql.exec.HiveTotalOrderPartitioner;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.PartitionKeySampler;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionPoolManager;
import org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.io.HiveOutputFormatImpl;
import org.apache.hadoop.hive.ql.io.IOPrepareCache;
import org.apache.hadoop.hive.ql.log.LogDivertAppender;
import org.apache.hadoop.hive.ql.log.NullAppender;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.stats.StatsCollectionContext;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;

/**
 * ExecDriver is the central class in co-ordinating execution of any map-reduce task.
 * It's main responsibilities are:
 *
 * - Converting the plan (MapredWork) into a MR Job (JobConf)
 * - Submitting a MR job to the cluster via JobClient and ExecHelper
 * - Executing MR job in local execution mode (where applicable)
 *
 */
public class ExecDriver extends Task<MapredWork> implements Serializable, HadoopJobExecHook {

  private static final long serialVersionUID = 1L;
  private static final String JOBCONF_FILENAME = "jobconf.xml";

  protected transient JobConf job;
  public static MemoryMXBean memoryMXBean;
  protected HadoopJobExecHelper jobExecHelper;
  private transient boolean isShutdown = false;
  private transient boolean jobKilled = false;

  protected static transient final Logger LOG = LoggerFactory.getLogger(ExecDriver.class);

  private RunningJob rj;

  /**
   * Constructor when invoked from QL.
   */
  public ExecDriver() {
    super();
    console = new LogHelper(LOG);
    job = new JobConf(ExecDriver.class);
    this.jobExecHelper = new HadoopJobExecHelper(job, console, this, this);
  }

  @Override
  public boolean requireLock() {
    return true;
  }

  private void initializeFiles(String prop, String files) {
    if (files != null && files.length() > 0) {
      job.set(prop, files);
    }
  }

  /**
   * Retrieve the resources from the current session and configuration for the given type.
   * @return Comma-separated list of resources
   */
  protected static String getResource(HiveConf conf, SessionState.ResourceType resType) {
    switch(resType) {
    case JAR:
      String addedJars = Utilities.getResourceFiles(conf, SessionState.ResourceType.JAR);
      String auxJars = conf.getAuxJars();
      String reloadableAuxJars = SessionState.get() == null ? null : SessionState.get().getReloadableAuxJars();
      return HiveStringUtils.joinIgnoringEmpty(new String[]{addedJars, auxJars, reloadableAuxJars}, ',');
    case FILE:
      return Utilities.getResourceFiles(conf, SessionState.ResourceType.FILE);
    case ARCHIVE:
      return Utilities.getResourceFiles(conf, SessionState.ResourceType.ARCHIVE);
    }

    return null;
  }

  /**
   * Initialization when invoked from QL.
   */
  @Override
  public void initialize(QueryState queryState, QueryPlan queryPlan, TaskQueue taskQueue, Context context) {
    super.initialize(queryState, queryPlan, taskQueue, context);

    job = new JobConf(conf, ExecDriver.class);

    initializeFiles("tmpjars", getResource(conf, SessionState.ResourceType.JAR));
    initializeFiles("tmpfiles", getResource(conf, SessionState.ResourceType.FILE));
    initializeFiles("tmparchives", getResource(conf, SessionState.ResourceType.ARCHIVE));

    conf.stripHiddenConfigurations(job);
    this.jobExecHelper = new HadoopJobExecHelper(job, console, this, this);
  }

  /**
   * Constructor/Initialization for invocation as independent utility.
   */
  public ExecDriver(MapredWork plan, JobConf job, boolean isSilent) throws HiveException {
    setWork(plan);
    this.job = job;
    console = new LogHelper(LOG, isSilent);
    this.jobExecHelper = new HadoopJobExecHelper(job, console, this, this);
  }

  /**
   * Fatal errors are those errors that cannot be recovered by retries. These are application
   * dependent. Examples of fatal errors include: - the small table in the map-side joins is too
   * large to be feasible to be handled by one mapper. The job should fail and the user should be
   * warned to use regular joins rather than map-side joins. Fatal errors are indicated by counters
   * that are set at execution time. If the counter is non-zero, a fatal error occurred. The value
   * of the counter indicates the error type.
   *
   * @return true if fatal errors happened during job execution, false otherwise.
   */
  @Override
  public boolean checkFatalErrors(Counters ctrs, StringBuilder errMsg) {
     Counters.Counter cntr = ctrs.findCounter(
        HiveConf.getVar(job, HiveConf.ConfVars.HIVE_COUNTER_GROUP),
        Operator.HIVE_COUNTER_FATAL);
    return cntr != null && cntr.getValue() > 0;
  }

   /**
   * Execute a query plan using Hadoop.
   */
  @SuppressWarnings({"deprecation", "unchecked"})
  @Override
  public int execute() {

    IOPrepareCache ioPrepareCache = IOPrepareCache.get();
    ioPrepareCache.clear();

    boolean success = true;

    boolean ctxCreated = false;
    Path emptyScratchDir;
    JobClient jc = null;

    if (taskQueue.isShutdown()) {
      LOG.warn("Task was cancelled");
      return 5;
    }

    MapWork mWork = work.getMapWork();
    ReduceWork rWork = work.getReduceWork();

    Context ctx = context;
    try {
      if (ctx == null) {
        ctx = new Context(job);
        ctxCreated = true;
      }

      emptyScratchDir = ctx.getMRTmpPath();
      FileSystem fs = emptyScratchDir.getFileSystem(job);
      fs.mkdirs(emptyScratchDir);
    } catch (IOException e) {
      console.printError("Error launching map-reduce job", "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return 5;
    }

    HiveFileFormatUtils.prepareJobOutput(job);
    //See the javadoc on HiveOutputFormatImpl and HadoopShims.prepareJobOutput()
    job.setOutputFormat(HiveOutputFormatImpl.class);

    job.setMapRunnerClass(ExecMapRunner.class);
    job.setMapperClass(ExecMapper.class);

    job.setMapOutputKeyClass(HiveKey.class);
    job.setMapOutputValueClass(BytesWritable.class);

    try {
      String partitioner = HiveConf.getVar(job, ConfVars.HIVE_PARTITIONER);
      job.setPartitionerClass(JavaUtils.loadClass(partitioner));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    propagateSplitSettings(job, mWork);

    job.setNumReduceTasks(rWork != null ? rWork.getNumReduceTasks() : 0);
    job.setReducerClass(ExecReducer.class);

    // set input format information if necessary
    setInputAttributes(job);

    // HIVE-23354 enforces that MR speculative execution is disabled
    job.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);
    job.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);

    String inpFormat = HiveConf.getVar(job, HiveConf.ConfVars.HIVE_INPUT_FORMAT);

    if (mWork.isUseBucketizedHiveInputFormat()) {
      inpFormat = BucketizedHiveInputFormat.class.getName();
    }

    LOG.info("Using " + inpFormat);

    try {
      job.setInputFormat(JavaUtils.loadClass(inpFormat));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e.getMessage(), e);
    }

    // No-Op - we don't really write anything here ..
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    int returnVal = 0;
    boolean noName = StringUtils.isEmpty(job.get(MRJobConfig.JOB_NAME));

    if (noName) {
      // This is for a special case to ensure unit tests pass
      job.set(MRJobConfig.JOB_NAME,
          "JOB" + ThreadLocalRandom.current().nextInt());
    }

    try{
      MapredLocalWork localwork = mWork.getMapRedLocalWork();
      if (localwork != null && localwork.hasStagedAlias()) {
        if (!ShimLoader.getHadoopShims().isLocalMode(job)) {
          Path localPath = localwork.getTmpPath();
          Path hdfsPath = mWork.getTmpHDFSPath();

          FileSystem hdfs = hdfsPath.getFileSystem(job);
          FileSystem localFS = localPath.getFileSystem(job);
          FileStatus[] hashtableFiles = localFS.listStatus(localPath);
          int fileNumber = hashtableFiles.length;
          String[] fileNames = new String[fileNumber];

          for ( int i = 0; i < fileNumber; i++){
            fileNames[i] = hashtableFiles[i].getPath().getName();
          }

          //package and compress all the hashtable files to an archive file
          String stageId = this.getId();
          String archiveFileName = Utilities.generateTarFileName(stageId);
          localwork.setStageID(stageId);

          CompressionUtils.tar(localPath.toUri().getPath(), fileNames,archiveFileName);
          Path archivePath = Utilities.generateTarPath(localPath, stageId);
          LOG.info("Archive "+ hashtableFiles.length+" hash table files to " + archivePath);

          //upload archive file to hdfs
          Path hdfsFilePath =Utilities.generateTarPath(hdfsPath, stageId);
          short replication = (short) job.getInt("mapred.submit.replication", 10);
          hdfs.copyFromLocalFile(archivePath, hdfsFilePath);
          hdfs.setReplication(hdfsFilePath, replication);
          LOG.info("Upload 1 archive file  from" + archivePath + " to: " + hdfsFilePath);

          //add the archive file to distributed cache
          DistributedCache.createSymlink(job);
          DistributedCache.addCacheArchive(hdfsFilePath.toUri(), job);
          LOG.info("Add 1 archive file to distributed cache. Archive file: " + hdfsFilePath.toUri());
        }
      }
      work.configureJobConf(job);
      List<Path> inputPaths = Utilities.getInputPaths(job, mWork, emptyScratchDir, ctx, false);
      Utilities.setInputPaths(job, inputPaths);

      Utilities.setMapRedWork(job, work, ctx.getMRTmpPath());

      if (mWork.getSamplingType() > 0 && rWork != null && job.getNumReduceTasks() > 1) {
        try {
          handleSampling(ctx, mWork, job);
          job.setPartitionerClass(HiveTotalOrderPartitioner.class);
        } catch (IllegalStateException e) {
          console.printInfo("Not enough sampling data.. Rolling back to single reducer task");
          rWork.setNumReduceTasks(1);
          job.setNumReduceTasks(1);
        } catch (Exception e) {
          LOG.error("Sampling error", e);
          console.printError(e.toString(),
              "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
          rWork.setNumReduceTasks(1);
          job.setNumReduceTasks(1);
        }
      }

      jc = new JobClient(job);
      // make this client wait if job tracker is not behaving well.
      Throttle.checkJobTracker(job, LOG);

      if (mWork.isGatheringStats() || (rWork != null && rWork.isGatheringStats())) {
        // initialize stats publishing table
        StatsPublisher statsPublisher;
        StatsFactory factory = StatsFactory.newFactory(job);
        if (factory != null) {
          statsPublisher = factory.getStatsPublisher();
          List<String> statsTmpDir = Utilities.getStatsTmpDirs(mWork, job);
          if (rWork != null) {
            statsTmpDir.addAll(Utilities.getStatsTmpDirs(rWork, job));
          }
          StatsCollectionContext sc = new StatsCollectionContext(job);
          sc.setStatsTmpDirs(statsTmpDir);
          if (!statsPublisher.init(sc)) { // creating stats table if not exists
            if (HiveConf.getBoolVar(job, HiveConf.ConfVars.HIVE_STATS_RELIABLE)) {
              throw
                new HiveException(ErrorMsg.STATSPUBLISHER_INITIALIZATION_ERROR.getErrorCodedMsg());
            }
          }
        }
      }

      Utilities.createTmpDirs(job, mWork);
      Utilities.createTmpDirs(job, rWork);

      SessionState ss = SessionState.get();
      // TODO: why is there a TezSession in MR ExecDriver?
      if (ss != null && HiveConf.getVar(job, ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
        // TODO: this is the only place that uses keepTmpDir. Why?
        TezSessionPoolManager.closeIfNotDefault(ss.getTezSession(), true);
      }

      HiveConfUtil.updateJobCredentialProviders(job);
      // Finally SUBMIT the JOB!
      if (taskQueue.isShutdown()) {
        LOG.warn("Task was cancelled");
        return 5;
      }

    rj = jc.submitJob(job);

      if (taskQueue.isShutdown()) {
        LOG.warn("Task was cancelled");
        killJob();
        return 5;
      }

      this.jobID = rj.getJobID();
      updateStatusInQueryDisplay();
      returnVal = jobExecHelper.progress(rj, jc, ctx);
      success = (returnVal == 0);
    } catch (Exception e) {
      setException(e);
      String mesg = " with exception '" + Utilities.getNameMessage(e) + "'";
      if (rj != null) {
        mesg = "Ended Job = " + rj.getJobID() + mesg;
      } else {
        mesg = "Job Submission failed" + mesg;
      }

      // Has to use full name to make sure it does not conflict with
      // org.apache.commons.lang3.StringUtils
      console.printError(mesg, "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));

      success = false;
      returnVal = 1;
    } finally {
      Utilities.clearWork(job);
      try {
        if (ctxCreated) {
          ctx.clear();
        }

        if (rj != null) {
          if (returnVal != 0) {
            killJob();
          }
          jobID = rj.getID().toString();
        }
        if (jc!=null) {
          jc.close();
        }
      } catch (Exception e) {
        LOG.warn("Failed while cleaning up ", e);
      } finally {
        HadoopJobExecHelper.runningJobs.remove(rj);
      }
    }

    // get the list of Dynamic partition paths
    try {
      if (rj != null) {
        if (mWork.getAliasToWork() != null) {
          for (Operator<? extends OperatorDesc> op : mWork.getAliasToWork().values()) {
            op.jobClose(job, success);
          }
        }
        if (rWork != null) {
          rWork.getReducer().jobClose(job, success);
        }
      }
    } catch (Exception e) {
      // jobClose needs to execute successfully otherwise fail task
      if (success) {
        setException(e);
        success = false;
        returnVal = 3;
        String mesg = "Job Commit failed with exception '" + Utilities.getNameMessage(e) + "'";
        console.printError(mesg, "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    }

    return (returnVal);
  }

  public static void propagateSplitSettings(JobConf job, MapWork work) {
    if (work.getNumMapTasks() != null) {
      job.setNumMapTasks(work.getNumMapTasks().intValue());
    }

    if (work.getMaxSplitSize() != null) {
      HiveConf.setLongVar(job, HiveConf.ConfVars.MAPRED_MAX_SPLIT_SIZE, work.getMaxSplitSize());
    }

    if (work.getMinSplitSize() != null) {
      HiveConf.setLongVar(job, HiveConf.ConfVars.MAPRED_MIN_SPLIT_SIZE, work.getMinSplitSize());
    }

    if (work.getMinSplitSizePerNode() != null) {
      HiveConf.setLongVar(job, HiveConf.ConfVars.MAPRED_MIN_SPLIT_SIZE_PER_NODE, work.getMinSplitSizePerNode());
    }

    if (work.getMinSplitSizePerRack() != null) {
      HiveConf.setLongVar(job, HiveConf.ConfVars.MAPRED_MIN_SPLIT_SIZE_PER_RACK, work.getMinSplitSizePerRack());
    }
  }

  private void handleSampling(Context context, MapWork mWork, JobConf job)
      throws Exception {
    assert mWork.getAliasToWork().keySet().size() == 1;

    String alias = mWork.getAliases().get(0);
    Operator<?> topOp = mWork.getAliasToWork().get(alias);
    PartitionDesc partDesc = mWork.getAliasToPartnInfo().get(alias);

    ArrayList<PartitionDesc> parts = mWork.getPartitionDescs();

    List<Path> inputPaths = mWork.getPaths();

    Path tmpPath = context.getExternalTmpPath(inputPaths.get(0));
    Path partitionFile = new Path(tmpPath, ".partitions");
    ShimLoader.getHadoopShims().setTotalOrderPartitionFile(job, partitionFile);
    PartitionKeySampler sampler = new PartitionKeySampler();

    if (mWork.getSamplingType() == MapWork.SAMPLING_ON_PREV_MR) {
      console.printInfo("Use sampling data created in previous MR");
      // merges sampling data from previous MR and make partition keys for total sort
      for (Path path : inputPaths) {
        FileSystem fs = path.getFileSystem(job);
        for (FileStatus status : fs.globStatus(new Path(path, ".sampling*"))) {
          sampler.addSampleFile(status.getPath(), job);
        }
      }
    } else if (mWork.getSamplingType() == MapWork.SAMPLING_ON_START) {
      console.printInfo("Creating sampling data..");
      assert topOp instanceof TableScanOperator;
      TableScanOperator ts = (TableScanOperator) topOp;

      FetchWork fetchWork;
      if (!partDesc.isPartitioned()) {
        assert inputPaths.size() == 1;
        fetchWork = new FetchWork(inputPaths.get(0), partDesc.getTableDesc());
      } else {
        fetchWork = new FetchWork(inputPaths, parts, partDesc.getTableDesc());
      }
      fetchWork.setSource(ts);

      // random sampling
      FetchOperator fetcher = PartitionKeySampler.createSampler(fetchWork, job, ts);
      try {
        ts.initialize(job, new ObjectInspector[]{fetcher.getOutputObjectInspector()});
        OperatorUtils.setChildrenCollector(ts.getChildOperators(), sampler);
        while (fetcher.pushRow()) { }
      } finally {
        fetcher.clearFetchContext();
      }
    } else {
      throw new IllegalArgumentException("Invalid sampling type " + mWork.getSamplingType());
    }
    sampler.writePartitionKeys(partitionFile, job);
  }

  /**
   * Set hive input format, and input format file if necessary.
   */
  protected void setInputAttributes(Configuration conf) {
    MapWork mWork = work.getMapWork();
    if (mWork.getInputformat() != null) {
      HiveConf.setVar(conf, ConfVars.HIVE_INPUT_FORMAT, mWork.getInputformat());
    }
    // Intentionally overwrites anything the user may have put here
    conf.setBoolean("hive.input.format.sorted", mWork.isInputFormatSorted());

    if (HiveConf.getVar(conf, ConfVars.HIVE_CURRENT_DATABASE, (String)null) == null) {
      HiveConf.setVar(conf, ConfVars.HIVE_CURRENT_DATABASE, getCurrentDB());
    }
  }

  private static String getCurrentDB() {
    String currentDB = null;
    if (SessionState.get() != null) {
      currentDB = SessionState.get().getCurrentDatabase();
    }
    return currentDB == null ? "default" : currentDB;
  }

  public boolean mapStarted() {
    return this.jobExecHelper.mapStarted();
  }

  public boolean reduceStarted() {
    return this.jobExecHelper.reduceStarted();
  }

  public boolean mapDone() {
    return this.jobExecHelper.mapDone();
  }

  public boolean reduceDone() {
    return this.jobExecHelper.reduceDone();
  }

  private static void printUsage() {
    System.err.println("ExecDriver -plan <plan-file> [-jobconffile <job conf file>]"
        + "[-files <file1>[,<file2>] ...]");
    System.exit(1);
  }

  /**
   * we are running the hadoop job via a sub-command. this typically happens when we are running
   * jobs in local mode. the log4j in this mode is controlled as follows: 1. if the admin provides a
   * log4j properties file especially for execution mode - then we pick that up 2. otherwise - we
   * default to the regular hive log4j properties if one is supplied 3. if none of the above two
   * apply - we don't do anything - the log4j properties would likely be determined by hadoop.
   *
   * The intention behind providing a separate option #1 is to be able to collect hive run time logs
   * generated in local mode in a separate (centralized) location if desired. This mimics the
   * behavior of hive run time logs when running against a hadoop cluster where they are available
   * on the tasktracker nodes.
   */

  private static void setupChildLog4j(Configuration conf) {
    try {
      LogUtils.initHiveExecLog4j();
      LogDivertAppender.registerRoutingAppender(conf);
      LogDivertAppenderForTest.registerRoutingAppenderIfInTest(conf);
    } catch (LogInitializationException e) {
      System.err.println(e.getMessage());
    }
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws IOException, HiveException {

    String planFileName = null;
    String jobConfFileName = null;
    boolean noLog = false;
    String files = null;
    String libjars = null;
    boolean localtask = false;
    try {
      for (int i = 0; i < args.length; i++) {
        if (args[i].equals("-plan")) {
          planFileName = args[++i];
        } else if (args[i].equals("-jobconffile")) {
          jobConfFileName = args[++i];
        } else if (args[i].equals("-nolog")) {
          noLog = true;
        } else if (args[i].equals("-files")) {
          files = args[++i];
        } else if (args[i].equals("-libjars")) {
          libjars = args[++i];
        }else if (args[i].equals("-localtask")) {
          localtask = true;
        }
      }
    } catch (IndexOutOfBoundsException e) {
      System.err.println("Missing argument to option");
      printUsage();
    }

    JobConf conf;
    if (localtask) {
      conf = new JobConf(MapredLocalTask.class);
    } else {
      conf = new JobConf(ExecDriver.class);
    }

    if (jobConfFileName != null) {
      conf.addResource(new Path(jobConfFileName));
    }

    // Initialize the resources from command line
    if (files != null) {
      conf.set("tmpfiles", files);
    }

    if (libjars != null) {
      conf.set("tmpjars", libjars);
    }

    if(UserGroupInformation.isSecurityEnabled()){
      String hadoopAuthToken = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
      if(hadoopAuthToken != null){
        conf.set("mapreduce.job.credentials.binary", hadoopAuthToken);
      }
    }

    boolean isSilent = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_SESSION_SILENT);

    String queryId = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID, "").trim();
    if(queryId.isEmpty()) {
      queryId = "unknown-" + System.currentTimeMillis();
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID, queryId);
    }
    System.setProperty(HiveConf.ConfVars.HIVE_QUERY_ID.toString(), queryId);

    LogUtils.registerLoggingContext(conf);

    if (noLog) {
      // If started from main(), and noLog is on, we should not output
      // any logs. To turn the log on, please set -Dtest.silent=false
      org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getRootLogger();
      NullAppender appender = NullAppender.createNullAppender();
      appender.addToLogger(logger.getName(), Level.ERROR);
      appender.start();
    } else {
      setupChildLog4j(conf);
    }

    Logger LOG = LoggerFactory.getLogger(ExecDriver.class.getName());
    LogHelper console = new LogHelper(LOG, isSilent);

    if (planFileName == null) {
      console.printError("Must specify Plan File Name");
      printUsage();
    }

    // print out the location of the log file for the user so
    // that it's easy to find reason for local mode execution failures
    for (Appender appender : ((org.apache.logging.log4j.core.Logger) LogManager.getRootLogger())
            .getAppenders().values()) {
      if (appender instanceof FileAppender) {
        console.printInfo("Execution log at: " + ((FileAppender) appender).getFileName());
      } else if (appender instanceof RollingFileAppender) {
        console.printInfo("Execution log at: " + ((RollingFileAppender) appender).getFileName());
      }
    }

    // the plan file should always be in local directory
    Path p = new Path(planFileName);
    FileSystem fs = FileSystem.getLocal(conf);
    InputStream pathData = fs.open(p);

    // this is workaround for hadoop-17 - libjars are not added to classpath of the
    // child process. so we add it here explicitly
    try {
      // see also - code in CliDriver.java
      ClassLoader loader = conf.getClassLoader();
      if (StringUtils.isNotBlank(libjars)) {
        AddToClassPathAction addAction = new AddToClassPathAction(
            loader, Arrays.asList(StringUtils.split(libjars, ",")));
        loader = AccessController.doPrivileged(addAction);
      }
      conf.setClassLoader(loader);
      // Also set this to the Thread ContextClassLoader, so new threads will
      // inherit
      // this class loader, and propagate into newly created Configurations by
      // those
      // new threads.
      Thread.currentThread().setContextClassLoader(loader);
    } catch (Exception e) {
      throw new HiveException(e.getMessage(), e);
    }
    int ret;
    if (localtask) {
      memoryMXBean = ManagementFactory.getMemoryMXBean();
      MapredLocalWork plan = SerializationUtilities.deserializePlan(pathData, MapredLocalWork.class);
      MapredLocalTask ed = new MapredLocalTask(plan, conf, isSilent);
      ed.initialize(null, null, new TaskQueue(), null);
      ret = ed.executeInProcess();

    } else {
      MapredWork plan = SerializationUtilities.deserializePlan(pathData, MapredWork.class);
      ExecDriver ed = new ExecDriver(plan, conf, isSilent);
      ed.setTaskQueue(new TaskQueue());
      ret = ed.execute();
    }

    if (ret != 0) {
      System.exit(ret);
    }
  }

  /**
   * Given a Hive Configuration object - generate a command line fragment for passing such
   * configuration information to ExecDriver.
   */
  public static String generateCmdLine(HiveConf hconf, Context ctx)
      throws IOException {
    HiveConf tempConf = new HiveConf();
    Path hConfFilePath = new Path(ctx.getLocalTmpPath(), JOBCONF_FILENAME);
    OutputStream out = null;

    Properties deltaP = hconf.getChangedProperties();
    boolean hadoopLocalMode = ShimLoader.getHadoopShims().isLocalMode(hconf);
    String hadoopSysDir = "mapred.system.dir";
    String hadoopWorkDir = "mapred.local.dir";

    for (Object one : deltaP.keySet()) {
      String oneProp = (String) one;

      if (hadoopLocalMode && (oneProp.equals(hadoopSysDir) || oneProp.equals(hadoopWorkDir))) {
        continue;
      }
      tempConf.set(oneProp, hconf.get(oneProp));
    }

    // Multiple concurrent local mode job submissions can cause collisions in
    // working dirs and system dirs
    // Workaround is to rename map red working dir to a temp dir in such cases
    if (hadoopLocalMode) {
      tempConf.set(hadoopSysDir, hconf.get(hadoopSysDir) + "/"
          + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
      tempConf.set(hadoopWorkDir, hconf.get(hadoopWorkDir) + "/"
          + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    }

    try {
      out = FileSystem.getLocal(hconf).create(hConfFilePath);
      tempConf.writeXml(out);
    } finally {
      if (out != null) {
        out.close();
      }
    }
    return " -jobconffile " + hConfFilePath.toString();
  }

  @Override
  public Collection<MapWork> getMapWork() {
    return Collections.<MapWork>singleton(getWork().getMapWork());
  }

  @Override
  public boolean isMapRedTask() {
    return true;
  }

  @Override
  public Collection<Operator<? extends OperatorDesc>> getTopOperators() {
    return getWork().getMapWork().getAliasToWork().values();
  }

  @Override
  public boolean hasReduce() {
    MapredWork w = getWork();
    return w.getReduceWork() != null;
  }

  @Override
  public StageType getType() {
    return StageType.MAPRED;
  }

  @Override
  public String getName() {
    return "MAPRED";
  }

  @Override
  public void logPlanProgress(SessionState ss) throws IOException {
    ss.getHiveHistory().logPlanProgress(queryPlan);
  }

  public boolean isTaskShutdown() {
    return isShutdown;
  }

  @Override
  public void shutdown() {
    super.shutdown();
    killJob();
    isShutdown = true;
  }

  @Override
  public String getExternalHandle() {
    return this.jobID;
  }

  private void killJob() {
    boolean needToKillJob = false;
    synchronized(this) {
      if (rj != null && !jobKilled) {
        jobKilled = true;
        needToKillJob = true;
      }
    }
    if (needToKillJob) {
      try {
        rj.killJob();
      } catch (Exception e) {
        LOG.warn("failed to kill job " + rj.getID(), e);
      }
    }
  }
}

