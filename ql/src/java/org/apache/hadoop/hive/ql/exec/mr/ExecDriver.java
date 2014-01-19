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

package org.apache.hadoop.hive.ql.exec.mr;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.CompressionUtils;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.FetchOperator;
import org.apache.hadoop.hive.ql.exec.HiveTotalOrderPartitioner;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.PartitionKeySampler;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.io.HiveOutputFormatImpl;
import org.apache.hadoop.hive.ql.io.IOPrepareCache;
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
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.LogManager;
import org.apache.log4j.varia.NullAppender;

/**
 * ExecDriver is the central class in co-ordinating execution of any map-reduce task.
 * It's main responsabilities are:
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

  protected static transient final Log LOG = LogFactory.getLog(ExecDriver.class);

  private RunningJob rj;

  /**
   * Constructor when invoked from QL.
   */
  public ExecDriver() {
    super();
    console = new LogHelper(LOG);
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
   * Initialization when invoked from QL.
   */
  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan, DriverContext driverContext) {
    super.initialize(conf, queryPlan, driverContext);

    job = new JobConf(conf, ExecDriver.class);

    // NOTE: initialize is only called if it is in non-local mode.
    // In case it's in non-local mode, we need to move the SessionState files
    // and jars to jobConf.
    // In case it's in local mode, MapRedTask will set the jobConf.
    //
    // "tmpfiles" and "tmpjars" are set by the method ExecDriver.execute(),
    // which will be called by both local and NON-local mode.
    String addedFiles = Utilities.getResourceFiles(job, SessionState.ResourceType.FILE);
    if (StringUtils.isNotBlank(addedFiles)) {
      HiveConf.setVar(job, ConfVars.HIVEADDEDFILES, addedFiles);
    }
    String addedJars = Utilities.getResourceFiles(job, SessionState.ResourceType.JAR);
    if (StringUtils.isNotBlank(addedJars)) {
      HiveConf.setVar(job, ConfVars.HIVEADDEDJARS, addedJars);
    }
    String addedArchives = Utilities.getResourceFiles(job, SessionState.ResourceType.ARCHIVE);
    if (StringUtils.isNotBlank(addedArchives)) {
      HiveConf.setVar(job, ConfVars.HIVEADDEDARCHIVES, addedArchives);
    }
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
  public boolean checkFatalErrors(Counters ctrs, StringBuilder errMsg) {
     Counters.Counter cntr = ctrs.findCounter(
        HiveConf.getVar(job, HiveConf.ConfVars.HIVECOUNTERGROUP),
        Operator.HIVECOUNTERFATAL);
    return cntr != null && cntr.getValue() > 0;
  }

   /**
   * Execute a query plan using Hadoop.
   */
  @SuppressWarnings({"deprecation", "unchecked"})
  @Override
  public int execute(DriverContext driverContext) {

    IOPrepareCache ioPrepareCache = IOPrepareCache.get();
    ioPrepareCache.clear();

    boolean success = true;

    Context ctx = driverContext.getCtx();
    boolean ctxCreated = false;
    Path emptyScratchDir;

    MapWork mWork = work.getMapWork();
    ReduceWork rWork = work.getReduceWork();

    try {
      if (ctx == null) {
        ctx = new Context(job);
        ctxCreated = true;
      }

      emptyScratchDir = ctx.getMRTmpPath();
      FileSystem fs = emptyScratchDir.getFileSystem(job);
      fs.mkdirs(emptyScratchDir);
    } catch (IOException e) {
      e.printStackTrace();
      console.printError("Error launching map-reduce job", "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return 5;
    }

    ShimLoader.getHadoopShims().prepareJobOutput(job);
    //See the javadoc on HiveOutputFormatImpl and HadoopShims.prepareJobOutput()
    job.setOutputFormat(HiveOutputFormatImpl.class);

    job.setMapperClass(ExecMapper.class);

    job.setMapOutputKeyClass(HiveKey.class);
    job.setMapOutputValueClass(BytesWritable.class);

    try {
      job.setPartitionerClass((Class<? extends Partitioner>) (Class.forName(HiveConf.getVar(job,
          HiveConf.ConfVars.HIVEPARTITIONER))));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e.getMessage());
    }

    if (mWork.getNumMapTasks() != null) {
      job.setNumMapTasks(mWork.getNumMapTasks().intValue());
    }

    if (mWork.getMaxSplitSize() != null) {
      HiveConf.setLongVar(job, HiveConf.ConfVars.MAPREDMAXSPLITSIZE, mWork.getMaxSplitSize().longValue());
    }

    if (mWork.getMinSplitSize() != null) {
      HiveConf.setLongVar(job, HiveConf.ConfVars.MAPREDMINSPLITSIZE, mWork.getMinSplitSize().longValue());
    }

    if (mWork.getMinSplitSizePerNode() != null) {
      HiveConf.setLongVar(job, HiveConf.ConfVars.MAPREDMINSPLITSIZEPERNODE, mWork.getMinSplitSizePerNode().longValue());
    }

    if (mWork.getMinSplitSizePerRack() != null) {
      HiveConf.setLongVar(job, HiveConf.ConfVars.MAPREDMINSPLITSIZEPERRACK, mWork.getMinSplitSizePerRack().longValue());
    }

    job.setNumReduceTasks(rWork != null ? rWork.getNumReduceTasks().intValue() : 0);
    job.setReducerClass(ExecReducer.class);

    // set input format information if necessary
    setInputAttributes(job);

    // Turn on speculative execution for reducers
    boolean useSpeculativeExecReducers = HiveConf.getBoolVar(job,
        HiveConf.ConfVars.HIVESPECULATIVEEXECREDUCERS);
    HiveConf.setBoolVar(job, HiveConf.ConfVars.HADOOPSPECULATIVEEXECREDUCERS,
        useSpeculativeExecReducers);

    String inpFormat = HiveConf.getVar(job, HiveConf.ConfVars.HIVEINPUTFORMAT);
    if ((inpFormat == null) || (!StringUtils.isNotBlank(inpFormat))) {
      inpFormat = ShimLoader.getHadoopShims().getInputFormatClassName();
    }

    if (mWork.isUseBucketizedHiveInputFormat()) {
      inpFormat = BucketizedHiveInputFormat.class.getName();
    }

    LOG.info("Using " + inpFormat);

    try {
      job.setInputFormat((Class<? extends InputFormat>) (Class.forName(inpFormat)));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e.getMessage());
    }


    // No-Op - we don't really write anything here ..
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Transfer HIVEAUXJARS and HIVEADDEDJARS to "tmpjars" so hadoop understands
    // it
    String auxJars = HiveConf.getVar(job, HiveConf.ConfVars.HIVEAUXJARS);
    String addedJars = HiveConf.getVar(job, HiveConf.ConfVars.HIVEADDEDJARS);
    if (StringUtils.isNotBlank(auxJars) || StringUtils.isNotBlank(addedJars)) {
      String allJars = StringUtils.isNotBlank(auxJars) ? (StringUtils.isNotBlank(addedJars) ? addedJars
          + "," + auxJars
          : auxJars)
          : addedJars;
      LOG.info("adding libjars: " + allJars);
      initializeFiles("tmpjars", allJars);
    }

    // Transfer HIVEADDEDFILES to "tmpfiles" so hadoop understands it
    String addedFiles = HiveConf.getVar(job, HiveConf.ConfVars.HIVEADDEDFILES);
    if (StringUtils.isNotBlank(addedFiles)) {
      initializeFiles("tmpfiles", addedFiles);
    }
    int returnVal = 0;
    boolean noName = StringUtils.isEmpty(HiveConf.getVar(job, HiveConf.ConfVars.HADOOPJOBNAME));

    if (noName) {
      // This is for a special case to ensure unit tests pass
      HiveConf.setVar(job, HiveConf.ConfVars.HADOOPJOBNAME, "JOB" + Utilities.randGen.nextInt());
    }
    String addedArchives = HiveConf.getVar(job, HiveConf.ConfVars.HIVEADDEDARCHIVES);
    // Transfer HIVEADDEDARCHIVES to "tmparchives" so hadoop understands it
    if (StringUtils.isNotBlank(addedArchives)) {
      initializeFiles("tmparchives", addedArchives);
    }

    try{
      MapredLocalWork localwork = mWork.getMapLocalWork();
      if (localwork != null) {
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
          hdfs.setReplication(hdfsFilePath, replication);
          hdfs.copyFromLocalFile(archivePath, hdfsFilePath);
          LOG.info("Upload 1 archive file  from" + archivePath + " to: " + hdfsFilePath);

          //add the archive file to distributed cache
          DistributedCache.createSymlink(job);
          DistributedCache.addCacheArchive(hdfsFilePath.toUri(), job);
          LOG.info("Add 1 archive file to distributed cache. Archive file: " + hdfsFilePath.toUri());
        }
      }
      work.configureJobConf(job);
      List<Path> inputPaths = Utilities.getInputPaths(job, mWork, emptyScratchDir, ctx);
      Utilities.setInputPaths(job, inputPaths);

      Utilities.setMapRedWork(job, work, ctx.getMRTmpPath());

      if (mWork.getSamplingType() > 0 && rWork != null && rWork.getNumReduceTasks() > 1) {
        try {
          handleSampling(driverContext, mWork, job, conf);
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

      // remove the pwd from conf file so that job tracker doesn't show this
      // logs
      String pwd = HiveConf.getVar(job, HiveConf.ConfVars.METASTOREPWD);
      if (pwd != null) {
        HiveConf.setVar(job, HiveConf.ConfVars.METASTOREPWD, "HIVE");
      }
      JobClient jc = new JobClient(job);
      // make this client wait if job trcker is not behaving well.
      Throttle.checkJobTracker(job, LOG);

      if (mWork.isGatheringStats() || (rWork != null && rWork.isGatheringStats())) {
        // initialize stats publishing table
        StatsPublisher statsPublisher;
        StatsFactory factory = StatsFactory.newFactory(job);
        if (factory != null) {
          statsPublisher = factory.getStatsPublisher();
          if (!statsPublisher.init(job)) { // creating stats table if not exists
            if (HiveConf.getBoolVar(job, HiveConf.ConfVars.HIVE_STATS_RELIABLE)) {
              throw
                new HiveException(ErrorMsg.STATSPUBLISHER_INITIALIZATION_ERROR.getErrorCodedMsg());
            }
          }
        }
      }

      Utilities.createTmpDirs(job, mWork);
      Utilities.createTmpDirs(job, rWork);

      // Finally SUBMIT the JOB!
      rj = jc.submitJob(job);
      // replace it back
      if (pwd != null) {
        HiveConf.setVar(job, HiveConf.ConfVars.METASTOREPWD, pwd);
      }

      returnVal = jobExecHelper.progress(rj, jc);
      success = (returnVal == 0);
    } catch (Exception e) {
      e.printStackTrace();
      String mesg = " with exception '" + Utilities.getNameMessage(e) + "'";
      if (rj != null) {
        mesg = "Ended Job = " + rj.getJobID() + mesg;
      } else {
        mesg = "Job Submission failed" + mesg;
      }

      // Has to use full name to make sure it does not conflict with
      // org.apache.commons.lang.StringUtils
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
            rj.killJob();
          }
          HadoopJobExecHelper.runningJobKillURIs.remove(rj.getJobID());
          jobID = rj.getID().toString();
        }
      } catch (Exception e) {
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
        success = false;
        returnVal = 3;
        String mesg = "Job Commit failed with exception '" + Utilities.getNameMessage(e) + "'";
        console.printError(mesg, "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    }

    return (returnVal);
  }

  private void handleSampling(DriverContext context, MapWork mWork, JobConf job, HiveConf conf)
      throws Exception {
    assert mWork.getAliasToWork().keySet().size() == 1;

    String alias = mWork.getAliases().get(0);
    Operator<?> topOp = mWork.getAliasToWork().get(alias);
    PartitionDesc partDesc = mWork.getAliasToPartnInfo().get(alias);

    ArrayList<String> paths = mWork.getPaths();
    ArrayList<PartitionDesc> parts = mWork.getPartitionDescs();

    List<Path> inputPaths = new ArrayList<Path>(paths.size());
    for (String path : paths) {
      inputPaths.add(new Path(path));
    }

    Path tmpPath = context.getCtx().getExternalTmpPath(inputPaths.get(0).toUri());
    Path partitionFile = new Path(tmpPath, ".partitions");
    ShimLoader.getHadoopShims().setTotalOrderPartitionFile(job, partitionFile);
    PartitionKeySampler sampler = new PartitionKeySampler();

    if (mWork.getSamplingType() == MapWork.SAMPLING_ON_PREV_MR) {
      console.printInfo("Use sampling data created in previous MR");
      // merges sampling data from previous MR and make paritition keys for total sort
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
        assert paths.size() == 1;
        fetchWork = new FetchWork(inputPaths.get(0), partDesc.getTableDesc());
      } else {
        fetchWork = new FetchWork(inputPaths, parts, partDesc.getTableDesc());
      }
      fetchWork.setSource(ts);

      // random sampling
      FetchOperator fetcher = PartitionKeySampler.createSampler(fetchWork, conf, job, ts);
      try {
        ts.initialize(conf, new ObjectInspector[]{fetcher.getOutputObjectInspector()});
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
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVEINPUTFORMAT, mWork.getInputformat());
    }
    if (mWork.getIndexIntermediateFile() != null) {
      conf.set("hive.index.compact.file", mWork.getIndexIntermediateFile());
      conf.set("hive.index.blockfilter.file", mWork.getIndexIntermediateFile());
    }

    // Intentionally overwrites anything the user may have put here
    conf.setBoolean("hive.input.format.sorted", mWork.isInputFormatSorted());
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
        } else if (args[i].equals("-localtask")) {
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

    if (files != null) {
      conf.set("tmpfiles", files);
    }

    if(ShimLoader.getHadoopShims().isSecurityEnabled()){
      String hadoopAuthToken =
          System.getenv(ShimLoader.getHadoopShims().getTokenFileLocEnvName());
      if(hadoopAuthToken != null){
        conf.set("mapreduce.job.credentials.binary", hadoopAuthToken);
      }
    }

    boolean isSilent = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVESESSIONSILENT);

    String queryId = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYID, "").trim();
    if(queryId.isEmpty()) {
      queryId = "unknown-" + System.currentTimeMillis();
    }
    System.setProperty(HiveConf.ConfVars.HIVEQUERYID.toString(), queryId);

    if (noLog) {
      // If started from main(), and noLog is on, we should not output
      // any logs. To turn the log on, please set -Dtest.silent=false
      BasicConfigurator.resetConfiguration();
      BasicConfigurator.configure(new NullAppender());
    } else {
      setupChildLog4j(conf);
    }

    Log LOG = LogFactory.getLog(ExecDriver.class.getName());
    LogHelper console = new LogHelper(LOG, isSilent);

    if (planFileName == null) {
      console.printError("Must specify Plan File Name");
      printUsage();
    }

    // print out the location of the log file for the user so
    // that it's easy to find reason for local mode execution failures
    for (Appender appender : Collections.list((Enumeration<Appender>) LogManager.getRootLogger()
        .getAllAppenders())) {
      if (appender instanceof FileAppender) {
        console.printInfo("Execution log at: " + ((FileAppender) appender).getFile());
      }
    }

    // the plan file should always be in local directory
    Path p = new Path(planFileName);
    FileSystem fs = FileSystem.getLocal(conf);
    InputStream pathData = fs.open(p);

    // this is workaround for hadoop-17 - libjars are not added to classpath of the
    // child process. so we add it here explicitly

    String auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS);
    String addedJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEADDEDJARS);
    try {
      // see also - code in CliDriver.java
      ClassLoader loader = conf.getClassLoader();
      if (StringUtils.isNotBlank(auxJars)) {
        loader = Utilities.addToClassPath(loader, StringUtils.split(auxJars, ","));
      }
      if (StringUtils.isNotBlank(addedJars)) {
        loader = Utilities.addToClassPath(loader, StringUtils.split(addedJars, ","));
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
      MapredLocalWork plan = Utilities.deserializePlan(pathData, MapredLocalWork.class, conf);
      MapredLocalTask ed = new MapredLocalTask(plan, conf, isSilent);
      ret = ed.executeFromChildJVM(new DriverContext());

    } else {
      MapredWork plan = Utilities.deserializePlan(pathData, MapredWork.class, conf);
      ExecDriver ed = new ExecDriver(plan, conf, isSilent);
      ret = ed.execute(new DriverContext());
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
      tempConf.set(hadoopSysDir, hconf.get(hadoopSysDir) + "/" + Utilities.randGen.nextInt());
      tempConf.set(hadoopWorkDir, hconf.get(hadoopWorkDir) + "/" + Utilities.randGen.nextInt());
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

  @Override
  public void shutdown() {
    super.shutdown();
    if (rj != null) {
      try {
        rj.killJob();
      } catch (Exception e) {
        LOG.warn("failed to kill job " + rj.getID(), e);
      }
      rj = null;
    }
  }
}

