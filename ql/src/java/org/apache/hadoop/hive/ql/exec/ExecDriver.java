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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormatImpl;
import org.apache.hadoop.hive.ql.io.IOPrepareCache;
import org.apache.hadoop.hive.ql.io.OneNullRowInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.varia.NullAppender;

/**
 * ExecDriver.
 *
 */
public class ExecDriver extends Task<MapredWork> implements Serializable, HadoopJobExecHook {

  private static final long serialVersionUID = 1L;
  private static final String JOBCONF_FILENAME = "jobconf.xml";

  protected transient JobConf job;
  public static MemoryMXBean memoryMXBean;
  protected HadoopJobExecHelper jobExecHelper;

  protected static transient final Log LOG = LogFactory.getLog(ExecDriver.class);

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
      ShimLoader.getHadoopShims().setTmpFiles(prop, files);
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
    for (Operator<? extends OperatorDesc> op : work.getAliasToWork().values()) {
      if (op.checkFatalErrors(ctrs, errMsg)) {
        return true;
      }
    }
    if (work.getReducer() != null) {
      if (work.getReducer().checkFatalErrors(ctrs, errMsg)) {
        return true;
      }
    }
    return false;
  }

  protected void createTmpDirs() throws IOException {
    // fix up outputs
    Map<String, ArrayList<String>> pa = work.getPathToAliases();
    if (pa != null) {
      List<Operator<? extends OperatorDesc>> opList =
        new ArrayList<Operator<? extends OperatorDesc>>();

      if (work.getReducer() != null) {
        opList.add(work.getReducer());
      }

      for (List<String> ls : pa.values()) {
        for (String a : ls) {
          opList.add(work.getAliasToWork().get(a));

          while (!opList.isEmpty()) {
            Operator<? extends OperatorDesc> op = opList.remove(0);

            if (op instanceof FileSinkOperator) {
              FileSinkDesc fdesc = ((FileSinkOperator) op).getConf();
              String tempDir = fdesc.getDirName();

              if (tempDir != null) {
                Path tempPath = Utilities.toTempPath(new Path(tempDir));
                LOG.info("Making Temp Directory: " + tempDir);
                FileSystem fs = tempPath.getFileSystem(job);
                fs.mkdirs(tempPath);
              }
            }

            if (op.getChildOperators() != null) {
              opList.addAll(op.getChildOperators());
            }
          }
        }
      }
    }
  }

   /**
   * Execute a query plan using Hadoop.
   */
  @Override
  public int execute(DriverContext driverContext) {

    IOPrepareCache ioPrepareCache = IOPrepareCache.get();
    ioPrepareCache.clear();

    boolean success = true;

    String invalidReason = work.isInvalid();
    if (invalidReason != null) {
      throw new RuntimeException("Plan invalid, Reason: " + invalidReason);
    }

    Context ctx = driverContext.getCtx();
    boolean ctxCreated = false;
    String emptyScratchDirStr;
    Path emptyScratchDir;

    try {
      if (ctx == null) {
        ctx = new Context(job);
        ctxCreated = true;
      }

      emptyScratchDirStr = ctx.getMRTmpFileURI();
      emptyScratchDir = new Path(emptyScratchDirStr);
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

    if (work.getNumMapTasks() != null) {
      job.setNumMapTasks(work.getNumMapTasks().intValue());
    }

    if (work.getMaxSplitSize() != null) {
      HiveConf.setLongVar(job, HiveConf.ConfVars.MAPREDMAXSPLITSIZE, work.getMaxSplitSize().longValue());
    }

    if (work.getMinSplitSize() != null) {
      HiveConf.setLongVar(job, HiveConf.ConfVars.MAPREDMINSPLITSIZE, work.getMinSplitSize().longValue());
    }

    if (work.getMinSplitSizePerNode() != null) {
      HiveConf.setLongVar(job, HiveConf.ConfVars.MAPREDMINSPLITSIZEPERNODE, work.getMinSplitSizePerNode().longValue());
    }

    if (work.getMinSplitSizePerRack() != null) {
      HiveConf.setLongVar(job, HiveConf.ConfVars.MAPREDMINSPLITSIZEPERRACK, work.getMinSplitSizePerRack().longValue());
    }

    job.setNumReduceTasks(work.getNumReduceTasks().intValue());
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

    if (getWork().isUseBucketizedHiveInputFormat()) {
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
    RunningJob rj = null;
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
      MapredLocalWork localwork = work.getMapLocalWork();
      if (localwork != null) {
        if (!ShimLoader.getHadoopShims().isLocalMode(job)) {
          Path localPath = new Path(localwork.getTmpFileURI());
          Path hdfsPath = new Path(work.getTmpHDFSFileURI());

          FileSystem hdfs = hdfsPath.getFileSystem(job);
          FileSystem localFS = localPath.getFileSystem(job);
          FileStatus[] hashtableFiles = localFS.listStatus(localPath);
          int fileNumber = hashtableFiles.length;
          String[] fileNames = new String[fileNumber];

          for ( int i = 0; i < fileNumber; i++){
            fileNames[i] = hashtableFiles[i].getPath().getName();
          }

          //package and compress all the hashtable files to an archive file
          String parentDir = localPath.toUri().getPath();
          String stageId = this.getId();
          String archiveFileURI = Utilities.generateTarURI(parentDir, stageId);
          String archiveFileName = Utilities.generateTarFileName(stageId);
          localwork.setStageID(stageId);

          CompressionUtils.tar(parentDir, fileNames,archiveFileName);
          Path archivePath = new Path(archiveFileURI);
          LOG.info("Archive "+ hashtableFiles.length+" hash table files to " + archiveFileURI);

          //upload archive file to hdfs
          String hdfsFile =Utilities.generateTarURI(hdfsPath, stageId);
          Path hdfsFilePath = new Path(hdfsFile);
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

      addInputPaths(job, work, emptyScratchDirStr, ctx);

      Utilities.setMapRedWork(job, work, ctx.getMRTmpFileURI());
      // remove the pwd from conf file so that job tracker doesn't show this
      // logs
      String pwd = HiveConf.getVar(job, HiveConf.ConfVars.METASTOREPWD);
      if (pwd != null) {
        HiveConf.setVar(job, HiveConf.ConfVars.METASTOREPWD, "HIVE");
      }
      JobClient jc = new JobClient(job);
      // make this client wait if job trcker is not behaving well.
      Throttle.checkJobTracker(job, LOG);

      if (work.isGatheringStats()) {
        // initialize stats publishing table
        StatsPublisher statsPublisher;
        String statsImplementationClass = HiveConf.getVar(job, HiveConf.ConfVars.HIVESTATSDBCLASS);
        if (StatsFactory.setImplementation(statsImplementationClass, job)) {
          statsPublisher = StatsFactory.getStatsPublisher();
          if (!statsPublisher.init(job)) { // creating stats table if not exists
            if (HiveConf.getBoolVar(job, HiveConf.ConfVars.HIVE_STATS_RELIABLE)) {
              throw
                new HiveException(ErrorMsg.STATSPUBLISHER_INITIALIZATION_ERROR.getErrorCodedMsg());
            }
          }
        }
      }

      this.createTmpDirs();

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
      Utilities.clearMapRedWork(job);
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
        JobCloseFeedBack feedBack = new JobCloseFeedBack();
        if (work.getAliasToWork() != null) {
          for (Operator<? extends OperatorDesc> op : work.getAliasToWork().values()) {
            op.jobClose(job, success, feedBack);
          }
        }
        if (work.getReducer() != null) {
          work.getReducer().jobClose(job, success, feedBack);
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

  /**
   * Set hive input format, and input format file if necessary.
   */
  protected void setInputAttributes(Configuration conf) {
    if (work.getInputformat() != null) {
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVEINPUTFORMAT, work.getInputformat());
    }
    if (work.getIndexIntermediateFile() != null) {
      conf.set("hive.index.compact.file", work.getIndexIntermediateFile());
      conf.set("hive.index.blockfilter.file", work.getIndexIntermediateFile());
    }

    // Intentionally overwrites anything the user may have put here
    conf.setBoolean("hive.input.format.sorted", work.isInputFormatSorted());
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
    URL hive_l4j = ExecDriver.class.getClassLoader().getResource(LogUtils.HIVE_EXEC_L4J);
    if (hive_l4j == null) {
      hive_l4j = ExecDriver.class.getClassLoader().getResource(LogUtils.HIVE_L4J);
    }

    if (hive_l4j != null) {
      // setting queryid so that log4j configuration can use it to generate
      // per query log file
      System.setProperty(HiveConf.ConfVars.HIVEQUERYID.toString(), HiveConf.getVar(conf,
          HiveConf.ConfVars.HIVEQUERYID));
      LogManager.resetConfiguration();
      PropertyConfigurator.configure(hive_l4j);
    }
  }

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

    boolean isSilent = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVESESSIONSILENT);

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
      MapredLocalWork plan = Utilities.deserializeMapRedLocalWork(pathData, conf);
      MapredLocalTask ed = new MapredLocalTask(plan, conf, isSilent);
      ret = ed.executeFromChildJVM(new DriverContext());

    } else {
      MapredWork plan = Utilities.deserializeMapRedWork(pathData, conf);
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
    Path hConfFilePath = new Path(ctx.getLocalTmpFileURI(), JOBCONF_FILENAME);
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

      tempConf.set(oneProp, deltaP.getProperty(oneProp));
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
    return getWork().getAliasToWork().values();
  }

  @Override
  public boolean hasReduce() {
    MapredWork w = getWork();
    return w.getReducer() != null;
  }

  /**
   * Handle a empty/null path for a given alias.
   */
  private static int addInputPath(String path, JobConf job, MapredWork work, String hiveScratchDir,
      int numEmptyPaths, boolean isEmptyPath, String alias) throws Exception {
    // either the directory does not exist or it is empty
    assert path == null || isEmptyPath;

    // The input file does not exist, replace it by a empty file
    Class<? extends HiveOutputFormat> outFileFormat = null;
    boolean nonNative = true;
    boolean oneRow = false;
    Properties props;
    if (isEmptyPath) {
      PartitionDesc partDesc = work.getPathToPartitionInfo().get(path);
      props = partDesc.getProperties();
      outFileFormat = partDesc.getOutputFileFormatClass();
      nonNative = partDesc.getTableDesc().isNonNative();
      oneRow = partDesc.getInputFileFormatClass() == OneNullRowInputFormat.class;
    } else {
      TableDesc tableDesc = work.getAliasToPartnInfo().get(alias).getTableDesc();
      props = tableDesc.getProperties();
      outFileFormat = tableDesc.getOutputFileFormatClass();
      nonNative = tableDesc.isNonNative();
    }

    if (nonNative) {
      FileInputFormat.addInputPaths(job, path);
      LOG.info("Add a non-native table " + path);
      return numEmptyPaths;
    }

    // create a dummy empty file in a new directory
    String newDir = hiveScratchDir + File.separator + (++numEmptyPaths);
    Path newPath = new Path(newDir);
    FileSystem fs = newPath.getFileSystem(job);
    fs.mkdirs(newPath);
    //Qualify the path against the filesystem. The user configured path might contain default port which is skipped
    //in the file status. This makes sure that all paths which goes into PathToPartitionInfo are always listed status
    //filepath.
    newPath = fs.makeQualified(newPath);
    String newFile = newDir + File.separator + "emptyFile";
    Path newFilePath = new Path(newFile);

    LOG.info("Changed input file to " + newPath.toString());

    // toggle the work

    LinkedHashMap<String, ArrayList<String>> pathToAliases = work.getPathToAliases();

    if (isEmptyPath) {
      assert path != null;
      pathToAliases.put(newPath.toUri().toString(), pathToAliases.get(path));
      pathToAliases.remove(path);
    } else {
      assert path == null;
      ArrayList<String> newList = new ArrayList<String>();
      newList.add(alias);
      pathToAliases.put(newPath.toUri().toString(), newList);
    }

    work.setPathToAliases(pathToAliases);

    LinkedHashMap<String, PartitionDesc> pathToPartitionInfo = work.getPathToPartitionInfo();
    if (isEmptyPath) {
      pathToPartitionInfo.put(newPath.toUri().toString(), pathToPartitionInfo.get(path));
      pathToPartitionInfo.remove(path);
    } else {
      PartitionDesc pDesc = work.getAliasToPartnInfo().get(alias).clone();
      pathToPartitionInfo.put(newPath.toUri().toString(), pDesc);
    }
    work.setPathToPartitionInfo(pathToPartitionInfo);

    String onefile = newPath.toString();
    RecordWriter recWriter = outFileFormat.newInstance().getHiveRecordWriter(job, newFilePath,
        Text.class, false, props, null);
    if (oneRow) {
      // empty files are ommited at CombineHiveInputFormat.
      // for metadata only query, it effectively makes partition columns disappear..
      // this could be fixed by other methods, but this seemed to be the most easy (HIVEV-2955)
      recWriter.write(new Text("empty"));  // written via HiveIgnoreKeyTextOutputFormat
    }
    recWriter.close(false);
    FileInputFormat.addInputPaths(job, onefile);
    return numEmptyPaths;
  }

  public static void addInputPaths(JobConf job, MapredWork work, String hiveScratchDir, Context ctx)
      throws Exception {
    int numEmptyPaths = 0;

    Set<String> pathsProcessed = new HashSet<String>();
    List<String> pathsToAdd = new LinkedList<String>();
    // AliasToWork contains all the aliases
    for (String oneAlias : work.getAliasToWork().keySet()) {
      LOG.info("Processing alias " + oneAlias);
      List<String> emptyPaths = new ArrayList<String>();

      // The alias may not have any path
      String path = null;
      for (String onefile : work.getPathToAliases().keySet()) {
        List<String> aliases = work.getPathToAliases().get(onefile);
        if (aliases.contains(oneAlias)) {
          path = onefile;

          // Multiple aliases can point to the same path - it should be
          // processed only once
          if (pathsProcessed.contains(path)) {
            continue;
          }

          pathsProcessed.add(path);

          LOG.info("Adding input file " + path);
          if (Utilities.isEmptyPath(job, path, ctx)) {
            emptyPaths.add(path);
          } else {
            pathsToAdd.add(path);
          }
        }
      }

      // Create a empty file if the directory is empty
      for (String emptyPath : emptyPaths) {
        numEmptyPaths = addInputPath(emptyPath, job, work, hiveScratchDir, numEmptyPaths, true,
            oneAlias);
      }

      // If the query references non-existent partitions
      // We need to add a empty file, it is not acceptable to change the
      // operator tree
      // Consider the query:
      // select * from (select count(1) from T union all select count(1) from
      // T2) x;
      // If T is empty and T2 contains 100 rows, the user expects: 0, 100 (2
      // rows)
      if (path == null) {
        numEmptyPaths = addInputPath(null, job, work, hiveScratchDir, numEmptyPaths, false,
            oneAlias);
      }
    }
    setInputPaths(job, pathsToAdd);
  }

  private static void setInputPaths(JobConf job, List<String> pathsToAdd) {
    Path[] addedPaths = FileInputFormat.getInputPaths(job);
    List<Path> toAddPathList = new ArrayList<Path>();
    if(addedPaths != null) {
      for(Path added: addedPaths) {
        toAddPathList.add(added);
      }
    }
    for(String toAdd: pathsToAdd) {
      toAddPathList.add(new Path(toAdd));
    }
    FileInputFormat.setInputPaths(job, toAddPathList.toArray(new Path[0]));
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
  protected void localizeMRTmpFilesImpl(Context ctx) {

    // localize any map-reduce input paths
    ctx.localizeKeys((Map<String, Object>) ((Object) work.getPathToAliases()));
    ctx.localizeKeys((Map<String, Object>) ((Object) work.getPathToPartitionInfo()));

    // localize any input paths for maplocal work
    MapredLocalWork l = work.getMapLocalWork();
    if (l != null) {
      Map<String, FetchWork> m = l.getAliasToFetchWork();
      if (m != null) {
        for (FetchWork fw : m.values()) {
          String s = fw.getTblDir();
          if ((s != null) && ctx.isMRTmpFileURI(s)) {
            fw.setTblDir(ctx.localizeMRTmpFileURI(s));
          }
        }
      }
    }

    // fix up outputs
    Map<String, ArrayList<String>> pa = work.getPathToAliases();
    if (pa != null) {
      for (List<String> ls : pa.values()) {
        for (String a : ls) {
          ArrayList<Operator<? extends OperatorDesc>> opList =
            new ArrayList<Operator<? extends OperatorDesc>>();
          opList.add(work.getAliasToWork().get(a));

          while (!opList.isEmpty()) {
            Operator<? extends OperatorDesc> op = opList.remove(0);

            if (op instanceof FileSinkOperator) {
              FileSinkDesc fdesc = ((FileSinkOperator) op).getConf();
              String s = fdesc.getDirName();
              if ((s != null) && ctx.isMRTmpFileURI(s)) {
                fdesc.setDirName(ctx.localizeMRTmpFileURI(s));
              }
              ((FileSinkOperator) op).setConf(fdesc);
            }

            if (op.getChildOperators() != null) {
              opList.addAll(op.getChildOperators());
            }
          }
        }
      }
    }
  }

  @Override
  public void updateCounters(Counters ctrs, RunningJob rj) throws IOException {
    for (Operator<? extends OperatorDesc> op : work.getAliasToWork().values()) {
      op.updateCounters(ctrs);
    }
    if (work.getReducer() != null) {
      work.getReducer().updateCounters(ctrs);
    }
  }

  @Override
  public void logPlanProgress(SessionState ss) throws IOException {
    ss.getHiveHistory().logPlanProgress(queryPlan);
  }
}
