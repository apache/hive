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

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.net.URI;
import java.net.URLEncoder;
import java.net.URLDecoder;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.io.*;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.varia.NullAppender;

public class ExecDriver extends Task<mapredWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  transient protected JobConf job;
  transient protected int mapProgress = 0;
  transient protected int reduceProgress = 0;

  /**
   * Constructor when invoked from QL
   */
  public ExecDriver() {
    super();
  }

  public static String getResourceFiles(Configuration conf, SessionState.ResourceType t) {
    // fill in local files to be added to the task environment
    SessionState ss = SessionState.get();
    Set<String> files = (ss == null) ? null : ss.list_resource(t, null);
    if (files != null) {
      ArrayList<String> realFiles = new ArrayList<String>(files.size());
      for (String one : files) {
        try {
          realFiles.add(Utilities.realFile(one, conf));
        } catch (IOException e) {
          throw new RuntimeException("Cannot validate file " + one
              + "due to exception: " + e.getMessage(), e);
        }
      }
      return StringUtils.join(realFiles, ",");
    } else {
      return "";
    }
  }

  private void initializeFiles(String prop, String files) {
    if (files != null && files.length() > 0) {
      job.set(prop, files);
      ShimLoader.getHadoopShims().setTmpFiles(prop, files);
    }
  }

  /**
   * Initialization when invoked from QL
   */
  public void initialize(HiveConf conf, QueryPlan queryPlan) {
    super.initialize(conf, queryPlan);
    job = new JobConf(conf, ExecDriver.class);
    // NOTE: initialize is only called if it is in non-local mode.
    // In case it's in non-local mode, we need to move the SessionState files
    // and jars to jobConf.
    // In case it's in local mode, MapRedTask will set the jobConf.
    //
    // "tmpfiles" and "tmpjars" are set by the method ExecDriver.execute(),
    // which will be called by both local and NON-local mode.
    String addedFiles = getResourceFiles(job, SessionState.ResourceType.FILE);
    if (StringUtils.isNotBlank(addedFiles)) {
      HiveConf.setVar(job, ConfVars.HIVEADDEDFILES, addedFiles);
    }
    String addedJars = getResourceFiles(job, SessionState.ResourceType.JAR);
    if (StringUtils.isNotBlank(addedJars)) {
      HiveConf.setVar(job, ConfVars.HIVEADDEDJARS, addedJars);
    }
  }

  /**
   * Constructor/Initialization for invocation as independent utility
   */
  public ExecDriver(mapredWork plan, JobConf job, boolean isSilent)
      throws HiveException {
    setWork(plan);
    this.job = job;
    LOG = LogFactory.getLog(this.getClass().getName());
    console = new LogHelper(LOG, isSilent);
  }

  /**
   * A list of the currently running jobs spawned in this Hive instance that is
   * used to kill all running jobs in the event of an unexpected shutdown -
   * i.e., the JVM shuts down while there are still jobs running.
   */
  public static HashMap<String, String> runningJobKillURIs = new HashMap<String, String>();

  /**
   * In Hive, when the user control-c's the command line, any running jobs
   * spawned from that command line are best-effort killed.
   * 
   * This static constructor registers a shutdown thread to iterate over all the
   * running job kill URLs and do a get on them.
   * 
   */
  static {
    if (new org.apache.hadoop.conf.Configuration().getBoolean(
        "webinterface.private.actions", false)) {
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          for (Iterator<String> elems = runningJobKillURIs.values().iterator(); elems
              .hasNext();) {
            String uri = elems.next();
            try {
              System.err.println("killing job with: " + uri);
              int retCode = ((java.net.HttpURLConnection) new java.net.URL(uri)
                  .openConnection()).getResponseCode();
              if (retCode != 200) {
                System.err.println("Got an error trying to kill job with URI: "
                    + uri + " = " + retCode);
              }
            } catch (Exception e) {
              System.err.println("trying to kill job, caught: " + e);
              // do nothing
            }
          }
        }
      });
    }
  }

  /**
   * from StreamJob.java
   */
  public void jobInfo(RunningJob rj) {
    if (job.get("mapred.job.tracker", "local").equals("local")) {
      console.printInfo("Job running in-process (local Hadoop)");
    } else {
      String hp = job.get("mapred.job.tracker");
      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setTaskProperty(
            SessionState.get().getQueryId(), getId(),
            Keys.TASK_HADOOP_ID, rj.getJobID());
      }
      console.printInfo("Starting Job = " + rj.getJobID() + ", Tracking URL = "
          + rj.getTrackingURL());
      console.printInfo("Kill Command = "
          + HiveConf.getVar(job, HiveConf.ConfVars.HADOOPBIN)
          + " job  -Dmapred.job.tracker=" + hp + " -kill " + rj.getJobID());
    }
  }
  
  /**
   * This class contains the state of the running task
   * Going forward, we will return this handle from execute
   * and Driver can split execute into start, monitorProgess and postProcess
   */
  public static class ExecDriverTaskHandle extends TaskHandle {
    JobClient jc;
    RunningJob rj;
    JobClient getJobClient() {
      return jc;
    }
    RunningJob getRunningJob() {
      return rj;
    }
    public ExecDriverTaskHandle(JobClient jc, RunningJob rj) {
      this.jc = jc;
      this.rj = rj;
    }
    public void setRunningJob(RunningJob job) {
      this.rj = job;
    }
    public Counters getCounters() throws IOException {
      return rj.getCounters();
    }
  }

  public void progress(TaskHandle taskHandle) throws IOException {
    ExecDriverTaskHandle th = (ExecDriverTaskHandle)taskHandle; 
    JobClient jc = th.getJobClient();
    RunningJob rj = th.getRunningJob();
    String lastReport = "";
    SimpleDateFormat dateFormat
        = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
    long reportTime = System.currentTimeMillis();
    long maxReportInterval = 60 * 1000; // One minute
    while (!rj.isComplete()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      th.setRunningJob(jc.getJob(rj.getJobID()));
      updateCounters(th);
      
      String report = " map = " + this.mapProgress + "%,  reduce = " + this.reduceProgress + "%";

      if (!report.equals(lastReport)
          || System.currentTimeMillis() >= reportTime + maxReportInterval) {

        // write out serialized plan with counters to log file
        // LOG.info(queryPlan);
        String output = dateFormat.format(Calendar.getInstance().getTime()) + report;
        SessionState ss = SessionState.get();
        if (ss != null) {
          ss.getHiveHistory().setTaskCounters(
              SessionState.get().getQueryId(), getId(), rj);
          ss.getHiveHistory().setTaskProperty(
              SessionState.get().getQueryId(), getId(),
              Keys.TASK_HADOOP_PROGRESS, output);
          ss.getHiveHistory().progressTask(
              SessionState.get().getQueryId(), this);
          ss.getHiveHistory().logPlanProgress(queryPlan);
        }
        console.printInfo(output);
        lastReport = report;
        reportTime = System.currentTimeMillis();
      }
    }
    setDone();
    th.setRunningJob(jc.getJob(rj.getJobID()));
    updateCounters(th);
    SessionState ss = SessionState.get();
    if (ss != null) {
      ss.getHiveHistory().logPlanProgress(queryPlan);
    }
    //LOG.info(queryPlan);
  }

  /**
   * Estimate the number of reducers needed for this job, based on job input,
   * and configuration parameters.
   * @return the number of reducers.
   */
  public int estimateNumberOfReducers(HiveConf hive, JobConf job, mapredWork work) throws IOException {
    if (hive == null) {
      hive = new HiveConf();
    }
    long bytesPerReducer = hive.getLongVar(HiveConf.ConfVars.BYTESPERREDUCER);
    int maxReducers = hive.getIntVar(HiveConf.ConfVars.MAXREDUCERS);
    long totalInputFileSize = getTotalInputFileSize(job, work);

    LOG.info("BytesPerReducer=" + bytesPerReducer + " maxReducers=" + maxReducers 
        + " totalInputFileSize=" + totalInputFileSize);

    int reducers = (int)((totalInputFileSize + bytesPerReducer - 1) / bytesPerReducer);
    reducers = Math.max(1, reducers);
    reducers = Math.min(maxReducers, reducers);
    return reducers;    
  }

  /**
   * Set the number of reducers for the mapred work.
   */
  protected void setNumberOfReducers() throws IOException {
    // this is a temporary hack to fix things that are not fixed in the compiler
    Integer numReducersFromWork = work.getNumReduceTasks();
    
    if(work.getReducer() == null) {
      console.printInfo("Number of reduce tasks is set to 0 since there's no reduce operator");
      work.setNumReduceTasks(Integer.valueOf(0));
    } else {
      if (numReducersFromWork >= 0) {
        console.printInfo("Number of reduce tasks determined at compile time: " + work.getNumReduceTasks()); 
      } else if (job.getNumReduceTasks() > 0) {
        int reducers = job.getNumReduceTasks();
        work.setNumReduceTasks(reducers);
        console.printInfo("Number of reduce tasks not specified. Defaulting to jobconf value of: " + reducers);
      } else {
        int reducers = estimateNumberOfReducers(conf, job, work);
        work.setNumReduceTasks(reducers);
        console.printInfo("Number of reduce tasks not specified. Estimated from input data size: " + reducers);

      }
      console.printInfo("In order to change the average load for a reducer (in bytes):");
      console.printInfo("  set " + HiveConf.ConfVars.BYTESPERREDUCER.varname + "=<number>");
      console.printInfo("In order to limit the maximum number of reducers:");
      console.printInfo("  set " + HiveConf.ConfVars.MAXREDUCERS.varname + "=<number>");
      console.printInfo("In order to set a constant number of reducers:");
      console.printInfo("  set " + HiveConf.ConfVars.HADOOPNUMREDUCERS + "=<number>");
    }
  }

  /**
   * Calculate the total size of input files.
   * @param job the hadoop job conf.
   * @return the total size in bytes.
   * @throws IOException 
   */
  public long getTotalInputFileSize(JobConf job, mapredWork work) throws IOException {
    long r = 0;
    // For each input path, calculate the total size.
    for (String path: work.getPathToAliases().keySet()) {
      try {
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(job);
        ContentSummary cs = fs.getContentSummary(p);
        r += cs.getLength();
      } catch (IOException e) {
        LOG.info("Cannot get size of " + path + ". Safely ignored.");
      }
    }
    return r;
  }

  /**
   * update counters relevant to this task
   */
  @Override
  public void updateCounters(TaskHandle t) throws IOException {
    ExecDriverTaskHandle th = (ExecDriverTaskHandle)t;
    RunningJob rj = th.getRunningJob();
    this.mapProgress = Math.round(rj.mapProgress() * 100);
    this.reduceProgress = Math.round(rj.mapProgress() * 100);
    taskCounters.put("CNTR_NAME_" + getId() + "_MAP_PROGRESS", Long.valueOf(this.mapProgress));
    taskCounters.put("CNTR_NAME_" + getId() + "_REDUCE_PROGRESS", Long.valueOf(this.reduceProgress));
    Counters ctrs = th.getCounters();
    for (Operator<? extends Serializable> op: work.getAliasToWork().values()) {
      op.updateCounters(ctrs);
    }
    if (work.getReducer() != null) {
      work.getReducer().updateCounters(ctrs);
    }
  }

  public boolean mapStarted() {
    return mapProgress > 0;
  }

  public boolean reduceStarted() {
    return reduceProgress > 0;
  }

  public boolean mapDone() {
    return mapProgress == 100;
  }

  public boolean reduceDone() {
    return reduceProgress == 100;
  }

  
  /**
   * Execute a query plan using Hadoop
   */
  public int execute() {
    try {
      setNumberOfReducers();
    } catch(IOException e) {
      String statusMesg = "IOException while accessing HDFS to estimate the number of reducers: "
        + e.getMessage();
      console.printError(statusMesg, "\n"
                         + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return 1;
    }

    String invalidReason = work.isInvalid();
    if (invalidReason != null) {
      throw new RuntimeException("Plan invalid, Reason: " + invalidReason);
    }


    String hiveScratchDir = HiveConf.getVar(job, HiveConf.ConfVars.SCRATCHDIR);
    Path jobScratchDir = new Path(hiveScratchDir + Utilities.randGen.nextInt());
    FileOutputFormat.setOutputPath(job, jobScratchDir);
    job.setMapperClass(ExecMapper.class);

    job.setMapOutputKeyClass(HiveKey.class);
    job.setMapOutputValueClass(Text.class);

    job.setNumReduceTasks(work.getNumReduceTasks().intValue());
    job.setReducerClass(ExecReducer.class);

    job.setInputFormat(org.apache.hadoop.hive.ql.io.HiveInputFormat.class);

    // No-Op - we don't really write anything here ..
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Transfer HIVEAUXJARS and HIVEADDEDJARS to "tmpjars" so hadoop understands it
    String auxJars = HiveConf.getVar(job, HiveConf.ConfVars.HIVEAUXJARS);
    String addedJars = HiveConf.getVar(job, HiveConf.ConfVars.HIVEADDEDJARS);
    if (StringUtils.isNotBlank(auxJars) || StringUtils.isNotBlank(addedJars)) {
      String allJars = 
        StringUtils.isNotBlank(auxJars)
        ? (StringUtils.isNotBlank(addedJars) ? addedJars + "," + auxJars : auxJars)
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
    RunningJob rj = null, orig_rj = null;
    boolean success = false;

    try {
      addInputPaths(job, work, hiveScratchDir);
      Utilities.setMapRedWork(job, work);

      // remove the pwd from conf file so that job tracker doesn't show this logs
      String pwd = job.get(HiveConf.ConfVars.METASTOREPWD.varname);
      if (pwd != null)
        job.set(HiveConf.ConfVars.METASTOREPWD.varname, "HIVE");
      JobClient jc = new JobClient(job);
      

      // make this client wait if job trcker is not behaving well.
      Throttle.checkJobTracker(job, LOG);

      orig_rj = rj = jc.submitJob(job);
      // replace it back
      if (pwd != null)
        job.set(HiveConf.ConfVars.METASTOREPWD.varname, pwd);

      // add to list of running jobs so in case of abnormal shutdown can kill
      // it.
      runningJobKillURIs.put(rj.getJobID(), rj.getTrackingURL()
          + "&action=kill");

      TaskHandle th = new ExecDriverTaskHandle(jc, rj);
      jobInfo(rj);
      progress(th);

      if (rj == null) {
        // in the corner case where the running job has disappeared from JT memory
        // remember that we did actually submit the job.
        rj = orig_rj;
        success = false;
      } else {
        success = rj.isSuccessful();
      }

      String statusMesg = "Ended Job = " + rj.getJobID();
      if (!success) {
        statusMesg += " with errors";
        returnVal = 2;
        console.printError(statusMesg);
      } else {
        console.printInfo(statusMesg);
      }
    } catch (Exception e) {
      String mesg = " with exception '" + Utilities.getNameMessage(e) + "'";
      if (rj != null) {
        mesg = "Ended Job = " + rj.getJobID() + mesg;
      } else {
        mesg = "Job Submission failed" + mesg;
      }
      // Has to use full name to make sure it does not conflict with
      // org.apache.commons.lang.StringUtils
      console.printError(mesg, "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));

      success = false;
      returnVal = 1;
    } finally {
      Utilities.clearMapRedWork(job);
      try {
        FileSystem fs = jobScratchDir.getFileSystem(job);
        fs.delete(jobScratchDir, true);
        if (returnVal != 0 && rj != null) {
          rj.killJob();
        }
        runningJobKillURIs.remove(rj.getJobID());
      } catch (Exception e) {
      }
    }

    try {
      if (rj != null) {
        if(work.getAliasToWork() != null) {
          for(Operator<? extends Serializable> op:
                work.getAliasToWork().values()) {
            op.jobClose(job, success);
          }
        }
        if(work.getReducer() != null) {
          work.getReducer().jobClose(job, success);
        }
      }
    } catch (Exception e) {
      // jobClose needs to execute successfully otherwise fail task
      if(success) {
        success = false;
        returnVal = 3;
        String mesg = "Job Commit failed with exception '" + Utilities.getNameMessage(e) + "'";
        console.printError(mesg, "\n"
                           + org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    }

    return (returnVal);
  }

  private static void printUsage() {
    System.out
        .println("ExecDriver -plan <plan-file> [-jobconf k1=v1 [-jobconf k2=v2] ...] "
            + "[-files <file1>[,<file2>] ...]");
    System.exit(1);
  }

  public static void main(String[] args) throws IOException, HiveException {

    String planFileName = null;
    ArrayList<String> jobConfArgs = new ArrayList<String>();
    boolean isSilent = false;
    String files = null;

    try {
      for (int i = 0; i < args.length; i++) {
        if (args[i].equals("-plan")) {
          planFileName = args[++i];
          System.out.println("plan = " + planFileName);
        } else if (args[i].equals("-jobconf")) {
          jobConfArgs.add(args[++i]);
        } else if (args[i].equals("-silent")) {
          isSilent = true;
        } else if (args[i].equals("-files")) {
          files = args[++i];
        }
      }
    } catch (IndexOutOfBoundsException e) {
      System.err.println("Missing argument to option");
      printUsage();
    }

    // If started from main(), and isSilent is on, we should not output
    // any logs.
    // To turn the error log on, please set -Dtest.silent=false
    if (isSilent) {
      BasicConfigurator.resetConfiguration();
      BasicConfigurator.configure(new NullAppender());
    }
    
    if (planFileName == null) {
      System.err.println("Must specify Plan File Name");
      printUsage();
    }

    JobConf conf = new JobConf(ExecDriver.class);
    for (String one : jobConfArgs) {
      int eqIndex = one.indexOf('=');
      if (eqIndex != -1) {
        try {
          conf.set(one.substring(0, eqIndex), URLDecoder.decode(one
              .substring(eqIndex + 1), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
          System.err.println("Unexpected error " + e.getMessage()
              + " while encoding " + one.substring(eqIndex + 1));
          System.exit(3);
        }
      }
    }

    if (files != null) {
      conf.set("tmpfiles", files);
    }

    URI pathURI = (new Path(planFileName)).toUri();
    InputStream pathData;
    if (StringUtils.isEmpty(pathURI.getScheme())) {
      // default to local file system
      pathData = new FileInputStream(planFileName);
    } else {
      // otherwise may be in hadoop ..
      FileSystem fs = FileSystem.get(conf);
      pathData = fs.open(new Path(planFileName));
    }

    // workaround for hadoop-17 - libjars are not added to classpath. this
    // affects local
    // mode execution
    boolean localMode = HiveConf.getVar(conf, HiveConf.ConfVars.HADOOPJT)
        .equals("local");
    if (localMode) {
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
        // Also set this to the Thread ContextClassLoader, so new threads will inherit
        // this class loader, and propagate into newly created Configurations by those
        // new threads.
        Thread.currentThread().setContextClassLoader(loader);
      } catch (Exception e) {
        throw new HiveException(e.getMessage(), e);
      }
    }

    mapredWork plan = Utilities.deserializeMapRedWork(pathData, conf);
    ExecDriver ed = new ExecDriver(plan, conf, isSilent);

    int ret = ed.execute();
    if (ret != 0) {
      System.out.println("Job Failed");
      System.exit(2);
    }
  }

  /**
   * Given a Hive Configuration object - generate a command line fragment for
   * passing such configuration information to ExecDriver
   */
  public static String generateCmdLine(HiveConf hconf) {
    try {
      StringBuilder sb = new StringBuilder();
      Properties deltaP = hconf.getChangedProperties();
      boolean localMode = hconf.getVar(HiveConf.ConfVars.HADOOPJT).equals(
          "local");
      String hadoopSysDir = "mapred.system.dir";
      String hadoopWorkDir = "mapred.local.dir";

      for (Object one : deltaP.keySet()) {
        String oneProp = (String) one;

        if (localMode
            && (oneProp.equals(hadoopSysDir) || oneProp.equals(hadoopWorkDir)))
          continue;

        String oneValue = deltaP.getProperty(oneProp);

        sb.append("-jobconf ");
        sb.append(oneProp);
        sb.append("=");
        sb.append(URLEncoder.encode(oneValue, "UTF-8"));
        sb.append(" ");
      }

      // Multiple concurrent local mode job submissions can cause collisions in
      // working dirs
      // Workaround is to rename map red working dir to a temp dir in such a
      // case
      if (localMode) {
        sb.append("-jobconf ");
        sb.append(hadoopSysDir);
        sb.append("=");
        sb.append(URLEncoder.encode(hconf.get(hadoopSysDir) + "/"
            + Utilities.randGen.nextInt(), "UTF-8"));

        sb.append(" ");
        sb.append("-jobconf ");
        sb.append(hadoopWorkDir);
        sb.append("=");
        sb.append(URLEncoder.encode(hconf.get(hadoopWorkDir) + "/"
            + Utilities.randGen.nextInt(), "UTF-8"));
      }

      return sb.toString();
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isMapRedTask() {
    return true;
  }

  @Override
  public boolean hasReduce() {
    mapredWork w = getWork();
    return w.getReducer() != null;
  }

  private boolean isEmptyPath(JobConf job, String path) throws Exception {
    Path dirPath = new Path(path);
    FileSystem inpFs = dirPath.getFileSystem(job);

    if (inpFs.exists(dirPath)) {
      FileStatus[] fStats = inpFs.listStatus(dirPath);
      if (fStats.length > 0)
        return false;
    }
    return true;
  }

  /**
   * Handle a empty/null path for a given alias
   */
  private int addInputPath(String path, JobConf job, mapredWork work, String hiveScratchDir, int numEmptyPaths, boolean isEmptyPath,
                           String alias) 
    throws Exception {
    // either the directory does not exist or it is empty
    assert path == null || isEmptyPath;

    // The input file does not exist, replace it by a empty file
    Class<? extends HiveOutputFormat> outFileFormat = null;
 
    if (isEmptyPath) 
      outFileFormat = work.getPathToPartitionInfo().get(path).getTableDesc().getOutputFileFormatClass();
    else
      outFileFormat = (Class<? extends HiveOutputFormat>)(HiveSequenceFileOutputFormat.class);
    
    String newFile = hiveScratchDir + File.separator + (++numEmptyPaths);
    Path newPath = new Path(newFile);
    LOG.info("Changed input file to " + newPath.toString());
    
    // toggle the work
    LinkedHashMap<String, ArrayList<String>> pathToAliases = work.getPathToAliases();
    if (isEmptyPath) {
      assert path != null;
      pathToAliases.put(newPath.toUri().toString(), pathToAliases.get(path));
      pathToAliases.remove(path);
    }
    else {
      assert path == null;
      ArrayList<String> newList = new ArrayList<String>();
      newList.add(alias);
      pathToAliases.put(newPath.toUri().toString(), newList);
    }

    work.setPathToAliases(pathToAliases);
    
    LinkedHashMap<String,partitionDesc> pathToPartitionInfo = work.getPathToPartitionInfo();
    if (isEmptyPath) {
      pathToPartitionInfo.put(newPath.toUri().toString(), pathToPartitionInfo.get(path));
      pathToPartitionInfo.remove(path);
    }
    else {
      partitionDesc pDesc = work.getAliasToPartnInfo().get(alias).clone();
      Class<? extends InputFormat>      inputFormat = SequenceFileInputFormat.class;
      pDesc.getTableDesc().setInputFileFormatClass(inputFormat);
      pathToPartitionInfo.put(newPath.toUri().toString(), pDesc);
    }
    work.setPathToPartitionInfo(pathToPartitionInfo);
    
    String onefile = newPath.toString();
    RecordWriter recWriter = outFileFormat.newInstance().getHiveRecordWriter(job, newPath, Text.class, false, new Properties(), null);
    recWriter.close(false);
    FileInputFormat.addInputPaths(job, onefile);
    return numEmptyPaths;
  }

  private void addInputPaths(JobConf job, mapredWork work, String hiveScratchDir) throws Exception {
    int numEmptyPaths = 0;

    List<String> pathsProcessed = new ArrayList<String>();

    // AliasToWork contains all the aliases
    for (String oneAlias : work.getAliasToWork().keySet()) {
      LOG.info("Processing alias " + oneAlias);
      List<String> emptyPaths     = new ArrayList<String>();

      // The alias may not have any path 
      String path = null;
      for (String onefile : work.getPathToAliases().keySet()) {
        List<String> aliases = work.getPathToAliases().get(onefile);
        if (aliases.contains(oneAlias)) {
          path = onefile;
      
          // Multiple aliases can point to the same path - it should be processed only once
          if (pathsProcessed.contains(path))
            continue;
          pathsProcessed.add(path);

          LOG.info("Adding input file " + path);

          if (!isEmptyPath(job, path))
            FileInputFormat.addInputPaths(job, path);
          else
            emptyPaths.add(path);
        }
      }

      // Create a empty file if the directory is empty
      for (String emptyPath : emptyPaths)
        numEmptyPaths = addInputPath(emptyPath, job, work, hiveScratchDir, numEmptyPaths, true, oneAlias);

      // If the query references non-existent partitions
      // We need to add a empty file, it is not acceptable to change the operator tree
      // Consider the query:
      //  select * from (select count(1) from T union all select count(1) from T2) x;
      // If T is empty and T2 contains 100 rows, the user expects: 0, 100 (2 rows)
      if (path == null)
        numEmptyPaths = addInputPath(null, job, work, hiveScratchDir, numEmptyPaths, false, oneAlias);
    }
  }
}
