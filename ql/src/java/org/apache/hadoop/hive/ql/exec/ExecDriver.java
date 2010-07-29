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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URL;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.exec.errors.ErrorAndSolution;
import org.apache.hadoop.hive.ql.exec.errors.TaskLogProcessor;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.varia.NullAppender;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

/**
 * ExecDriver.
 *
 */
public class ExecDriver extends Task<MapredWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  protected transient JobConf job;
  protected transient int mapProgress = 0;
  protected transient int reduceProgress = 0;
  protected transient boolean success = false; // if job execution is successful

  /**
   * Constructor when invoked from QL.
   */
  public ExecDriver() {
    super();
  }

  protected static String getResourceFiles(Configuration conf,
      SessionState.ResourceType t) {
    // fill in local files to be added to the task environment
    SessionState ss = SessionState.get();
    Set<String> files = (ss == null) ? null : ss.list_resource(t, null);
    if (files != null) {
      List<String> realFiles = new ArrayList<String>(files.size());
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
   * Initialization when invoked from QL.
   */
  @Override
  public void initialize(HiveConf conf, QueryPlan queryPlan,
      DriverContext driverContext) {
    super.initialize(conf, queryPlan, driverContext);
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
    String addedArchives = getResourceFiles(job,
        SessionState.ResourceType.ARCHIVE);
    if (StringUtils.isNotBlank(addedArchives)) {
      HiveConf.setVar(job, ConfVars.HIVEADDEDARCHIVES, addedArchives);
    }
  }

  /**
   * Constructor/Initialization for invocation as independent utility.
   */
  public ExecDriver(MapredWork plan, JobConf job, boolean isSilent) throws HiveException {
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
  private static Map<String, String> runningJobKillURIs =
      Collections.synchronizedMap(new HashMap<String, String>());

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
        @Override
        public void run() {
          synchronized (runningJobKillURIs) {
            for (String uri : runningJobKillURIs.values()) {
              try {
                System.err.println("killing job with: " + uri);
                java.net.HttpURLConnection conn = (java.net.HttpURLConnection) new java.net.URL(
                    uri).openConnection();
                conn.setRequestMethod("POST");
                int retCode = conn.getResponseCode();
                if (retCode != 200) {
                  System.err
                      .println("Got an error trying to kill job with URI: "
                      + uri + " = " + retCode);
                }
              } catch (Exception e) {
                System.err.println("trying to kill job, caught: " + e);
                // do nothing
              }
            }
          }
        }
      });
    }
  }

  /**
   * from StreamJob.java.
   */
  private void jobInfo(RunningJob rj) {
    if (job.get("mapred.job.tracker", "local").equals("local")) {
      console.printInfo("Job running in-process (local Hadoop)");
    } else {
      String hp = job.get("mapred.job.tracker");
      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setTaskProperty(
            SessionState.get().getQueryId(), getId(), Keys.TASK_HADOOP_ID,
            rj.getJobID());
      }
      console.printInfo(ExecDriver.getJobStartMsg(rj.getJobID())
          + ", Tracking URL = " + rj.getTrackingURL());
      console.printInfo("Kill Command = "
          + HiveConf.getVar(job, HiveConf.ConfVars.HADOOPBIN)
          + " job  -Dmapred.job.tracker=" + hp + " -kill " + rj.getJobID());
    }
  }

  /**
   * This class contains the state of the running task Going forward, we will
   * return this handle from execute and Driver can split execute into start,
   * monitorProgess and postProcess.
   */
  private static class ExecDriverTaskHandle extends TaskHandle {
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
      rj = job;
    }

    @Override
    public Counters getCounters() throws IOException {
      return rj.getCounters();
    }
  }

  /**
   * Fatal errors are those errors that cannot be recovered by retries. These
   * are application dependent. Examples of fatal errors include: - the small
   * table in the map-side joins is too large to be feasible to be handled by
   * one mapper. The job should fail and the user should be warned to use
   * regular joins rather than map-side joins. Fatal errors are indicated by
   * counters that are set at execution time. If the counter is non-zero, a
   * fatal error occurred. The value of the counter indicates the error type.
   *
   * @return true if fatal errors happened during job execution, false
   *         otherwise.
   */
  private boolean checkFatalErrors(ExecDriverTaskHandle th, StringBuilder errMsg) {
    RunningJob rj = th.getRunningJob();
    try {
      Counters ctrs = th.getCounters();
      // HIVE-1422
      if (ctrs == null) {
        return false;
      }
      for (Operator<? extends Serializable> op : work.getAliasToWork().values()) {
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
    } catch (IOException e) {
      // this exception can be tolerated
      e.printStackTrace();
      return false;
    }
  }

  private void progress(ExecDriverTaskHandle th) throws IOException {
    JobClient jc = th.getJobClient();
    RunningJob rj = th.getRunningJob();
    String lastReport = "";
    SimpleDateFormat dateFormat = new SimpleDateFormat(
        "yyyy-MM-dd HH:mm:ss,SSS");
    long reportTime = System.currentTimeMillis();
    long maxReportInterval = 60 * 1000; // One minute
    boolean fatal = false;
    StringBuilder errMsg = new StringBuilder();
    long pullInterval = HiveConf.getLongVar(job,
                                        HiveConf.ConfVars.HIVECOUNTERSPULLINTERVAL);
    boolean initializing = true;
    while (!rj.isComplete()) {
      try {
        Thread.sleep(pullInterval);
      } catch (InterruptedException e) {
      }

      if (initializing &&
          ShimLoader.getHadoopShims().isJobPreparing(rj)) {
        // No reason to poll untill the job is initialized
        continue;
      } else {
        // By now the job is initialized so no reason to do
        // rj.getJobState() again and we do not want to do an extra RPC call
        initializing = false;
      }
      th.setRunningJob(jc.getJob(rj.getJobID()));

      // If fatal errors happen we should kill the job immediately rather than
      // let the job retry several times, which eventually lead to failure.
      if (fatal) {
        continue; // wait until rj.isComplete
      }
      if (fatal = checkFatalErrors(th, errMsg)) {
        success = false;
        console.printError("[Fatal Error] " + errMsg.toString()
            + ". Killing the job.");
        rj.killJob();
        continue;
      }
      errMsg.setLength(0);

      updateCounters(th);

      String report = " " + getId() + " map = " + mapProgress + "%,  reduce = "
          + reduceProgress + "%";

      if (!report.equals(lastReport)
          || System.currentTimeMillis() >= reportTime + maxReportInterval) {

        // write out serialized plan with counters to log file
        // LOG.info(queryPlan);
        String output = dateFormat.format(Calendar.getInstance().getTime())
            + report;
        SessionState ss = SessionState.get();
        if (ss != null) {
          ss.getHiveHistory().setTaskCounters(SessionState.get().getQueryId(),
              getId(), rj);
          ss.getHiveHistory().setTaskProperty(SessionState.get().getQueryId(),
              getId(), Keys.TASK_HADOOP_PROGRESS, output);
          ss.getHiveHistory().progressTask(SessionState.get().getQueryId(),
              this);
          ss.getHiveHistory().logPlanProgress(queryPlan);
        }
        console.printInfo(output);
        lastReport = report;
        reportTime = System.currentTimeMillis();
      }
    }
    // check for fatal error again in case it occurred after the last check
    // before the job is completed
    if (!fatal && (fatal = checkFatalErrors(th, errMsg))) {
      console.printError("[Fatal Error] " + errMsg.toString());
      success = false;
    } else {
      success = rj.isSuccessful();
    }

    setDone();
    th.setRunningJob(jc.getJob(rj.getJobID()));
    updateCounters(th);
    SessionState ss = SessionState.get();
    if (ss != null) {
      ss.getHiveHistory().logPlanProgress(queryPlan);
    }
    // LOG.info(queryPlan);
  }

  /**
   * Update counters relevant to this task.
   */
  private void updateCounters(ExecDriverTaskHandle th) throws IOException {
    RunningJob rj = th.getRunningJob();
    mapProgress = Math.round(rj.mapProgress() * 100);
    reduceProgress = Math.round(rj.reduceProgress() * 100);
    taskCounters.put("CNTR_NAME_" + getId() + "_MAP_PROGRESS", Long
        .valueOf(mapProgress));
    taskCounters.put("CNTR_NAME_" + getId() + "_REDUCE_PROGRESS", Long
        .valueOf(reduceProgress));
    Counters ctrs = th.getCounters();
    // HIVE-1422
    if (ctrs == null) {
      return;
    }
    for (Operator<? extends Serializable> op : work.getAliasToWork().values()) {
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
   * Execute a query plan using Hadoop.
   */
  @Override
  public int execute(DriverContext driverContext) {

    success = true;

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

    ShimLoader.getHadoopShims().setNullOutputFormat(job);
    job.setMapperClass(ExecMapper.class);

    job.setMapOutputKeyClass(HiveKey.class);
    job.setMapOutputValueClass(BytesWritable.class);

    try {
      job.setPartitionerClass((Class<? extends Partitioner>)
         (Class.forName(HiveConf.getVar(job, HiveConf.ConfVars.HIVEPARTITIONER))));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e.getMessage());
    }

    if (work.getNumMapTasks() != null) {
      job.setNumMapTasks(work.getNumMapTasks().intValue());
    }
    if (work.getMinSplitSize() != null) {
      HiveConf.setIntVar(job, HiveConf.ConfVars.MAPREDMINSPLITSIZE,
          work.getMinSplitSize().intValue());
    }
    job.setNumReduceTasks(work.getNumReduceTasks().intValue());
    job.setReducerClass(ExecReducer.class);

    if (work.getInputformat() != null) {
      HiveConf.setVar(job, HiveConf.ConfVars.HIVEINPUTFORMAT, work.getInputformat());
    }

    // Turn on speculative execution for reducers
    boolean useSpeculativeExecReducers =
        HiveConf.getBoolVar(job, HiveConf.ConfVars.HIVESPECULATIVEEXECREDUCERS);
    HiveConf.setBoolVar(
      job,
      HiveConf.ConfVars.HADOOPSPECULATIVEEXECREDUCERS,
      useSpeculativeExecReducers);

    String inpFormat = HiveConf.getVar(job, HiveConf.ConfVars.HIVEINPUTFORMAT);
    if ((inpFormat == null) || (!StringUtils.isNotBlank(inpFormat))) {
      inpFormat = ShimLoader.getHadoopShims().getInputFormatClassName();
    }

    LOG.info("Using " + inpFormat);

    try {
      job.setInputFormat((Class<? extends InputFormat>) (Class
          .forName(inpFormat)));
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
      String allJars = StringUtils.isNotBlank(auxJars) ? (StringUtils
          .isNotBlank(addedJars) ? addedJars + "," + auxJars : auxJars)
          : addedJars;
      LOG.info("adding libjars: " + allJars);
      initializeFiles("tmpjars", allJars);
    }

    // Transfer HIVEADDEDFILES to "tmpfiles" so hadoop understands it
    String addedFiles = HiveConf.getVar(job, HiveConf.ConfVars.HIVEADDEDFILES);
    if (StringUtils.isNotBlank(addedFiles)) {
      initializeFiles("tmpfiles", addedFiles);
    }
    // Transfer HIVEADDEDARCHIVES to "tmparchives" so hadoop understands it
    String addedArchives = HiveConf.getVar(job,
        HiveConf.ConfVars.HIVEADDEDARCHIVES);
    if (StringUtils.isNotBlank(addedArchives)) {
      initializeFiles("tmparchives", addedArchives);
    }

    int returnVal = 0;
    RunningJob rj = null, orig_rj = null;

    boolean noName = StringUtils.isEmpty(HiveConf.getVar(job,
        HiveConf.ConfVars.HADOOPJOBNAME));

    if (noName) {
      // This is for a special case to ensure unit tests pass
      HiveConf.setVar(job, HiveConf.ConfVars.HADOOPJOBNAME, "JOB"
          + Utilities.randGen.nextInt());
    }

    try {
      addInputPaths(job, work, emptyScratchDirStr);

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

      orig_rj = rj = jc.submitJob(job);
      // replace it back
      if (pwd != null) {
        HiveConf.setVar(job, HiveConf.ConfVars.METASTOREPWD, pwd);
      }

      // add to list of running jobs to kill in case of abnormal shutdown
      runningJobKillURIs.put(rj.getJobID(), rj.getTrackingURL()
          + "&action=kill");

      ExecDriverTaskHandle th = new ExecDriverTaskHandle(jc, rj);
      jobInfo(rj);
      progress(th); // success status will be setup inside progress

      if (rj == null) {
        // in the corner case where the running job has disappeared from JT
        // memory remember that we did actually submit the job.
        rj = orig_rj;
        success = false;
      }

      String statusMesg = getJobEndMsg(rj.getJobID());
      if (!success) {
        statusMesg += " with errors";
        returnVal = 2;
        console.printError(statusMesg);
        showJobFailDebugInfo(job, rj);
      } else {
        console.printInfo(statusMesg);
      }
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
      console.printError(mesg, "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));

      success = false;
      returnVal = 1;
    } finally {
      Utilities.clearMapRedWork(job);
      try {
        if(ctxCreated)
          ctx.clear();

        if (returnVal != 0 && rj != null) {
          rj.killJob();
        }
        runningJobKillURIs.remove(rj.getJobID());
      } catch (Exception e) {
      }
    }

    // get the list of Dynamic partition paths
    ArrayList<String> dpPaths = new ArrayList<String>();
    try {
      if (rj != null) {
        JobCloseFeedBack feedBack = new JobCloseFeedBack();
        if (work.getAliasToWork() != null) {
          for (Operator<? extends Serializable> op : work.getAliasToWork()
              .values()) {
            op.jobClose(job, success, feedBack);
            ArrayList<Object> dirs = feedBack.get(JobCloseFeedBack.FeedBackType.DYNAMIC_PARTITIONS);
            if (dirs != null) {
              for (Object o: dirs) {
                if (o instanceof String) {
                  dpPaths.add((String)o);
                }
              }
            }
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
        String mesg = "Job Commit failed with exception '"
            + Utilities.getNameMessage(e) + "'";
        console.printError(mesg, "\n"
            + org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    }

    return (returnVal);
  }

  /**
   * This msg pattern is used to track when a job is started.
   *
   * @param jobId
   * @return
   */
  private static String getJobStartMsg(String jobId) {
    return "Starting Job = " + jobId;
  }

  /**
   * this msg pattern is used to track when a job is successfully done.
   *
   * @param jobId
   * @return
   */
  public static String getJobEndMsg(String jobId) {
    return "Ended Job = " + jobId;
  }

  private String getTaskAttemptLogUrl(String taskTrackerHttpAddress,
      String taskAttemptId) {
    return taskTrackerHttpAddress + "/tasklog?taskid=" + taskAttemptId + "&all=true";
  }

  // Used for showJobFailDebugInfo
  private static class TaskInfo {
    String jobId;
    HashSet<String> logUrls;

    public TaskInfo(String jobId) {
      this.jobId = jobId;
      logUrls = new HashSet<String>();
    }
    public void addLogUrl(String logUrl) {
      logUrls.add(logUrl);
    }
    public HashSet<String> getLogUrls() {
      return logUrls;
    }
    public String getJobId() {
      return jobId;
    }
  }

  @SuppressWarnings("deprecation")
  private void showJobFailDebugInfo(JobConf conf, RunningJob rj) throws IOException {
    // Mapping from task ID to the number of failures
    Map<String, Integer> failures = new HashMap<String, Integer>();
    // Successful task ID's
    Set<String> successes = new HashSet<String>();

    Map<String, TaskInfo> taskIdToInfo = new HashMap<String, TaskInfo>();

    int startIndex = 0;

    // Loop to get all task completion events because getTaskCompletionEvents
    // only returns a subset per call
    while (true) {
      TaskCompletionEvent[] taskCompletions =
        rj.getTaskCompletionEvents(startIndex);

      if (taskCompletions == null || taskCompletions.length == 0) {
        break;
      }

      boolean more = true;
      for (TaskCompletionEvent t : taskCompletions) {
        // getTaskJobIDs returns Strings for compatibility with Hadoop versions
        // without TaskID or TaskAttemptID
        String[] taskJobIds = ShimLoader.getHadoopShims().getTaskJobIDs(t);

        if (taskJobIds == null) {
          console.printError("Task attempt info is unavailable in " +
                             "this Hadoop version");
          more = false;
          break;
        }

        // For each task completion event, get the associated task id, job id
        // and the logs
        String taskId = taskJobIds[0];
        String jobId = taskJobIds[1];

        TaskInfo ti = taskIdToInfo.get(taskId);
        if(ti == null) {
          ti = new TaskInfo(jobId);
          taskIdToInfo.put(taskId, ti);
        }
        // These tasks should have come from the same job.
        assert(ti.getJobId() == jobId);
        ti.getLogUrls().add(
            getTaskAttemptLogUrl(t.getTaskTrackerHttp(), t.getTaskId()));

        // If a task failed, then keep track of the total number of failures
        // for that task (typically, a task gets re-run up to 4 times if it
        // fails

        if (t.getTaskStatus() != TaskCompletionEvent.Status.SUCCEEDED) {
          Integer failAttempts = failures.get(taskId);
          if (failAttempts == null) {
            failAttempts = Integer.valueOf(0);
          }
          failAttempts = Integer.valueOf(failAttempts.intValue() + 1);
          failures.put(taskId, failAttempts);
        } else {
          successes.add(taskId);
        }
      }
      if (!more) {
        break;
      }
      startIndex += taskCompletions.length;
    }
    // Remove failures for tasks that succeeded
    for (String task : successes) {
      failures.remove(task);
    }

    if (failures.keySet().size() == 0) {
      return;
    }

    // Find the highest failure count
    int maxFailures = 0;
    for (Integer failCount : failures.values()) {
      if (maxFailures < failCount.intValue()) {
        maxFailures = failCount.intValue();
      }
    }

    // Display Error Message for tasks with the highest failure count
    String jtUrl = JobTrackerURLResolver.getURL(conf);

    for (String task : failures.keySet()) {
      if (failures.get(task).intValue() == maxFailures) {
        TaskInfo ti = taskIdToInfo.get(task);
        String jobId = ti.getJobId();
        String taskUrl = jtUrl + "/taskdetails.jsp?jobid=" + jobId + "&tipid=" +
        task.toString();

        TaskLogProcessor tlp = new TaskLogProcessor(conf);
        for(String logUrl : ti.getLogUrls()) {
          tlp.addTaskAttemptLogUrl(logUrl);
        }

        List<ErrorAndSolution> errors = tlp.getErrors();

        StringBuilder sb = new StringBuilder();
        // We use a StringBuilder and then call printError only once as
        // printError will write to both stderr and the error log file. In
        // situations where both the stderr and the log file output is
        // simultaneously output to a single stream, this will look cleaner.
        sb.append("\n");
        sb.append("Task with the most failures(" + maxFailures + "): \n");
        sb.append("-----\n");
        sb.append("Task ID:\n  " + task + "\n\n");
        sb.append("URL:\n  " + taskUrl + "\n");

        for(ErrorAndSolution e : errors) {
          sb.append("\n");
          sb.append("Possible error:\n  " + e.getError() + "\n\n");
          sb.append("Solution:\n  " + e.getSolution() + "\n");
        }
        sb.append("-----\n");

        console.printError(sb.toString());

        // Only print out one task because that's good enough for debugging.
        break;
      }
    }
    return;

  }

  private static void printUsage() {
    System.err
        .println("ExecDriver -plan <plan-file> [-jobconf k1=v1 [-jobconf k2=v2] ...] "
        + "[-files <file1>[,<file2>] ...]");
    System.exit(1);
  }

  /**
   * we are running the hadoop job via a sub-command. this typically
   * happens when we are running jobs in local mode. the log4j in this
   * mode is controlled as follows:
   * 1. if the admin provides a log4j properties file especially for
   *    execution mode - then we pick that up
   * 2. otherwise - we default to the regular hive log4j properties if
   *    one is supplied
   * 3. if none of the above two apply - we don't do anything - the log4j
   *    properties would likely be determined by hadoop.
   *
   * The intention behind providing a separate option #1 is to be able to
   * collect hive run time logs generated in local mode in a separate
   * (centralized) location if desired. This mimics the behavior of hive
   * run time logs when running against a hadoop cluster where they are available
   * on the tasktracker nodes.
   */

  private static void setupChildLog4j(Configuration conf) {
    URL hive_l4j = ExecDriver.class.getClassLoader().getResource
      (SessionState.HIVE_EXEC_L4J);
    if(hive_l4j == null)
      hive_l4j = ExecDriver.class.getClassLoader().getResource
      (SessionState.HIVE_L4J);

    if (hive_l4j != null) {
        // setting queryid so that log4j configuration can use it to generate
        // per query log file
        System.setProperty
          (HiveConf.ConfVars.HIVEQUERYID.toString(),
           HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYID));
        LogManager.resetConfiguration();
        PropertyConfigurator.configure(hive_l4j);
      }
  }

  public static void main(String[] args) throws IOException, HiveException {

    String planFileName = null;
    ArrayList<String> jobConfArgs = new ArrayList<String>();
    boolean noLog = false;
    String files = null;

    try {
      for (int i = 0; i < args.length; i++) {
        if (args[i].equals("-plan")) {
          planFileName = args[++i];
        } else if (args[i].equals("-jobconf")) {
          jobConfArgs.add(args[++i]);
        } else if (args[i].equals("-nolog")) {
          noLog = true;
        } else if (args[i].equals("-files")) {
          files = args[++i];
        }
      }
    } catch (IndexOutOfBoundsException e) {
      System.err.println("Missing argument to option");
      printUsage();
    }

    JobConf conf = new JobConf(ExecDriver.class);
    StringBuilder sb = new StringBuilder("JobConf:\n");

    for (String one : jobConfArgs) {
      int eqIndex = one.indexOf('=');
      if (eqIndex != -1) {
        try {
          String key = one.substring(0, eqIndex);
          String value = URLDecoder.decode(one.substring(eqIndex + 1),
                                           "UTF-8");
          conf.set(key, value);
          sb.append(key).append("=").append(value).append("\n");
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

    console.printInfo("plan = " + planFileName);

    // log the list of job conf parameters for reference
    LOG.info(sb.toString());

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
        loader = Utilities.addToClassPath(loader, StringUtils.split(auxJars,
                                                                    ","));
      }
      if (StringUtils.isNotBlank(addedJars)) {
        loader = Utilities.addToClassPath(loader, StringUtils.split(
                                                                    addedJars, ","));
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


    MapredWork plan = Utilities.deserializeMapRedWork(pathData, conf);
    ExecDriver ed = new ExecDriver(plan, conf, isSilent);

    int ret = ed.execute(new DriverContext());
    if (ret != 0) {
      System.exit(2);
    }
  }

  /**
   * Given a Hive Configuration object - generate a command line fragment for
   * passing such configuration information to ExecDriver.
   */
  public static String generateCmdLine(HiveConf hconf) {
    try {
      StringBuilder sb = new StringBuilder();
      Properties deltaP = hconf.getChangedProperties();
      boolean hadoopLocalMode = hconf.getVar(HiveConf.ConfVars.HADOOPJT).equals(
          "local");
      String hadoopSysDir = "mapred.system.dir";
      String hadoopWorkDir = "mapred.local.dir";

      for (Object one : deltaP.keySet()) {
        String oneProp = (String) one;

        if (hadoopLocalMode
            && (oneProp.equals(hadoopSysDir) || oneProp.equals(hadoopWorkDir))) {
          continue;
        }

        String oneValue = deltaP.getProperty(oneProp);

        sb.append("-jobconf ");
        sb.append(oneProp);
        sb.append("=");
        sb.append(URLEncoder.encode(oneValue, "UTF-8"));
        sb.append(" ");
      }

      // Multiple concurrent local mode job submissions can cause collisions in
      // working dirs
      // Workaround is to rename map red working dir to a temp dir in such cases

      if (hadoopLocalMode) {
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
    MapredWork w = getWork();
    return w.getReducer() != null;
  }

  /**
   * Handle a empty/null path for a given alias.
   */
  private int addInputPath(String path, JobConf job, MapredWork work,
      String hiveScratchDir, int numEmptyPaths, boolean isEmptyPath,
      String alias) throws Exception {
    // either the directory does not exist or it is empty
    assert path == null || isEmptyPath;

    // The input file does not exist, replace it by a empty file
    Class<? extends HiveOutputFormat> outFileFormat = null;
    boolean nonNative = true;
    if (isEmptyPath) {
      PartitionDesc partDesc = work.getPathToPartitionInfo().get(path);
      outFileFormat = partDesc.getOutputFileFormatClass();
      nonNative = partDesc.getTableDesc().isNonNative();
    } else {
      TableDesc tableDesc =  work.getAliasToPartnInfo().get(alias).getTableDesc();
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
    String newFile = newDir + File.separator + "emptyFile";
    Path newFilePath = new Path(newFile);

    LOG.info("Changed input file to " + newPath.toString());

    // toggle the work
    LinkedHashMap<String, ArrayList<String>> pathToAliases = work
        .getPathToAliases();
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

    LinkedHashMap<String, PartitionDesc> pathToPartitionInfo = work
        .getPathToPartitionInfo();
    if (isEmptyPath) {
      pathToPartitionInfo.put(newPath.toUri().toString(), pathToPartitionInfo
          .get(path));
      pathToPartitionInfo.remove(path);
    } else {
      PartitionDesc pDesc = work.getAliasToPartnInfo().get(alias).clone();
      pathToPartitionInfo.put(newPath.toUri().toString(), pDesc);
    }
    work.setPathToPartitionInfo(pathToPartitionInfo);

    String onefile = newPath.toString();
    RecordWriter recWriter = outFileFormat.newInstance().getHiveRecordWriter(
        job, newFilePath, Text.class, false, new Properties(), null);
    recWriter.close(false);
    FileInputFormat.addInputPaths(job, onefile);
    return numEmptyPaths;
  }

  private void addInputPaths(JobConf job, MapredWork work, String hiveScratchDir)
      throws Exception {
    int numEmptyPaths = 0;

    List<String> pathsProcessed = new ArrayList<String>();

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

          if (!Utilities.isEmptyPath(job, path)) {
            FileInputFormat.addInputPaths(job, path);
          } else {
            emptyPaths.add(path);
          }
        }
      }

      // Create a empty file if the directory is empty
      for (String emptyPath : emptyPaths) {
        numEmptyPaths = addInputPath(emptyPath, job, work, hiveScratchDir,
            numEmptyPaths, true, oneAlias);
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
        numEmptyPaths = addInputPath(null, job, work, hiveScratchDir,
            numEmptyPaths, false, oneAlias);
      }
    }
  }

  @Override
  public int getType() {
    return StageType.MAPRED;
  }

  @Override
  public String getName() {
    return "MAPRED";
  }

  @Override
  protected void localizeMRTmpFilesImpl(Context ctx) {

    // localize any map-reduce input paths
    ctx.localizeKeys((Map<String, Object>)((Object)work.getPathToAliases()));
    ctx.localizeKeys((Map<String, Object>)((Object)work.getPathToPartitionInfo()));

    // localize any input paths for maplocal work
    MapredLocalWork l = work.getMapLocalWork();
    if (l != null) {
      Map<String, FetchWork> m = l.getAliasToFetchWork();
      if (m != null) {
        for (FetchWork fw: m.values()) {
          String s = fw.getTblDir();
          if ((s != null) && ctx.isMRTmpFileURI(s))
            fw.setTblDir(ctx.localizeMRTmpFileURI(s));
        }
      }
    }

    // fix up outputs
    Map<String, ArrayList<String>> pa = work.getPathToAliases();
    if (pa != null) {
      for (List<String> ls: pa.values())
        for (String a: ls) {
          ArrayList<Operator<? extends Serializable>> opList = new
            ArrayList<Operator<? extends Serializable>> ();
          opList.add(work.getAliasToWork().get(a));
          
          while (!opList.isEmpty()) {
            Operator<? extends Serializable> op = opList.remove(0);

            if (op instanceof FileSinkOperator) {
              FileSinkDesc fdesc = ((FileSinkOperator)op).getConf();
              String s = fdesc.getDirName();
              if ((s != null) && ctx.isMRTmpFileURI(s))
                fdesc.setDirName(ctx.localizeMRTmpFileURI(s));
              ((FileSinkOperator)op).setConf(fdesc);
            }

            if (op.getChildOperators() != null)
              opList.addAll(op.getChildOperators());
          }
        }
    }
  }
}
