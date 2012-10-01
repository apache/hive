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
import java.lang.Exception;
import java.net.MalformedURLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.exec.Operator.ProgressCounter;
import org.apache.hadoop.hive.ql.exec.errors.ErrorAndSolution;
import org.apache.hadoop.hive.ql.exec.errors.TaskLogProcessor;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.plan.ReducerTimeStatsPerJob;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.stats.ClientStatsPublisher;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.LogManager;

public class HadoopJobExecHelper {

  static final private Log LOG = LogFactory.getLog(HadoopJobExecHelper.class.getName());

  protected transient JobConf job;
  protected Task<? extends Serializable> task;

  protected transient int mapProgress = 0;
  protected transient int reduceProgress = 0;
  public transient String jobId;
  private LogHelper console;
  private HadoopJobExecHook callBackObj;

  /**
   * Update counters relevant to this task.
   */
  private void updateCounters(Counters ctrs, RunningJob rj) throws IOException {
    mapProgress = Math.round(rj.mapProgress() * 100);
    reduceProgress = Math.round(rj.reduceProgress() * 100);
    task.taskCounters.put("CNTR_NAME_" + task.getId() + "_MAP_PROGRESS", Long.valueOf(mapProgress));
    task.taskCounters.put("CNTR_NAME_" + task.getId() + "_REDUCE_PROGRESS", Long.valueOf(reduceProgress));
    if (ctrs == null) {
      // hadoop might return null if it cannot locate the job.
      // we may still be able to retrieve the job status - so ignore
      return;
    }
    if(callBackObj != null) {
      callBackObj.updateCounters(ctrs, rj);
    }
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
   * @return the job end message
   */
  public static String getJobEndMsg(String jobId) {
    return "Ended Job = " + jobId;
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


  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }


  public HadoopJobExecHelper() {
  }

  public HadoopJobExecHelper(JobConf job, LogHelper console,
      Task<? extends Serializable> task, HadoopJobExecHook hookCallBack) {
    this.job = job;
    this.console = console;
    this.task = task;
    this.callBackObj = hookCallBack;
  }


  /**
   * A list of the currently running jobs spawned in this Hive instance that is used to kill all
   * running jobs in the event of an unexpected shutdown - i.e., the JVM shuts down while there are
   * still jobs running.
   */
  public static Map<String, String> runningJobKillURIs = Collections
      .synchronizedMap(new HashMap<String, String>());


  /**
   * In Hive, when the user control-c's the command line, any running jobs spawned from that command
   * line are best-effort killed.
   *
   * This static constructor registers a shutdown thread to iterate over all the running job kill
   * URLs and do a get on them.
   *
   */
  static {
    if (new org.apache.hadoop.conf.Configuration()
        .getBoolean("webinterface.private.actions", false)) {
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          killRunningJobs();
        }
      });
    }
  }

  public static void killRunningJobs() {
    synchronized (runningJobKillURIs) {
      for (String uri : runningJobKillURIs.values()) {
        try {
          System.err.println("killing job with: " + uri);
          java.net.HttpURLConnection conn = (java.net.HttpURLConnection) new java.net.URL(uri)
               .openConnection();
          conn.setRequestMethod("POST");
          int retCode = conn.getResponseCode();
          if (retCode != 200) {
            System.err.println("Got an error trying to kill job with URI: " + uri + " = "
                + retCode);
          }
        } catch (Exception e) {
          System.err.println("trying to kill job, caught: " + e);
          // do nothing
        }
      }
    }
  }

  public boolean checkFatalErrors(Counters ctrs, StringBuilder errMsg) {
    if (ctrs == null) {
      // hadoop might return null if it cannot locate the job.
      // we may still be able to retrieve the job status - so ignore
      return false;
    }
    // check for number of created files
    long numFiles = ctrs.getCounter(ProgressCounter.CREATED_FILES);
    long upperLimit = HiveConf.getLongVar(job, HiveConf.ConfVars.MAXCREATEDFILES);
    if (numFiles > upperLimit) {
      errMsg.append("total number of created files now is " + numFiles + ", which exceeds ").append(upperLimit);
      return true;
    }
    return this.callBackObj.checkFatalErrors(ctrs, errMsg);
  }

  private MapRedStats progress(ExecDriverTaskHandle th) throws IOException {
    JobClient jc = th.getJobClient();
    RunningJob rj = th.getRunningJob();
    String lastReport = "";
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    //DecimalFormat longFormatter = new DecimalFormat("###,###");
    long reportTime = System.currentTimeMillis();
    long maxReportInterval =
        HiveConf.getLongVar(job, HiveConf.ConfVars.HIVE_LOG_INCREMENTAL_PLAN_PROGRESS_INTERVAL);
    boolean fatal = false;
    StringBuilder errMsg = new StringBuilder();
    long pullInterval = HiveConf.getLongVar(job, HiveConf.ConfVars.HIVECOUNTERSPULLINTERVAL);
    boolean initializing = true;
    boolean initOutputPrinted = false;
    long cpuMsec = -1;
    int numMap = -1;
    int numReduce = -1;
    List<ClientStatsPublisher> clientStatPublishers = getClientStatPublishers();

    while (!rj.isComplete()) {
      try {
        Thread.sleep(pullInterval);
      } catch (InterruptedException e) {
      }

      if (initializing && ShimLoader.getHadoopShims().isJobPreparing(rj)) {
        // No reason to poll untill the job is initialized
        continue;
      } else {
        // By now the job is initialized so no reason to do
        // rj.getJobState() again and we do not want to do an extra RPC call
        initializing = false;
      }

      if (!initOutputPrinted) {
        SessionState ss = SessionState.get();

        String logMapper;
        String logReducer;

        TaskReport[] mappers = jc.getMapTaskReports(rj.getJobID());
        if (mappers == null) {
          logMapper = "no information for number of mappers; ";
        } else {
          numMap = mappers.length;
          if (ss != null) {
            ss.getHiveHistory().setTaskProperty(SessionState.get().getQueryId(), getId(),
              Keys.TASK_NUM_MAPPERS, Integer.toString(numMap));
          }
          logMapper = "number of mappers: " + numMap + "; ";
        }

        TaskReport[] reducers = jc.getReduceTaskReports(rj.getJobID());
        if (reducers == null) {
          logReducer = "no information for number of reducers. ";
        } else {
          numReduce = reducers.length;
          if (ss != null) {
            ss.getHiveHistory().setTaskProperty(SessionState.get().getQueryId(), getId(),
              Keys.TASK_NUM_REDUCERS, Integer.toString(numReduce));
          }
          logReducer = "number of reducers: " + numReduce;
        }

        console
            .printInfo("Hadoop job information for " + getId() + ": " + logMapper + logReducer);
        initOutputPrinted = true;
      }

      RunningJob newRj = jc.getJob(rj.getJobID());
      if (newRj == null) {
        // under exceptional load, hadoop may not be able to look up status
        // of finished jobs (because it has purged them from memory). From
        // hive's perspective - it's equivalent to the job having failed.
        // So raise a meaningful exception
        throw new IOException("Could not find status of job:" + rj.getJobID());
      } else {
        th.setRunningJob(newRj);
        rj = newRj;
      }

      // If fatal errors happen we should kill the job immediately rather than
      // let the job retry several times, which eventually lead to failure.
      if (fatal) {
        continue; // wait until rj.isComplete
      }

      Counters ctrs = th.getCounters();

      if (fatal = checkFatalErrors(ctrs, errMsg)) {
        console.printError("[Fatal Error] " + errMsg.toString() + ". Killing the job.");
        rj.killJob();
        continue;
      }
      errMsg.setLength(0);

      updateCounters(ctrs, rj);

      // Prepare data for Client Stat Publishers (if any present) and execute them
      if (clientStatPublishers.size() > 0 && ctrs != null) {
        Map<String, Double> exctractedCounters = extractAllCounterValues(ctrs);
        for (ClientStatsPublisher clientStatPublisher : clientStatPublishers) {
          try {
            clientStatPublisher.run(exctractedCounters, rj.getID().toString());
          } catch (RuntimeException runtimeException) {
            LOG.error("Exception " + runtimeException.getClass().getCanonicalName()
                + " thrown when running clientStatsPublishers. The stack trace is: ",
                runtimeException);
          }
        }
      }

      String report = " " + getId() + " map = " + mapProgress + "%,  reduce = " + reduceProgress
          + "%";


      if (!report.equals(lastReport)
          || System.currentTimeMillis() >= reportTime + maxReportInterval) {
        // find out CPU msecs
        // In the case that we can't find out this number, we just skip the step to print
        // it out.
        if (ctrs != null) {
          Counter counterCpuMsec = ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter",
              "CPU_MILLISECONDS");
          if (counterCpuMsec != null) {
            long newCpuMSec = counterCpuMsec.getValue();
            if (newCpuMSec > 0) {
              cpuMsec = newCpuMSec;
              report += ", Cumulative CPU "
                + (cpuMsec / 1000D) + " sec";
            }
          }
        }

        // write out serialized plan with counters to log file
        // LOG.info(queryPlan);
        String output = dateFormat.format(Calendar.getInstance().getTime()) + report;
        SessionState ss = SessionState.get();
        if (ss != null) {
          ss.getHiveHistory().setTaskCounters(SessionState.get().getQueryId(), getId(), ctrs);
          ss.getHiveHistory().setTaskProperty(SessionState.get().getQueryId(), getId(),
              Keys.TASK_HADOOP_PROGRESS, output);
          if (ss.getConf().getBoolVar(HiveConf.ConfVars.HIVE_LOG_INCREMENTAL_PLAN_PROGRESS)) {
            ss.getHiveHistory().progressTask(SessionState.get().getQueryId(), this.task);
            this.callBackObj.logPlanProgress(ss);
          }
        }
        console.printInfo(output);
        lastReport = report;
        reportTime = System.currentTimeMillis();
      }
    }

    if (cpuMsec > 0) {
      console.printInfo("MapReduce Total cumulative CPU time: "
          + Utilities.formatMsecToStr(cpuMsec));
    }

    boolean success;

    Counters ctrs = th.getCounters();
    if (fatal) {
      success = false;
    } else {
      // check for fatal error again in case it occurred after
      // the last check before the job is completed
      if (checkFatalErrors(ctrs, errMsg)) {
        console.printError("[Fatal Error] " + errMsg.toString());
        success = false;
      } else {
        SessionState ss = SessionState.get();
        if (ss != null) {
          ss.getHiveHistory().setTaskCounters(SessionState.get().getQueryId(), getId(), ctrs);
        }
        success = rj.isSuccessful();
      }
    }

    if (ctrs != null) {
      Counter counterCpuMsec = ctrs.findCounter("org.apache.hadoop.mapred.Task$Counter",
          "CPU_MILLISECONDS");
      if (counterCpuMsec != null) {
        long newCpuMSec = counterCpuMsec.getValue();
        if (newCpuMSec > cpuMsec) {
          cpuMsec = newCpuMSec;
        }
      }
    }

    MapRedStats mapRedStats = new MapRedStats(numMap, numReduce, cpuMsec, success, rj.getID().toString());
    mapRedStats.setCounters(ctrs);

    // update based on the final value of the counters
    updateCounters(ctrs, rj);

    SessionState ss = SessionState.get();
    if (ss != null) {
      this.callBackObj.logPlanProgress(ss);
    }
    // LOG.info(queryPlan);
    return mapRedStats;
  }

  private String getId() {
    return this.task.getId();
  }

  /**
   * from StreamJob.java.
   */
  public void jobInfo(RunningJob rj) {
    if (ShimLoader.getHadoopShims().isLocalMode(job)) {
      console.printInfo("Job running in-process (local Hadoop)");
    } else {
      if (SessionState.get() != null) {
        SessionState.get().getHiveHistory().setTaskProperty(SessionState.get().getQueryId(),
            getId(), Keys.TASK_HADOOP_ID, rj.getJobID());
      }
      console.printInfo(getJobStartMsg(rj.getJobID()) + ", Tracking URL = "
          + rj.getTrackingURL());
      console.printInfo("Kill Command = " + HiveConf.getVar(job, HiveConf.ConfVars.HADOOPBIN)
          + " job  -kill " + rj.getJobID());
    }
  }

  /**
   * This class contains the state of the running task Going forward, we will return this handle
   * from execute and Driver can split execute into start, monitorProgess and postProcess.
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
  private void showJobFailDebugInfo(JobConf conf, RunningJob rj)
    throws IOException, MalformedURLException {
    // Mapping from task ID to the number of failures
    Map<String, Integer> failures = new HashMap<String, Integer>();
    // Successful task ID's
    Set<String> successes = new HashSet<String>();

    Map<String, TaskInfo> taskIdToInfo = new HashMap<String, TaskInfo>();

    int startIndex = 0;

    console.printError("Error during job, obtaining debugging information...");
    // Loop to get all task completion events because getTaskCompletionEvents
    // only returns a subset per call
    while (true) {
      TaskCompletionEvent[] taskCompletions = rj.getTaskCompletionEvents(startIndex);

      if (taskCompletions == null || taskCompletions.length == 0) {
        break;
      }

      boolean more = true;
      boolean firstError = true;
      for (TaskCompletionEvent t : taskCompletions) {
        // getTaskJobIDs returns Strings for compatibility with Hadoop versions
        // without TaskID or TaskAttemptID
        String[] taskJobIds = ShimLoader.getHadoopShims().getTaskJobIDs(t);

        if (taskJobIds == null) {
          console.printError("Task attempt info is unavailable in this Hadoop version");
          more = false;
          break;
        }

        // For each task completion event, get the associated task id, job id
        // and the logs
        String taskId = taskJobIds[0];
        String jobId = taskJobIds[1];
        if (firstError) {
          console.printError("Examining task ID: " + taskId + " (and more) from job " + jobId);
          firstError = false;
        }

        TaskInfo ti = taskIdToInfo.get(taskId);
        if (ti == null) {
          ti = new TaskInfo(jobId);
          taskIdToInfo.put(taskId, ti);
        }
        // These tasks should have come from the same job.
        assert (ti.getJobId() != null && ti.getJobId().equals(jobId));
        String taskAttemptLogUrl = ShimLoader.getHadoopShims().getTaskAttemptLogUrl(
          conf, t.getTaskTrackerHttp(), t.getTaskId());
        if (taskAttemptLogUrl != null) {
          ti.getLogUrls().add(taskAttemptLogUrl);
        }

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
        String taskUrl = jtUrl + "/taskdetails.jsp?jobid=" + jobId + "&tipid=" + task.toString();

        TaskLogProcessor tlp = new TaskLogProcessor(conf);
        for (String logUrl : ti.getLogUrls()) {
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

        for (ErrorAndSolution e : errors) {
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

  public void localJobDebugger(int exitVal, String taskId) {
    StringBuilder sb = new StringBuilder();
    sb.append("\n");
    sb.append("Task failed!\n");
    sb.append("Task ID:\n  " + taskId + "\n\n");
    sb.append("Logs:\n");
    console.printError(sb.toString());

    for (Appender a : Collections.list((Enumeration<Appender>)
          LogManager.getRootLogger().getAllAppenders())) {
      if (a instanceof FileAppender) {
        console.printError(((FileAppender)a).getFile());
      }
    }
  }

  public int progressLocal(Process runningJob, String taskId) {
    int exitVal = -101;
    try {
      exitVal = runningJob.waitFor(); //TODO: poll periodically
    } catch (InterruptedException e) {
    }

    if (exitVal != 0) {
      console.printError("Execution failed with exit status: " + exitVal);
      console.printError("Obtaining error information");
      if (HiveConf.getBoolVar(job, HiveConf.ConfVars.SHOW_JOB_FAIL_DEBUG_INFO)) {
        // Since local jobs are run sequentially, all relevant information is already available
        // Therefore, no need to fetch job debug info asynchronously
        localJobDebugger(exitVal, taskId);
      }
    } else {
      console.printInfo("Execution completed successfully");
      console.printInfo("Mapred Local Task Succeeded . Convert the Join into MapJoin");
    }
    return exitVal;
  }


  public int progress(RunningJob rj, JobClient jc) throws IOException {
    jobId = rj.getJobID();

    int returnVal = 0;

    // remove the pwd from conf file so that job tracker doesn't show this
    // logs
    String pwd = HiveConf.getVar(job, HiveConf.ConfVars.METASTOREPWD);
    if (pwd != null) {
      HiveConf.setVar(job, HiveConf.ConfVars.METASTOREPWD, "HIVE");
    }

    // replace it back
    if (pwd != null) {
      HiveConf.setVar(job, HiveConf.ConfVars.METASTOREPWD, pwd);
    }

    // add to list of running jobs to kill in case of abnormal shutdown

    runningJobKillURIs.put(rj.getJobID(), rj.getTrackingURL() + "&action=kill");

    ExecDriverTaskHandle th = new ExecDriverTaskHandle(jc, rj);
    jobInfo(rj);
    MapRedStats mapRedStats = progress(th);

    // Not always there is a SessionState. Sometimes ExeDriver is directly invoked
    // for special modes. In that case, SessionState.get() is empty.
    if (SessionState.get() != null) {
      SessionState.get().getLastMapRedStatsList().add(mapRedStats);

      // Computes the skew for all the MapReduce irrespective
      // of Success or Failure
      if (this.task.getQueryPlan() != null) {
        computeReducerTimeStatsPerJob(rj);
      }
    }

    boolean success = mapRedStats.isSuccess();

    String statusMesg = getJobEndMsg(rj.getJobID());
    if (!success) {
      statusMesg += " with errors";
      returnVal = 2;
      console.printError(statusMesg);
      if (HiveConf.getBoolVar(job, HiveConf.ConfVars.SHOW_JOB_FAIL_DEBUG_INFO) ||
          HiveConf.getBoolVar(job, HiveConf.ConfVars.JOB_DEBUG_CAPTURE_STACKTRACES)) {
        try {
          JobDebugger jd;
          if (SessionState.get() != null) {
            jd = new JobDebugger(job, rj, console, SessionState.get().getStackTraces());
          } else {
            jd = new JobDebugger(job, rj, console);
          }
          Thread t = new Thread(jd);
          t.start();
          t.join(HiveConf.getIntVar(job, HiveConf.ConfVars.JOB_DEBUG_TIMEOUT));
          int ec = jd.getErrorCode();
          if (ec > 0) {
            returnVal = ec;
          }
        } catch (InterruptedException e) {
          console.printError("Timed out trying to grab more detailed job failure"
              + " information, please check jobtracker for more info");
        }
      }
    } else {
      console.printInfo(statusMesg);
    }

    return returnVal;
  }


  private void computeReducerTimeStatsPerJob(RunningJob rj) throws IOException {
    TaskCompletionEvent[] taskCompletions = rj.getTaskCompletionEvents(0);
    List<Integer> reducersRunTimes = new ArrayList<Integer>();

    for (TaskCompletionEvent taskCompletion : taskCompletions) {
      String[] taskJobIds = ShimLoader.getHadoopShims().getTaskJobIDs(taskCompletion);
      if (taskJobIds == null) {
        // Task attempt info is unavailable in this Hadoop version");
        continue;
      }
      String taskId = taskJobIds[0];
      if (!taskCompletion.isMapTask()) {
        reducersRunTimes.add(new Integer(taskCompletion.getTaskRunTime()));
      }
    }
    // Compute the reducers run time statistics for the job
    ReducerTimeStatsPerJob reducerTimeStatsPerJob = new ReducerTimeStatsPerJob(reducersRunTimes,
        new String(this.jobId));
    // Adding the reducers run time statistics for the job in the QueryPlan
    this.task.getQueryPlan().getReducerTimeStatsPerJobList().add(reducerTimeStatsPerJob);
    return;
  }


  private Map<String, Double> extractAllCounterValues(Counters counters) {
    Map<String, Double> exctractedCounters = new HashMap<String, Double>();
    for (Counters.Group cg : counters) {
      for (Counter c : cg) {
        exctractedCounters.put(cg.getName() + "::" + c.getName(), new Double(c.getCounter()));
      }
    }
    return exctractedCounters;
  }

  private List<ClientStatsPublisher> getClientStatPublishers() {
    List<ClientStatsPublisher> clientStatsPublishers = new ArrayList<ClientStatsPublisher>();
    String confString = HiveConf.getVar(job, HiveConf.ConfVars.CLIENTSTATSPUBLISHERS);
    confString = confString.trim();
    if (confString.equals("")) {
      return clientStatsPublishers;
    }

    String[] clientStatsPublisherClasses = confString.split(",");

    for (String clientStatsPublisherClass : clientStatsPublisherClasses) {
      try {
        clientStatsPublishers.add((ClientStatsPublisher) Class.forName(
            clientStatsPublisherClass.trim(), true, JavaUtils.getClassLoader()).newInstance());
      } catch (Exception e) {
        LOG.warn(e.getClass().getName() + " occured when trying to create class: "
            + clientStatsPublisherClass.trim() + " implementing ClientStatsPublisher interface");
        LOG.warn("The exception message is: " + e.getMessage());
        LOG.warn("Program will continue, but without this ClientStatsPublisher working");
      }
    }
    return clientStatsPublishers;
  }
}
