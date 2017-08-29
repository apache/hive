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
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskHandle;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.plan.ReducerTimeStatsPerJob;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.stats.ClientStatsPublisher;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hive.common.util.ShutdownHookManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.slf4j.LoggerFactory;

public class HadoopJobExecHelper {

  static final private org.slf4j.Logger LOG = LoggerFactory.getLogger(HadoopJobExecHelper.class.getName());

  protected transient JobConf job;
  protected Task<? extends Serializable> task;

  protected transient int mapProgress = -1;
  protected transient int reduceProgress = -1;

  protected transient int lastMapProgress;
  protected transient int lastReduceProgress;

  public transient JobID jobId;
  private final LogHelper console;
  private final HadoopJobExecHook callBackObj;
  private final String queryId;

  /**
   * Update counters relevant to this task.
   */
  private void updateCounters(Counters ctrs, RunningJob rj) throws IOException {
    lastMapProgress = mapProgress;
    lastReduceProgress = reduceProgress;
    mapProgress = Math.round(rj.mapProgress() * 100);
    mapProgress = mapProgress == 100 ? (int)Math.floor(rj.mapProgress() * 100) : mapProgress;
    reduceProgress = Math.round(rj.reduceProgress() * 100);
    reduceProgress = reduceProgress == 100 ? (int)Math.floor(rj.reduceProgress() * 100) : reduceProgress;
    task.taskCounters.put("CNTR_NAME_" + task.getId() + "_MAP_PROGRESS", Long.valueOf(mapProgress));
    task.taskCounters.put("CNTR_NAME_" + task.getId() + "_REDUCE_PROGRESS", Long.valueOf(reduceProgress));

    if (SessionState.get() != null) {
      final float progress = (rj.mapProgress() + rj.reduceProgress()) * 0.5f;
      SessionState.get().updateProgressedPercentage(progress);
    }
  }

  /**
   * This msg pattern is used to track when a job is started.
   *
   * @param jobId
   * @return
   */
  private static String getJobStartMsg(JobID jobId) {
    return "Starting Job = " + jobId;
  }

  /**
   * this msg pattern is used to track when a job is successfully done.
   *
   * @param jobId
   * @return the job end message
   */
  public static String getJobEndMsg(JobID jobId) {
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


  public JobID getJobId() {
    return jobId;
  }

  public void setJobId(JobID jobId) {
    this.jobId = jobId;
  }

  public HadoopJobExecHelper(JobConf job, LogHelper console,
      Task<? extends Serializable> task, HadoopJobExecHook hookCallBack) {
    this.queryId = HiveConf.getVar(job, HiveConf.ConfVars.HIVEQUERYID, "unknown-" + System.currentTimeMillis());
    this.job = job;
    this.console = console;
    this.task = task;
    this.callBackObj = hookCallBack;

    if (job != null) {
      // even with tez on some jobs are run as MR. disable the flag in
      // the conf, so that the backend runs fully as MR.
      HiveConf.setVar(job, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
    }
  }


  /**
   * A list of the currently running jobs spawned in this Hive instance that is used to kill all
   * running jobs in the event of an unexpected shutdown - i.e., the JVM shuts down while there are
   * still jobs running.
   */
  public static List<RunningJob> runningJobs = Collections
      .synchronizedList(new LinkedList<RunningJob>());


  /**
   * In Hive, when the user control-c's the command line, any running jobs spawned from that command
   * line are best-effort killed.
   *
   * This static constructor registers a shutdown thread to iterate over all the running job kill
   * URLs and do a get on them.
   *
   */
  static {
    ShutdownHookManager.addShutdownHook(new Runnable() {
        @Override
        public void run() {
          killRunningJobs();
        }
      });
  }

  public static void killRunningJobs() {
    synchronized (runningJobs) {
      for (RunningJob rj : runningJobs) {
        try {
          System.err.println("killing job with: " + rj.getID());
          rj.killJob();
        } catch (Exception e) {
          LOG.warn("Failed to kill job", e);
          System.err.println("Failed to kill job: "+ rj.getID());
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
    Counters.Counter cntr = ctrs.findCounter(HiveConf.getVar(job, ConfVars.HIVECOUNTERGROUP),
        Operator.HIVECOUNTERCREATEDFILES);
    long numFiles = cntr != null ? cntr.getValue() : 0;
    long upperLimit = HiveConf.getLongVar(job, HiveConf.ConfVars.MAXCREATEDFILES);
    if (numFiles > upperLimit) {
      errMsg.append("total number of created files now is " + numFiles + ", which exceeds ").append(upperLimit);
      return true;
    }
    return this.callBackObj.checkFatalErrors(ctrs, errMsg);
  }

  private MapRedStats progress(ExecDriverTaskHandle th) throws IOException, LockException {
    JobClient jc = th.getJobClient();
    RunningJob rj = th.getRunningJob();
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    //DecimalFormat longFormatter = new DecimalFormat("###,###");
    long reportTime = System.currentTimeMillis();
    long maxReportInterval = HiveConf.getTimeVar(
        job, HiveConf.ConfVars.HIVE_LOG_INCREMENTAL_PLAN_PROGRESS_INTERVAL, TimeUnit.MILLISECONDS);
    boolean fatal = false;
    StringBuilder errMsg = new StringBuilder();
    long pullInterval = HiveConf.getLongVar(job, HiveConf.ConfVars.HIVECOUNTERSPULLINTERVAL);
    boolean initializing = true;
    boolean initOutputPrinted = false;
    long cpuMsec = -1;
    int numMap = -1;
    int numReduce = -1;
    List<ClientStatsPublisher> clientStatPublishers = getClientStatPublishers();
    final boolean localMode = ShimLoader.getHadoopShims().isLocalMode(job);

    while (!rj.isComplete()) {
      if (th.getContext() != null) {
        th.getContext().checkHeartbeaterLockException();
      }

      try {
        Thread.sleep(pullInterval);
      } catch (InterruptedException e) {
      }

      if (initializing && rj.getJobState() == JobStatus.PREP) {
        // No reason to poll untill the job is initialized
        continue;
      } else {
        // By now the job is initialized so no reason to do
        // rj.getJobState() again and we do not want to do an extra RPC call
        initializing = false;
      }

      if (!localMode) {
        if (!initOutputPrinted) {
          SessionState ss = SessionState.get();

          String logMapper;
          String logReducer;
          TaskReport[] mappers = jc.getMapTaskReports(rj.getID());
          if (mappers == null) {
            logMapper = "no information for number of mappers; ";
          } else {
            numMap = mappers.length;
            if (ss != null) {
              ss.getHiveHistory().setTaskProperty(queryId, getId(),
                Keys.TASK_NUM_MAPPERS, Integer.toString(numMap));
            }
            logMapper = "number of mappers: " + numMap + "; ";
          }

          TaskReport[] reducers = jc.getReduceTaskReports(rj.getID());
          if (reducers == null) {
            logReducer = "no information for number of reducers. ";
          } else {
            numReduce = reducers.length;
            if (ss != null) {
              ss.getHiveHistory().setTaskProperty(queryId, getId(),
                Keys.TASK_NUM_REDUCERS, Integer.toString(numReduce));
            }
            logReducer = "number of reducers: " + numReduce;
          }

          console
              .printInfo("Hadoop job information for " + getId() + ": " + logMapper + logReducer);
          initOutputPrinted = true;
        }

        RunningJob newRj = jc.getJob(rj.getID());
        if (newRj == null) {
          // under exceptional load, hadoop may not be able to look up status
          // of finished jobs (because it has purged them from memory). From
          // hive's perspective - it's equivalent to the job having failed.
          // So raise a meaningful exception
          throw new IOException("Could not find status of job:" + rj.getID());
        } else {
          th.setRunningJob(newRj);
          rj = newRj;
        }
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

      if (mapProgress == lastMapProgress && reduceProgress == lastReduceProgress &&
          System.currentTimeMillis() < reportTime + maxReportInterval) {
        continue;
      }
      StringBuilder report = new StringBuilder();
      report.append(dateFormat.format(Calendar.getInstance().getTime()));

      report.append(' ').append(getId());
      report.append(" map = ").append(mapProgress).append("%, ");
      report.append(" reduce = ").append(reduceProgress).append('%');

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
            report.append(", Cumulative CPU ").append((cpuMsec / 1000D)).append(" sec");
          }
        }
      }

      // write out serialized plan with counters to log file
      // LOG.info(queryPlan);
      String output = report.toString();
      SessionState ss = SessionState.get();
      if (ss != null) {
        ss.getHiveHistory().setTaskCounters(queryId, getId(), ctrs);
        ss.getHiveHistory().setTaskProperty(queryId, getId(),
            Keys.TASK_HADOOP_PROGRESS, output);
        if (ss.getConf().getBoolVar(HiveConf.ConfVars.HIVE_LOG_INCREMENTAL_PLAN_PROGRESS)) {
          ss.getHiveHistory().progressTask(queryId, this.task);
          this.callBackObj.logPlanProgress(ss);
        }
      }
      console.printInfo(output);
      task.setStatusMessage(output);
      reportTime = System.currentTimeMillis();
    }

    Counters ctrs = th.getCounters();

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

    if (cpuMsec > 0) {
      String status = "MapReduce Total cumulative CPU time: " + Utilities.formatMsecToStr(cpuMsec);
      console.printInfo(status);
      task.setStatusMessage(status);
    }

    boolean success;

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
          ss.getHiveHistory().setTaskCounters(queryId, getId(), ctrs);
        }
        success = rj.isSuccessful();
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
        SessionState.get().getHiveHistory().setTaskProperty(queryId,
            getId(), Keys.TASK_HADOOP_ID, rj.getID().toString());
      }
      console.printInfo(getJobStartMsg(rj.getID()) + ", Tracking URL = "
          + rj.getTrackingURL());
      console.printInfo("Kill Command = " + HiveConf.getVar(job, HiveConf.ConfVars.HADOOPBIN)
          + " job  -kill " + rj.getID());
    }
  }

  /**
   * This class contains the state of the running task Going forward, we will return this handle
   * from execute and Driver can split execute into start, monitorProgess and postProcess.
   */
  private static class ExecDriverTaskHandle extends TaskHandle {
    JobClient jc;
    RunningJob rj;
    Context ctx;

    JobClient getJobClient() {
      return jc;
    }

    RunningJob getRunningJob() {
      return rj;
    }

    Context getContext() {
      return ctx;
    }

    public ExecDriverTaskHandle(JobClient jc, RunningJob rj, Context ctx) {
      this.jc = jc;
      this.rj = rj;
      this.ctx = ctx;
    }

    public void setRunningJob(RunningJob job) {
      rj = job;
    }

    @Override
    public Counters getCounters() throws IOException {
      return rj.getCounters();
    }
  }


  public void localJobDebugger(int exitVal, String taskId) {
    StringBuilder sb = new StringBuilder();
    sb.append("\n");
    sb.append("Task failed!\n");
    sb.append("Task ID:\n  " + taskId + "\n\n");
    sb.append("Logs:\n");
    console.printError(sb.toString());

    for (Appender appender : ((Logger) LogManager.getRootLogger()).getAppenders().values()) {
      if (appender instanceof FileAppender) {
        console.printError(((FileAppender) appender).getFileName());
      } else if (appender instanceof RollingFileAppender) {
        console.printError(((RollingFileAppender) appender).getFileName());
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
      console.printInfo("MapredLocal task succeeded");
    }
    return exitVal;
  }


  public int progress(RunningJob rj, JobClient jc, Context ctx) throws IOException, LockException {
    jobId = rj.getID();

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

    runningJobs.add(rj);

    ExecDriverTaskHandle th = new ExecDriverTaskHandle(jc, rj, ctx);
    jobInfo(rj);
    MapRedStats mapRedStats = progress(th);

    this.task.taskHandle = th;
    // Not always there is a SessionState. Sometimes ExeDriver is directly invoked
    // for special modes. In that case, SessionState.get() is empty.
    if (SessionState.get() != null) {
      SessionState.get().getMapRedStats().put(getId(), mapRedStats);

      // Computes the skew for all the MapReduce irrespective
      // of Success or Failure
      if (this.task.getQueryPlan() != null) {
        computeReducerTimeStatsPerJob(rj);
      }
    }

    boolean success = mapRedStats.isSuccess();

    String statusMesg = getJobEndMsg(rj.getID());
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
          task.setDiagnosticMessage(jd.getDiagnosticMesg());
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
      if (!taskCompletion.isMapTask()) {
        reducersRunTimes.add(new Integer(taskCompletion.getTaskRunTime()));
      }
    }
    // Compute the reducers run time statistics for the job
    ReducerTimeStatsPerJob reducerTimeStatsPerJob = new ReducerTimeStatsPerJob(reducersRunTimes);
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
            clientStatsPublisherClass.trim(), true, Utilities.getSessionSpecifiedClassLoader()).newInstance());
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
