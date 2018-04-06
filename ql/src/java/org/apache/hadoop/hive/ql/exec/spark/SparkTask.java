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

package org.apache.hadoop.hive.ql.exec.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatisticsNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatistic;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatisticGroup;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatistics;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManager;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobStatus;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkStageProgress;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.Lists;
import org.apache.spark.SparkException;

public class SparkTask extends Task<SparkWork> {
  private static final String CLASS_NAME = SparkTask.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private static final LogHelper console = new LogHelper(LOG);
  private PerfLogger perfLogger;
  private static final long serialVersionUID = 1L;
  // The id of the actual Spark job
  private transient int sparkJobID;
  // The id of the JobHandle used to track the actual Spark job
  private transient String sparkJobHandleId;
  private transient SparkStatistics sparkStatistics;
  private transient long submitTime;
  private transient long startTime;
  private transient long finishTime;
  private transient int succeededTaskCount;
  private transient int totalTaskCount;
  private transient int failedTaskCount;
  private transient List<Integer> stageIds;
  private transient SparkJobRef jobRef = null;
  private transient boolean isShutdown = false;
  private transient boolean jobKilled = false;

  @Override
  public void initialize(QueryState queryState, QueryPlan queryPlan, DriverContext driverContext,
      CompilationOpContext opContext) {
    super.initialize(queryState, queryPlan, driverContext, opContext);
  }

  @Override
  public int execute(DriverContext driverContext) {

    int rc = 0;
    perfLogger = SessionState.getPerfLogger();
    SparkSession sparkSession = null;
    SparkSessionManager sparkSessionManager = null;
    try {
      printConfigInfo();
      sparkSessionManager = SparkSessionManagerImpl.getInstance();
      sparkSession = SparkUtilities.getSparkSession(conf, sparkSessionManager);

      SparkWork sparkWork = getWork();
      sparkWork.setRequiredCounterPrefix(getOperatorCounters());

      // Submit the Spark job
      perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_SUBMIT_JOB);
      submitTime = perfLogger.getStartTime(PerfLogger.SPARK_SUBMIT_JOB);
      jobRef = sparkSession.submit(driverContext, sparkWork);
      perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_SUBMIT_JOB);

      // If the driver context has been shutdown (due to query cancellation) kill the Spark job
      if (driverContext.isShutdown()) {
        LOG.warn("Killing Spark job");
        killJob();
        throw new HiveException("Operation is cancelled.");
      }

      // Get the Job Handle id associated with the Spark job
      sparkJobHandleId = jobRef.getJobId();

      // Add Spark job handle id to the Hive History
      addToHistory(Keys.SPARK_JOB_HANDLE_ID, jobRef.getJobId());

      LOG.debug("Starting Spark job with job handle id " + sparkJobHandleId);

      // Get the application id of the Spark app
      jobID = jobRef.getSparkJobStatus().getAppID();

      // Start monitoring the Spark job, returns when the Spark job has completed / failed, or if
      // a timeout occurs
      rc = jobRef.monitorJob();

      // Get the id the Spark job that was launched, returns -1 if no Spark job was launched
      sparkJobID = jobRef.getSparkJobStatus().getJobId();

      // Add Spark job id to the Hive History
      addToHistory(Keys.SPARK_JOB_ID, Integer.toString(sparkJobID));

      // Get the final state of the Spark job and parses its job info
      SparkJobStatus sparkJobStatus = jobRef.getSparkJobStatus();
      getSparkJobInfo(sparkJobStatus, rc);

      if (rc == 0) {
        sparkStatistics = sparkJobStatus.getSparkStatistics();
        printExcessiveGCWarning();
        if (LOG.isInfoEnabled() && sparkStatistics != null) {
          LOG.info(sparkStatisticsToString(sparkStatistics, sparkJobID));
        }
        LOG.info("Successfully completed Spark job[" + sparkJobID + "] with application ID " +
                jobID + " and task ID " + getId());
      } else if (rc == 2) { // Cancel job if the monitor found job submission timeout.
        // TODO: If the timeout is because of lack of resources in the cluster, we should
        // ideally also cancel the app request here. But w/o facilities from Spark or YARN,
        // it's difficult to do it on hive side alone. See HIVE-12650.
        LOG.debug("Failed to submit Spark job with job handle id " + sparkJobHandleId);
        LOG.info("Failed to submit Spark job for application id " + (Strings.isNullOrEmpty(jobID)
                ? "UNKNOWN" : jobID));
        killJob();
      } else if (rc == 4) {
        LOG.info("The spark job or one stage of it has too many tasks" +
            ". Cancelling Spark job " + sparkJobID + " with application ID " + jobID );
        killJob();
      }

      if (this.jobID == null) {
        this.jobID = sparkJobStatus.getAppID();
      }
      sparkJobStatus.cleanup();
    } catch (Exception e) {
      String msg = "Failed to execute spark task, with exception '" + Utilities.getNameMessage(e) + "'";

      // Has to use full name to make sure it does not conflict with
      // org.apache.commons.lang.StringUtils
      console.printError(msg, "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      LOG.error(msg, e);
      setException(e);
      if (e instanceof HiveException) {
        HiveException he = (HiveException) e;
        rc = he.getCanonicalErrorMsg().getErrorCode();
      } else {
        rc = 1;
      }
    } finally {
      startTime = perfLogger.getEndTime(PerfLogger.SPARK_SUBMIT_TO_RUNNING);
      // The startTime may not be set if the sparkTask finished too fast,
      // because SparkJobMonitor will sleep for 1 second then check the state,
      // right after sleep, the spark job may be already completed.
      // In this case, set startTime the same as submitTime.
      if (startTime < submitTime) {
        startTime = submitTime;
      }
      finishTime = perfLogger.getEndTime(PerfLogger.SPARK_RUN_JOB);
      Utilities.clearWork(conf);
      if (sparkSession != null && sparkSessionManager != null) {
        rc = close(rc);
        try {
          sparkSessionManager.returnSession(sparkSession);
        } catch (HiveException ex) {
          LOG.error("Failed to return the session to SessionManager", ex);
        }
      }
    }
    return rc;
  }

  /**
   * Use the Spark metrics and calculate how much task executione time was spent performing GC
   * operations. If more than a defined threshold of time is spent, print out a warning on the
   * console.
   */
  private void printExcessiveGCWarning() {
    SparkStatisticGroup sparkStatisticGroup = sparkStatistics.getStatisticGroup(
            SparkStatisticsNames.SPARK_GROUP_NAME);
    if (sparkStatisticGroup != null) {
      long taskDurationTime = Long.parseLong(sparkStatisticGroup.getSparkStatistic(
              SparkStatisticsNames.TASK_DURATION_TIME).getValue());
      long jvmGCTime = Long.parseLong(sparkStatisticGroup.getSparkStatistic(
              SparkStatisticsNames.JVM_GC_TIME).getValue());

      // Threshold percentage to trigger the GC warning
      double threshold = 0.1;

      if (jvmGCTime > taskDurationTime * threshold) {
        long percentGcTime = Math.round((double) jvmGCTime / taskDurationTime * 100);
        String gcWarning = String.format("WARNING: Spark Job[%s] Spent %s%% (%s ms / %s ms) of " +
                "task time in GC", sparkJobID, percentGcTime, jvmGCTime, taskDurationTime);
        console.printInfo(gcWarning);
      }
    }
  }

  private void addToHistory(Keys key, String value) {
    if (SessionState.get() != null) {
      SessionState.get().getHiveHistory().setQueryProperty(queryState.getQueryId(), key, value);
    }
  }

  @VisibleForTesting
  static String sparkStatisticsToString(SparkStatistics sparkStatistic, int sparkJobID) {
    StringBuilder sparkStatsString = new StringBuilder();
    sparkStatsString.append("\n\n");
    sparkStatsString.append(String.format("=====Spark Job[%d] Statistics=====", sparkJobID));
    sparkStatsString.append("\n\n");

    Iterator<SparkStatisticGroup> groupIterator = sparkStatistic.getStatisticGroups();
    while (groupIterator.hasNext()) {
      SparkStatisticGroup group = groupIterator.next();
      sparkStatsString.append(group.getGroupName()).append("\n");
      Iterator<SparkStatistic> statisticIterator = group.getStatistics();
      while (statisticIterator.hasNext()) {
        SparkStatistic statistic = statisticIterator.next();
        sparkStatsString.append("\t").append(statistic.getName()).append(": ").append(
                statistic.getValue()).append("\n");
      }
    }
    return sparkStatsString.toString();
  }

  /**
   * Close will move the temp files into the right place for the fetch
   * task. If the job has failed it will clean up the files.
   */
  private int close(int rc) {
    try {
      List<BaseWork> ws = work.getAllWork();
      for (BaseWork w: ws) {
        for (Operator<?> op: w.getAllOperators()) {
          op.jobClose(conf, rc == 0);
        }
      }
    } catch (Exception e) {
      // jobClose needs to execute successfully otherwise fail task
      if (rc == 0) {
        rc = 3;
        String mesg = "Job Commit failed with exception '"
            + Utilities.getNameMessage(e) + "'";
        console.printError(mesg, "\n" + StringUtils.stringifyException(e));
        setException(e);
      }
    }
    return rc;
  }

  @Override
  public void updateTaskMetrics(Metrics metrics) {
    metrics.incrementCounter(MetricsConstant.HIVE_SPARK_TASKS);
  }

  @Override
  public boolean isMapRedTask() {
    return true;
  }

  @Override
  public StageType getType() {
    return StageType.MAPRED;
  }

  @Override
  public String getName() {
    return "SPARK";
  }

  @Override
  public Collection<MapWork> getMapWork() {
    List<MapWork> result = Lists.newArrayList();
    for (BaseWork w : getWork().getRoots()) {
        result.add((MapWork) w);
    }
    return result;
  }

  @Override
  public Operator<? extends OperatorDesc> getReducer(MapWork mapWork) {
    List<BaseWork> children = getWork().getChildren(mapWork);
    if (children.size() != 1) {
      return null;
    }

    if (!(children.get(0) instanceof ReduceWork)) {
      return null;
    }

    return ((ReduceWork) children.get(0)).getReducer();
  }

  public int getSparkJobID() {
    return sparkJobID;
  }

  public SparkStatistics getSparkStatistics() {
    return sparkStatistics;
  }

  public int getSucceededTaskCount() {
    return succeededTaskCount;
  }

  public int getTotalTaskCount() {
    return totalTaskCount;
  }

  public int getFailedTaskCount() {
    return failedTaskCount;
  }

  public List<Integer> getStageIds() {
    return stageIds;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  public long getFinishTime() {
    return finishTime;
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

  private void killJob() {
    LOG.debug("Killing Spark job with job handle id " + sparkJobHandleId);
    boolean needToKillJob = false;
    if (jobRef != null && !jobKilled) {
      synchronized (this) {
        if (!jobKilled) {
          jobKilled = true;
          needToKillJob = true;
        }
      }
    }
    if (needToKillJob) {
      try {
        jobRef.cancelJob();
      } catch (Exception e) {
        LOG.warn("Failed to kill Spark job", e);
      }
    }
  }

  /**
   * Set the number of reducers for the spark work.
   */
  private void printConfigInfo() throws IOException {

    console.printInfo("In order to change the average load for a reducer (in bytes):");
    console.printInfo("  set " + HiveConf.ConfVars.BYTESPERREDUCER.varname + "=<number>");
    console.printInfo("In order to limit the maximum number of reducers:");
    console.printInfo("  set " + HiveConf.ConfVars.MAXREDUCERS.varname + "=<number>");
    console.printInfo("In order to set a constant number of reducers:");
    console.printInfo("  set " + HiveConf.ConfVars.HADOOPNUMREDUCERS + "=<number>");
  }

  private Map<String, List<String>> getOperatorCounters() {
    String groupName = HiveConf.getVar(conf, HiveConf.ConfVars.HIVECOUNTERGROUP);
    Map<String, List<String>> counters = new HashMap<String, List<String>>();
    List<String> hiveCounters = new LinkedList<String>();
    counters.put(groupName, hiveCounters);
    hiveCounters.add(Operator.HIVE_COUNTER_CREATED_FILES);
    // MapOperator is out of SparkWork, SparkMapRecordHandler use it to bridge
    // Spark transformation and Hive operators in SparkWork.
    for (MapOperator.Counter counter : MapOperator.Counter.values()) {
      hiveCounters.add(counter.toString());
    }
    SparkWork sparkWork = this.getWork();
    for (BaseWork work : sparkWork.getAllWork()) {
      for (Operator<? extends OperatorDesc> operator : work.getAllOperators()) {
        if (operator instanceof FileSinkOperator) {
          for (FileSinkOperator.Counter counter : FileSinkOperator.Counter.values()) {
            hiveCounters.add(((FileSinkOperator) operator).getCounterName(counter));
          }
        } else if (operator instanceof ReduceSinkOperator) {
          final String contextName = conf.get(Operator.CONTEXT_NAME_KEY, "");
          for (ReduceSinkOperator.Counter counter : ReduceSinkOperator.Counter.values()) {
            hiveCounters.add(Utilities.getVertexCounterName(counter.name(), contextName));
          }
        } else if (operator instanceof ScriptOperator) {
          for (ScriptOperator.Counter counter : ScriptOperator.Counter.values()) {
            hiveCounters.add(counter.toString());
          }
        } else if (operator instanceof JoinOperator) {
          for (JoinOperator.SkewkeyTableCounter counter : JoinOperator.SkewkeyTableCounter.values()) {
            hiveCounters.add(counter.toString());
          }
        }
      }
    }

    return counters;
  }

  private void getSparkJobInfo(SparkJobStatus sparkJobStatus, int rc) {
    try {
      stageIds = new ArrayList<Integer>();
      int[] ids = sparkJobStatus.getStageIds();
      if (ids != null) {
        for (int stageId : ids) {
          stageIds.add(stageId);
        }
      }
      Map<String, SparkStageProgress> progressMap = sparkJobStatus.getSparkStageProgress();
      int sumTotal = 0;
      int sumComplete = 0;
      int sumFailed = 0;
      for (String s : progressMap.keySet()) {
        SparkStageProgress progress = progressMap.get(s);
        final int complete = progress.getSucceededTaskCount();
        final int total = progress.getTotalTaskCount();
        final int failed = progress.getFailedTaskCount();
        sumTotal += total;
        sumComplete += complete;
        sumFailed += failed;
      }
      succeededTaskCount = sumComplete;
      totalTaskCount = sumTotal;
      failedTaskCount = sumFailed;
      if (rc != 0) {
        Throwable error = sparkJobStatus.getError();
        if (error != null) {
          if ((error instanceof InterruptedException) ||
              (error instanceof HiveException &&
              error.getCause() instanceof InterruptedException)) {
            LOG.info("Killing Spark job since query was interrupted");
            killJob();
          }
          HiveException he;
          if (isOOMError(error)) {
            he = new HiveException(error, ErrorMsg.SPARK_RUNTIME_OOM);
          } else {
            he = new HiveException(error, ErrorMsg.SPARK_JOB_RUNTIME_ERROR);
          }
          setException(he);
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to get Spark job information", e);
    }
  }

  private boolean isOOMError(Throwable error) {
    while (error != null) {
      if (error instanceof OutOfMemoryError) {
        return true;
      } else if (error instanceof SparkException) {
        String sts = Throwables.getStackTraceAsString(error);
        return sts.contains("Container killed by YARN for exceeding memory limits");
      }
      error = error.getCause();
    }
    return false;
  }
}
