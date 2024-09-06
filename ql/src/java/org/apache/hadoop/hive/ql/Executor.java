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

package org.apache.hadoop.hive.ql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.cache.results.CacheUsage;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache.CacheEntry;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.TaskResult;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.PrivateHookContext;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

/**
 * Executes the Query Plan.
 */
public class Executor {
  private static final String CLASS_NAME = Driver.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private static final LogHelper CONSOLE = new LogHelper(LOG);

  private final Context context;
  private final DriverContext driverContext;
  private final DriverState driverState;
  private final TaskQueue taskQueue;

  private HookContext hookContext;

  public Executor(Context context, DriverContext driverContext, DriverState driverState, TaskQueue taskQueue) {
    this.context = context;
    this.driverContext = driverContext;
    this.driverState = driverState;
    this.taskQueue = taskQueue;
  }

  public void execute() throws CommandProcessorException {
    SessionState.getPerfLogger().perfLogBegin(CLASS_NAME, PerfLogger.DRIVER_EXECUTE);

    boolean noName = Strings.isNullOrEmpty(driverContext.getConf().get(MRJobConfig.JOB_NAME));

    checkState();

    // Whether there's any error occurred during query execution. Used for query lifetime hook.
    boolean executionError = false;

    try {
      LOG.info("Executing command(queryId=" + driverContext.getQueryId() + "): " + driverContext.getQueryString());

      // TODO: should this use getUserFromAuthenticator?
      hookContext = new PrivateHookContext(driverContext, context);

      preExecutionActions();
      preExecutionCacheActions();
      // Disable HMS cache so any metadata calls during execution get fresh responses.
      driverContext.getQueryState().disableHMSCache();
      runTasks(noName);
      driverContext.getQueryState().enableHMSCache();
      postExecutionCacheActions();
      postExecutionActions();
    } catch (CommandProcessorException cpe) {
      executionError = true;
      throw cpe;
    } catch (Throwable e) {
      executionError = true;
      DriverUtils.checkInterrupted(driverState, driverContext, "during query execution: \n" + e.getMessage(),
          hookContext, SessionState.getPerfLogger());
      handleException(hookContext, e);
    } finally {
      cleanUp(noName, hookContext, executionError);
      driverContext.getQueryState().enableHMSCache();
    }
  }

  private void checkState() throws CommandProcessorException {
    driverState.lock();
    try {
      // if query is not in compiled state, or executing state which is carried over from
      // a combined compile/execute in runInternal, throws the error
      if (!driverState.isCompiled() && !driverState.isExecuting()) {
        String errorMessage = "FAILED: unexpected driverstate: " + driverState + ", for query " +
            driverContext.getQueryString();
        CONSOLE.printError(errorMessage);
        throw DriverUtils.createProcessorException(driverContext, 1000, errorMessage, "HY008", null);
      } else {
        driverState.executing();
      }
    } finally {
      driverState.unlock();
    }
  }

  private void preExecutionActions() throws Exception {
    // compile and execute can get called from different threads in case of HS2
    // so clear timing in this thread's Hive object before proceeding.
    Hive.get().clearMetaCallTiming();

    driverContext.getPlan().setStarted();

    SessionState.get().getHiveHistory().startQuery(driverContext.getQueryString(), driverContext.getQueryId());
    SessionState.get().getHiveHistory().logPlanProgress(driverContext.getPlan());
    driverContext.setResStream(null);

    hookContext.setHookType(HookContext.HookType.PRE_EXEC_HOOK);
    driverContext.getHookRunner().runPreHooks(hookContext);

    // Trigger query hooks before query execution.
    driverContext.getHookRunner().runBeforeExecutionHook(driverContext.getQueryString(), hookContext);

    setQueryDisplays(driverContext.getPlan().getRootTasks());

    // A runtime that launches runnable tasks as separate Threads through TaskRunners
    // As soon as a task isRunnable, it is put in a queue
    // At any time, at most maxthreads tasks can be running
    // The main thread polls the TaskRunners to check if they have finished.

    DriverUtils.checkInterrupted(driverState, driverContext, "before running tasks.", hookContext,
        SessionState.getPerfLogger());

    taskQueue.prepare(driverContext.getPlan());

    context.setHDFSCleanup(true);

    SessionState.get().setMapRedStats(new LinkedHashMap<>());
    SessionState.get().setStackTraces(new HashMap<>());
    SessionState.get().setLocalMapRedErrors(new HashMap<>());

    // Add root Tasks to runnable
    Metrics metrics = MetricsFactory.getInstance();
    for (Task<?> task : driverContext.getPlan().getRootTasks()) {
      // This should never happen, if it does, it's a bug with the potential to produce
      // incorrect results.
      assert task.getParentTasks() == null || task.getParentTasks().isEmpty();
      taskQueue.addToRunnable(task);

      if (metrics != null) {
        task.updateTaskMetrics(metrics);
      }
    }
  }

  private void setQueryDisplays(List<Task<?>> tasks) {
    if (tasks != null) {
      Set<Task<?>> visited = new HashSet<Task<?>>();
      while (!tasks.isEmpty()) {
        tasks = setQueryDisplays(tasks, visited);
      }
    }
  }

  private List<Task<?>> setQueryDisplays(List<Task<?>> tasks, Set<Task<?>> visited) {
    List<Task<?>> childTasks = new ArrayList<>();
    for (Task<?> task : tasks) {
      if (visited.contains(task)) {
        continue;
      }
      task.setQueryDisplay(driverContext.getQueryDisplay());
      if (task.getDependentTasks() != null) {
        childTasks.addAll(task.getDependentTasks());
      }
      visited.add(task);
    }
    return childTasks;
  }

  private void preExecutionCacheActions() throws Exception {
    if (driverContext.getCacheUsage() == null) {
      return;
    }

    if (driverContext.getCacheUsage().getStatus() == CacheUsage.CacheStatus.CAN_CACHE_QUERY_RESULTS &&
        driverContext.getPlan().getFetchTask() != null) {
      ValidTxnWriteIdList txnWriteIdList = null;
      if (driverContext.getPlan().hasAcidReadWrite()) {
        txnWriteIdList = AcidUtils.getValidTxnWriteIdList(driverContext.getConf());
      }
      // The results of this query execution might be cacheable.
      // Add a placeholder entry in the cache so other queries know this result is pending.
      CacheEntry pendingCacheEntry =
          QueryResultsCache.getInstance().addToCache(driverContext.getCacheUsage().getQueryInfo(), txnWriteIdList);
      if (pendingCacheEntry != null) {
        // Update cacheUsage to reference the pending entry.
        this.driverContext.getCacheUsage().setCacheEntry(pendingCacheEntry);
      }
    }
  }

  private void runTasks(boolean noName) throws Exception {
    SessionState.getPerfLogger().perfLogBegin(CLASS_NAME, PerfLogger.RUN_TASKS);

    int jobCount = getJobCount();
    String jobName = getJobName();

    // Loop while you either have tasks running, or tasks queued up
    while (taskQueue.isRunning()) {
      launchTasks(noName, jobCount, jobName);
      handleFinished();
    }

    SessionState.getPerfLogger().perfLogEnd(CLASS_NAME, PerfLogger.RUN_TASKS);
  }

  private void handleFinished() throws Exception {
    // poll the Tasks to see which one completed
    TaskRunner taskRun = taskQueue.pollFinished();
    if (taskRun == null) {
      return;
    }
    /*
      This should be removed eventually. HIVE-17814 gives more detail
      explanation of whats happening and HIVE-17815 as to why this is done.
      Briefly for replication the graph is huge and so memory pressure is going to be huge if
      we keep a lot of references around.
    */
    String opName = driverContext.getPlan().getOperationName();
    boolean isReplicationOperation = opName.equals(HiveOperation.REPLDUMP.getOperationName())
        || opName.equals(HiveOperation.REPLLOAD.getOperationName());
    if (!isReplicationOperation) {
      hookContext.addCompleteTask(taskRun);
    }

    driverContext.getQueryDisplay().setTaskResult(taskRun.getTask().getId(), taskRun.getTaskResult());

    Task<?> task = taskRun.getTask();
    TaskResult result = taskRun.getTaskResult();

    int exitVal = result.getExitVal();
    SessionState.get().getHiveHistory().setTaskProperty(driverContext.getQueryId(), task.getId(),
        Keys.TASK_RET_CODE, String.valueOf(exitVal));
    SessionState.get().getHiveHistory().endTask(driverContext.getQueryId(), task);

    DriverUtils.checkInterrupted(driverState, driverContext, "when checking the execution result.", hookContext,
        SessionState.getPerfLogger());

    if (exitVal != 0) {
      handleTaskFailure(task, result, exitVal);
      return;
    }

    taskQueue.finished(taskRun);

    if (task.getChildTasks() != null) {
      for (Task<?> child : task.getChildTasks()) {
        if (TaskQueue.isLaunchable(child)) {
          taskQueue.addToRunnable(child);
        }
      }
    }
  }

  private String getJobName() {
    int maxlen = driverContext.getConf().getIntVar(HiveConf.ConfVars.HIVE_JOBNAME_LENGTH);
    return Utilities.abbreviate(driverContext.getQueryString(), maxlen - 6);
  }

  private int getJobCount() {
    int mrJobCount = Utilities.getMRTasks(driverContext.getPlan().getRootTasks()).size();
    int jobCount = mrJobCount + Utilities.getTezTasks(driverContext.getPlan().getRootTasks()).size();
    if (jobCount > 0) {
      if (mrJobCount > 0 && "mr".equals(HiveConf.getVar(driverContext.getConf(), ConfVars.HIVE_EXECUTION_ENGINE))) {
        LOG.warn(HiveConf.generateMrDeprecationWarning());
      }
      CONSOLE.printInfo("Query ID = " + driverContext.getPlan().getQueryId());
      CONSOLE.printInfo("Total jobs = " + jobCount);
    }
    if (SessionState.get() != null) {
      SessionState.get().getHiveHistory().setQueryProperty(driverContext.getPlan().getQueryId(), Keys.QUERY_NUM_TASKS,
          String.valueOf(jobCount));
      SessionState.get().getHiveHistory().setIdToTableMap(driverContext.getPlan().getIdToTableNameMap());
    }
    return jobCount;
  }

  private void launchTasks(boolean noName, int jobCount, String jobName) throws HiveException {
    // Launch upto maxthreads tasks
    Task<?> task;
    int maxthreads = HiveConf.getIntVar(driverContext.getConf(), HiveConf.ConfVars.EXEC_PARALLEL_THREAD_NUMBER);
    while ((task = taskQueue.getRunnable(maxthreads)) != null) {
      TaskRunner runner = launchTask(task, noName, jobName, jobCount);
      if (!runner.isRunning()) {
        break;
      }
    }
  }

  private TaskRunner launchTask(Task<?> task, boolean noName, String jobName, int jobCount) throws HiveException {
    SessionState.get().getHiveHistory().startTask(driverContext.getQueryId(), task, task.getClass().getName());

    if (task.isMapRedTask() && !(task instanceof ConditionalTask)) {
      if (noName) {
        driverContext.getConf().set(MRJobConfig.JOB_NAME, jobName + " (" + task.getId() + ")");
      }
      taskQueue.incCurJobNo(1);
      CONSOLE.printInfo("Launching Job " + taskQueue.getCurJobNo() + " out of " + jobCount);
    }

    task.initialize(driverContext.getQueryState(), driverContext.getPlan(), taskQueue, context);
    TaskRunner taskRun = new TaskRunner(task, taskQueue);
    taskQueue.launching(taskRun);

    if (HiveConf.getBoolVar(task.getConf(), HiveConf.ConfVars.EXEC_PARALLEL) && task.canExecuteInParallel()) {
      LOG.info("Starting task [" + task + "] in parallel");
      taskRun.start();
    } else {
      LOG.info("Starting task [" + task + "] in serial mode");
      taskRun.runSequential();
    }
    return taskRun;
  }

  private void handleTaskFailure(Task<?> task, TaskResult result, int exitVal)
      throws HiveException, Exception, CommandProcessorException {
    Task<?> backupTask = task.getAndInitBackupTask();
    if (backupTask != null) {
      String errorMessage = getErrorMsgAndDetail(exitVal, result.getTaskError(), task);
      CONSOLE.printError(errorMessage);
      errorMessage = "ATTEMPT: Execute BackupTask: " + backupTask.getClass().getName();
      CONSOLE.printError(errorMessage);

      // add backup task to runnable
      if (TaskQueue.isLaunchable(backupTask)) {
        taskQueue.addToRunnable(backupTask);
      }
    } else {
      String errorMessage = getErrorMsgAndDetail(exitVal, result.getTaskError(), task);
      if (taskQueue.isShutdown()) {
        errorMessage = "FAILED: Operation cancelled. " + errorMessage;
      }
      DriverUtils.invokeFailureHooks(driverContext, SessionState.getPerfLogger(), hookContext,
          errorMessage + Strings.nullToEmpty(task.getDiagnosticsMessage()), result.getTaskError());
      String sqlState = "08S01";

      // 08S01 (Communication error) is the default sql state.  Override the sqlstate
      // based on the ErrorMsg set in HiveException.
      if (result.getTaskError() instanceof HiveException) {
        ErrorMsg errorMsg = ((HiveException) result.getTaskError()).
            getCanonicalErrorMsg();
        if (errorMsg != ErrorMsg.GENERIC_ERROR) {
          sqlState = errorMsg.getSQLState();
        }
      }

      CONSOLE.printError(errorMessage);
      taskQueue.shutdown();
      // in case we decided to run everything in local mode, restore the
      // the jobtracker setting to its initial value
      context.restoreOriginalTracker();
      throw DriverUtils.createProcessorException(driverContext, exitVal, errorMessage, sqlState,
          result.getTaskError());
    }
  }

  private String getErrorMsgAndDetail(int exitVal, Throwable downstreamError, Task<?> task) {
    String errorMessage = "FAILED: Execution Error, return code " + exitVal + " from " + task.getClass().getName();
    if (downstreamError != null) {
      //here we assume that upstream code may have parametrized the msg from ErrorMsg so we want to keep it
      if (downstreamError.getMessage() != null) {
        errorMessage += ". " + downstreamError.getMessage();
      } else {
        errorMessage += ". " + StringUtils.stringifyException(downstreamError);
      }
    } else {
      ErrorMsg em = ErrorMsg.getErrorMsg(exitVal);
      if (em != null) {
        errorMessage += ". " +  em.getMsg();
      }
    }

    return errorMessage;
  }

  private void postExecutionCacheActions() throws Exception {
    if (driverContext.getCacheUsage() == null) {
      return;
    }

    if (driverContext.getCacheUsage().getStatus() == CacheUsage.CacheStatus.QUERY_USING_CACHE) {
      // Using a previously cached result.
      CacheEntry cacheEntry = driverContext.getCacheUsage().getCacheEntry();

      // Reader count already incremented during cache lookup.
      // Save to usedCacheEntry to ensure reader is released after query.
      driverContext.setUsedCacheEntry(cacheEntry);
    } else if (driverContext.getCacheUsage().getStatus() == CacheUsage.CacheStatus.CAN_CACHE_QUERY_RESULTS &&
        driverContext.getCacheUsage().getCacheEntry() != null && driverContext.getPlan().getFetchTask() != null) {
      // Save results to the cache for future queries to use.
      SessionState.getPerfLogger().perfLogBegin(CLASS_NAME, PerfLogger.SAVE_TO_RESULTS_CACHE);

      CacheEntry cacheEntry = driverContext.getCacheUsage().getCacheEntry();
      boolean savedToCache = QueryResultsCache.getInstance().setEntryValid(cacheEntry,
          driverContext.getPlan().getFetchTask().getWork());
      LOG.info("savedToCache: {} ({})", savedToCache, cacheEntry);
      if (savedToCache) {
        useFetchFromCache(driverContext.getCacheUsage().getCacheEntry());
        // setEntryValid() already increments the reader count. Set usedCacheEntry so it gets released.
        driverContext.setUsedCacheEntry(driverContext.getCacheUsage().getCacheEntry());
      }

      SessionState.getPerfLogger().perfLogEnd(CLASS_NAME, PerfLogger.SAVE_TO_RESULTS_CACHE);
    }
  }

  private void useFetchFromCache(CacheEntry cacheEntry) {
    // Change query FetchTask to use new location specified in results cache.
    FetchTask fetchTaskFromCache = (FetchTask) TaskFactory.get(cacheEntry.getFetchWork());
    fetchTaskFromCache.initialize(driverContext.getQueryState(), driverContext.getPlan(), null, context);
    driverContext.getPlan().setFetchTask(fetchTaskFromCache);
    driverContext.setCacheUsage(new CacheUsage(CacheUsage.CacheStatus.QUERY_USING_CACHE, cacheEntry));
  }

  private void postExecutionActions() throws Exception {
    // in case we decided to run everything in local mode, restore the the jobtracker setting to its initial value
    context.restoreOriginalTracker();

    if (taskQueue.isShutdown()) {
      String errorMessage = "FAILED: Operation cancelled";
      DriverUtils.invokeFailureHooks(driverContext, SessionState.getPerfLogger(), hookContext, errorMessage, null);
      CONSOLE.printError(errorMessage);
      throw DriverUtils.createProcessorException(driverContext, 1000, errorMessage, "HY008", null);
    }

    // Remove incomplete outputs.
    // Some incomplete outputs may be added at the beginning, for eg: for dynamic partitions, remove them
    driverContext.getPlan().getOutputs().removeIf(x -> !x.isComplete());

    hookContext.setHookType(HookContext.HookType.POST_EXEC_HOOK);
    driverContext.getHookRunner().runPostExecHooks(hookContext);

    SessionState.get().getHiveHistory().setQueryProperty(driverContext.getQueryId(), Keys.QUERY_RET_CODE,
        String.valueOf(0));
    SessionState.get().getHiveHistory().printRowCount(driverContext.getQueryId());
    releasePlan(driverContext.getPlan());
  }

  private void releasePlan(QueryPlan plan) {
    // Plan maybe null if Driver.close is called in another thread for the same Driver object
    driverState.lock();
    try {
      if (plan != null) {
        plan.setDone();
        if (SessionState.get() != null) {
          try {
            SessionState.get().getHiveHistory().logPlanProgress(plan);
          } catch (Exception e) {
            // Log and ignore
            LOG.warn("Could not log query plan progress", e);
          }
        }
      }
    } finally {
      driverState.unlock();
    }
  }

  private void handleException(HookContext hookContext, Throwable e) throws CommandProcessorException {
    context.restoreOriginalTracker();
    if (SessionState.get() != null) {
      SessionState.get().getHiveHistory().setQueryProperty(driverContext.getQueryId(), Keys.QUERY_RET_CODE,
          String.valueOf(12));
    }
    // TODO: do better with handling types of Exception here
    String errorMessage = "FAILED: Hive Internal Error: " + Utilities.getNameMessage(e);
    if (hookContext != null) {
      try {
        DriverUtils.invokeFailureHooks(driverContext, SessionState.getPerfLogger(), hookContext, errorMessage, e);
      } catch (Exception t) {
        LOG.warn("Failed to invoke failure hook", t);
      }
    }
    CONSOLE.printError(errorMessage + "\n" + StringUtils.stringifyException(e));
    throw DriverUtils.createProcessorException(driverContext, 12, errorMessage, "08S01", e);
  }

  private void cleanUp(boolean noName, HookContext hookContext, boolean executionError) {
    // Trigger query hooks after query completes its execution.
    try {
      driverContext.getHookRunner().runAfterExecutionHook(driverContext.getQueryString(), hookContext, executionError);
    } catch (Exception e) {
      LOG.warn("Failed when invoking query after execution hook", e);
    }

    SessionState.get().getHiveHistory().endQuery(driverContext.getQueryId());
    if (noName) {
      driverContext.getConf().set(MRJobConfig.JOB_NAME, "");
    }
    double duration = SessionState.getPerfLogger().perfLogEnd(CLASS_NAME, PerfLogger.DRIVER_EXECUTE) / 1000.00;

    ImmutableMap<String, Long> executionHMSTimings = Hive.dumpMetaCallTimingWithoutEx("execution");
    driverContext.getQueryDisplay().setHmsTimings(QueryDisplay.Phase.EXECUTION, executionHMSTimings);

    logExecutionResourceUsage();

    driverState.executionFinishedWithLocking(executionError);

    if (driverState.isAborted()) {
      LOG.info("Executing command(queryId={}) has been interrupted after {} seconds", driverContext.getQueryId(),
          duration);
    } else {
      LOG.info("Completed executing command(queryId={}); Time taken: {} seconds", driverContext.getQueryId(), duration);
    }
  }

  private void logExecutionResourceUsage() {
    Map<String, MapRedStats> stats = SessionState.get().getMapRedStats();
    if (stats != null && !stats.isEmpty()) {
      long totalCpu = 0;
      long numModifiedRows = 0;
      CONSOLE.printInfo("MapReduce Jobs Launched: ");
      for (Map.Entry<String, MapRedStats> entry : stats.entrySet()) {
        CONSOLE.printInfo("Stage-" + entry.getKey() + ": " + entry.getValue());
        totalCpu += entry.getValue().getCpuMSec();

        if (numModifiedRows > -1) {
          //if overflow, then numModifiedRows is set as -1. Else update numModifiedRows with the sum.
          try {
            numModifiedRows = Math.addExact(numModifiedRows, entry.getValue().getNumModifiedRows());
          } catch (ArithmeticException e) {
            numModifiedRows = -1;
          }
        }
      }
      driverContext.getQueryState().setNumModifiedRows(numModifiedRows);
      CONSOLE.printInfo("Total MapReduce CPU Time Spent: " + Utilities.formatMsecToStr(totalCpu));
    }
  }
}
