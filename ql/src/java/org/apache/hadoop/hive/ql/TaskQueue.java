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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.NodeUtils;
import org.apache.hadoop.hive.ql.exec.NodeUtils.Function;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.StatsTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the queue of tasks that should be executed by the driver.
 */
public class TaskQueue {

  private static final Logger LOG = LoggerFactory.getLogger(Driver.class.getName());
  private static final SessionState.LogHelper CONSOLE = new SessionState.LogHelper(LOG);

  private static final int SLEEP_TIME = 2000;

  private final Queue<Task<?>> runnable = new ConcurrentLinkedQueue<Task<?>>();;
  private final List<TaskRunner> running = new ArrayList<TaskRunner>();
  private final Map<String, StatsTask> statsTasks = new HashMap<>(1);
  private final Context ctx;

  // how many jobs have been started
  private int curJobNo;
  private boolean shutdown;

  public TaskQueue() {
    this(null);
  }

  public TaskQueue(Context ctx) {
    this.ctx = ctx;
  }

  public synchronized boolean isShutdown() {
    return shutdown;
  }

  public synchronized boolean isRunning() {
    return !shutdown && (!running.isEmpty() || !runnable.isEmpty());
  }

  public synchronized void remove(Task<?> task) {
    runnable.remove(task);
  }

  public synchronized void launching(TaskRunner runner) throws HiveException {
    checkShutdown();
    running.add(runner);
  }

  public synchronized Task<?> getRunnable(int maxthreads) throws HiveException {
    checkShutdown();
    if (runnable.peek() != null && running.size() < maxthreads) {
      return runnable.remove();
    }
    return null;
  }

  public synchronized void releaseRunnable() {
    //release the waiting poller.
    notify();
  }

  /**
   * Polls running tasks to see if a task has ended.
   *
   * @return The result object for any completed/failed task
   */
  public synchronized TaskRunner pollFinished() throws InterruptedException {
    while (!shutdown) {
      Iterator<TaskRunner> it = running.iterator();
      while (it.hasNext()) {
        TaskRunner runner = it.next();
        if (runner != null && !runner.isRunning()) {
          it.remove();
          return runner;
        }
      }
      wait(SLEEP_TIME);
    }
    return null;
  }

  private void checkShutdown() throws HiveException {
    if (shutdown) {
      throw new HiveException("FAILED: Operation cancelled");
    }
  }

  /**
   * Cleans up remaining tasks in case of failure.
   */
  public synchronized void shutdown() {
    LOG.debug("Shutting down query " + ctx.getCmd());
    shutdown = true;
    for (TaskRunner runner : running) {
      if (runner.isRunning()) {
        Task<?> task = runner.getTask();
        LOG.warn("Shutting down task : " + task);
        try {
          task.shutdown();
        } catch (Exception e) {
          CONSOLE.printError("Exception on shutting down task " + task.getId() + ": " + e);
        }
        Thread thread = runner.getRunner();
        if (thread != null) {
          thread.interrupt();
        }
      }
    }
    running.clear();
  }

  /**
   * Checks if a task can be launched.
   *
   * @param tsk
   *          the task to be checked
   * @return true if the task is launchable, false otherwise
   */

  public static boolean isLaunchable(Task<?> tsk) {
    // A launchable task is one that hasn't been queued, hasn't been
    // initialized, and is runnable.
    return tsk.isNotInitialized() && tsk.isRunnable();
  }

  public synchronized boolean addToRunnable(Task<?> tsk) throws HiveException {
    if (runnable.contains(tsk)) {
      return false;
    }
    checkShutdown();
    runnable.add(tsk);
    tsk.setQueued();
    return true;
  }

  public int getCurJobNo() {
    return curJobNo;
  }

  public void incCurJobNo(int amount) {
    this.curJobNo = this.curJobNo + amount;
  }

  public void prepare(QueryPlan plan) {
    // extract stats keys from StatsTask
    List<Task<?>> rootTasks = plan.getRootTasks();
    NodeUtils.iterateTask(rootTasks, StatsTask.class, new Function<StatsTask>() {
      @Override
      public void apply(StatsTask statsTask) {
        if (statsTask.getWork().isAggregating()) {
          statsTasks.put(statsTask.getWork().getAggKey(), statsTask);
        }
      }
    });
  }

  public void finished(TaskRunner runner) {
    if (statsTasks.isEmpty() || !(runner.getTask() instanceof MapRedTask)) {
      return;
    }
    MapRedTask mapredTask = (MapRedTask) runner.getTask();

    MapWork mapWork = mapredTask.getWork().getMapWork();
    ReduceWork reduceWork = mapredTask.getWork().getReduceWork();
    List<Operator> operators = new ArrayList<Operator>(mapWork.getAliasToWork().values());
    if (reduceWork != null) {
      operators.add(reduceWork.getReducer());
    }
    final List<String> statKeys = new ArrayList<String>(1);
    NodeUtils.iterate(operators, FileSinkOperator.class, new Function<FileSinkOperator>() {
      @Override
      public void apply(FileSinkOperator fsOp) {
        if (fsOp.getConf().isGatherStats()) {
          statKeys.add(fsOp.getConf().getStatsAggPrefix());
        }
      }
    });
    for (String statKey : statKeys) {
      if (statsTasks.containsKey(statKey)) {
        statsTasks.get(statKey).getWork().setSourceTask(mapredTask);
      } else {
        LOG.debug("There is no correspoing statTask for: " + statKey);
      }
    }
  }
}
