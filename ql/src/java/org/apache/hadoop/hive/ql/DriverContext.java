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

package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.NodeUtils;
import org.apache.hadoop.hive.ql.exec.NodeUtils.Function;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.StatsTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * DriverContext.
 *
 */
public class DriverContext {

  Queue<Task<? extends Serializable>> runnable = new LinkedList<Task<? extends Serializable>>();

  // how many jobs have been started
  int curJobNo;

  Context ctx;

  final Map<String, StatsTask> statsTasks = new HashMap<String, StatsTask>(1);

  public DriverContext() {
    this.runnable = null;
    this.ctx = null;
  }

  public DriverContext(Queue<Task<? extends Serializable>> runnable, Context ctx) {
    this.runnable = runnable;
    this.ctx = ctx;
  }

  public Queue<Task<? extends Serializable>> getRunnable() {
    return runnable;
  }

  /**
   * Checks if a task can be launched.
   *
   * @param tsk
   *          the task to be checked
   * @return true if the task is launchable, false otherwise
   */

  public static boolean isLaunchable(Task<? extends Serializable> tsk) {
    // A launchable task is one that hasn't been queued, hasn't been
    // initialized, and is runnable.
    return !tsk.getQueued() && !tsk.getInitialized() && tsk.isRunnable();
  }

  public void addToRunnable(Task<? extends Serializable> tsk) {
    runnable.add(tsk);
    tsk.setQueued();
  }

  public int getCurJobNo() {
    return curJobNo;
  }

  public Context getCtx() {
    return ctx;
  }

  public void incCurJobNo(int amount) {
    this.curJobNo = this.curJobNo + amount;
  }

  public void prepare(QueryPlan plan) {
    // extract stats keys from StatsTask
    List<Task<?>> rootTasks = plan.getRootTasks();
    NodeUtils.iterateTask(rootTasks, StatsTask.class, new Function<StatsTask>() {
      public void apply(StatsTask statsTask) {
        statsTasks.put(statsTask.getWork().getAggKey(), statsTask);
      }
    });
  }

  public void prepare(TaskRunner runner) {
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
      public void apply(FileSinkOperator fsOp) {
        if (fsOp.getConf().isGatherStats()) {
          statKeys.add(fsOp.getConf().getStatsAggPrefix());
        }
      }
    });
    for (String statKey : statKeys) {
      statsTasks.get(statKey).getWork().setSourceTask(mapredTask);
    }
  }
}
