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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.TaskQueue;
import org.apache.hadoop.hive.ql.QueryDisplay;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.history.HiveHistory;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Task implementation.
 **/

public abstract class Task<T extends Serializable> implements Serializable, Node {

  private static final long serialVersionUID = 1L;
  public transient HashMap<String, Long> taskCounters;
  public transient TaskHandle taskHandle;
  protected transient HiveConf conf;
  protected transient QueryState queryState;
  protected transient LogHelper console;
  protected transient QueryPlan queryPlan;
  protected transient TaskQueue taskQueue;
  protected transient Context context;
  protected transient boolean clonedConf = false;
  protected transient String jobID;
  protected Task<?> backupTask;
  protected List<Task<?>> backupChildrenTasks = new ArrayList<Task<?>>();
  protected static transient Logger LOG = LoggerFactory.getLogger(Task.class);
  protected int taskTag;
  private boolean isLocalMode =false;

  public static final int NO_TAG = 0;
  public static final int COMMON_JOIN = 1;
  public static final int HINTED_MAPJOIN = 2;
  public static final int HINTED_MAPJOIN_LOCAL = 3;
  public static final int CONVERTED_MAPJOIN = 4;
  public static final int CONVERTED_MAPJOIN_LOCAL = 5;
  public static final int BACKUP_COMMON_JOIN = 6;
  // The join task is converted to a mapjoin task. This can only happen if
  // hive.auto.convert.join.noconditionaltask is set to true. No conditional task was
  // created in case the mapjoin failed.
  public static final int MAPJOIN_ONLY_NOBACKUP = 7;
  public static final int CONVERTED_SORTMERGEJOIN = 8;
  public QueryDisplay queryDisplay = null;
  // Descendants tasks who subscribe feeds from this task
  protected transient List<Task<?>> feedSubscribers;

  protected String id;
  protected T work;
  private TaskState taskState = TaskState.CREATED;
  private String statusMessage;
  private String diagnosticMesg;
  private transient boolean fetchSource;

  public void setDiagnosticMessage(String diagnosticMesg) {
    this.diagnosticMesg = diagnosticMesg;
  }

  public String getDiagnosticsMessage() {
    return diagnosticMesg;
  }

  public void setStatusMessage(String statusMessage) {
    this.statusMessage = statusMessage;
    updateStatusInQueryDisplay();
  }

  public String getStatusMessage() {
    return statusMessage;
  }

  public enum FeedType {
    DYNAMIC_PARTITIONS, // list of dynamic partitions
  }

  /**
   * Order of the States here is important as the ordinal values are used
   * determine the progression of taskState over its lifeCycle which is then
   * used to make some decisions in Driver.execute
   */
  public enum TaskState {
    // Task state is unkown
    UNKNOWN,
    // Task is just created
    CREATED,
    // Task data structures have been initialized
    INITIALIZED,
    // Task has been queued for execution by the driver
    QUEUED,
    // Task is currently running
    RUNNING,
    // Task has completed
    FINISHED
  }

  // Bean methods

  protected boolean rootTask;

  protected List<Task<?>> childTasks;
  protected List<Task<?>> parentTasks;
  /**
   * this can be set by the Task, to provide more info about the failure in TaskResult
   * where the Driver can find it.  This is checked if {@link Task#execute(org.apache.hadoop.hive.ql.TaskQueue)}
   * returns non-0 code.
   */
  private Throwable exception;

  public Task() {
    this.taskCounters = new HashMap<String, Long>();
    taskTag = Task.NO_TAG;
  }

  public TaskHandle getTaskHandle() {
    return taskHandle;
  }

  public void initialize(QueryState queryState, QueryPlan queryPlan, TaskQueue taskQueue, Context context) {
    this.queryPlan = queryPlan;
    setInitialized();
    this.queryState = queryState;
    if (null == this.conf && queryState != null) {
      this.conf = queryState.getConf();
    }
    this.taskQueue = taskQueue;
    this.context = context;
    this.console = new LogHelper(LOG);
  }
  public void setQueryDisplay(QueryDisplay queryDisplay) {
    this.queryDisplay = queryDisplay;
  }

  protected void updateStatusInQueryDisplay() {
    if (queryDisplay != null) {
      queryDisplay.updateTaskStatus(this);
    }
  }

  protected void setState(TaskState state) {
    this.taskState = state;
    updateStatusInQueryDisplay();
  }

  protected Hive getHive() {
    try {
      // Hive.getWithFastCheck shouldn't be used here as it always re-opens metastore connection.
      // The conf object in HMS client is always different from the one used here.
      return Hive.get(conf);
    } catch (HiveException e) {
      LOG.error("Failed to get Hive", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * This method is called in the Driver on every task. It updates counters and calls execute(),
   * which is overridden in each task
   *
   * @return return value of execute()
   */
  public int executeTask(HiveHistory hiveHistory) {
    try {
      this.setStarted();
      if (hiveHistory != null) {
        hiveHistory.logPlanProgress(queryPlan);
      }

      if (conf != null) {
        LOG.debug("Task getting executed using mapred tag : " + conf.get(MRJobConfig.JOB_TAGS));
      }
      int retval = execute();
      this.setDone();
      if (hiveHistory != null) {
        hiveHistory.logPlanProgress(queryPlan);
      }
      return retval;
    } catch (IOException e) {
      throw new RuntimeException("Unexpected error: " + e.getMessage(), e);
    }
  }

  /**
   * This method is overridden in each Task. TODO execute should return a TaskHandle.
   *
   * @return status of executing the task
   */
  public abstract int execute();

  public boolean isRootTask() {
    return rootTask;
  }

  public void setRootTask(boolean rootTask) {
    this.rootTask = rootTask;
  }

  public void setChildTasks(List<Task<?>> childTasks) {
    this.childTasks = childTasks;
  }

  @Override
  public List<? extends Node> getChildren() {
    return getChildTasks();
  }

  public List<Task<?>> getChildTasks() {
    return childTasks;
  }

  public int getNumChild() {
    return childTasks == null ? 0 : childTasks.size();
  }

  public void setParentTasks(List<Task<?>> parentTasks) {
    this.parentTasks = parentTasks;
  }

  public List<Task<?>> getParentTasks() {
    return parentTasks;
  }

  public int getNumParent() {
    return parentTasks == null ? 0 : parentTasks.size();
  }

  public Task<?> getBackupTask() {
    return backupTask;
  }

  public void setBackupTask(Task<?> backupTask) {
    this.backupTask = backupTask;
  }

  public List<Task<?>> getBackupChildrenTasks() {
    return backupChildrenTasks;
  }

  public void setBackupChildrenTasks(List<Task<?>> backupChildrenTasks) {
    this.backupChildrenTasks = backupChildrenTasks;
  }

  public Task<?> getAndInitBackupTask() {
    if (backupTask != null) {
      // first set back the backup task with its children task.
      if( backupChildrenTasks!= null) {
        for (Task<?> backupChild : backupChildrenTasks) {
          backupChild.getParentTasks().add(backupTask);
        }
      }

      // recursively remove task from its children tasks if this task doesn't have any parent task
      this.removeFromChildrenTasks();
    }
    return backupTask;
  }

  public void removeFromChildrenTasks() {

    List<Task<?>> childrenTasks = this.getChildTasks();
    if (childrenTasks == null) {
      return;
    }

    for (Task<?> childTsk : childrenTasks) {
      // remove this task from its children tasks
      childTsk.getParentTasks().remove(this);

      // recursively remove non-parent task from its children
      List<Task<?>> siblingTasks = childTsk.getParentTasks();
      if (siblingTasks == null || siblingTasks.size() == 0) {
        childTsk.removeFromChildrenTasks();
      }
    }
  }


  /**
   * The default dependent tasks are just child tasks, but different types could implement their own
   * (e.g. ConditionalTask will use the listTasks as dependents).
   *
   * @return a list of tasks that are dependent on this task.
   */
  public List<Task<?>> getDependentTasks() {
    return getChildTasks();
  }

  /**
   * Add a dependent task on the current task. Return if the dependency already existed or is this a
   * new one
   *
   * @return true if the task got added false if it already existed
   */
  public boolean addDependentTask(Task<?> dependent) {
    boolean ret = false;
    if (getChildTasks() == null) {
      setChildTasks(new ArrayList<Task<?>>());
    }
    if (!getChildTasks().contains(dependent)) {
      ret = true;
      getChildTasks().add(dependent);
      if (dependent.getParentTasks() == null) {
        dependent.setParentTasks(new ArrayList<Task<?>>());
      }
      if (!dependent.getParentTasks().contains(this)) {
        dependent.getParentTasks().add(this);
      }
    }
    return ret;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static List<Task<?>>
      findLeafs(List<Task<? extends Serializable>> rootTasks) {
    final List<Task<? extends Serializable>> leafTasks = new ArrayList<Task<?>>();

    NodeUtils.iterateTask(rootTasks, Task.class, new NodeUtils.Function<Task>() {
      @Override
      public void apply(Task task) {
        List dependents = task.getDependentTasks();
        if (dependents == null || dependents.isEmpty()) {
          leafTasks.add(task);
        }
      }
    });
    return leafTasks;
  }

  /**
   * Remove the dependent task.
   *
   * @param dependent
   *          the task to remove
   */
  public void removeDependentTask(Task<?> dependent) {
    if ((getChildTasks() != null) && (getChildTasks().contains(dependent))) {
      getChildTasks().remove(dependent);
      if ((dependent.getParentTasks() != null) && (dependent.getParentTasks().contains(this))) {
        dependent.getParentTasks().remove(this);
      }
    }
  }

  public synchronized void setStarted() {
    setState(TaskState.RUNNING);
  }

  public synchronized boolean started() {
    return taskState == TaskState.RUNNING;
  }

  public synchronized boolean done() {
    return taskState == TaskState.FINISHED;
  }

  public synchronized void setDone() {
    setState(TaskState.FINISHED);
  }

  public synchronized void setQueued() {
    setState(TaskState.QUEUED);
  }

  public synchronized boolean getQueued() {
    return taskState == TaskState.QUEUED;
  }

  public synchronized void setInitialized() {
    setState(TaskState.INITIALIZED);
  }

  public synchronized boolean getInitialized() {
    return taskState == TaskState.INITIALIZED;
  }

  public synchronized boolean isNotInitialized() {
    return taskState.ordinal() < TaskState.INITIALIZED.ordinal();
  }


  public boolean isRunnable() {
    boolean isrunnable = true;
    if (parentTasks != null) {
      for (Task<?> parent : parentTasks) {
        if (!parent.done()) {
          isrunnable = false;
          break;
        }
      }
    }
    return isrunnable;
  }

  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

  public HiveConf getConf() {
    return this.conf;
  }

  public void setWork(T work) {
    this.work = work;
  }

  public T getWork() {
    return work;
  }

  public Collection<MapWork> getMapWork() {
    return Collections.<MapWork>emptyList();
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public String getExternalHandle() {
    return null;
  }

  public TaskState getTaskState() {
    return taskState;
  }

  public boolean isMapRedTask() {
    return false;
  }

  public boolean isMapRedLocalTask() {
    return false;
  }

  public Collection<Operator<? extends OperatorDesc>> getTopOperators() {
    return new LinkedList<Operator<? extends OperatorDesc>>();
  }

  public boolean hasReduce() {
    return false;
  }

  public Operator<? extends OperatorDesc> getReducer(MapWork work) {
    return null;
  }

  public HashMap<String, Long> getCounters() {
    return taskCounters;
  }

  /**
   * Should be overridden to return the type of the specific task among the types in StageType.
   *
   * @return StageType.* or null if not overridden
   */
  public abstract StageType getType();

  /**
   * Subscribe the feed of publisher. To prevent cycles, a task can only subscribe to its ancestor.
   * Feed is a generic form of execution-time feedback (type, value) pair from one task to another
   * task. Examples include dynamic partitions (which are only available at execution time). The
   * MoveTask may pass the list of dynamic partitions to the StatsTask since after the MoveTask the
   * list of dynamic partitions are lost (MoveTask moves them to the table's destination directory
   * which is mixed with old partitions).
   *
   * @param publisher
   *          this feed provider.
   */
  public void subscribeFeed(Task<?> publisher) {
    if (publisher != this && publisher.ancestorOrSelf(this)) {
      if (publisher.getFeedSubscribers() == null) {
        publisher.setFeedSubscribers(new LinkedList<Task<?>>());
      }
      publisher.getFeedSubscribers().add(this);
    }
  }

  // return true if this task is an ancestor of itself of parameter desc
  private boolean ancestorOrSelf(Task<?> desc) {
    if (this == desc) {
      return true;
    }
    List<Task<?>> deps = getDependentTasks();
    if (deps != null) {
      for (Task<?> d : deps) {
        if (d.ancestorOrSelf(desc)) {
          return true;
        }
      }
    }
    return false;
  }

  public List<Task<?>> getFeedSubscribers() {
    return feedSubscribers;
  }

  public void setFeedSubscribers(List<Task<?>> s) {
    feedSubscribers = s;
  }

  // push the feed to its subscribers
  protected void pushFeed(FeedType feedType, Object feedValue) {
    if (feedSubscribers != null) {
      for (Task<?> s : feedSubscribers) {
        s.receiveFeed(feedType, feedValue);
      }
    }
  }

  // a subscriber accept the feed and do something depending on the Task type
  protected void receiveFeed(FeedType feedType, Object feedValue) {
  }

  protected void cloneConf() {
    if (!clonedConf) {
      clonedConf = true;
      conf = new HiveConf(conf);
    }
  }

  /**
   * Provide metrics on the type and number of tasks executed by the HiveServer
   * @param metrics
   */
  public void updateTaskMetrics(Metrics metrics) {
    // no metrics gathered by default
   }

  public int getTaskTag() {
    return taskTag;
  }

  public void setTaskTag(int taskTag) {
    this.taskTag = taskTag;
  }

  public boolean isLocalMode() {
    return isLocalMode;
  }

  public void setLocalMode(boolean isLocalMode) {
    this.isLocalMode = isLocalMode;
  }

  public boolean requireLock() {
    return false;
  }

  public QueryPlan getQueryPlan() {
    return queryPlan;
  }

  public TaskQueue getTaskQueue() {
    return taskQueue;
  }

  public void setTaskQueue(TaskQueue taskQueue) {
    this.taskQueue = taskQueue;
  }

  public Context getContext() {
    return context;
  }

  public void setQueryPlan(QueryPlan queryPlan) {
    this.queryPlan = queryPlan;
  }

  public String getJobID() {
    return jobID;
  }

  public void shutdown() {
  }

  public Throwable getException() {
    return exception;
  }

  public void setException(Throwable ex) {
    exception = ex;
  }

  public void setConsole(LogHelper console) {
    this.console = console;
  }

  public boolean isFetchSource() {
    return fetchSource;
  }

  public void setFetchSource(boolean fetchSource) {
    this.fetchSource = fetchSource;
  }

  @Override
  public String toString() {
    return getId() + ":" + getType();
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return toString().equals(String.valueOf(obj));
  }

  public boolean canExecuteInParallel(){
    return true;
  }

  public QueryState getQueryState() {
    return queryState;
  }

  public HiveTxnManager getTxnMgr() {
    return context.getHiveTxnManager();
  }
}
