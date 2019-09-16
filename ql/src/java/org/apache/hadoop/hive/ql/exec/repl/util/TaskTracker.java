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
package org.apache.hadoop.hive.ql.exec.repl.util;

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.ReplicationState;
import org.apache.hadoop.hive.ql.exec.util.DAGTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class will be responsible to track how many tasks have been created,
 * organization of tasks such that after the number of tasks for next execution are created
 * we create a dependency collection task(DCT) -&gt; another bootstrap task,
 * and then add DCT as dependent to all existing tasks that are created so the cycle can continue.
 */
public class TaskTracker {
  private static Logger LOG = LoggerFactory.getLogger(TaskTracker.class);
  /**
   * used to identify the list of tasks at root level for a given level like table /  db / partition.
   * this does not include the task dependency notion of "table tasks < ---- partition task"
   */
  private final List<Task<?>> tasks = new ArrayList<>();
  private ReplicationState replicationState = null;
  // since tasks themselves can be graphs we want to limit the number of created
  // tasks including all of dependencies.
  private int numberOfTasks = 0;
  private final int maxTasksAllowed;

  public TaskTracker(int defaultMaxTasks) {
    maxTasksAllowed = defaultMaxTasks;
  }

  public TaskTracker(TaskTracker existing) {
    maxTasksAllowed = existing.maxTasksAllowed - existing.numberOfTasks;
  }

  /**
   * this method is used to identify all the tasks in a graph.
   * the graph however might get created in a disjoint fashion, in which case we can just update
   * the number of tasks using the "update" method.
   */
  public void addTask(Task<?> task) {
    tasks.add(task);

    List <Task<?>> visited = new ArrayList<>();
    updateTaskCount(task, visited);
  }

  public void addTaskList(List <Task<?>> taskList) {
    List <Task<?>> visited = new ArrayList<>();
    for (Task<?> task : taskList) {
      if (!visited.contains(task)) {
        tasks.add(task);
        updateTaskCount(task, visited);
      }
    }
  }

  // This method is used to traverse the DAG created in tasks list and add the dependent task to
  // the tail of each task chain.
  public void addDependentTask(Task<?> dependent) {
    if (tasks.isEmpty()) {
      addTask(dependent);
    } else {
      DAGTraversal.traverse(tasks, new AddDependencyToLeaves(dependent));

      List<Task<?>> visited = new ArrayList<>();
      updateTaskCount(dependent, visited);
    }
  }

  private void updateTaskCount(Task<?> task,
                               List <Task<?>> visited) {
    numberOfTasks += 1;
    visited.add(task);
    if (task.getChildTasks() != null) {
      for (Task<?> childTask : task.getChildTasks()) {
        if (visited.contains(childTask)) {
          continue;
        }
        updateTaskCount(childTask, visited);
      }
    }
  }

  public boolean canAddMoreTasks() {
    return numberOfTasks < maxTasksAllowed;
  }

  public boolean hasTasks() {
    return numberOfTasks != 0;
  }

  public void update(TaskTracker withAnother) {
    numberOfTasks += withAnother.numberOfTasks;
    if (withAnother.hasReplicationState()) {
      this.replicationState = withAnother.replicationState;
    }
  }

  public void setReplicationState(ReplicationState state) {
    this.replicationState = state;
  }

  public boolean hasReplicationState() {
    return replicationState != null;
  }

  public ReplicationState replicationState() {
    return replicationState;
  }

  public List<Task<?>> tasks() {
    return tasks;
  }

  public void debugLog(String forEventType) {
    LOG.debug("{} event with total / root number of tasks:{}/{}", forEventType, numberOfTasks,
        tasks.size());
  }

  public int numberOfTasks() {
    return numberOfTasks;
  }
}
