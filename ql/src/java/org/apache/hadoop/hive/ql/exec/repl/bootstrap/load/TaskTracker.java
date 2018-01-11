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
package org.apache.hadoop.hive.ql.exec.repl.bootstrap.load;

import org.apache.hadoop.hive.ql.exec.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class will be responsible to track how many tasks have been created,
 * organization of tasks such that after the number of tasks for next execution are created
 * we create a dependency collection task(DCT) -> another bootstrap task,
 * and then add DCT as dependent to all existing tasks that are created so the cycle can continue.
 */
public class TaskTracker {
  private static Logger LOG = LoggerFactory.getLogger(TaskTracker.class);
  /**
   * used to identify the list of tasks at root level for a given level like table /  db / partition.
   * this does not include the task dependency notion of "table tasks < ---- partition task"
   */
  private final List<Task<? extends Serializable>> tasks = new ArrayList<>();
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
  public void addTask(Task<? extends Serializable> task) {
    tasks.add(task);

    List <Task<? extends Serializable>> visited = new ArrayList<>();
    updateTaskCount(task, visited);
  }

  public void updateTaskCount(Task<? extends Serializable> task,
                              List <Task<? extends Serializable>> visited) {
    numberOfTasks += 1;
    visited.add(task);
    if (task.getChildTasks() != null) {
      for (Task<? extends Serializable> childTask : task.getChildTasks()) {
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

  public List<Task<? extends Serializable>> tasks() {
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