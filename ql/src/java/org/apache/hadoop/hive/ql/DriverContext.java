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

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.hive.ql.exec.Task;

public class DriverContext {

  Queue<Task<? extends Serializable>> runnable = new LinkedList<Task<? extends Serializable>>();

  public DriverContext(Queue<Task<? extends Serializable>> runnable) {
    this.runnable = runnable;
  }

  public Queue<Task<? extends Serializable>> getRunnable() {
    return runnable;
  }

  /**
   * Checks if a task can be launched
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

}
