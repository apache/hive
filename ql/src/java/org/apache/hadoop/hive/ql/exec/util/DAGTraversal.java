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

package org.apache.hadoop.hive.ql.exec.util;

import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.Task;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The dag traversal done here is written to be not recursion based as large DAG's will lead to
 * stack overflow's, hence iteration based.
 */
public class DAGTraversal {
  public static void traverse(List<Task<? extends Serializable>> tasks, Function function) {
    List<Task<? extends Serializable>> listOfTasks = new ArrayList<>(tasks);
    while (!listOfTasks.isEmpty()) {
      List<Task<? extends Serializable>> children = new ArrayList<>();
      for (Task<? extends Serializable> task : listOfTasks) {
        // skip processing has to be done first before continuing
        if (function.skipProcessing(task)) {
          continue;
        }
        // Add list tasks from conditional tasks
        if (task instanceof ConditionalTask) {
          children.addAll(((ConditionalTask) task).getListTasks());
        }
        if (task.getDependentTasks() != null) {
          children.addAll(task.getDependentTasks());
        }
        function.process(task);
      }
      listOfTasks = children;
    }
  }

  public interface Function {
    void process(Task<? extends Serializable> task);

    boolean skipProcessing(Task<? extends Serializable> task);
  }
}
