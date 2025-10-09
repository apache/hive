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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The dag traversal done here is written to be not recursion based as large DAG's will lead to
 * stack overflow's, hence iteration based.
 */
public class DAGTraversal {
  public static void traverse(List<Task<?>> tasks, Function function) {
    List<Task<?>> listOfTasks = new ArrayList<>(tasks);
    while (!listOfTasks.isEmpty()) {
      // HashSet will make sure that no duplicate children are added. If a task is added multiple
      // time to the children list then it may cause the list to grow exponentially. Let's take an example of
      // incremental load with 2 events. The DAG will look something similar as below.
      //
      //                 --->ev1.task1--                          --->ev2.task1--
      //                /               \                        /               \
      //  evTaskRoot-->*---->ev1.task2---*--> ev1.barrierTask-->*---->ev2.task2---*->ev2.barrierTask-------
      //                \               /
      //                 --->ev1.task3--
      //
      // While traversing the DAG, if the filter is not added then  ev1.barrierTask will be added 3 times in
      // the children list and in next iteration ev2.task1 will be added 3 times and ev2.task2 will be added
      // 3 times. So in next iteration ev2.barrierTask will be added 6 times. As it goes like this, the next barrier
      // task will be added 12-15 times and may reach millions with large number of events.
      Set<Task<?>> children = new HashSet<>();
      for (Task<?> task : listOfTasks) {
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
      listOfTasks = new ArrayList<>(children);
    }
  }

  public interface Function {
    void process(Task<?> task);

    boolean skipProcessing(Task<?> task);
  }
}
