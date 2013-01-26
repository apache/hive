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

package org.apache.hadoop.hive.ql.hooks;

import java.util.Collection;
import java.util.List;
import java.util.Iterator;
import java.io.Serializable;
import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.mapred.Counters;

public class PostTestOperatorHook implements ExecuteWithHookContext {
  private void logEnterExitCounters(Task<? extends Serializable> task) throws IOException {
    if(task.getTaskHandle() != null) {
      Counters counters = task.getTaskHandle().getCounters();
      if(counters != null) {
        logCounterValue(counters, "TEST_OPERATOR_HOOK_");
      } else {
        SessionState.getConsole().printError("counters are null");
      }
    } else {
      SessionState.getConsole().printError("task handle is null");
    }
  }

  private void logCounterValue(Counters ctr, String name) {
    Collection <String> counterGroups = ctr.getGroupNames();
    for (String groupName : counterGroups) {
      Counters.Group group = ctr.getGroup(groupName);
      Iterator<Counters.Counter> it = group.iterator();
      while (it.hasNext()) {
        Counters.Counter counter = it.next();
        if(counter.getName().contains(name)) {
          SessionState.getConsole().printError(counter.getName() + ": " +  counter.getValue());
        }
      }
    }
  }

  public void run(HookContext hookContext) {
    HiveConf conf = hookContext.getConf();
    List<TaskRunner> completedTasks = hookContext.getCompleteTaskList();
    if (completedTasks != null) {
      for (TaskRunner taskRunner : completedTasks) {
        Task<? extends Serializable> task = taskRunner.getTask();
        if (task.isMapRedTask() && !task.isMapRedLocalTask()) {
          try {
              logEnterExitCounters(task);

          } catch (Exception e) {
            SessionState.getConsole().printError("Error in get counters: " + e.toString());
          }
        }
      }
    }
  }
}
