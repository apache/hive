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
package org.apache.hadoop.hive.ql.hooks;

import java.util.List;

import org.junit.Assert;

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;

public class VerifyIsLocalModeHook implements ExecuteWithHookContext {

  public void run(HookContext hookContext) {
    if (hookContext.getHookType().equals(HookType.POST_EXEC_HOOK)) {
      List<TaskRunner> taskRunners = hookContext.getCompleteTaskList();
      for (TaskRunner taskRunner : taskRunners) {
        Task task = taskRunner.getTask();
        if (task.isMapRedTask()) {
          Assert.assertTrue("VerifyIsLocalModeHook fails because a isLocalMode was not set for a task.",
              task.isLocalMode());
        }
      }
    }
  }
}
