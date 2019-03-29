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

import java.io.Serializable;
import java.util.ArrayList;

import org.junit.Assert;

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.plan.MapredWork;

public class VerifyHiveSortedInputFormatUsedHook implements ExecuteWithHookContext {

  public void run(HookContext hookContext) {
    if (hookContext.getHookType().equals(HookType.POST_EXEC_HOOK)) {

      // Go through the root tasks, and verify the input format of the map reduce task(s) is
      // HiveSortedInputFormat
      ArrayList<Task<? extends Serializable>> rootTasks =
          hookContext.getQueryPlan().getRootTasks();
      for (Task<? extends Serializable> rootTask : rootTasks) {
        if (rootTask.getWork() instanceof MapredWork) {
          Assert.assertTrue("The root map reduce task's input was not marked as sorted.",
              ((MapredWork)rootTask.getWork()).getMapWork().isInputFormatSorted());
        }
      }
    }
  }
}
