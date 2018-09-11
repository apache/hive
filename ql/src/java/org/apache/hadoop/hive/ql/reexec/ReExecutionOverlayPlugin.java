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

package org.apache.hadoop.hive.ql.reexec;

import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;

/**
 * Re-Executes a query only adding an extra overlay
 */
public class ReExecutionOverlayPlugin implements IReExecutionPlugin {

  private Driver driver;
  private Map<String, String> subtree;

  class LocalHook implements ExecuteWithHookContext {

    @Override
    public void run(HookContext hookContext) throws Exception {
      if (hookContext.getHookType() == HookType.ON_FAILURE_HOOK) {
        Throwable exception = hookContext.getException();
        if (exception != null) {
          if (exception.getMessage() != null && exception.getMessage().contains("Vertex failed,")) {
            retryPossible = true;
          }
        }
      }
    }
  }

  @Override
  public void initialize(Driver driver) {
    this.driver = driver;
    driver.getHookRunner().addOnFailureHook(new LocalHook());
    HiveConf conf = driver.getConf();
    subtree = conf.subtree("reexec.overlay");
  }

  private boolean retryPossible;

  @Override
  public void prepareToReExecute() {
    HiveConf conf = driver.getConf();
    conf.verifyAndSetAll(subtree);
  }

  @Override
  public boolean shouldReExecute(int executionNum) {
    return executionNum == 1 && !subtree.isEmpty() && retryPossible;
  }

  @Override
  public boolean shouldReExecute(int executionNum, PlanMapper pm1, PlanMapper pm2) {
    return executionNum == 1;
  }

  @Override
  public void beforeExecute(int executionIndex, boolean explainReOptimization) {
  }

  @Override
  public void afterExecute(PlanMapper planMapper, boolean success) {
  }

}
