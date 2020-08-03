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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;

import java.util.regex.Pattern;

public class ReExecuteLostAMQueryPlugin implements IReExecutionPlugin {
  private boolean retryPossible;
  private int maxExecutions = 1;

  // Lost am container have exit code -100, due to node failures.
  private Pattern lostAMContainerErrorPattern = Pattern.compile(".*AM Container for .* exited .* exitCode: -100.*");

  class LocalHook implements ExecuteWithHookContext {

    @Override
    public void run(HookContext hookContext) throws Exception {
      if (hookContext.getHookType() == HookContext.HookType.ON_FAILURE_HOOK) {
        Throwable exception = hookContext.getException();

        if (exception != null && exception.getMessage() != null
            && lostAMContainerErrorPattern.matcher(exception.getMessage()).matches()) {
          retryPossible = true;
        }
      }
    }
  }

  @Override
  public void initialize(Driver driver) {
    driver.getHookRunner().addOnFailureHook(new LocalHook());
    maxExecutions = 1 + driver.getConf().getIntVar(HiveConf.ConfVars.HIVE_QUERY_MAX_REEXECUTION_COUNT);
  }

  @Override
  public void beforeExecute(int executionIndex, boolean explainReOptimization) {
  }

  @Override
  public boolean shouldReExecute(int executionNum, CommandProcessorException ex) {
    return (executionNum < maxExecutions) && retryPossible;
  }

  @Override
  public void prepareToReExecute() {
  }

  @Override
  public boolean shouldReExecute(int executionNum, PlanMapper oldPlanMapper, PlanMapper newPlanMapper) {
    return retryPossible;
  }

  @Override
  public void afterExecute(PlanMapper planMapper, boolean successfull) {
  }
}
