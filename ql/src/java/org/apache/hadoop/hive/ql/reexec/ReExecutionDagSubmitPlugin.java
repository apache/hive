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

import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Re-Executes a query when DAG submission fails after get session returned successfully. There could be race condition
 * where getSession could return a healthy AM but by the time DAG is submitted the AM could become unhealthy/unreachable
 * (possible DNS or network issues) which can fail tez DAG submission. Since the DAG hasn't started execution yet this
 * failure can be safely restarted/retried.
 */
public class ReExecutionDagSubmitPlugin implements IReExecutionPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(ReExecutionDagSubmitPlugin.class);

  class LocalHook implements ExecuteWithHookContext {

    @Override
    public void run(HookContext hookContext) throws Exception {
      if (hookContext.getHookType() == HookType.ON_FAILURE_HOOK) {
        Throwable exception = hookContext.getException();
        if (exception != null && exception.getMessage() != null) {
          if (exception.getMessage().contains("Dag submit failed")) {
            retryPossible = true;
          }
          LOG.info("Got exception message: {} retryPossible: {}", exception.getMessage(), retryPossible);
        }
      }
    }
  }

  @Override
  public void initialize(Driver driver) {
    driver.getHookRunner().addOnFailureHook(new LocalHook());
  }

  private boolean retryPossible;

  @Override
  public boolean shouldReExecuteAfterCompile(int executionNum, PlanMapper pm1, PlanMapper pm2) {
    return retryPossible;
  }

  @Override
  public boolean shouldReExecute(int executionNum) {
    return retryPossible;
  }

}
