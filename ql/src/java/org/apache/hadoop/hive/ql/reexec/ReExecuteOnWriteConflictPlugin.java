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

import com.google.common.base.Throwables;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

public class ReExecuteOnWriteConflictPlugin implements IReExecutionPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(ReExecuteOnWriteConflictPlugin.class);
  private static boolean retryPossible;

  private static final Pattern writeConflictErrorPattern = Pattern.compile("^Found.*conflicting.*files(.*)");
  private static final String validationException = "org.apache.iceberg.exceptions.ValidationException";

  private static final class LocalHook implements ExecuteWithHookContext {
    @Override
    public void run(HookContext hookContext) throws Exception {
      if (hookContext.getHookType() == HookContext.HookType.ON_FAILURE_HOOK) {
        Throwable exception = hookContext.getException();
        
        if (exception != null && exception.getMessage() != null) {
          Throwable cause = Throwables.getRootCause(exception);

          if (cause.getClass().getName().equals(validationException) &&
              cause.getMessage().matches(writeConflictErrorPattern.pattern())) {
            retryPossible = true;
            LOG.info("Retrying query due to write conflict.");
          }
          LOG.info("Got exception message: {} retryPossible: {}", exception.getMessage(), retryPossible);
        }
      }
    }
  }

  @Override
  public void initialize(Driver driver) {
    driver.getHookRunner().addOnFailureHook(new ReExecuteOnWriteConflictPlugin.LocalHook());
  }

  @Override
  public boolean shouldReExecute(int executionNum) {
    return retryPossible;
  }

  @Override
  public boolean shouldReExecuteAfterCompile(int executionNum, PlanMapper oldPlanMapper, PlanMapper newPlanMapper) {
    return retryPossible;
  }
}
