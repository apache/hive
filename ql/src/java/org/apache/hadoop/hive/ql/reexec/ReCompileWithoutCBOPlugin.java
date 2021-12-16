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
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHook;
import org.apache.hadoop.hive.ql.hooks.QueryLifeTimeHookContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Re-compiles the query without CBO
 */
public class ReCompileWithoutCBOPlugin implements IReExecutionPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(ReCompileWithoutCBOPlugin.class);


  private Driver driver;
  private boolean retryPossible;
  private String cboMsg;

  class LocalHook implements QueryLifeTimeHook {
    @Override
    public void beforeCompile(QueryLifeTimeHookContext ctx) {
      // noop
    }

    @Override
    public void afterCompile(QueryLifeTimeHookContext ctx, boolean hasError) {
      if (hasError) {
        Throwable throwable = ctx.getHookContext().getException();
        retryPossible = throwable != null && throwable instanceof ReCompileException;
        cboMsg = retryPossible ? ((ReCompileException) throwable).getCboMessage() : null;
        LOG.debug("Recompile check result {} with CBO message {}", retryPossible, cboMsg);
      } else {
        retryPossible = false;
      }
    }

    @Override
    public void beforeExecution(QueryLifeTimeHookContext ctx) {
      // noop
    }

    @Override
    public void afterExecution(QueryLifeTimeHookContext ctx, boolean hasError) {
      // noop
    }
  }

  @Override
  public void initialize(Driver driver) {
    this.driver = driver;
    driver.getHookRunner().addLifeTimeHook(new ReCompileWithoutCBOPlugin.LocalHook());
  }

  @Override
  public void prepareToReCompile() {
    HiveConf conf = driver.getConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_CBO_ENABLED, false);
    driver.getContext().setCboInfo(cboMsg);
  }

  @Override
  public boolean shouldReCompile(int executionNum) {
    return retryPossible && executionNum == 1;
  }
}
