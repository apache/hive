/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.hooks;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.wm.WmContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Post execution (success or failure) hook to print hive workload manager events summary.
 */
public class PostExecWMEventsSummaryPrinter implements ExecuteWithHookContext {
  private static final Logger LOG = LoggerFactory.getLogger(PostExecWMEventsSummaryPrinter.class.getName());

  @Override
  public void run(HookContext hookContext) throws Exception {
    assert (hookContext.getHookType() == HookContext.HookType.POST_EXEC_HOOK ||
      hookContext.getHookType() == HookContext.HookType.ON_FAILURE_HOOK);
    HiveConf conf = hookContext.getConf();
    if (!"tez".equals(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE))) {
      return;
    }

    LOG.info("Executing post execution hook to print workload manager events summary..");
    SessionState.LogHelper console = SessionState.getConsole();
    QueryPlan plan = hookContext.getQueryPlan();
    if (plan == null) {
      return;
    }

    List<TezTask> rootTasks = Utilities.getTezTasks(plan.getRootTasks());
    for (TezTask tezTask : rootTasks) {
      WmContext wmContext = tezTask.getDriverContext().getCtx().getWmContext();
      if (wmContext != null) {
        wmContext.shortPrint(console);
      }
    }
  }

}
