/**
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;

/**
 * Post execution hook to print hive tez counters to console error stream.
 */
public class PostExecTezSummaryPrinter implements ExecuteWithHookContext {
  private static final Logger LOG = LoggerFactory.getLogger(PostExecTezSummaryPrinter.class.getName());

  @Override
  public void run(HookContext hookContext) throws Exception {
    assert (hookContext.getHookType() == HookContext.HookType.POST_EXEC_HOOK);
    HiveConf conf = hookContext.getConf();
    if (!"tez".equals(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE))) {
      return;
    }

    LOG.info("Executing post execution hook to print tez summary..");
    SessionState ss = SessionState.get();
    SessionState.LogHelper console = ss.getConsole();
    QueryPlan plan = hookContext.getQueryPlan();
    if (plan == null) {
      return;
    }

    List<TezTask> rootTasks = Utilities.getTezTasks(plan.getRootTasks());
    for (TezTask tezTask : rootTasks) {
      LOG.info("Printing summary for tez task: " + tezTask.getName());
      TezCounters counters = tezTask.getTezCounters();
      if (counters != null) {
        for (CounterGroup group : counters) {
          if ("HIVE".equals(group.getDisplayName())) {
            console.printError(tezTask.getId() + " HIVE COUNTERS:");
            for (TezCounter counter : group) {
              console.printError("   " + counter.getDisplayName() + ": " + counter.getValue());
            }
          }
        }
      }
    }
  }

}
