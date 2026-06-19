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
import java.util.Set;

import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.CompileTimeCounters;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.tez.common.counters.TezCounters;

/**
 * Implementation of a pre execute hook that adds compile time tez counters to tez tasks.
 */
public class CompileTimeCounterPreHook implements ExecuteWithHookContext {

  @Override
  public void run(HookContext hookContext) throws Exception {
    assert(hookContext.getHookType() == HookType.PRE_EXEC_HOOK);
    QueryPlan plan = hookContext.getQueryPlan();
    if (plan == null) {
      return;
    }

    int numMrJobs = Utilities.getMRTasks(plan.getRootTasks()).size();
    List<TezTask> rootTasks = Utilities.getTezTasks(plan.getRootTasks());
    int numTezJobs = rootTasks.size();
    if (numMrJobs + numTezJobs <= 0) {
      return; // ignore client only queries
    }

    for (TezTask tezTask : rootTasks) {
      TezCounters tezCounters = new TezCounters();
      String groupName = CompileTimeCounters.class.getName();
      long totalFileSize = 0;
      long totalRawDataSize = 0;
      for (TezTask currTask : Utilities.getTezTasks(plan.getRootTasks())) {
        TezWork work = currTask.getWork();
        for (BaseWork w : work.getAllWork()) {
          for (Operator<?> op : w.getAllRootOperators()) {
            Set<TableScanOperator> tsOpSet = OperatorUtils.findOperators(op, TableScanOperator.class);
            // iterate all TS op from all work, and get the total file size (make appropriate DPP adjustment)
            for (TableScanOperator sourceTsOp : tsOpSet) {
              String vertexName = w.getName();
              String counterName = Utilities.getVertexCounterName(CompileTimeCounters.TOTAL_FILE_SIZE.name(), vertexName);
              totalFileSize += sourceTsOp.getStatistics().getTotalFileSize();
              totalRawDataSize += sourceTsOp.getStatistics().getDataSize();
              tezCounters.findCounter(groupName, counterName).increment(sourceTsOp.getStatistics().getTotalFileSize());
              counterName = Utilities.getVertexCounterName(CompileTimeCounters.RAW_DATA_SIZE.name(), vertexName);
              tezCounters.findCounter(groupName, counterName).increment(sourceTsOp.getStatistics().getDataSize());
            }
          }
        }
      }
      tezCounters.findCounter(groupName, CompileTimeCounters.TOTAL_FILE_SIZE.name()).increment(totalFileSize);
      tezCounters.findCounter(groupName, CompileTimeCounters.RAW_DATA_SIZE.name()).increment(totalRawDataSize);
      tezTask.setTezCounters(tezCounters);
    }
  }
}
