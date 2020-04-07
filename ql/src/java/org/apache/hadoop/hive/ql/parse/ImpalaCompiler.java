/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ImpalaQueryOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaCompiledPlan;
import org.apache.hadoop.hive.ql.plan.impala.work.ImpalaWork;
import org.apache.impala.thrift.TExecRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Creates a single task that encapsulates all work and context required for Impala execution.
 */
public class ImpalaCompiler extends TaskCompiler {
    protected static final Logger LOG = LoggerFactory.getLogger(ImpalaCompiler.class);

    /* When isPlanned is true, a fully planned ExecRequest is expected, otherwise we expect only a query string */
    private boolean isPlanned;
    /* Number of rows fetch from Impala per fetch when streaming */
    private long requestedFetchSize;

    ImpalaCompiler(boolean isPlanned, long requestedFetchSize) {
        this.isPlanned = isPlanned;
        this.requestedFetchSize = requestedFetchSize;
    }

    @Override
    protected void generateTaskTree(List<Task<?>> rootTasks, ParseContext pCtx,
                                    List<Task<MoveWork>> mvTask, Set<ReadEntity> inputs, Set<WriteEntity> outputs)
            throws SemanticException {
        // CDPD-6976: Add Perf logging for Impala Execution
        ImpalaWork work = null;
        if (isPlanned) {
            TExecRequest execRequest = getExecRequest(pCtx);
            work = new ImpalaWork(execRequest, pCtx.getQueryState().getQueryString(),
                pCtx.getFetchTask(), requestedFetchSize);
        } else {
            // CDPD-7172: Investigate security implications of Impala query execution mode
            work = new ImpalaWork(pCtx.getQueryState().getQueryString(), pCtx.getFetchTask(), requestedFetchSize);
        }
        rootTasks.add(TaskFactory.get(work));
    }

    @Override
    protected void optimizeOperatorPlan(ParseContext pCtx, Set<ReadEntity> inputs,
                                        Set<WriteEntity> outputs) throws SemanticException {
    }

    @Override
    protected void decideExecMode(List<Task<?>> rootTasks, Context ctx,
                                  GlobalLimitCtx globalLimitCtx) throws SemanticException {
    }

    @Override
    protected void optimizeTaskPlan(List<Task<?>> rootTasks, ParseContext pCtx, Context ctx) throws SemanticException {
    }

    @Override
    protected void setInputFormat(Task<?> rootTask) {
    }

    private TExecRequest getExecRequest(ParseContext pCtx) {
      Collection<Operator<?>> tableScanOps =
          Lists.<Operator<?>>newArrayList(pCtx.getTopOps().values());
      Set<ImpalaQueryOperator> fsOps = OperatorUtils.findOperators(tableScanOps, ImpalaQueryOperator.class);
      if (fsOps.isEmpty()) {
        throw new RuntimeException("No ImpalaQueryOperator found in the ImpalaCompiler");
      }
      ImpalaCompiledPlan compiledPlan = fsOps.iterator().next().getCompiledPlan();
      return ((ImpalaCompiledPlan) compiledPlan).getExecRequest();

    }
}
