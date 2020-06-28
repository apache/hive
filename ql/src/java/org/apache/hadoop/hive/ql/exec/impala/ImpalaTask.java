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
package org.apache.hadoop.hive.ql.exec.impala;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ImpalaExecutionMode;
import org.apache.hadoop.hive.conf.HiveConf.ImpalaResultMethod;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.TaskQueue;
import org.apache.hadoop.hive.ql.exec.FetchOperator;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.plan.impala.work.ImpalaWork;
import org.apache.hive.service.rpc.thrift.TOperationHandle;

import com.google.common.base.Preconditions;

/**
 * Implementation of a Task for managing the execution of ImpalaWork. It starts execution of the desired Impala query
 * or plan and passes the context required for the ImpalaStreamingFetchOperator to retrieve the execution results.
 */
public class ImpalaTask extends Task<ImpalaWork> {

    @Override
    public void initialize(QueryState queryState, QueryPlan queryPlan, TaskQueue taskQueue, Context context) {
        super.initialize(queryState, queryPlan, taskQueue, context);
    }

    @Override
    public int execute() {
        // zero is success, non-zero is failure
        int rc = 0;
        HiveConf conf = getQueryState().getConf();
        boolean isPlannedMode = conf.getImpalaExecutionMode() == ImpalaExecutionMode.PLAN;
        boolean isStreaming = conf.getImpalaResultMethod() == ImpalaResultMethod.STREAMING;
        try {
            ImpalaSession session = ImpalaSessionManager.getInstance().getSession(conf);
            TOperationHandle opHandle;
            if (work.hasPlannedWork()) {
                Preconditions.checkState(isPlannedMode);
                opHandle = session.executePlan(work.getQuery(), work.getExecRequest());
            } else {
                Preconditions.checkState(isPlannedMode == false, "Tried to pass-through query unexpectedly");
                opHandle = session.execute(work.getQuery());
            }

            FetchTask fetch = work.getFetch();
            if(fetch != null) {
              FetchOperator fetchOp = fetch.getFetchOp();
              if (fetchOp instanceof ImpalaStreamingFetchOperator) {
                  Preconditions.checkState(isStreaming);
                  ImpalaStreamingFetchOperator impFetchOp = (ImpalaStreamingFetchOperator) fetchOp;
                  impFetchOp.setImpalaFetchContext(new ImpalaFetchContext(session, opHandle, work.getFetchSize()));
              } else {
                Preconditions.checkState(isStreaming == false);
                try {
                  // Will block until results are ready
                  session.fetch(opHandle, 1);
                } finally {
                  // Always close operation independently on whether
                  // it was successful or not
                  try {
                    session.closeOperation(opHandle);
                  } catch (HiveException e) {
                    LOG.warn("Could not close operation", e);
                  }
                }
              }
            }

        } catch (Throwable e) {
            setException(e);
            rc = 1;
        }

        return rc;
    }

    @Override
    public StageType getType() {
        return StageType.MAPRED;
    }

    @Override
    public String getName() {
        return "IMPALA";
    }

    @Override
    public String toString() {
        return getId() + ":IMPALA";
    }
}
