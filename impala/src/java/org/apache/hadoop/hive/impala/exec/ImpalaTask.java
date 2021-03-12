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
package org.apache.hadoop.hive.impala.exec;

import java.io.Serializable;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ResultMethod;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.TaskQueue;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.engine.EngineSession;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.impala.work.ImpalaWork;
import org.apache.hive.service.rpc.thrift.TOperationHandle;

/**
 * Implementation of a Task for managing the execution of ImpalaWork. It starts execution of the desired Impala query
 * or plan and passes the context required for the ImpalaStreamingFetchOperator to retrieve the execution results.
 */
public class ImpalaTask extends Task<ImpalaWork> implements Serializable {

    private EngineSession session;

    @Override
    public void initialize(QueryState queryState, QueryPlan queryPlan, TaskQueue taskQueue, Context context) {
        super.initialize(queryState, queryPlan, taskQueue, context);
    }

    private void closeOperation(TOperationHandle opHandle) {
      // Always close operation independently on whether
      // it was successful or not
      try {
        session.closeOperation(opHandle);
      } catch (HiveException e) {
        LOG.warn("Could not close operation", e);
      }
    }

    @Override
    public int execute() {
        // zero is success, non-zero is failure
        int rc = 0;
        HiveConf conf = getQueryState().getConf();
        boolean isStreaming;
        try {
            ImpalaSessionImpl sessionImpl = ImpalaSessionManager.getInstance().getSession(conf);
            this.session = sessionImpl;
            TOperationHandle opHandle;
            switch (work.getType()) {
            case COMPILED_PLAN:
                isStreaming = conf.getResultMethod() == ResultMethod.STREAMING;
                opHandle = sessionImpl.executePlan(work.getQuery(), work.getCompiledPlan());
                break;
            case COMPILED_QUERY:
                Preconditions.checkState(
                        queryPlan.getOperation() == HiveOperation.ANALYZE_TABLE ||
                        queryPlan.getOperation() == HiveOperation.DROP_STATS ||
                        queryPlan.getOperation() == HiveOperation.REFRESH_TABLE);
                if (work.getInvalidateTableMetadataQuery() != null) {
                  TOperationHandle opHandleInvalidate = session.execute(work.getInvalidateTableMetadataQuery(), false);
                  closeOperation(opHandleInvalidate);
                }
                //  isStreaming is true only for ANALYZE_TABLE || DROP_STATS hive operation
                // For REFRESH_TABLE op, refresh query in Impala does not produce any output
                // and so we execute the sql query in sync mode.
                isStreaming = (queryPlan.getOperation() == HiveOperation.ANALYZE_TABLE ||
                        queryPlan.getOperation() == HiveOperation.DROP_STATS);
                opHandle = session.execute(work.getQuery(), !work.isRunInSyncMode());
                break;
            case QUERY:
                isStreaming = conf.getResultMethod() == ResultMethod.STREAMING;
                opHandle = session.execute(work.getQuery(), true);
                break;
            default:
                throw new RuntimeException("Type not recognized: " + work.getType());
            }

            FetchTask fetch = work.getFetch();
            if(fetch != null && (work.getType() != ImpalaWork.WorkType.COMPILED_PLAN || !work.getCompiledPlan().getIsExplain())) {
              if (fetch.getFetchOp() instanceof ImpalaStreamingFetchOperator) {
                  Preconditions.checkState(isStreaming);
                  ImpalaStreamingFetchOperator impFetchOp = (ImpalaStreamingFetchOperator) fetch.getFetchOp();
                  impFetchOp.setImpalaFetchContext(new ImpalaFetchContext(session, opHandle, work.getFetchSize()));
              } else {
                // Non streaming mode: This will block until results are ready.
                Preconditions.checkState(isStreaming == false);
                try {
                  session.fetch(opHandle, 1);
                } finally {
                  closeOperation(opHandle);
                }
              }
            } else {
              closeOperation(opHandle);
            }

        } catch (Throwable e) {
            setException(e);
            rc = 1;
        }

        return rc;
    }

    @Override
    public void shutdown() {
      if(session != null) {
        session.notifyShutdown();
      }
      super.shutdown();
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
