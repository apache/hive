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

/**
 * Implementation of a Task for managing the execution of ImpalaWork. It starts execution of the desired Impala query
 * or plan and passes the context required for the ImpalaStreamingFetchOperator to retrieve the execution results.
 */
public class ImpalaTask extends Task<ImpalaWork> {
    ImpalaSession session = null;

    @Override
    public void shutdown() {
        LOG.info("ImpalaTask shutdown()");
        if (session != null) {
            session.close();
        }
    }

    @Override
    public void initialize(QueryState queryState, QueryPlan queryPlan, TaskQueue taskQueue, Context context) {
        super.initialize(queryState, queryPlan, taskQueue, context);
    }

    @Override
    public int execute() {
        // zero is success, non-zero is failure
        int rc = 0;
        try {
            // CDPD-6966: Cache Impala Session connection for a user
            // Currently this is recreated with each query.
            session = new ImpalaSession(getQueryState().getConf().getVar(HiveConf.ConfVars.HIVE_IMPALA_ADDRESS));
            session.open();

            ImpalaWork work = getWork();
            TOperationHandle opHandle = null;
            if (work.hasPlannedWork()) {
                opHandle = session.executePlan(work.getQuery(), work.getExecRequest());
            } else {
                opHandle = session.execute(work.getQuery());
            }

            FetchTask fetch = work.getFetch();
            FetchOperator fetchOp = fetch.getFetchOp();
            if (fetchOp instanceof ImpalaStreamingFetchOperator) {
                ImpalaStreamingFetchOperator impFetchOp = (ImpalaStreamingFetchOperator) fetchOp;
                impFetchOp.setImpalaFetchContext(new ImpalaFetchContext(session, opHandle, work.getFetchSize()));
            } else {
                throw new HiveException("Unexpected Fetch operator");
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
