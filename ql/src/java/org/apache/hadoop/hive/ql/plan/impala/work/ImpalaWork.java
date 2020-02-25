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
package org.apache.hadoop.hive.ql.plan.impala.work;

import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.impala.thrift.TExecRequest;

import java.io.Serializable;

/**
 *  Encapsulates information required for Impala Execution
 */
@Explain(displayName = "Impala", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
public class ImpalaWork implements Serializable {
    /* Query that generated this ImpalaWork */
    private final String query;
    /* Fully formed Impala execution request (a planned Impala query). Is NULL if passing query directly to Impala. */
    private final TExecRequest execRequest;
    /* Fetch task associated with this work. */
    private final FetchTask fetch;
    /* Desired row batch size (number of rows returned in each request) */
    private int fetchSize;

    public ImpalaWork(TExecRequest execRequest, String query, FetchTask fetch, int fetchSize) {
        this.execRequest = execRequest;
        this.query = query;
        this.fetch = fetch;
        this.fetchSize = fetchSize;
    }

    public ImpalaWork(String query, FetchTask fetch, int rowBatchSize) {
        this(null, query, fetch, rowBatchSize);
    }

    public boolean hasPlannedWork() {
        return execRequest != null;
    }

    @Explain(displayName = "Impala Plan")
    public String getImpalaExplain() {
        return "\n" + execRequest.getQuery_exec_request().getQuery_plan();
    }

    public TExecRequest getExecRequest() {
        return execRequest;
    }

    public String getQuery() {
        return query;
    }

    public FetchTask getFetch() {
        return fetch;
    }

    public int getFetchSize() {
        return fetchSize;
    }
}
