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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.impala.thrift.TExecRequest;

import java.io.Serializable;

/**
 * Encapsulates information required for Impala Execution
 */
@Explain(displayName = "Impala", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
public class ImpalaWork implements Serializable {
    /* Type of Impala work. */
    private final WorkType type;
    /* Generated query if type is PLANNED_QUERY. Otherwise, query that generated this ImpalaWork. */
    private final String query;
    /* Fully formed Impala execution request (a planned Impala query). Is NULL if passing query directly to Impala. */
    private final TExecRequest execRequest;
    /* Fetch task associated with this work. */
    private final FetchTask fetch;
    /* Desired row batch size (number of rows returned in each request) */
    private final long fetchSize;

    private ImpalaWork(WorkType type, TExecRequest execRequest, String query, FetchTask fetch, long fetchSize) {
        this.type = type;
        this.execRequest = execRequest;
        this.query = query;
        this.fetch = fetch;
        this.fetchSize = fetchSize;
    }

    public static ImpalaWork createPlannedWork(TExecRequest execRequest, String query, FetchTask fetch, long fetchSize) {
        return new ImpalaWork(WorkType.PLANNED_EXEC_REQUEST, execRequest, query, fetch, fetchSize);
    }

    public static ImpalaWork createPlannedWork(String query, FetchTask fetch, long fetchSize) {
        return new ImpalaWork(WorkType.PLANNED_QUERY, null, query, fetch, fetchSize);
    }

    public static ImpalaWork createQuery(String query, FetchTask fetch, long fetchSize) {
      return new ImpalaWork(WorkType.QUERY, null, query, fetch, fetchSize);
    }

    public WorkType getType() {
        return type;
    }

    public TExecRequest getExecRequest() {
        Preconditions.checkState(type == WorkType.PLANNED_EXEC_REQUEST);
        return execRequest;
    }

    public String getQuery() {
        return query;
    }

    public FetchTask getFetch() {
        return fetch;
    }

    public long getFetchSize() {
        return fetchSize;
    }

    @Explain(displayName = "Impala Plan")
    public String getImpalaExplain() {
        return type == WorkType.PLANNED_EXEC_REQUEST ?
            "\n" + execRequest.getQuery_exec_request().getQuery_plan() : null;
    }

    @Explain(displayName = "Impala Query")
    public String getImpalaQuery() {
        return type == WorkType.PLANNED_QUERY || type == WorkType.QUERY ? query : null;
    }

    /**
     * Whether this is a generated plan, a generated query, or a query.
     */
    public enum WorkType {
        PLANNED_EXEC_REQUEST,
        PLANNED_QUERY,
        QUERY
    }
}
