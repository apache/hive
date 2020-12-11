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
package org.apache.hadoop.hive.ql.impala.work;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.engine.EngineWork;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.impala.plan.ImpalaCompiledPlan;
import java.io.Serializable;

/**
 * Encapsulates information required for Impala Execution
 */
@Explain(displayName = "Impala", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
public class ImpalaWork extends EngineWork implements Serializable {
    /* Type of Impala work. */
    private final WorkType type;
    /* Generated query if type is COMPILED_QUERY. Otherwise, query that generated this ImpalaWork. */
    private final String query;
    /* Fully formed Impala execution request (a planned Impala query). Is NULL if passing query directly to Impala. */
    private final ImpalaCompiledPlan plan;
    /* Fetch task associated with this work. */
    private final FetchTask fetch;
    /* Desired row batch size (number of rows returned in each request) */
    private final long fetchSize;

    private ImpalaWork(WorkType type, ImpalaCompiledPlan plan, String query, FetchTask fetch, long fetchSize) {
        this.type = type;
        this.plan = plan;
        this.query = query;
        this.fetch = fetch;
        this.fetchSize = fetchSize;
    }

    public static ImpalaWork createPlannedWork(ImpalaCompiledPlan plan, String query, FetchTask fetch, long fetchSize) {
        return new ImpalaWork(WorkType.COMPILED_PLAN, plan, query, fetch, fetchSize);
    }

    public static ImpalaWork createPlannedWork(String query, FetchTask fetch, long fetchSize) {
        return new ImpalaWork(WorkType.COMPILED_QUERY, null, query, fetch, fetchSize);
    }

    public static ImpalaWork createQuery(String query, FetchTask fetch, long fetchSize) {
      return new ImpalaWork(WorkType.QUERY, null, query, fetch, fetchSize);
    }

    public WorkType getType() {
        return type;
    }

    public ImpalaCompiledPlan getCompiledPlan() {
        Preconditions.checkState(type == WorkType.COMPILED_PLAN);
        return plan;
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
        return type == WorkType.COMPILED_PLAN ? "\n" + plan.getExplain() : null;
    }

    @Explain(displayName = "Impala Query")
    public String getImpalaQuery() {
        return type == WorkType.COMPILED_QUERY || type == WorkType.QUERY ? query : null;
    }

    /**
     * Whether this is a generated plan, a generated query, or a query.
     */
    public enum WorkType {
        COMPILED_PLAN,
        COMPILED_QUERY,
        QUERY
    }
}
