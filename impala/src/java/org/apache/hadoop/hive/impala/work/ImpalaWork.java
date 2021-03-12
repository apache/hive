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
package org.apache.hadoop.hive.impala.work;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.TaskQueue;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.engine.EngineWork;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.impala.plan.ImpalaCompiledPlan;
import org.apache.hadoop.hive.ql.plan.api.Query;

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
    /* Whether to submit explain plan to backend to register its profile. */
    private final boolean submitExplainToBackend;
    /* Query state (used to instantiate task to send explain plan to coordinator). */
    private final QueryState queryState;
    /* Context (used to instantiate task to send explain plan to coordinator). */
    private final Context context;
    /* Explain plan */
    private String explain;
    /* Generated invalidate metadata query for some compiled queries, e.g., compute stats. */
    private final String invalidateTableMetadataQuery;

    /* Execute in async mode */
    private final boolean runAsync;

    private ImpalaWork(WorkType type, ImpalaCompiledPlan plan, String query, FetchTask fetch, long fetchSize,
        boolean submitExplainToBackend, QueryState queryState, Context context, String invalidateTableMetadataQuery,
        boolean runAsync) {
      this.type = type;
      this.plan = plan;
      this.query = query;
      this.fetch = fetch;
      this.fetchSize = fetchSize;
      this.submitExplainToBackend = submitExplainToBackend;
      this.queryState = queryState;
      this.context = context;
      this.explain = null;
      this.invalidateTableMetadataQuery = invalidateTableMetadataQuery;
      this.runAsync = runAsync;
    }

    public static ImpalaWork createPlannedWork(ImpalaCompiledPlan plan, QueryState queryState, FetchTask fetch, long fetchSize,
        boolean submitExplainToBackend, Context context) {
      return new ImpalaWork(WorkType.COMPILED_PLAN, plan, queryState.getQueryString(), fetch, fetchSize,
          submitExplainToBackend, queryState, context, null, true);
    }

    public static ImpalaWork createPlannedWork(String query, FetchTask fetch, long fetchSize, String invalidateTableMetadataQuery) {
      return new ImpalaWork(WorkType.COMPILED_QUERY, null, query, fetch, fetchSize,
          false, null, null, invalidateTableMetadataQuery, true);
    }

    public static ImpalaWork createPlannedWork(String query, FetchTask fetch, long fetchSize) {
      return createPlannedWork(query, fetch, fetchSize, true);
    }

    public static ImpalaWork createPlannedWork(String query, FetchTask fetch, long fetchSize, boolean runAsync) {
        return new ImpalaWork(WorkType.COMPILED_QUERY, null, query, fetch, fetchSize,
                false, null, null,null, runAsync);
    }

    public static ImpalaWork createQuery(String query, FetchTask fetch, long fetchSize) {
      return new ImpalaWork(WorkType.QUERY, null, query, fetch, fetchSize,
          false, null, null, null, true);
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
      if (type == WorkType.COMPILED_PLAN) {
        if (explain == null) {
          explain = "\n" + plan.getExplain();
          if (submitExplainToBackend) {
            submitExplain();
          }
        }
        return explain;
      }
      return null;
    }

    private void submitExplain() {
      Preconditions.checkState(plan.getIsExplain());
      // Send plan to backend by executing this task. The configuration
      // should already be set properly to avoid executing the query
      // and rather register this as an EXPLAIN query
      Task<ImpalaWork> task = TaskFactory.get(this);
      task.initialize(queryState, null, new TaskQueue(context), context);
      task.execute();
      if (task.getException() != null) {
        throw new RuntimeException("Unexpected error: " + task.getException().getMessage(),
            task.getException());
      }
    }

    @Explain(displayName = "Impala Query")
    public String getImpalaQuery() {
      return type == WorkType.COMPILED_QUERY || type == WorkType.QUERY ? query : null;
    }

    @Explain(displayName = "Invalidate Table Metadata")
    public Boolean isInvalidateTableMetadata() {
      return type == WorkType.COMPILED_QUERY ? invalidateTableMetadataQuery != null : null;
    }

    public String getInvalidateTableMetadataQuery() {
      return invalidateTableMetadataQuery;
    }

    @Explain(displayName = "Run In Sync Mode", displayOnlyOnTrue = true)
    public Boolean isRunInSyncMode() {
        return runAsync == false;
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
