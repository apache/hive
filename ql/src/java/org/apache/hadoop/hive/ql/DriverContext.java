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

package org.apache.hadoop.hive.ql;

import java.io.DataInput;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.ql.cache.results.CacheUsage;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache.CacheEntry;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.tez.TezRuntimeContext;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.RewriteSemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSource;

/**
 * Context for the procedure managed by the Driver.
 */
public class DriverContext {
  public static final String DRIVER_DEFAULT_USER_NAME_PROP = "hive.driver.default.user.name";
  public static final String DRIVER_DEFAULT_OPERATION_ID_PROP = "hive.driver.default.operation.id";

  // For WebUI.  Kept alive after queryPlan is freed.
  private final QueryDisplay queryDisplay = new QueryDisplay();

  private final QueryState queryState;
  private final QueryInfo queryInfo;
  private final HiveConf conf;
  private final HookRunner hookRunner;

  // Transaction manager the Driver has been initialized with (can be null).
  // If this is set then this Transaction manager will be used during query
  // compilation/execution rather than using the current session's transaction manager.
  // This might be needed in a situation where a Driver is nested within an already
  // running Driver/query - the nested Driver requires a separate transaction manager
  // so as not to conflict with the outer Driver/query which is using the session
  // transaction manager.
  private final HiveTxnManager initTxnManager;

  private QueryPlan plan;
  private Schema schema;

  private FetchTask fetchTask;
  // Transaction manager used for the query. This will be set at compile time based on
  // either initTxnMgr or from the SessionState, in that order.
  private HiveTxnManager txnManager;
  private TxnType txnType = TxnType.DEFAULT;
  private boolean outdatedTxn;
  private StatsSource statsSource;

  // Boolean to store information about whether valid txn list was generated
  // for current query.
  private boolean validTxnListsGenerated;

  private CacheUsage cacheUsage;
  private CacheEntry usedCacheEntry;

  private boolean retrial = false;

  private DataInput resStream;

  // HS2 operation handle guid string
  private String operationId;
  private String queryErrorMessage;

  private TezRuntimeContext runtimeContext;
  private QueryProperties queryProperties;

  private String queryType;
  private String ddlType;
  private String explainPlan;

  public DriverContext(QueryState queryState, QueryInfo queryInfo, HookRunner hookRunner,
      HiveTxnManager initTxnManager) {
    this.queryState = queryState;
    this.queryInfo = queryInfo;
    this.conf = queryState.getConf();
    this.hookRunner = hookRunner;
    this.initTxnManager = initTxnManager;
  }

  public QueryDisplay getQueryDisplay() {
    return queryDisplay;
  }

  public String getQueryId() {
    return queryDisplay.getQueryId();
  }

  public String getQueryString() {
    return queryDisplay.getQueryString();
  }

  public QueryState getQueryState() {
    return queryState;
  }

  public QueryInfo getQueryInfo() {
    return queryInfo;
  }

  public HiveConf getConf() {
    return conf;
  }

  public HookRunner getHookRunner() {
    return hookRunner;
  }

  public HiveTxnManager getInitTxnManager() {
    return initTxnManager;
  }

  public QueryPlan getPlan() {
    return plan;
  }

  public void setPlan(QueryPlan plan) {
    this.plan = plan;
    // only set runtimeContext if the plan is not null
    // we don't want to nullify runtimeContext if this method is called with plan=null, which is the case when e.g.
    // driver.releasePlan() tries to release resources/objects that are known to be heavy
    if (plan != null) {
      this.runtimeContext = Utilities.getFirstTezTask(plan.getRootTasks())
          .map(TezTask::getRuntimeContext)
          .orElse(null);
      this.queryProperties = plan.getQueryProperties();
    }
  }

  public String getQueryType() {
    return queryType == null ? "UNKNOWN" : queryType;
  }

  public String getDdlType() {
    return ddlType == null ? "" : ddlType;
  }

  public TezRuntimeContext getRuntimeContext() {
    return runtimeContext;
  }

  public QueryProperties getQueryProperties() {
    return queryProperties;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public FetchTask getFetchTask() {
    return fetchTask;
  }

  public void setFetchTask(FetchTask fetchTask) {
    this.fetchTask = fetchTask;
  }

  public HiveTxnManager getTxnManager() {
    return txnManager;
  }

  public void setTxnManager(HiveTxnManager txnManager) {
    this.txnManager = txnManager;
  }

  public TxnType getTxnType() {
    return txnType;
  }

  public void setTxnType(TxnType txnType) {
    this.txnType = txnType;
  }

  public boolean isOutdatedTxn() {
    return outdatedTxn;
  }

  public void setOutdatedTxn(boolean outdated) {
    this.outdatedTxn = outdated;
  }

  public StatsSource getStatsSource() {
    return statsSource;
  }

  public void setStatsSource(StatsSource statsSource) {
    this.statsSource = statsSource;
  }

  public boolean isValidTxnListsGenerated() {
    return validTxnListsGenerated;
  }

  public void setValidTxnListsGenerated(boolean validTxnListsGenerated) {
    this.validTxnListsGenerated = validTxnListsGenerated;
  }

  public CacheUsage getCacheUsage() {
    return cacheUsage;
  }

  public void setCacheUsage(CacheUsage cacheUsage) {
    this.cacheUsage = cacheUsage;
  }

  public CacheEntry getUsedCacheEntry() {
    return usedCacheEntry;
  }

  public void setUsedCacheEntry(CacheEntry usedCacheEntry) {
    this.usedCacheEntry = usedCacheEntry;
  }

  public boolean isRetrial() {
    return retrial;
  }

  public void setRetrial(boolean retrial) {
    this.retrial = retrial;
  }

  public DataInput getResStream() {
    return resStream;
  }

  public void setResStream(DataInput resStream) {
    this.resStream = resStream;
  }

  public String getOperationId() {
    return operationId;
  }

  public void setOperationId(String operationId) {
    this.operationId = operationId;
  }

  public String getQueryErrorMessage() {
    return queryErrorMessage;
  }

  public void setQueryErrorMessage(String queryErrorMessage) {
    this.queryErrorMessage = queryErrorMessage;
  }

  public long getQueryStartTime() {
    return getQueryInfo() != null ? getQueryInfo().getBeginTime() : getQueryDisplay().getQueryStartTime();
  }

  /**
   * Resolves the query type from the SemanticAnalyzer and the query AST, which might look magic.
   * Magic is needed because a query can fail due to semantic exceptions, and at that time, from syntax point of view,
   * it's already clear what's the query type, even if the SemanticAnalyzer bailed out, typical problems:
   * 1. general semantic exception
   * 2. transaction manager validation (throwin exception)
   * What a user expects here is something like "QUERY", "DDL", "DML", so this magic will do its best to tell.
   * any kind of "analyze": STATS
   * DML operations (INSERT, UPDATE, DELETE, MERGE): DML
   * MAPRED: QUERY, DML (depending on QueryProperties achieved in compile time)
   * FETCH: QUERY: a simple fetch task is a QUERY
   * UNKNOWN: if we can't determine the type of the query:
   * e.g. when ParseException happens, we won't do further magic,
   * even if it's obvious by reading the sql statement that user wanted to run e.g. a select query
   * @param sem the semantic analyzer which already analyzed the query
   * @param tree the root ASTNode of the query
   */
  public void setQueryType(BaseSemanticAnalyzer sem, ASTNode tree) {
    List<Task<? extends Serializable>> rootTasks = sem.getAllRootTasks();
    QueryProperties props = sem.getQueryProperties();
    queryType = isAnalyze(props) ? "STATS" : (isDML(props) ? "DML" : null);
    // common confusion whether CTAS is DML or DDL, let's pick DDL here
    if (props != null && props.isCTAS()) {
      queryType = "DDL";
    }
    if (queryType != null) {
      return; //already figured out
    }
    queryType = rootTasks.stream().findFirst().map(t -> {
      String type = t.getType().toString();
      // a MAPRED stage could mean an INSERT query also
      if (props != null && type.equalsIgnoreCase("MAPRED")) {
        return props.isInsert ? "DML" : "QUERY";
      }
      return type;
    }).orElseGet(() -> {
      // in case of a semantic exception (e.g. a table not found or something else)
      // the root AST Node can still imply if this is a query, try to fall back to that
      // instead of "UNKNOWN"
      if (tree.getText().equalsIgnoreCase("TOK_QUERY")){
        return "QUERY";
      }
      // if there is a fetch task, this is a query
      if (sem.getFetchTask() != null) {
        return "QUERY";
      }
      // from this point, best efforts come
      // whether it's a DDL?
      if (DDLSemanticAnalyzerFactory.handles(tree)) {
        return "DDL";
      }
      // whether it's DML? UPDATE/DELETE/MERGE queries are handled with an instance of RewriteSemanticAnalyzer
      if (RewriteSemanticAnalyzer.class.isAssignableFrom(sem.getClass())) {
        return "DML";
      }
      return "UNKNOWN";
    });

    // in case of DDL queries, the DDL type can be figured out from the tree root token, e.g.:
    // CREATETABLE, ALTERTABLE_ADDPARTS, SHOWDATABASES, SHOWTABLES, etc.
    this.ddlType = "DDL".equalsIgnoreCase(queryType) ? tree.getText().substring("TOK_".length()) : "";
  }

  /**
   * The return value of either isAnalyzeCommand() or isAnalyzeRewrite() is always true for analyze commands:
   * isAnalyzeCommand=true for "compute statistics",
   * isAnalyzeRewrite=true for "compute statistics for columns".
   * @return
   */
  private boolean isAnalyze(QueryProperties queryProperties) {
    return queryProperties != null && (queryProperties.isAnalyzeCommand() || queryProperties.isAnalyzeRewrite());
  }

  /**
   * DML (Data Manipulation Language) operations are operations that modify data,
   * queryProperties.isInsert seems to reflect this very well: true for INSERT [OVERWRITE], UPDATE, DELETE, MERGE
   * @return
   */
  private boolean isDML(QueryProperties queryProperties) {
    return queryProperties != null && queryProperties.isInsert;
  }

  public boolean isQueryHistoryExplainEnabled() {
    return conf.getBoolVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_SERVICE_ENABLED) &&
        conf.getBoolVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_SERVICE_EXPLAIN_PLAN_ENABLED);
  }

  public void setExplainPlan(String explainPlan) {
    this.explainPlan = explainPlan;
  }

  public String getExplainPlan() {
    return explainPlan;
  }

  public static String getUserNameFromConf(HiveConf conf) {
    return conf.get(DRIVER_DEFAULT_USER_NAME_PROP);
  }
}
