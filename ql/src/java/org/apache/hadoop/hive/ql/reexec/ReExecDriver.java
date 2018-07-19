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

package org.apache.hadoop.hive.ql.reexec;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.QueryDisplay;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSource;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Enables to use re-execution logics.
 *
 * Covers the IDriver interface, handles query re-execution; and asks clear questions from the underlying re-execution plugins.
 */
public class ReExecDriver implements IDriver {

  private class HandleReOptimizationExplain implements HiveSemanticAnalyzerHook {

    @Override
    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast) throws SemanticException {
      if (ast.getType() == HiveParser.TOK_EXPLAIN) {
        int childCount = ast.getChildCount();
        for (int i = 1; i < childCount; i++) {
          if (ast.getChild(i).getType() == HiveParser.KW_REOPTIMIZATION) {
            explainReOptimization = true;
            ast.deleteChild(i);
            break;
          }
        }
        if (explainReOptimization && firstExecution()) {
          Tree execTree = ast.getChild(0);
          execTree.setParent(ast.getParent());
          ast.getParent().setChild(0, execTree);
          return (ASTNode) execTree;
        }
      }
      return ast;
    }

    @Override
    public void postAnalyze(HiveSemanticAnalyzerHookContext context, List<Task<?>> rootTasks)
        throws SemanticException {
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ReExecDriver.class);
  private boolean explainReOptimization;
  protected Driver coreDriver;
  private QueryState queryState;
  private String currentQuery;
  private int executionIndex;

  private ArrayList<IReExecutionPlugin> plugins;

  @Override
  public HiveConf getConf() {
    return queryState.getConf();
  }

  public boolean firstExecution() {
    return executionIndex == 0;
  }

  public ReExecDriver(QueryState queryState, String userName, QueryInfo queryInfo,
      ArrayList<IReExecutionPlugin> plugins) {
    this.queryState = queryState;
    coreDriver = new Driver(queryState, userName, queryInfo, null);
    coreDriver.getHookRunner().addSemanticAnalyzerHook(new HandleReOptimizationExplain());
    this.plugins = plugins;

    for (IReExecutionPlugin p : plugins) {
      p.initialize(coreDriver);
    }
  }

  @Override
  public int compile(String string) {
    return coreDriver.compile(string);
  }

  @Override
  public CommandProcessorResponse compileAndRespond(String statement) {
    currentQuery = statement;
    return coreDriver.compileAndRespond(statement);
  }

  @Override
  public QueryPlan getPlan() {
    return coreDriver.getPlan();
  }

  @Override
  public QueryDisplay getQueryDisplay() {
    return coreDriver.getQueryDisplay();
  }

  @Override
  public void setOperationId(String guid64) {
    coreDriver.setOperationId(guid64);
  }

  @Override
  public CommandProcessorResponse run() {
    executionIndex = 0;
    int maxExecutuions = 1 + coreDriver.getConf().getIntVar(ConfVars.HIVE_QUERY_MAX_REEXECUTION_COUNT);


    while (true) {
      executionIndex++;
      for (IReExecutionPlugin p : plugins) {
        p.beforeExecute(executionIndex, explainReOptimization);
      }
      coreDriver.getContext().setExecutionIndex(executionIndex);
      LOG.info("Execution #{} of query", executionIndex);
      CommandProcessorResponse cpr = coreDriver.run();

      PlanMapper oldPlanMapper = coreDriver.getPlanMapper();
      afterExecute(oldPlanMapper, cpr.getResponseCode() == 0);

      boolean shouldReExecute = explainReOptimization && executionIndex==1;
      shouldReExecute |= cpr.getResponseCode() != 0 && shouldReExecute();

      if (executionIndex >= maxExecutuions || !shouldReExecute) {
        return cpr;
      }
      LOG.info("Preparing to re-execute query");
      prepareToReExecute();
      CommandProcessorResponse compile_resp = coreDriver.compileAndRespond(currentQuery);
      if (compile_resp.failed()) {
        LOG.error("Recompilation of the query failed; this is unexpected.");
        // FIXME: somehow place pointers that re-execution compilation have failed; the query have been successfully compiled before?
        return compile_resp;
      }

      PlanMapper newPlanMapper = coreDriver.getPlanMapper();
      if (!explainReOptimization && !shouldReExecuteAfterCompile(oldPlanMapper, newPlanMapper)) {
        LOG.info("re-running the query would probably not yield better results; returning with last error");
        // FIXME: retain old error; or create a new one?
        return cpr;
      }
    }
  }

  private void afterExecute(PlanMapper planMapper, boolean success) {
    for (IReExecutionPlugin p : plugins) {
      p.afterExecute(planMapper, success);
    }
  }

  private boolean shouldReExecuteAfterCompile(PlanMapper oldPlanMapper, PlanMapper newPlanMapper) {
    boolean ret = false;
    for (IReExecutionPlugin p : plugins) {
      boolean shouldReExecute = p.shouldReExecute(executionIndex, oldPlanMapper, newPlanMapper);
      LOG.debug("{}.shouldReExecuteAfterCompile = {}", p, shouldReExecute);
      ret |= shouldReExecute;
    }
    return ret;
  }

  private boolean shouldReExecute() {
    boolean ret = false;
    for (IReExecutionPlugin p : plugins) {
      boolean shouldReExecute = p.shouldReExecute(executionIndex);
      LOG.debug("{}.shouldReExecute = {}", p, shouldReExecute);
      ret |= shouldReExecute;
    }
    return ret;
  }

  @Override
  public CommandProcessorResponse run(String command) {
    CommandProcessorResponse r0 = compileAndRespond(command);
    if (r0.getResponseCode() != 0) {
      return r0;
    }
    return run();
  }

  protected void prepareToReExecute() {
    for (IReExecutionPlugin p : plugins) {
      p.prepareToReExecute();
    }
  }

  @Override
  public boolean getResults(List res) throws IOException {
    return coreDriver.getResults(res);
  }

  @Override
  public void setMaxRows(int maxRows) {
    coreDriver.setMaxRows(maxRows);
  }

  @Override
  public FetchTask getFetchTask() {
    return coreDriver.getFetchTask();
  }

  @Override
  public Schema getSchema() {
    if(explainReOptimization) {
      return coreDriver.getExplainSchema();
    }
    return coreDriver.getSchema();
  }

  @Override
  public boolean isFetchingTable() {
    return coreDriver.isFetchingTable();
  }

  @Override
  public void resetFetch() throws IOException {
    coreDriver.resetFetch();
  }

  @Override
  public void close() {
    coreDriver.close();
  }

  @Override
  public void destroy() {
    coreDriver.destroy();
  }

  @Override
  public final Context getContext() {
    return coreDriver.getContext();
  }

  @VisibleForTesting
  public void setStatsSource(StatsSource statsSource) {
    coreDriver.setStatsSource(statsSource);
  }

  @Override
  public boolean hasResultSet() {
    return explainReOptimization || coreDriver.hasResultSet();
  }

}
