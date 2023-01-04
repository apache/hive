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
import java.util.Arrays;
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
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CBOFallbackStrategy;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSource;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Enables to use re-execution logics.
 *
 * Covers the IDriver interface, handles query re-execution; and asks clear questions from the underlying re-execution plugins.
 */
public class ReExecDriver implements IDriver {
  private static final Logger LOG = LoggerFactory.getLogger(ReExecDriver.class);
  private static final SessionState.LogHelper CONSOLE = new SessionState.LogHelper(LOG);

  private final Driver coreDriver;
  private final QueryState queryState;
  private final List<IReExecutionPlugin> plugins;

  private boolean explainReOptimization;
  private String currentQuery;
  private int executionIndex;

  public ReExecDriver(QueryState queryState, QueryInfo queryInfo, List<IReExecutionPlugin> plugins) {
    this.queryState = queryState;
    this.coreDriver = new Driver(queryState, queryInfo, null);
    this.plugins = plugins;

    coreDriver.getHookRunner().addSemanticAnalyzerHook(new HandleReOptimizationExplain());
    plugins.forEach(p -> p.initialize(coreDriver));
  }

  @VisibleForTesting
  public int compile(String command, boolean resetTaskIds) {
    return coreDriver.compile(command, resetTaskIds);
  }

  private boolean firstExecution() {
    return executionIndex == 0;
  }

  private void checkHookConfig() throws CommandProcessorException {
    String strategies = coreDriver.getConf().getVar(ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES);
    CBOFallbackStrategy fallbackStrategy =
        CBOFallbackStrategy.valueOf(coreDriver.getConf().getVar(ConfVars.HIVE_CBO_FALLBACK_STRATEGY));
    if (fallbackStrategy.allowsRetry() &&
        (strategies == null || !Arrays.stream(strategies.split(",")).anyMatch("recompile_without_cbo"::equals))) {
      String errorMsg = "Invalid configuration. If fallbackStrategy is set to " + fallbackStrategy.name() + " then " +
          ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES.varname + " should contain 'recompile_without_cbo'";
      CONSOLE.printError(errorMsg);
      throw new CommandProcessorException(errorMsg);
    }
  }

  @Override
  public CommandProcessorResponse compileAndRespond(String statement) throws CommandProcessorException {
    currentQuery = statement;

    checkHookConfig();

    int compileIndex = 0;
    int maxCompilations = 1 + coreDriver.getConf().getIntVar(ConfVars.HIVE_QUERY_MAX_RECOMPILATION_COUNT);
    while (true) {
      compileIndex++;

      final int currentIndex = compileIndex;
      plugins.forEach(p -> p.beforeCompile(currentIndex));

      LOG.info("Compile #{} of query", compileIndex);
      CommandProcessorResponse cpr = null;
      CommandProcessorException cpe = null;
      try {
        cpr = coreDriver.compileAndRespond(statement);
      } catch (CommandProcessorException e) {
        cpe = e;
      }

      final boolean success = cpe == null;
      plugins.forEach(p -> p.afterCompile(success));

      // If the compilation was successful return the result
      if (success) {
        return cpr;
      }

      if (compileIndex >= maxCompilations || !plugins.stream().anyMatch(p -> p.shouldReCompile(currentIndex))) {
        // If we do not have to recompile, return the last error
        throw cpe;
      }

      // Prepare for the recompile and start the next loop
      plugins.forEach(IReExecutionPlugin::prepareToReCompile);
    }
  }

  @Override
  public HiveConf getConf() {
    return queryState.getConf();
  }

  @Override
  public QueryPlan getPlan() {
    return coreDriver.getPlan();
  }

  @Override
  public QueryState getQueryState() {
    return queryState;
  }

  @Override
  public QueryDisplay getQueryDisplay() {
    return coreDriver.getQueryDisplay();
  }

  @Override
  public void setOperationId(String operationId) {
    coreDriver.setOperationId(operationId);
  }

  @Override
  public CommandProcessorResponse run() throws CommandProcessorException {
    executionIndex = 0;
    int maxExecutions = 1 + coreDriver.getConf().getIntVar(ConfVars.HIVE_QUERY_MAX_REEXECUTION_COUNT);

    while (true) {
      executionIndex++;

      for (IReExecutionPlugin p : plugins) {
        p.beforeExecute(executionIndex, explainReOptimization);
      }
      coreDriver.getContext().setExecutionIndex(executionIndex);
      LOG.info("Execution #{} of query", executionIndex);
      CommandProcessorResponse cpr = null;
      CommandProcessorException cpe = null;
      try {
        cpr = coreDriver.run();
      } catch (CommandProcessorException e) {
        cpe = e;
      }

      PlanMapper oldPlanMapper = coreDriver.getPlanMapper();
      boolean success = cpr != null;
      plugins.forEach(p -> p.afterExecute(oldPlanMapper, success));

      boolean shouldReExecute = explainReOptimization && executionIndex==1;
      shouldReExecute |= cpr == null && plugins.stream().anyMatch(p -> p.shouldReExecute(executionIndex));

      if (executionIndex >= maxExecutions || !shouldReExecute) {
        if (cpr != null) {
          return cpr;
        } else {
          throw cpe;
        }
      }
      LOG.info("Preparing to re-execute query");
      plugins.forEach(IReExecutionPlugin::prepareToReExecute);

      try {
        coreDriver.compileAndRespond(currentQuery);
      } catch (CommandProcessorException e) {
        LOG.error("Recompilation of the query failed; this is unexpected.");
        // FIXME: somehow place pointers that re-execution compilation have failed; the query have been successfully compiled before?
        throw e;
      }

      PlanMapper newPlanMapper = coreDriver.getPlanMapper();
      if (!explainReOptimization &&
          !plugins.stream().anyMatch(p -> p.shouldReExecuteAfterCompile(executionIndex, oldPlanMapper, newPlanMapper))) {
        LOG.info("re-running the query would probably not yield better results; returning with last error");
        // FIXME: retain old error; or create a new one?
        return cpr;
      }
    }
  }

  @Override
  public CommandProcessorResponse run(String command) throws CommandProcessorException {
    compileAndRespond(command);
    return run();
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
    if (explainReOptimization) {
      return new Schema(ExplainTask.getResultSchema(), null);
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
}
