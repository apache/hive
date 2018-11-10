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

package org.apache.hadoop.hive.ql.parse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.VectorizationDetailLevel;
import org.apache.hadoop.hive.ql.plan.ExplainWork;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.stats.StatsAggregator;
import org.apache.hadoop.hive.ql.stats.StatsCollectionContext;
import org.apache.hadoop.hive.ql.stats.fs.FSStatsAggregator;

/**
 * ExplainSemanticAnalyzer.
 *
 */
public class ExplainSemanticAnalyzer extends BaseSemanticAnalyzer {
  List<FieldSchema> fieldList;
  ExplainConfiguration config;

  public ExplainSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
    config = new ExplainConfiguration();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    final int childCount = ast.getChildCount();
    int i = 1;   // Skip TOK_QUERY.
    while (i < childCount) {
      int explainOptions = ast.getChild(i).getType();
      if (explainOptions == HiveParser.KW_FORMATTED) {
        config.setFormatted(true);
      } else if (explainOptions == HiveParser.KW_EXTENDED) {
        config.setExtended(true);
      } else if (explainOptions == HiveParser.KW_DEPENDENCY) {
        config.setDependency(true);
      } else if (explainOptions == HiveParser.KW_CBO) {
        config.setCbo(true);
        if (i + 1 < childCount) {
          if (ast.getChild(i + 1).getType() == HiveParser.KW_EXTENDED) {
            config.setCboExtended(true);
            i++;
          }
        }
      } else if (explainOptions == HiveParser.KW_LOGICAL) {
        config.setLogical(true);
      } else if (explainOptions == HiveParser.KW_AUTHORIZATION) {
        config.setAuthorize(true);
      } else if (explainOptions == HiveParser.KW_ANALYZE) {
        config.setAnalyze(AnalyzeState.RUNNING);
        config.setExplainRootPath(ctx.getMRTmpPath());
      } else if (explainOptions == HiveParser.KW_VECTORIZATION) {
        config.setVectorization(true);
        if (i + 1 < childCount) {
          int vectorizationOption = ast.getChild(i + 1).getType();

          // [ONLY]
          if (vectorizationOption == HiveParser.TOK_ONLY) {
            config.setVectorizationOnly(true);
            i++;
            if (i + 1 >= childCount) {
              break;
            }
            vectorizationOption = ast.getChild(i + 1).getType();
          }

          // [SUMMARY|OPERATOR|EXPRESSION|DETAIL]
          if (vectorizationOption == HiveParser.TOK_SUMMARY) {
            config.setVectorizationDetailLevel(VectorizationDetailLevel.SUMMARY);
            i++;
          } else if (vectorizationOption == HiveParser.TOK_OPERATOR) {
            config.setVectorizationDetailLevel(VectorizationDetailLevel.OPERATOR);
            i++;
          } else if (vectorizationOption == HiveParser.TOK_EXPRESSION) {
            config.setVectorizationDetailLevel(VectorizationDetailLevel.EXPRESSION);
            i++;
          } else if (vectorizationOption == HiveParser.TOK_DETAIL) {
            config.setVectorizationDetailLevel(VectorizationDetailLevel.DETAIL);
            i++;
          }
        }
      } else if (explainOptions == HiveParser.KW_LOCKS) {
        config.setLocks(true);
      } else if (explainOptions == HiveParser.KW_AST){
        config.setAst(true);
      } else if (explainOptions == HiveParser.KW_DEBUG) {
        config.setDebug(true);
      } else {
        // UNDONE: UNKNOWN OPTION?
      }
      i++;
    }

    ctx.setExplainConfig(config);
    ctx.setExplainPlan(true);

    ASTNode input = (ASTNode) ast.getChild(0);
    // explain analyze is composed of two steps
    // step 1 (ANALYZE_STATE.RUNNING), run the query and collect the runtime #rows
    // step 2 (ANALYZE_STATE.ANALYZING), explain the query and provide the runtime #rows collected.
    if (config.getAnalyze() == AnalyzeState.RUNNING) {
      String query = ctx.getTokenRewriteStream().toString(input.getTokenStartIndex(),
          input.getTokenStopIndex());
      LOG.info("Explain analyze (running phase) for query " + query);
      conf.unset(ValidTxnList.VALID_TXNS_KEY);
      conf.unset(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY);
      Context runCtx = null;
      try {
        runCtx = new Context(conf);
        // runCtx and ctx share the configuration, but not isExplainPlan()
        runCtx.setExplainConfig(config);
        Driver driver = new Driver(conf, runCtx, queryState.getLineageState());
        CommandProcessorResponse ret = driver.run(query);
        if(ret.getResponseCode() == 0) {
          // Note that we need to call getResults for simple fetch optimization.
          // However, we need to skip all the results.
          while (driver.getResults(new ArrayList<String>())) {
          }
        } else {
          throw new SemanticException(ret.getErrorMessage(), ret.getException());
        }
        config.setOpIdToRuntimeNumRows(aggregateStats(config.getExplainRootPath()));
      } catch (IOException e1) {
        throw new SemanticException(e1);
      }
      ctx.resetOpContext();
      ctx.resetStream();
      TaskFactory.resetId();
      LOG.info("Explain analyze (analyzing phase) for query " + query);
      config.setAnalyze(AnalyzeState.ANALYZING);
    }
    //Creating new QueryState unfortunately causes all .q.out to change - do this in a separate ticket
    //Sharing QueryState between generating the plan and executing the query seems bad
    //BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(new QueryState(queryState.getConf()), input);
    BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(queryState, input);
    sem.analyze(input, ctx);
    sem.validate();
    inputs = sem.getInputs();
    outputs = sem.getOutputs();

    ctx.setResFile(ctx.getLocalTmpPath());
    List<Task<?>> tasks = sem.getAllRootTasks();
    if (tasks == null) {
      tasks = Collections.emptyList();
    }

    FetchTask fetchTask = sem.getFetchTask();
    if (fetchTask != null) {
      // Initialize fetch work such that operator tree will be constructed.
      fetchTask.getWork().initializeForFetch(ctx.getOpContext());
    }

    ParseContext pCtx = null;
    if (sem instanceof SemanticAnalyzer) {
      pCtx = ((SemanticAnalyzer)sem).getParseContext();
    }

    config.setUserLevelExplain(!config.isExtended()
        && !config.isFormatted()
        && !config.isDependency()
        && !config.isCbo()
        && !config.isLogical()
        && !config.isAuthorize()
        && (
             (
               HiveConf.getBoolVar(ctx.getConf(), HiveConf.ConfVars.HIVE_EXPLAIN_USER)
               &&
               HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")
             )
             ||
             (
               HiveConf.getBoolVar(ctx.getConf(), HiveConf.ConfVars.HIVE_SPARK_EXPLAIN_USER)
               &&
               HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")
             )
           )
        );

    ExplainWork work = new ExplainWork(ctx.getResFile(),
        pCtx,
        tasks,
        fetchTask,
        input,
        sem,
        config,
        ctx.getCboInfo(),
        ctx.getOptimizedSql(),
        ctx.getCalcitePlan());

    work.setAppendTaskType(
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEEXPLAINDEPENDENCYAPPENDTASKTYPES));

    ExplainTask explTask = (ExplainTask) TaskFactory.get(work);

    fieldList = explTask.getResultSchema();
    rootTasks.add(explTask);
  }

  private Map<String, Long> aggregateStats(Path localTmpPath) {
    Map<String, Long> opIdToRuntimeNumRows = new HashMap<String, Long>();
    // localTmpPath is the root of all the stats.
    // Under it, there will be SEL_1/statsfiles, SEL_2/statsfiles etc where SEL_1 and SEL_2 are the op ids.
    FileSystem fs;
    FileStatus[] statuses = null;
    try {
      fs = localTmpPath.getFileSystem(conf);
      statuses = fs.listStatus(localTmpPath, FileUtils.HIDDEN_FILES_PATH_FILTER);
      // statuses can be null if it is DDL, etc
    } catch (IOException e) {
      LOG.warn(e.toString());
    }
    if (statuses != null) {
      for (FileStatus status : statuses) {
        if (status.isDir()) {
          StatsCollectionContext scc = new StatsCollectionContext(conf);
          String[] names = status.getPath().toString().split(Path.SEPARATOR);
          String opId = names[names.length - 1];
          scc.setStatsTmpDir(status.getPath().toString());
          StatsAggregator statsAggregator = new FSStatsAggregator();
          if (!statsAggregator.connect(scc)) {
            // -1 means that there is no stats
            opIdToRuntimeNumRows.put(opId, -1L);
          } else {
            String value = statsAggregator.aggregateStats("", StatsSetupConst.RUN_TIME_ROW_COUNT);
            opIdToRuntimeNumRows.put(opId, Long.parseLong(value));
          }
          if (statsAggregator != null) {
            statsAggregator.closeConnection(scc);
          }
        }
      }
    }
    return opIdToRuntimeNumRows;
  }

  @Override
  public List<FieldSchema> getResultSchema() {
    return fieldList;
  }

  @Override
  public boolean skipAuthorization() {
    List<Task<?>> rootTasks = getRootTasks();
    assert rootTasks != null && rootTasks.size() == 1;
    Task task = rootTasks.get(0);
    return task instanceof ExplainTask && ((ExplainTask)task).getWork().isAuthorize();
  }

}
