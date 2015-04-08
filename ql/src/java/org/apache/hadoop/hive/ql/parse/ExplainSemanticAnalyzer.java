/**
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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.plan.ExplainWork;

/**
 * ExplainSemanticAnalyzer.
 *
 */
public class ExplainSemanticAnalyzer extends BaseSemanticAnalyzer {
  List<FieldSchema> fieldList;

  public ExplainSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {

    boolean extended = false;
    boolean formatted = false;
    boolean dependency = false;
    boolean logical = false;
    boolean authorize = false;
    for (int i = 1; i < ast.getChildCount(); i++) {
      int explainOptions = ast.getChild(i).getType();
      if (explainOptions == HiveParser.KW_FORMATTED) {
        formatted = true;
      } else if (explainOptions == HiveParser.KW_EXTENDED) {
        extended = true;
      } else if (explainOptions == HiveParser.KW_DEPENDENCY) {
        dependency = true;
      } else if (explainOptions == HiveParser.KW_LOGICAL) {
        logical = true;
      } else if (explainOptions == HiveParser.KW_AUTHORIZATION) {
        authorize = true;
      }
    }

    ctx.setExplain(true);
    ctx.setExplainLogical(logical);

    // Create a semantic analyzer for the query
    ASTNode input = (ASTNode) ast.getChild(0);
    BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, input);
    sem.analyze(input, ctx);
    sem.validate();

    ctx.setResFile(ctx.getLocalTmpPath());
    List<Task<? extends Serializable>> tasks = sem.getRootTasks();
    if (tasks == null) {
      tasks = Collections.emptyList();
    }
    
    FetchTask fetchTask = sem.getFetchTask();
    if (fetchTask != null) {
      // Initialize fetch work such that operator tree will be constructed.
      fetchTask.getWork().initializeForFetch();
    }

    ParseContext pCtx = null;
    if (sem instanceof SemanticAnalyzer) {
      pCtx = ((SemanticAnalyzer)sem).getParseContext();
    }

    boolean userLevelExplain = !extended && !formatted && !dependency && !logical && !authorize
        && HiveConf.getBoolVar(ctx.getConf(), HiveConf.ConfVars.HIVE_EXPLAIN_USER);
    ExplainWork work = new ExplainWork(ctx.getResFile(),
        pCtx,
        tasks,
        fetchTask,
        input.dump(),
        sem,
        extended,
        formatted,
        dependency,
        logical,
        authorize,
        userLevelExplain,
        ctx.getCboInfo());

    work.setAppendTaskType(
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEEXPLAINDEPENDENCYAPPENDTASKTYPES));

    ExplainTask explTask = (ExplainTask) TaskFactory.get(work, conf);

    fieldList = explTask.getResultSchema();
    rootTasks.add(explTask);
  }

  @Override
  public List<FieldSchema> getResultSchema() {
    return fieldList;
  }

  @Override
  public boolean skipAuthorization() {
    List<Task<? extends Serializable>> rootTasks = getRootTasks();
    assert rootTasks != null && rootTasks.size() == 1;
    Task task = rootTasks.get(0);
    return task instanceof ExplainTask && ((ExplainTask)task).getWork().isAuthorize();
  }
}
