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

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ExplainSQRewriteTask;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.plan.ExplainSQRewriteWork;

public class ExplainSQRewriteSemanticAnalyzer extends BaseSemanticAnalyzer {
  List<FieldSchema> fieldList;

  public ExplainSQRewriteSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {


    ctx.setExplain(true);

    // Create a semantic analyzer for the query
    ASTNode input = (ASTNode) ast.getChild(0);
    SemanticAnalyzer sem = (SemanticAnalyzer)
        SemanticAnalyzerFactory.get(conf, input);
    sem.analyze(input, ctx);
    sem.validate();

    ctx.setResFile(ctx.getLocalTmpPath());

    ExplainSQRewriteWork work = new ExplainSQRewriteWork(ctx.getResFile().toString(),
        sem.getQB(),
        input,
        ctx
        );

    ExplainSQRewriteTask explTask = (ExplainSQRewriteTask) TaskFactory.get(work, conf);

    fieldList = explTask.getResultSchema();
    rootTasks.add(explTask);
  }

  @Override
  public List<FieldSchema> getResultSchema() {
    return fieldList;
  }
}
