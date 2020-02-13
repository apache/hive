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

package org.apache.hadoop.hive.ql.ddl.table.partition.show;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveTableName;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for show partition commands.
 */
@DDLType(types = HiveParser.TOK_SHOWPARTITIONS)
public  class ShowPartitionAnalyzer extends BaseSemanticAnalyzer {
  public ShowPartitionAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    ctx.setResFile(ctx.getLocalTmpPath());

    String tableName = getUnescapedName((ASTNode) ast.getChild(0));

    List<Map<String, String>> partSpecs = getPartitionSpecs(getTable(tableName), ast);
    assert (partSpecs.size() <= 1);
    Map<String, String> partSpec = (partSpecs.size() > 0) ? partSpecs.get(0) : null;

    Table table = getTable(HiveTableName.of(tableName));
    inputs.add(new ReadEntity(table));

    ShowPartitionsDesc desc = new ShowPartitionsDesc(tableName, ctx.getResFile(), partSpec);
    Task<DDLWork> task = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc));
    rootTasks.add(task);

    task.setFetchSource(true);
    setFetchTask(createFetchTask(ShowPartitionsDesc.SCHEMA));
  }
}
