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

package org.apache.hadoop.hive.ql.ddl.table.create.show;

import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for showing table creation commands.
 */
@DDLType(types = HiveParser.TOK_SHOW_CREATETABLE)
public class ShowCreateTableAnalyzer extends BaseSemanticAnalyzer {
  public ShowCreateTableAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    ctx.setResFile(ctx.getLocalTmpPath());

    Entry<String, String> tableIdentifier = getDbTableNamePair((ASTNode) root.getChild(0));
    if (tableIdentifier.getValue().contains(".")) {
      throw new SemanticException("The SHOW CREATE TABLE command is not supported for metadata tables.");
    }
    Table table = getTable(tableIdentifier.getKey(), tableIdentifier.getValue(), true);

    inputs.add(new ReadEntity(table));

    // If no DB was specified in statement, do not include it in the final output
    ShowCreateTableDesc desc = new ShowCreateTableDesc(table.getDbName(), table.getTableName(),
        ctx.getResFile().toString(), StringUtils.isBlank(tableIdentifier.getKey()));
    Task<DDLWork> task = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc));
    rootTasks.add(task);

    task.setFetchSource(true);
    setFetchTask(createFetchTask(ShowCreateTableDesc.SCHEMA));
  }
}
