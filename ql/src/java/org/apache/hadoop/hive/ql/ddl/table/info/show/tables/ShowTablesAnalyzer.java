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

package org.apache.hadoop.hive.ql.ddl.table.info.show.tables;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Analyzer for show tables commands.
 */
@DDLType(types = HiveParser.TOK_SHOWTABLES)
public class ShowTablesAnalyzer extends BaseSemanticAnalyzer {
  public ShowTablesAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    if (root.getChildCount() > 4) {
      throw new SemanticException(ErrorMsg.INVALID_AST_TREE.getMsg(root.toStringTree()));
    }

    ctx.setResFile(ctx.getLocalTmpPath());

    String dbName = SessionState.get().getCurrentDatabase();
    String tableNames = null;
    TableType tableTypeFilter = null;
    boolean isExtended = false;
    for (int i = 0; i < root.getChildCount(); i++) {
      ASTNode child = (ASTNode) root.getChild(i);
      if (child.getType() == HiveParser.TOK_FROM) { // Specifies a DB
        dbName = unescapeIdentifier(root.getChild(++i).getText());
        db.validateDatabaseExists(dbName);
      } else if (child.getType() == HiveParser.TOK_TABLE_TYPE) { // Filter on table type
        String tableType = unescapeIdentifier(child.getChild(0).getText());
        if (!"table_type".equalsIgnoreCase(tableType)) {
          throw new SemanticException("SHOW TABLES statement only allows equality filter on table_type value");
        }
        tableTypeFilter = TableType.valueOf(unescapeSQLString(child.getChild(1).getText()));
      } else if (child.getType() == HiveParser.KW_EXTENDED) { // Include table type
        isExtended = true;
      } else { // Uses a pattern
        tableNames = unescapeSQLString(child.getText());
      }
    }

    inputs.add(new ReadEntity(getDatabase(dbName)));

    ShowTablesDesc desc = new ShowTablesDesc(ctx.getResFile(), dbName, tableNames, tableTypeFilter, isExtended);
    Task<DDLWork> task = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc));
    rootTasks.add(task);

    task.setFetchSource(true);
    setFetchTask(createFetchTask(desc.getSchema()));
  }
}
