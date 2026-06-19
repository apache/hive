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

package org.apache.hadoop.hive.ql.ddl.table.column.show;

import org.apache.hadoop.hive.ql.ErrorMsg;
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
 * Analyzer for show columns commands.
 */
@DDLType(types = HiveParser.TOK_SHOWCOLUMNS)
public class ShowColumnsAnalyzer extends BaseSemanticAnalyzer {
  public ShowColumnsAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    // table name has to be present so min child 1 and max child 5
    if (root.getChildCount() > 5 || root.getChildCount() < 1) {
      throw new SemanticException(ErrorMsg.INVALID_AST_TREE.getMsg(root.toStringTree()));
    }

    ctx.setResFile(ctx.getLocalTmpPath());

    String tableName = getUnescapedName((ASTNode) root.getChild(0));
    String pattern = null;
    boolean isSorted = (root.getFirstChildWithType(HiveParser.KW_SORTED) != null);
    int childCount = root.getChildCount();
    // If isSorted exist, remove one child count from childCount
    if (isSorted) {
      childCount--;
    }
    switch (childCount) {
    case 1: //  only tablename no pattern and db
      break;
    case 2: // tablename and pattern
      pattern = unescapeSQLString(root.getChild(1).getText());
      break;
    case 3: // specifies db
      if (tableName.contains(".")) {
        throw new SemanticException("Duplicates declaration for database name");
      }
      tableName = getUnescapedName((ASTNode) root.getChild(2)) + "." + tableName;
      break;
    case 4: // specifies db and pattern
      if (tableName.contains(".")) {
        throw new SemanticException("Duplicates declaration for database name");
      }
      tableName = getUnescapedName((ASTNode) root.getChild(2)) + "." + tableName;
      pattern = unescapeSQLString(root.getChild(3).getText());
      break;
    default:
      break;
    }

    Table table = getTable(tableName);
    inputs.add(new ReadEntity(table));

    ShowColumnsDesc desc = new ShowColumnsDesc(ctx.getResFile(), tableName, pattern, isSorted);
    Task<DDLWork> task = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc));
    rootTasks.add(task);

    task.setFetchSource(true);
    setFetchTask(createFetchTask(ShowColumnsDesc.SCHEMA));
  }
}
