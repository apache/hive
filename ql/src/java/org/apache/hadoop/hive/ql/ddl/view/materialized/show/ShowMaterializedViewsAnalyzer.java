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

package org.apache.hadoop.hive.ql.ddl.view.materialized.show;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Analyzer for show materialized views commands.
 */
@DDLType(types = HiveParser.TOK_SHOWMATERIALIZEDVIEWS)
public class ShowMaterializedViewsAnalyzer extends BaseSemanticAnalyzer {
  public ShowMaterializedViewsAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    if (root.getChildCount() > 3) {
      throw new SemanticException(ErrorMsg.GENERIC_ERROR.getMsg());
    }

    ctx.setResFile(ctx.getLocalTmpPath());

    String dbName = SessionState.get().getCurrentDatabase();
    String viewNames = null;
    switch (root.getChildCount()) {
    case 1: // Uses a pattern
      viewNames = unescapeSQLString(root.getChild(0).getText());
      break;
    case 2: // Specifies a DB
      assert (root.getChild(0).getType() == HiveParser.TOK_FROM);
      dbName = unescapeIdentifier(root.getChild(1).getText());
      db.validateDatabaseExists(dbName);
      break;
    case 3: // Uses a pattern and specifies a DB
      assert (root.getChild(0).getType() == HiveParser.TOK_FROM);
      dbName = unescapeIdentifier(root.getChild(1).getText());
      viewNames = unescapeSQLString(root.getChild(2).getText());
      db.validateDatabaseExists(dbName);
      break;
    default: // No pattern or DB
      break;
    }

    ShowMaterializedViewsDesc desc = new ShowMaterializedViewsDesc(ctx.getResFile(), dbName, viewNames);
    Task<DDLWork> task = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc));
    rootTasks.add(task);

    task.setFetchSource(true);
    setFetchTask(createFetchTask(ShowMaterializedViewsDesc.SCHEMA));
  }
}
