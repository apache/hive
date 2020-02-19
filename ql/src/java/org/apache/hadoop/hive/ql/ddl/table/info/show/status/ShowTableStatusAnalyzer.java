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

package org.apache.hadoop.hive.ql.ddl.table.info.show.status;

import java.util.Map;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveTableName;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Analyzer for show table status commands.
 */
@DDLType(types = HiveParser.TOK_SHOW_TABLESTATUS)
public class ShowTableStatusAnalyzer extends BaseSemanticAnalyzer {
  public ShowTableStatusAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    if (root.getChildCount() > 3 || root.getChildCount() < 1) {
      throw new SemanticException(ErrorMsg.INVALID_AST_TREE.getMsg());
    }

    ctx.setResFile(ctx.getLocalTmpPath());

    String tableNames = getUnescapedName((ASTNode) root.getChild(0));
    String dbName = SessionState.get().getCurrentDatabase();
    Map<String, String> partitionSpec = null;
    if (root.getChildCount() > 1) {
      for (int i = 1; i < root.getChildCount(); i++) {
        ASTNode child = (ASTNode) root.getChild(i);
        if (child.getToken().getType() == HiveParser.Identifier) {
          dbName = unescapeIdentifier(child.getText());
        } else if (child.getToken().getType() == HiveParser.TOK_PARTSPEC) {
          partitionSpec = getValidatedPartSpec(getTable(tableNames), child, conf, false);
        } else {
          throw new SemanticException(ErrorMsg.INVALID_AST_TREE.getMsg(
              child.toStringTree() + " , Invalid token " + child.getToken().getType()));
        }
      }
    }

    if (partitionSpec != null) {
      // validate that partition exists
      PartitionUtils.getPartition(db, getTable(HiveTableName.of(tableNames)), partitionSpec, true);
    }

    ShowTableStatusDesc desc = new ShowTableStatusDesc(ctx.getResFile(), dbName, tableNames, partitionSpec);
    Task<DDLWork> task = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc));
    rootTasks.add(task);

    task.setFetchSource(true);
    setFetchTask(createFetchTask(ShowTableStatusDesc.SCHEMA));
  }
}
