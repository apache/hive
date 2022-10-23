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

package org.apache.hadoop.hive.ql.ddl.process.show.compactions;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;


import java.util.Map;

/**
 * Analyzer for show compactions commands.
 */
@DDLType(types = HiveParser.TOK_SHOW_COMPACTIONS)
public class ShowCompactionsAnalyzer extends BaseSemanticAnalyzer {
  public ShowCompactionsAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    ctx.setResFile(ctx.getLocalTmpPath());
    String poolName = null;
    String dbName = null;
    String tbName = null;
    String compactionType = null;
    String compactionStatus = null;
    long compactionId = 0;
    Map<String, String> partitionSpec = null;
    if (root.getChildCount() > 6) {
      throw new SemanticException(ErrorMsg.INVALID_AST_TREE.getMsg(root.toStringTree()));
    }
    if (root.getType() == HiveParser.TOK_SHOW_COMPACTIONS) {
      for (int i = 0; i < root.getChildCount(); i++) {
        ASTNode child = (ASTNode) root.getChild(i);
        switch (child.getType()) {
          case HiveParser.TOK_TABTYPE:
            tbName = child.getChild(0).getText();
            if (child.getChildCount() == 2) {
              if (child.getChild(0).getChildCount() == 2) {
                dbName = DDLUtils.getFQName((ASTNode) child.getChild(0).getChild(0));
                tbName = DDLUtils.getFQName((ASTNode) child.getChild(0).getChild(1));
              }
              ASTNode partitionSpecNode = (ASTNode) child.getChild(1);
              partitionSpec = getValidatedPartSpec(getTable(dbName, tbName, true), partitionSpecNode, conf, false);
            }
            break;
          case HiveParser.TOK_COMPACT_POOL:
            poolName = unescapeSQLString(child.getChild(0).getText());
            break;
          case HiveParser.TOK_COMPACTION_TYPE:
            compactionType = unescapeSQLString(child.getChild(0).getText());
            break;
          case HiveParser.TOK_COMPACTION_STATUS:
            compactionStatus = unescapeSQLString(child.getChild(0).getText());
            break;
          case HiveParser.TOK_COMPACT_ID:
           compactionId = Long.parseLong(child.getChild(0).getText());
           break;
          default:
            dbName = child.getText();
        }
      }
    }
    ShowCompactionsDesc desc = new ShowCompactionsDesc(ctx.getResFile(), compactionId, dbName, tbName, poolName, compactionType,
      compactionStatus, partitionSpec);
    Task<DDLWork> task = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc));
    rootTasks.add(task);
    task.setFetchSource(true);
    setFetchTask(createFetchTask(ShowCompactionsDesc.SCHEMA));
  }
}
