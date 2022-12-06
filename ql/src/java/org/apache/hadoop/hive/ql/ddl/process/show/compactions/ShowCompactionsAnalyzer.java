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

import org.apache.commons.lang3.EnumUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    String dbName = null;
    String tblName = null;
    String poolName = null;
    String compactionType = null;
    String compactionStatus = null;
    long compactionId = 0;
    String orderBy = null;
    short limit = -1;
    Map<String, String> partitionSpec = null;
    if (root.getChildCount() > 6) {
      throw new SemanticException(ErrorMsg.INVALID_AST_TREE.getMsg(root.toStringTree()));
    }
    for (int i = 0; i < root.getChildCount(); i++) {
      ASTNode child = (ASTNode) root.getChild(i);
      switch (child.getType()) {
        case HiveParser.TOK_TABTYPE:
          tblName = child.getChild(0).getText();
          if (child.getChild(0).getChildCount() == 2) {
            dbName = child.getChild(0).getChild(0).getText();
            tblName = child.getChild(0).getChild(1).getText();
          }
          if (child.getChildCount() == 2) {
            ASTNode partitionSpecNode = (ASTNode) child.getChild(1);
            partitionSpec = getValidatedPartSpec(getTable(dbName, tblName, true), partitionSpecNode, conf, false);
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
        case HiveParser.TOK_LIMIT:
          limit = Short.valueOf((child.getChild(0)).getText());
          break;
        case HiveParser.TOK_ORDERBY:
          orderBy = processSortOrderSpec(child);
          break;
        default:
          dbName = stripQuotes(child.getText());
      }
    }
    ShowCompactionsDesc desc = new ShowCompactionsDesc(ctx.getResFile(), compactionId, dbName, tblName, poolName, compactionType,
      compactionStatus, partitionSpec, limit, orderBy);
    Task<DDLWork> task = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc));
    rootTasks.add(task);
    task.setFetchSource(true);
    setFetchTask(createFetchTask(ShowCompactionsDesc.SCHEMA));
  }

  private String processSortOrderSpec(ASTNode sortNode) {
    List<PTFInvocationSpec.OrderExpression> orderExp = processOrderSpec(sortNode).getExpressions();
    Map<String, String> orderByAttributes = orderExp.stream().
      collect(Collectors.toMap(x -> getDbColumnName(x.getExpression()), x -> x.getOrder().toString()));
    return orderByAttributes.entrySet().stream().map(e -> e.getKey() + "\t" + e.getValue()).collect(Collectors.joining(","));
  }

  private String getDbColumnName(ASTNode expression) {
    String dbColumnPrefix = "CC_";
    String dbColumnName = expression.getChild(0) == null ? expression.getText().replace("\'", "").toUpperCase() :
      expression.getChild(0).getText().toUpperCase();
    return EnumUtils.isValidEnum(CompactionColumn.class, dbColumnName) ? CompactionColumn.valueOf(dbColumnName).toString() :
      "\"" + dbColumnPrefix + dbColumnName + "\"";
  }

  private enum CompactionColumn {
    COMPACTIONID("\"CC_ID\""),
    DBNAME("\"CC_DATABASE\""),
    TABNAME("\"CC_TABLE\""),
    PARTNAME("\"CC_PARTITION\""),
    ENQUEUETIME("\"CC_ENQUEUE_TIME\""),
    STARTTIME("\"CC_START\""),
    POOLNAME("\"CC_POOL_NAME\""),
    NEXTTXNID("\"CC_NEXT_TXN_ID\""),
    HADOOPJOBID("\"CC_HADOOP_JOB_ID\""),
    WORKERHOST("\"CC_WORKER_ID\""),
    WORKERID("\"CC_WORKER_ID\""),
    DURATION("\"CC_END\""),
    TXNID("\"CC_TXN_ID\""),
    COMMITTIME("CC_COMMIT_TIME"),
    HIGHESTWRITEID("CC_HIGHEST_WRITE_ID");
    private final String colVal;

    CompactionColumn(String colVal) {
      this.colVal = colVal;
    }

    @Override
    public String toString() {
      return colVal;
    }
  }
}
