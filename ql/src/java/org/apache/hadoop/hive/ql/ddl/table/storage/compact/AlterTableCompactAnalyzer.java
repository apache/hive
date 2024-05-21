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

package org.apache.hadoop.hive.ql.ddl.table.storage.compact;

import java.util.Map;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for compact commands.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_COMPACT)
public class AlterTableCompactAnalyzer extends AbstractAlterTableAnalyzer {
  public AlterTableCompactAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    String type = unescapeSQLString(command.getChild(0).getText()).toLowerCase();
    int numberOfBuckets = 0;
    try {
      CompactionType.valueOf(type.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new SemanticException(ErrorMsg.INVALID_COMPACTION_TYPE.getMsg());
    }

    Map<String, String> mapProp = null;
    boolean isBlocking = false;
    String poolName = null;
    String orderBy = null;
    for (int i = 0; i < command.getChildCount(); i++) {
      Tree node = command.getChild(i);
      switch (node.getType()) {
        case HiveParser.TOK_TABLEPROPERTIES:
          mapProp = getProps((ASTNode)node.getChild(0));
          break;
        case HiveParser.TOK_BLOCKING:
          isBlocking = true;
          break;
        case HiveParser.TOK_COMPACT_POOL:
          poolName = unescapeSQLString(node.getChild(0).getText());
          break;
        case HiveParser.TOK_ALTERTABLE_BUCKETS:
          try {
            numberOfBuckets = Integer.parseInt(node.getChild(0).getText());
          } catch (NumberFormatException nfe) {
            throw new SemanticException("Could not parse bucket number: " + node.getChild(0).getText());
          }
          break;
        case HiveParser.TOK_ORDERBY:
          orderBy = this.ctx.getTokenRewriteStream().toOriginalString(node.getTokenStartIndex(), node.getTokenStopIndex());
          break;
        default:
          break;
      }
    }

    AlterTableCompactDesc desc = new AlterTableCompactDesc(tableName, partitionSpec, type, isBlocking, poolName,
        numberOfBuckets, mapProp, orderBy);
    addInputsOutputsAlterTable(tableName, partitionSpec, desc, desc.getType(), false);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }
  
  @Override
  protected void setAcidDdlDesc(DDLDescWithWriteId desc) {
    // doesn't need an open txn
  }
}
