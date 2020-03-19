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

package org.apache.hadoop.hive.ql.ddl.misc.msck;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.HiveMetaStoreChecker;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.function.AbstractFunctionAnalyzer;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for metastore check commands.
 */
@DDLType(types = HiveParser.TOK_MSCK)
public class MsckAnalyzer extends AbstractFunctionAnalyzer {

  private enum FilterType {
    String,

    Invalid;

    public static FilterType fromType(String colTypeStr) {
      if (colTypeStr.equals(ColumnType.STRING_TYPE_NAME)) {
        return FilterType.String;
      }

      return FilterType.Invalid;
    }
  }

  public MsckAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    if (root.getChildCount() == 0) {
      throw new SemanticException("MSCK command must have arguments");
    }

    ctx.setResFile(ctx.getLocalTmpPath());

    boolean repair = root.getChild(0).getType() == HiveParser.KW_REPAIR;
    int offset = repair ? 1 : 0;
    String tableName = getUnescapedName((ASTNode) root.getChild(0 + offset));

    boolean addPartitions = true;
    boolean dropPartitions = false;
    if (root.getChildCount() > 1 + offset) {
      addPartitions = isMsckAddPartition(root.getChild(1 + offset).getType());
      dropPartitions = isMsckDropPartition(root.getChild(1 + offset).getType());
    }

    Table table = getTable(tableName);
    List<Map<String, String>> specs = getPartitionSpecs(table, root);
    Map<String, String> partColNameTypeMap = new HashMap<>();
    for (FieldSchema fieldSchema : table.getPartitionKeys()) {
      partColNameTypeMap.put(fieldSchema.getName(), fieldSchema.getType());
    }

    // Construct partition filter from AST
    String partitionFilterStr = getPartitionFilter(root, partColNameTypeMap);
    if (repair && AcidUtils.isTransactionalTable(table)) {
      outputs.add(new WriteEntity(table, WriteType.DDL_EXCLUSIVE));
    } else {
      outputs.add(new WriteEntity(table, WriteEntity.WriteType.DDL_SHARED));
    }
    MsckDesc desc = new MsckDesc(tableName, specs, ctx.getResFile(), repair, addPartitions,
        dropPartitions, partitionFilterStr);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }

  private boolean isMsckAddPartition(int type) {
    return type == HiveParser.KW_SYNC || type == HiveParser.KW_ADD;
  }

  private boolean isMsckDropPartition(int type) {
    return type == HiveParser.KW_SYNC || type == HiveParser.KW_DROP;
  }

  private String getPartitionFilter(CommonTree ast, Map<String, String> partColNameTypeMap) throws SemanticException {
    for (int childIndex = 0; childIndex < ast.getChildCount(); childIndex++) {
      ASTNode partSpecNode = (ASTNode) ast.getChild(childIndex);
      if (partSpecNode.getType() == HiveParser.TOK_WHERE) {
        return constructPartitionFilterFromAST((ASTNode) partSpecNode.getChild(0), "", partColNameTypeMap);
      }
    }
    return null;
  }

  private String constructPartitionFilterFromAST(ASTNode node, String filterStr,
                                                 Map<String, String> partColNameTypeMap) throws SemanticException {
    String operator = node.getText().toLowerCase();
    if (HiveMetaStoreChecker.Operator.isValidOperator(operator)) {
      String partKey = node.getChild(0).getChild(0).getText();

      // Sanity Check
      if (partColNameTypeMap.get(partKey) == null) {
        throw new SemanticException("Filter predicate on non-partitioned column: " + partKey + " not supported");
      }
      //todo: Add Support For Other Data Types
      if (FilterType.fromType(partColNameTypeMap.get(partKey)) == FilterType.Invalid) {
        throw new SemanticException("Filter predicate only supported for string data types");
      }

      if (node.getChild(1).getChild(0) != null) {
        throw new SemanticException("Unsupported filter predicate");
      }

      filterStr = String.join(" ", filterStr, partKey, operator,
          node.getChild(1).getText());
      return filterStr;
    }

    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      filterStr =  constructPartitionFilterFromAST(child, filterStr, partColNameTypeMap);
      if (i != node.getChildCount() - 1) {
        filterStr = String.join(" ", filterStr, operator);
      }
    }
    return filterStr;
  }
}
