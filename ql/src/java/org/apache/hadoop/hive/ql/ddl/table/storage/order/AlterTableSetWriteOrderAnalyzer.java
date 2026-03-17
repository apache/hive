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

package org.apache.hadoop.hive.ql.ddl.table.storage.order;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.misc.sortoder.SortOrderUtils;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for ALTER TABLE ... SET WRITE [LOCALLY] ORDERED BY commands.
 * Supports both Z-ORDER and Natural Order for Iceberg tables.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_SET_WRITE_ORDER)
public class AlterTableSetWriteOrderAnalyzer extends AbstractAlterTableAnalyzer {

  public AlterTableSetWriteOrderAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    
    // Validate if this is an Iceberg table
    Table table = getTable(tableName);
    DDLUtils.validateTableIsIceberg(table);
    
    ASTNode orderNode = (ASTNode) command.getChild(0);
    if (orderNode.getType() == HiveParser.TOK_WRITE_LOCALLY_ORDERED_BY_ZORDER) {
      // Handle Z-ORDER
      handleZOrder(tableName, partitionSpec, orderNode);
    } else if (orderNode.getType() == HiveParser.TOK_WRITE_LOCALLY_ORDERED) {
      // Handle natural ORDERED BY
      handleNaturalOrder(tableName, partitionSpec, orderNode);
    } else {
      throw new SemanticException("Unexpected token type: " + orderNode.getType());
    }
  }

  /**
   * Handles Z-ORDER syntax: ALTER TABLE ... SET WRITE ORDERED BY ZORDER(col1, col2, ...)
   */
  private void handleZOrder(TableName tableName, Map<String, String> partitionSpec, ASTNode orderNode)
      throws SemanticException {
    ASTNode columnListNode = (ASTNode) orderNode.getChild(0);
    List<String> columnNames = new ArrayList<>();
    for (int i = 0; i < columnListNode.getChildCount(); i++) {
      ASTNode child = (ASTNode) columnListNode.getChild(i);
      columnNames.add(unescapeIdentifier(child.getText()).toLowerCase());
    }

    if (columnNames.isEmpty()) {
      throw new SemanticException("Z-order requires at least one column");
    }

    // Set Z-order properties: sort.order=ZORDER and sort.columns=col1,col2,...
    Map<String, String> props = Map.of(
        "sort.order", "ZORDER",
        "sort.columns", String.join(",", columnNames)
    );

    createAndAddTask(tableName, partitionSpec, props);
  }

  /**
   * Handles regular ORDERED BY syntax: ALTER TABLE ... SET WRITE ORDERED BY (col1 ASC, col2 DESC NULLS LAST, ...)
   * Creates a Hive-native SortFields JSON that will be converted to Iceberg format by the metahook.
   */
  private void handleNaturalOrder(TableName tableName, Map<String, String> partitionSpec, ASTNode orderNode)
      throws SemanticException {
    ASTNode sortColumnListNode = (ASTNode) orderNode.getChild(0);

    // Parse and serialize to JSON using the utility
    String sortOrderJson = SortOrderUtils.parseSortOrderToJson(sortColumnListNode);
    if (sortOrderJson == null) {
      throw new SemanticException("Failed to serialize sort order specification");
    }

    // Set the sort order JSON in table properties
    // The metahook will detect this and convert to Iceberg format
    Map<String, String> props = Map.of(
        "default-sort-order", sortOrderJson
    );

    createAndAddTask(tableName, partitionSpec, props);
  }

  /**
   * Creates the DDL descriptor, sets up inputs/outputs, and adds the task to rootTasks.
   */
  private void createAndAddTask(TableName tableName, Map<String, String> partitionSpec, 
      Map<String, String> props) throws SemanticException {
    AlterTableSetWriteOrderDesc desc =
        new AlterTableSetWriteOrderDesc(tableName, partitionSpec, props);
    addInputsOutputsAlterTable(
        tableName, partitionSpec, desc, desc.getType(), false);
    rootTasks.add(
        TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }
}
