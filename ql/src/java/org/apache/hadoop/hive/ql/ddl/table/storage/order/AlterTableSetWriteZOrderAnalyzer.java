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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for ALTER TABLE ... SET WRITE [LOCALLY] ORDERED BY ZORDER(...) commands.
 * Sets sort.order=ZORDER and sort.columns properties directly for Iceberg tables.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_SET_WRITE_ZORDER)
public class AlterTableSetWriteZOrderAnalyzer extends AbstractAlterTableAnalyzer {

  public AlterTableSetWriteZOrderAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    
    // Validate if this is an Iceberg table
    Table table = getTable(tableName);
    DDLUtils.validateTableIsIceberg(table);
    
    ASTNode columnListNode = (ASTNode) command.getChild(0);
    List<String> columnNames = new ArrayList<>();
    for (int i = 0; i < columnListNode.getChildCount(); i++) {
      ASTNode child = (ASTNode) columnListNode.getChild(i);
      String name = unescapeIdentifier(child.getText()).toLowerCase();
      columnNames.add(name);
    }

    if (columnNames.isEmpty()) {
      throw new SemanticException("Z-order requires at least one column");
    }

    // Set Z-order properties in table props sort.order=ZORDER and sort.columns=col1,col2,...
    Map<String, String> props = new HashMap<>();
    props.put("sort.order", "ZORDER");
    props.put("sort.columns", String.join(",", columnNames));

    AlterTableSetWriteZOrderDesc desc = new AlterTableSetWriteZOrderDesc(tableName, partitionSpec, props);
    addInputsOutputsAlterTable(tableName, partitionSpec, desc, desc.getType(), false);
    
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }
}



