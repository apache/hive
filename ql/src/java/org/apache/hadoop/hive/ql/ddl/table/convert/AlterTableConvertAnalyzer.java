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

package org.apache.hadoop.hive.ql.ddl.table.convert;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AlterTableConvertSpec;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.HashMap;
import java.util.Map;

/**
 * Analyzer for ALTER TABLE ... CONVERT commands.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_CONVERT)
public class AlterTableConvertAnalyzer extends AbstractAlterTableAnalyzer {

  public AlterTableConvertAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    Table table = getTable(tableName);
    // the first child must be the addprops operation type
    validateAlterTableType(table, AlterTableType.ADDPROPS, false);
    inputs.add(new ReadEntity(table));
    ASTNode targetType = (ASTNode) command.getChild(0);
    Map<String, String> properties = new HashMap<>();
    if (command.getChildCount() == 2) {
      properties = getProps((ASTNode) (command.getChild(1)).getChild(0));
    }
    AlterTableConvertSpec spec = new AlterTableConvertSpec(targetType.getText(), properties);
    AlterTableConvertDesc desc = new AlterTableConvertDesc(tableName, spec);

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }
}
