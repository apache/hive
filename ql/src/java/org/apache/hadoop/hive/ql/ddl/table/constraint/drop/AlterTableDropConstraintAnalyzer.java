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

package org.apache.hadoop.hive.ql.ddl.table.constraint.drop;

import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for drop constraint commands.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_DROPCONSTRAINT)
public class AlterTableDropConstraintAnalyzer extends AbstractAlterTableAnalyzer {
  public AlterTableDropConstraintAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    String constraintName = unescapeIdentifier(command.getChild(0).getText());

    AlterTableDropConstraintDesc desc = new AlterTableDropConstraintDesc(tableName, null, constraintName);

    Table table = getTable(tableName);
    WriteEntity.WriteType writeType = null;
    if (AcidUtils.isTransactionalTable(table)) {
      setAcidDdlDesc(desc);
      writeType = WriteType.DDL_EXCL_WRITE;
    } else {
      writeType = WriteEntity.determineAlterTableWriteType(AlterTableType.DROP_CONSTRAINT, table, conf);
    }
    inputs.add(new ReadEntity(table));
    WriteEntity alterTableOutput = new WriteEntity(table, writeType);
    outputs.add(alterTableOutput);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }
}
