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
package org.apache.hadoop.hive.ql.ddl.table.snapshotref.branch.rename;

import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.ddl.table.snapshotref.AlterTableSnapshotRefDesc;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AlterTableSnapshotRefSpec;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

@DDLSemanticAnalyzerFactory.DDLType(types = HiveParser.TOK_ALTERTABLE_RENAME_BRANCH)
public class AlterTableRenameSnapshotRefAnalyzer extends AbstractAlterTableAnalyzer {

  protected AlterTableType alterTableType;

  public AlterTableRenameSnapshotRefAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
    alterTableType = AlterTableType.RENAME_BRANCH;
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    Table table = getTable(tableName);
    DDLUtils.validateTableIsIceberg(table);
    inputs.add(new ReadEntity(table));
    validateAlterTableType(table, alterTableType, false);
    String sourceBranch = command.getChild(0).getText();
    String targetBranch = command.getChild(1).getText();

    AlterTableSnapshotRefSpec.RenameSnapshotrefSpec renameSnapshotrefSpec =
        new AlterTableSnapshotRefSpec.RenameSnapshotrefSpec(sourceBranch, targetBranch);
    AlterTableSnapshotRefSpec<AlterTableSnapshotRefSpec.RenameSnapshotrefSpec> alterTableSnapshotRefSpec =
        new AlterTableSnapshotRefSpec(alterTableType, renameSnapshotrefSpec);
    AbstractAlterTableDesc alterTableDesc =
        new AlterTableSnapshotRefDesc(alterTableType, tableName, alterTableSnapshotRefSpec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTableDesc)));
  }
}
