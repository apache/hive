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

package org.apache.hadoop.hive.ql.ddl.table.branch.create;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AlterTableCreateBranchSpec;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

@DDLSemanticAnalyzerFactory.DDLType(types = HiveParser.TOK_ALTERTABLE_CREATE_BRANCH)
public class AlterTableCreateBranchAnalyzer extends AbstractAlterTableAnalyzer {

  public AlterTableCreateBranchAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    Table table = getTable(tableName);
    validateAlterTableType(table, AlterTableType.CREATEBRANCH, false);
    if (!"ICEBERG".equalsIgnoreCase(table.getParameters().get("table_type"))) {
      throw new SemanticException("Cannot perform ALTER CREATE BRANCH statement on non-iceberg table.");
    }
    inputs.add(new ReadEntity(table));

    String branchName = command.getChild(0).getText();
    Long snapshotId = null;
    Long maxRefAgeMs = null;
    Integer minSnapshotsToKeep = null;
    Long maxSnapshotAgeMs = null;
    for (int i = 1; i < command.getChildCount(); i++) {
      ASTNode childNode = (ASTNode) command.getChild(i);
      switch (childNode.getToken().getType()) {
      case HiveParser.TOK_AS_OF_VERSION_BRANCH:
        snapshotId = Long.valueOf(childNode.getChild(0).getText());
        break;
      case HiveParser.TOK_RETAIN:
        String maxRefAge = childNode.getChild(0).getText();
        String timeUnitOfBranchRetain = childNode.getChild(1).getText();
        maxRefAgeMs = TimeUnit.valueOf(timeUnitOfBranchRetain.toUpperCase(Locale.ENGLISH)).toMillis(Long.valueOf(maxRefAge));
        break;
      case HiveParser.TOK_WITH_SNAPSHOT_RETENTION:
        minSnapshotsToKeep = Integer.valueOf(childNode.getChild(0).getText());
        if (childNode.getChildren().size() > 1) {
          String maxSnapshotAge = childNode.getChild(1).getText();
          String timeUnitOfSnapshotsRetention = childNode.getChild(2).getText();
          maxSnapshotAgeMs = TimeUnit.valueOf(timeUnitOfSnapshotsRetention.toUpperCase(Locale.ENGLISH)).toMillis(Long.valueOf(maxSnapshotAge));
        }
        break;
      default:
        throw new SemanticException("Unrecognized token in ALTER CREATE BRANCH statement");
      }
    }

    AlterTableCreateBranchSpec spec = new AlterTableCreateBranchSpec(branchName, snapshotId, maxRefAgeMs, minSnapshotsToKeep, maxSnapshotAgeMs);
    AlterTableCreateBranchDesc desc = new AlterTableCreateBranchDesc(tableName, spec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }
}
