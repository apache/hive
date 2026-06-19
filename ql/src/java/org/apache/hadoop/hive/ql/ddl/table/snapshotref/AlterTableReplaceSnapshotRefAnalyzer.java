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
package org.apache.hadoop.hive.ql.ddl.table.snapshotref;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.jetbrains.annotations.NotNull;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AlterTableSnapshotRefSpec;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import static org.apache.hadoop.hive.ql.parse.HiveParser_AlterClauseParser.KW_BRANCH;
import static org.apache.hadoop.hive.ql.parse.HiveParser_AlterClauseParser.KW_SYSTEM_VERSION;

@DDLSemanticAnalyzerFactory.DDLType(types = HiveParser.TOK_ALTERTABLE_REPLACE_SNAPSHOTREF)
public class AlterTableReplaceSnapshotRefAnalyzer extends AbstractAlterTableAnalyzer {

  protected AlterTableType alterTableType;

  public AlterTableReplaceSnapshotRefAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
    alterTableType = AlterTableType.REPLACE_SNAPSHOTREF;
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    Table table = getTable(tableName);
    DDLUtils.validateTableIsIceberg(table);
    inputs.add(new ReadEntity(table));
    validateAlterTableType(table, alterTableType, false);
    AlterTableSnapshotRefSpec.ReplaceSnapshotrefSpec replaceSnapshotrefSpec;
    boolean isReplaceBranch = command.getChild(0).getType() == KW_BRANCH;
    String sourceRef = command.getChild(1).getText();
    if (isReplaceBranch) {
      replaceSnapshotrefSpec = getReplaceBranchSpec(command, sourceRef);
      replaceSnapshotrefSpec.setIsReplaceBranch();
    } else {
      replaceSnapshotrefSpec = getReplaceTagSpec(command, sourceRef);
    }

    AlterTableSnapshotRefSpec<AlterTableSnapshotRefSpec.ReplaceSnapshotrefSpec> alterTableSnapshotRefSpec =
        new AlterTableSnapshotRefSpec<>(alterTableType, replaceSnapshotrefSpec);
    AbstractAlterTableDesc alterTableDesc =
        new AlterTableSnapshotRefDesc(alterTableType, tableName, alterTableSnapshotRefSpec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTableDesc)));
  }

  @NotNull
  private AlterTableSnapshotRefSpec.ReplaceSnapshotrefSpec getReplaceTagSpec(ASTNode command, String sourceBranch)
      throws SemanticException {
    AlterTableSnapshotRefSpec.ReplaceSnapshotrefSpec replaceSnapshotrefSpec;
    long targetSnapshot = Long.parseLong(command.getChild(2).getText());
    replaceSnapshotrefSpec = new AlterTableSnapshotRefSpec.ReplaceSnapshotrefSpec(sourceBranch, targetSnapshot);

    for (int childNodeNum = 3; childNodeNum < command.getChildCount(); childNodeNum++) {
      AlterTableReplaceSnapshotRefAnalyzer.extractOptionalArguments(command, replaceSnapshotrefSpec, childNodeNum,
          alterTableType);
    }
    return replaceSnapshotrefSpec;
  }

  @NotNull
  private AlterTableSnapshotRefSpec.ReplaceSnapshotrefSpec getReplaceBranchSpec(ASTNode command,
      String sourceBranch) throws SemanticException {
    AlterTableSnapshotRefSpec.ReplaceSnapshotrefSpec replaceSnapshotrefSpec;
    int childNodeNum;
    if (command.getChild(2).getType() == KW_SYSTEM_VERSION) {
      long targetSnapshot = Long.parseLong(command.getChild(3).getText());
      replaceSnapshotrefSpec = new AlterTableSnapshotRefSpec.ReplaceSnapshotrefSpec(sourceBranch, targetSnapshot);
      childNodeNum = 4;
    } else {
      String targetBranch = command.getChild(2).getText();
      replaceSnapshotrefSpec = new AlterTableSnapshotRefSpec.ReplaceSnapshotrefSpec(sourceBranch, targetBranch);
      childNodeNum = 3;
    }

    for (; childNodeNum < command.getChildCount(); childNodeNum++) {
      extractOptionalArguments(command, replaceSnapshotrefSpec, childNodeNum, alterTableType);
    }
    return replaceSnapshotrefSpec;
  }

  public static void extractOptionalArguments(ASTNode command,
      AlterTableSnapshotRefSpec.ReplaceSnapshotrefSpec replaceSnapshotrefSpec, int childNodeNum,
      AlterTableType alterTableType) throws SemanticException {
    ASTNode childNode = (ASTNode) command.getChild(childNodeNum);
    switch (childNode.getToken().getType()) {
    case HiveParser.TOK_RETAIN:
      String maxRefAge = childNode.getChild(0).getText();
      String timeUnitOfBranchRetain = childNode.getChild(1).getText();
      long maxRefAgeMs =
          TimeUnit.valueOf(timeUnitOfBranchRetain.toUpperCase(Locale.ENGLISH)).toMillis(Long.parseLong(maxRefAge));
      replaceSnapshotrefSpec.setMaxRefAgeMs(maxRefAgeMs);
      break;
    case HiveParser.TOK_WITH_SNAPSHOT_RETENTION:
      int minSnapshotsToKeep = Integer.parseInt(childNode.getChild(0).getText());
       replaceSnapshotrefSpec.setMinSnapshotsToKeep(minSnapshotsToKeep);
      if (childNode.getChildren().size() > 1) {
        String maxSnapshotAge = childNode.getChild(1).getText();
        String timeUnitOfSnapshotsRetention = childNode.getChild(2).getText();
        long maxSnapshotAgeMs = TimeUnit.valueOf(timeUnitOfSnapshotsRetention.toUpperCase(Locale.ENGLISH))
            .toMillis(Long.parseLong(maxSnapshotAge));
        replaceSnapshotrefSpec.setMaxSnapshotAgeMs(maxSnapshotAgeMs);
      }
      break;
    default:
      throw new SemanticException("Unrecognized token in ALTER " + alterTableType.getName() + " statement");
    }
  }
}
