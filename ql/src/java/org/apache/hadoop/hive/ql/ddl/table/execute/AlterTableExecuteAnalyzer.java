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

package org.apache.hadoop.hive.ql.ddl.table.execute;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec;
import org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.ExpireSnapshotsSpec;
import org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.FastForwardSpec;
import org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.RollbackSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.time.ZoneId;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.ExecuteOperationType.EXPIRE_SNAPSHOT;
import static org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.ExecuteOperationType.FAST_FORWARD;
import static org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.ExecuteOperationType.ROLLBACK;
import static org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.ExecuteOperationType.SET_CURRENT_SNAPSHOT;
import static org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.RollbackSpec.RollbackType.TIME;
import static org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.RollbackSpec.RollbackType.VERSION;

/**
 * Analyzer for ALTER TABLE ... EXECUTE commands.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_EXECUTE)
public class AlterTableExecuteAnalyzer extends AbstractAlterTableAnalyzer {

  private static final Pattern EXPIRE_SNAPSHOT_BY_ID_REGEX = Pattern.compile("\\d+(\\s*,\\s*\\d+)*");

  public AlterTableExecuteAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    Table table = getTable(tableName);
    // the first child must be the execute operation type
    ASTNode executeCommandType = (ASTNode) command.getChild(0);
    validateAlterTableType(table, AlterTableType.EXECUTE, false);
    inputs.add(new ReadEntity(table));
    AlterTableExecuteDesc desc = null;
    if (HiveParser.KW_ROLLBACK == executeCommandType.getType()) {
      AlterTableExecuteSpec<AlterTableExecuteSpec.RollbackSpec> spec;
      // the second child must be the rollback parameter
      ASTNode child = (ASTNode) command.getChild(1);

      if (child.getType() == HiveParser.StringLiteral) {
        ZoneId timeZone = SessionState.get() == null ? new HiveConf().getLocalTimeZone() : SessionState.get().getConf()
            .getLocalTimeZone();
        TimestampTZ time = TimestampTZUtil.parse(PlanUtils.stripQuotes(child.getText()), timeZone);
        spec = new AlterTableExecuteSpec(ROLLBACK, new RollbackSpec(TIME, time.toEpochMilli()));
      } else {
        spec = new AlterTableExecuteSpec(ROLLBACK, new RollbackSpec(VERSION,
            Long.valueOf(child.getText())));
      }
      desc = new AlterTableExecuteDesc(tableName, partitionSpec, spec);
    } else if (HiveParser.KW_EXPIRE_SNAPSHOTS == executeCommandType.getType()) {
      AlterTableExecuteSpec<AlterTableExecuteSpec.ExpireSnapshotsSpec> spec;
      // the second child must be the rollback parameter
      ASTNode child = (ASTNode) command.getChild(1);

      ZoneId timeZone = SessionState.get() == null ? new HiveConf().getLocalTimeZone() : SessionState.get().getConf()
          .getLocalTimeZone();
      String childText = PlanUtils.stripQuotes(child.getText().trim());
      if (EXPIRE_SNAPSHOT_BY_ID_REGEX.matcher(childText).matches()) {
         spec = new AlterTableExecuteSpec(EXPIRE_SNAPSHOT, new ExpireSnapshotsSpec(childText));
      } else {
        TimestampTZ time = TimestampTZUtil.parse(childText, timeZone);
        spec = new AlterTableExecuteSpec(EXPIRE_SNAPSHOT, new ExpireSnapshotsSpec(time.toEpochMilli()));
      }
      desc = new AlterTableExecuteDesc(tableName, partitionSpec, spec);
    } else if (HiveParser.KW_SET_CURRENT_SNAPSHOT == executeCommandType.getType()) {
      ASTNode child = (ASTNode) command.getChild(1);
      AlterTableExecuteSpec<AlterTableExecuteSpec.SetCurrentSnapshotSpec> spec =
          new AlterTableExecuteSpec(SET_CURRENT_SNAPSHOT,
              new AlterTableExecuteSpec.SetCurrentSnapshotSpec(Long.valueOf(child.getText())));
      desc = new AlterTableExecuteDesc(tableName, partitionSpec, spec);
    } else if (HiveParser.KW_FAST_FORWARD == executeCommandType.getType()) {
      String branchName;
      String targetBranchName;
      ASTNode child1 = (ASTNode) command.getChild(1);
      if (command.getChildCount() == 2) {
        branchName = "main";
        targetBranchName = PlanUtils.stripQuotes(child1.getText());
      } else {
        ASTNode child2 = (ASTNode) command.getChild(2);
        branchName = PlanUtils.stripQuotes(child1.getText());
        targetBranchName = PlanUtils.stripQuotes(child2.getText());
      }

      AlterTableExecuteSpec spec =
          new AlterTableExecuteSpec(FAST_FORWARD, new FastForwardSpec(branchName, targetBranchName));
      desc = new AlterTableExecuteDesc(tableName, partitionSpec, spec);
    }

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }
}
