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
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec;
import org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.CherryPickSpec;
import org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.DeleteOrphanFilesDesc;
import org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.ExpireSnapshotsSpec;
import org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.FastForwardSpec;
import org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.RollbackSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.ExecuteOperationType.CHERRY_PICK;
import static org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.ExecuteOperationType.DELETE_ORPHAN_FILES;
import static org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.ExecuteOperationType.EXPIRE_SNAPSHOT;
import static org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.ExecuteOperationType.FAST_FORWARD;
import static org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.ExecuteOperationType.ROLLBACK;
import static org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.ExecuteOperationType.SET_CURRENT_SNAPSHOT;
import static org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.RollbackSpec.RollbackType.TIME;
import static org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec.RollbackSpec.RollbackType.VERSION;
import static org.apache.hadoop.hive.ql.parse.HiveLexer.KW_RETAIN;

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
    switch (executeCommandType.getType()) {
      case HiveParser.KW_ROLLBACK:
        desc = getRollbackDesc(tableName, partitionSpec, (ASTNode) command.getChild(1));
        break;
      case HiveParser.KW_EXPIRE_SNAPSHOTS:
        desc = getExpireSnapshotDesc(tableName, partitionSpec,  command.getChildren());
        break;
      case HiveParser.KW_SET_CURRENT_SNAPSHOT:
        desc = getSetCurrentSnapshotDesc(tableName, partitionSpec, (ASTNode) command.getChild(1));
        break;
      case HiveParser.KW_FAST_FORWARD:
        desc = getFastForwardDesc(tableName, partitionSpec, command);
        break;
      case HiveParser.KW_CHERRY_PICK:
        desc = getCherryPickDesc(tableName, partitionSpec, (ASTNode) command.getChild(1));
        break;
      case HiveParser.KW_ORPHAN_FILES:
        desc = getDeleteOrphanFilesDesc(tableName, partitionSpec,  command.getChildren());
        break;        
    }

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }

  private static AlterTableExecuteDesc getCherryPickDesc(TableName tableName, Map<String, String> partitionSpec,
      ASTNode childNode) throws SemanticException {
    long snapshotId = Long.parseLong(childNode.getText());
    AlterTableExecuteSpec spec = new AlterTableExecuteSpec(CHERRY_PICK, new CherryPickSpec(snapshotId));
    return new AlterTableExecuteDesc(tableName, partitionSpec, spec);
  }

  private static AlterTableExecuteDesc getFastForwardDesc(TableName tableName, Map<String, String> partitionSpec,
      ASTNode command) throws SemanticException {
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
    return new AlterTableExecuteDesc(tableName, partitionSpec, spec);
  }

  private static AlterTableExecuteDesc getSetCurrentSnapshotDesc(TableName tableName, Map<String, String> partitionSpec,
      ASTNode childNode) throws SemanticException {
    AlterTableExecuteSpec<AlterTableExecuteSpec.SetCurrentSnapshotSpec> spec =
        new AlterTableExecuteSpec(SET_CURRENT_SNAPSHOT,
            new AlterTableExecuteSpec.SetCurrentSnapshotSpec(childNode.getText()));
    return new AlterTableExecuteDesc(tableName, partitionSpec, spec);
  }

  private static AlterTableExecuteDesc getExpireSnapshotDesc(TableName tableName, Map<String, String> partitionSpec,
      List<Node> children) throws SemanticException {
    AlterTableExecuteSpec<ExpireSnapshotsSpec> spec;
    if (children.size() == 1) {
      spec = new AlterTableExecuteSpec(EXPIRE_SNAPSHOT, null);
      return new AlterTableExecuteDesc(tableName, partitionSpec, spec);
    }
    ZoneId timeZone = SessionState.get() == null ?
        new HiveConf().getLocalTimeZone() :
        SessionState.get().getConf().getLocalTimeZone();
    ASTNode firstNode = (ASTNode) children.get(1);
    String firstNodeText = PlanUtils.stripQuotes(firstNode.getText().trim());
    if (firstNode.getType() == KW_RETAIN) {
      ASTNode numRetainLastNode = (ASTNode) children.get(2);
      String numToRetainText = PlanUtils.stripQuotes(numRetainLastNode.getText());
      int numToRetain = Integer.parseInt(numToRetainText);
      spec = new AlterTableExecuteSpec(EXPIRE_SNAPSHOT, new ExpireSnapshotsSpec(numToRetain));
    } else if (children.size() == 3) {
      ASTNode secondNode = (ASTNode) children.get(2);
      String secondNodeText = PlanUtils.stripQuotes(secondNode.getText().trim());
      TimestampTZ fromTime = TimestampTZUtil.parse(firstNodeText, timeZone);
      TimestampTZ toTime = TimestampTZUtil.parse(secondNodeText, timeZone);
      spec = new AlterTableExecuteSpec(EXPIRE_SNAPSHOT,
          new ExpireSnapshotsSpec(fromTime.toEpochMilli(), toTime.toEpochMilli()));
    } else if (EXPIRE_SNAPSHOT_BY_ID_REGEX.matcher(firstNodeText).matches()) {
      spec = new AlterTableExecuteSpec(EXPIRE_SNAPSHOT, new ExpireSnapshotsSpec(firstNodeText));
    } else {
      TimestampTZ time = TimestampTZUtil.parse(firstNodeText, timeZone);
      spec = new AlterTableExecuteSpec(EXPIRE_SNAPSHOT, new ExpireSnapshotsSpec(time.toEpochMilli()));
    }
    return new AlterTableExecuteDesc(tableName, partitionSpec, spec);
  }

  private static AlterTableExecuteDesc getRollbackDesc(TableName tableName, Map<String, String> partitionSpec,
      ASTNode childNode) throws SemanticException {
    AlterTableExecuteSpec<RollbackSpec> spec;
    // the child must be the rollback parameter
    if (childNode.getType() == HiveParser.StringLiteral) {
      ZoneId timeZone = SessionState.get() == null ?
          new HiveConf().getLocalTimeZone() :
          SessionState.get().getConf().getLocalTimeZone();
      TimestampTZ time = TimestampTZUtil.parse(PlanUtils.stripQuotes(childNode.getText()), timeZone);
      spec = new AlterTableExecuteSpec(ROLLBACK, new RollbackSpec(TIME, time.toEpochMilli()));
    } else {
      spec = new AlterTableExecuteSpec(ROLLBACK, new RollbackSpec(VERSION, Long.valueOf(childNode.getText())));
    }
    return new AlterTableExecuteDesc(tableName, partitionSpec, spec);
  }

  private static AlterTableExecuteDesc getDeleteOrphanFilesDesc(TableName tableName, Map<String, String> partitionSpec,
      List<Node> children) throws SemanticException {

    long time = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3);
    if (children.size() == 2) {
      time = getTimeStampMillis((ASTNode) children.get(1));
    }
    AlterTableExecuteSpec spec = new AlterTableExecuteSpec(DELETE_ORPHAN_FILES, new DeleteOrphanFilesDesc(time));
    return new AlterTableExecuteDesc(tableName, partitionSpec, spec);
  }

  private static long getTimeStampMillis(ASTNode childNode) {
    String childNodeText = PlanUtils.stripQuotes(childNode.getText());
    ZoneId timeZone = SessionState.get() == null ?
        new HiveConf().getLocalTimeZone() :
        SessionState.get().getConf().getLocalTimeZone();
    TimestampTZ time = TimestampTZUtil.parse(PlanUtils.stripQuotes(childNodeText), timeZone);
    return time.toEpochMilli();
  }
}
