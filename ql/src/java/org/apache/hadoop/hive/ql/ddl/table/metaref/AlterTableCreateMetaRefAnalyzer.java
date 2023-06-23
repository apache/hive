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

package org.apache.hadoop.hive.ql.ddl.table.metaref;

import java.time.ZoneId;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

public abstract class AlterTableCreateMetaRefAnalyzer extends AbstractAlterTableAnalyzer {
  protected static AbstractAlterTableDesc alterTableDesc;
  protected static AlterTableType alterTableType;
  protected abstract AbstractAlterTableDesc getAlterTableDesc(AlterTableTypeReq alterTableTypeReq)
      throws SemanticException;

  public AlterTableCreateMetaRefAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    Table table = getTable(tableName);
    DDLUtils.validateTableIsIceberg(table);
    inputs.add(new ReadEntity(table));
    validateAlterTableType(table, alterTableType, false);
    AlterTableTypeReq alterTableTypeReq = new AlterTableTypeReq();

    String metaRefName = command.getChild(0).getText();
    alterTableTypeReq.tableName = tableName;
    alterTableTypeReq.metaRefName = metaRefName;
    Long snapshotId = null;
    Long asOfTime = null;
    Long maxRefAgeMs = null;
    Integer minSnapshotsToKeep = null;
    Long maxSnapshotAgeMs = null;
    AlterTableType alterTableType = command.getType()
        == HiveParser.TOK_ALTERTABLE_CREATE_BRANCH ? AlterTableType.CREATE_BRANCH : AlterTableType.CREATE_TAG;
    for (int i = 1; i < command.getChildCount(); i++) {
      ASTNode childNode = (ASTNode) command.getChild(i);
      switch (childNode.getToken().getType()) {
      case HiveParser.TOK_AS_OF_VERSION:
        snapshotId = Long.parseLong(childNode.getChild(0).getText());
        alterTableTypeReq.snapshotId = snapshotId;
        break;
      case HiveParser.TOK_AS_OF_TIME:
        ZoneId timeZone = SessionState.get() == null ? new HiveConf().getLocalTimeZone() :
            SessionState.get().getConf().getLocalTimeZone();
        TimestampTZ ts = TimestampTZUtil.parse(stripQuotes(childNode.getChild(0).getText()), timeZone);
        asOfTime = ts.toEpochMilli();
        alterTableTypeReq.asOfTime = asOfTime;
        break;
      case HiveParser.TOK_RETAIN:
        String maxRefAge = childNode.getChild(0).getText();
        String timeUnitOfBranchRetain = childNode.getChild(1).getText();
        maxRefAgeMs =
            TimeUnit.valueOf(timeUnitOfBranchRetain.toUpperCase(Locale.ENGLISH)).toMillis(Long.parseLong(maxRefAge));
        alterTableTypeReq.maxRefAgeMs = maxRefAgeMs;
        break;
      case HiveParser.TOK_WITH_SNAPSHOT_RETENTION:
        minSnapshotsToKeep = Integer.valueOf(childNode.getChild(0).getText());
        alterTableTypeReq.minSnapshotsToKeep = minSnapshotsToKeep;
        if (childNode.getChildren().size() > 1) {
          String maxSnapshotAge = childNode.getChild(1).getText();
          String timeUnitOfSnapshotsRetention = childNode.getChild(2).getText();
          maxSnapshotAgeMs = TimeUnit.valueOf(timeUnitOfSnapshotsRetention.toUpperCase(Locale.ENGLISH))
              .toMillis(Long.parseLong(maxSnapshotAge));
          alterTableTypeReq.maxSnapshotAgeMs = maxSnapshotAgeMs;
        }
        break;
      default:
        throw new SemanticException("Unrecognized token in ALTER " + alterTableType.getName() + " statement");
      }
    }
    alterTableDesc = getAlterTableDesc(alterTableTypeReq);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), alterTableDesc)));
  }

  protected class AlterTableTypeReq{
    TableName tableName;
    String metaRefName;
    Long snapshotId, asOfTime, maxRefAgeMs, maxSnapshotAgeMs;
    Integer minSnapshotsToKeep;

    public TableName getTableName() {
      return tableName;
    }

    public String getMetaRefName() {
      return metaRefName;
    }

    public Long getSnapshotId() {
      return snapshotId;
    }

    public Long getAsOfTime() {
      return asOfTime;
    }

    public Long getMaxRefAgeMs() {
      return maxRefAgeMs;
    }

    public Long getMaxSnapshotAgeMs() {
      return maxSnapshotAgeMs;
    }

    public Integer getMinSnapshotsToKeep() {
      return minSnapshotsToKeep;
    }
  }
}
