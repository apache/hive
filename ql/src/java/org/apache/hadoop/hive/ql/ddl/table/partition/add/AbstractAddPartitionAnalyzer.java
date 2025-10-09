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

package org.apache.hadoop.hive.ql.ddl.table.partition.add;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for add partition commands.
 */
abstract class AbstractAddPartitionAnalyzer extends AbstractAlterTableAnalyzer {
  AbstractAddPartitionAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    Table table = getTable(tableName);
    validateAlterTableType(table, AlterTableType.ADDPARTITION, expectView());

    boolean ifNotExists = command.getChild(0).getType() == HiveParser.TOK_IFNOTEXISTS;
    outputs.add(new WriteEntity(table,
        /* use DDL_EXCL_WRITE to cause X_WRITE lock to prevent races between concurrent add partition calls with IF NOT EXISTS.
         * w/o this 2 concurrent calls to add the same partition may both add data since for transactional tables
         * creating partition metadata and moving data there are 2 separate actions. */
      ifNotExists && AcidUtils.isTransactionalTable(table) ?
          WriteType.DDL_EXCL_WRITE : WriteType.DDL_SHARED));

    List<AlterTableAddPartitionDesc.PartitionDesc> partitions = createPartitions(command, table, ifNotExists);
    if (partitions.isEmpty()) { // nothing to do
      return;
    }

    AlterTableAddPartitionDesc desc = new AlterTableAddPartitionDesc(table.getDbName(), table.getTableName(),
        ifNotExists, partitions);
    Task<DDLWork> ddlTask = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc));
    rootTasks.add(ddlTask);

    postProcess(tableName, table, desc, ddlTask);
  }

  protected abstract boolean expectView();

  private List<AlterTableAddPartitionDesc.PartitionDesc> createPartitions(ASTNode command, Table table,
      boolean ifNotExists) throws SemanticException {
    String currentLocation = null;
    Map<String, String> currentPart = null;
    List<AlterTableAddPartitionDesc.PartitionDesc> partitions = new ArrayList<>();
    for (int num = ifNotExists ? 1 : 0; num < command.getChildCount(); num++) {
      ASTNode child = (ASTNode) command.getChild(num);
      switch (child.getToken().getType()) {
      case HiveParser.TOK_PARTSPEC:
        if (currentPart != null) {
          partitions.add(createPartitionDesc(table, currentLocation, currentPart));
          currentLocation = null;
        }
        currentPart = getValidatedPartSpec(table, child, conf, true);
        PartitionUtils.validatePartitions(conf, currentPart); // validate reserved values
        break;
      case HiveParser.TOK_PARTITIONLOCATION:
        // if location specified, set in partition
        if (!allowLocation()) {
          throw new SemanticException("LOCATION clause illegal for view partition");
        }
        currentLocation = unescapeSQLString(child.getChild(0).getText());
        inputs.add(toReadEntity(currentLocation));
        break;
      default:
        throw new SemanticException("Unknown child: " + child);
      }
    }

    if (currentPart != null) { // add the last one
      partitions.add(createPartitionDesc(table, currentLocation, currentPart));
    }

    return partitions;
  }

  private AlterTableAddPartitionDesc.PartitionDesc createPartitionDesc(Table table, String location,
      Map<String, String> partitionSpec) {
    Map<String, String> params = null;
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER) && location == null) {
      params = new HashMap<String, String>();
      StatsSetupConst.setStatsStateForCreateTable(params,
          MetaStoreUtils.getColumnNames(table.getCols()), StatsSetupConst.TRUE);
    }
    return new AlterTableAddPartitionDesc.PartitionDesc(partitionSpec, location, params);
  }

  protected abstract boolean allowLocation();

  protected abstract void postProcess(TableName tableName, Table table, AlterTableAddPartitionDesc desc,
      Task<DDLWork> ddlTask) throws SemanticException;

}
