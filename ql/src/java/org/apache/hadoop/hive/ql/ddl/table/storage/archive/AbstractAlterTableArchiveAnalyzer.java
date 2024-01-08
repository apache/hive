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

package org.apache.hadoop.hive.ql.ddl.table.storage.archive;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Abstract ancestor of analyzer for archive / unarchive commands for tables.
 */
public abstract class AbstractAlterTableArchiveAnalyzer extends AbstractAlterTableAnalyzer {
  public AbstractAlterTableArchiveAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  // partSpec coming from the input is not applicable here as archiver gets its partitions from a different part of
  // the AST tree
  protected void analyzeCommand(TableName tableName, Map<String, String> partSpec, ASTNode command)
      throws SemanticException {
    if (!conf.getBoolVar(HiveConf.ConfVars.HIVE_ARCHIVE_ENABLED)) {
      throw new SemanticException(ErrorMsg.ARCHIVE_METHODS_DISABLED.getMsg());
    }

    Table table = getTable(tableName);
    validateAlterTableType(table, AlterTableType.ARCHIVE, false);

    List<Map<String, String>> partitionSpecs = getPartitionSpecs(table, command);
    if (partitionSpecs.size() > 1) {
      throw new SemanticException(getMultiPartsErrorMessage().getMsg());
    }
    if (partitionSpecs.size() == 0) {
      throw new SemanticException(ErrorMsg.ARCHIVE_ON_TABLE.getMsg());
    }

    Map<String, String> partitionSpec = partitionSpecs.get(0);
    try {
      isValidPrefixSpec(table, partitionSpec);
    } catch (HiveException e) {
      throw new SemanticException(e.getMessage(), e);
    }

    inputs.add(new ReadEntity(table));
    PartitionUtils.addTablePartsOutputs(db, outputs, table, partitionSpecs, true, WriteEntity.WriteType.DDL_NO_LOCK);

    DDLDesc archiveDesc = createDesc(tableName, partitionSpec);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), archiveDesc)));
  }

  protected abstract ErrorMsg getMultiPartsErrorMessage();

  protected abstract DDLDesc createDesc(TableName tableName, Map<String, String> partitionSpec);
}
