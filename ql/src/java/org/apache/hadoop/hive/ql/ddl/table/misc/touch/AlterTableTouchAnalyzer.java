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

package org.apache.hadoop.hive.ql.ddl.table.misc.touch;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for touch commands.
 *
 * Rewrite the metadata for one or more partitions in a table. Useful when an external process modifies files on HDFS
 * and you want the pre/post hooks to be fired for the specified partition.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_TOUCH)
public class AlterTableTouchAnalyzer extends AbstractAlterTableAnalyzer {
  public AlterTableTouchAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpecFromFramework, ASTNode command)
      throws SemanticException {
    Table table = getTable(tableName);
    validateAlterTableType(table, AlterTableType.TOUCH, false);
    inputs.add(new ReadEntity(table));

    List<Map<String, String>> partitionSpecs = getPartitionSpecs(table, command);

    if (partitionSpecs.isEmpty()) {
      AlterTableTouchDesc desc = new AlterTableTouchDesc(tableName.getNotEmptyDbTable(), null);
      outputs.add(new WriteEntity(table, WriteEntity.WriteType.DDL_NO_LOCK));
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
    } else {
      PartitionUtils.addTablePartsOutputs(db, outputs, table, partitionSpecs, false, WriteEntity.WriteType.DDL_NO_LOCK);
      for (Map<String, String> partitionSpec : partitionSpecs) {
        AlterTableTouchDesc desc = new AlterTableTouchDesc(tableName.getNotEmptyDbTable(), partitionSpec);
        rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
      }
    }
  }
}
