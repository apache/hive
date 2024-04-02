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

package org.apache.hadoop.hive.ql.ddl.table.misc.columnstats;

import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.exec.ColumnStatsDropTask;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ColumnStatsDropWork;

/**
 * Analyzer for update column statistics commands.
 */
@DDLType(types = {HiveParser.TOK_ALTERTABLE_DROPCOLSTATS, HiveParser.TOK_ALTERPARTITION_DROPCOLSTATS})
public class AlterTableDropColumnStatisticsAnalyzer extends AbstractAlterTableAnalyzer {
  public AlterTableDropColumnStatisticsAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    Table table = getTable(tableName);
    String columnName = getUnescapedName((ASTNode) command.getChild(0));

    String partitionName = AcidUtils.getPartitionName(partitionSpec);
    String columnType = getColumnType(table, columnName);

    ColumnStatsDropWork work = new ColumnStatsDropWork(partitionName, table.getDbName(),
        table.getTableName(), columnName, columnType);
    ColumnStatsDropTask task = (ColumnStatsDropTask) TaskFactory.get(work);
    // TODO: doesn't look like this path is actually ever exercised. Maybe this needs to be removed.
    addInputsOutputsAlterTable(tableName, partitionSpec, null, AlterTableType.DROPSTATS, false);
    if (AcidUtils.isTransactionalTable(table)) {
      setAcidDdlDesc(work);
    }

    rootTasks.add(task);
  }



  private String getColumnType(Table table, String columnName) throws SemanticException {
    for (FieldSchema column : table.getCols()) {
      if (columnName.equalsIgnoreCase(column.getName())) {
        return column.getType();
      }
    }

    throw new SemanticException("column type not found");
  }
}
