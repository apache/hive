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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
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
 * Analyzer for drop column statistics commands.
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
    List<String> columnNames = command.getChildCount() == 0 ? 
        new ArrayList<>() : 
        getColumnNames((ASTNode) command.getChild(0));
    String partitionName = AcidUtils.getPartitionName(partitionSpec);

    ColumnStatsDropWork work = 
        new ColumnStatsDropWork(partitionName, table.getDbName(), table.getTableName(), columnNames);
    ColumnStatsDropTask task = (ColumnStatsDropTask) TaskFactory.get(work);
    
    addInputsOutputsAlterTable(tableName, partitionSpec, null, AlterTableType.DROP_COL_STATS, false);
    if (AcidUtils.isTransactionalTable(table)) {
      setAcidDdlDesc(work);
    }

    rootTasks.add(task);
  }
}
