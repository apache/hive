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

package org.apache.hadoop.hive.ql.ddl.table.misc.rename;

import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for rename table like entities commands.
 */
public abstract class AbstractAlterTableRenameAnalyzer extends AbstractAlterTableAnalyzer {
  public AbstractAlterTableRenameAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    TableName target = getQualifiedTableName((ASTNode) command.getChild(0));

    AlterTableRenameDesc desc = new AlterTableRenameDesc(tableName, null, isView(), target.getNotEmptyDbTable());
    Table table = getTable(tableName.getNotEmptyDbTable(), true);
    if (AcidUtils.isTransactionalTable(table)) {
      setAcidDdlDesc(desc);
    }
    addInputsOutputsAlterTable(tableName, null, desc, desc.getType(), false);
    String newDatabaseName = target.getDb() != null ? target.getDb() : table.getDbName(); // extract new database name from new table name, if not specified, then src dbname is used
    DDLUtils.addDbAndTableToOutputs(getDatabase(newDatabaseName), target,
            table.getTableType(), table.isTemporary(), table.getParameters(), outputs);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }

  protected abstract boolean isView();
}
