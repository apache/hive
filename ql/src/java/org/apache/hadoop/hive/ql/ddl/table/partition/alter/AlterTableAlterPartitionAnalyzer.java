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

package org.apache.hadoop.hive.ql.ddl.table.partition.alter;

import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for alter partition commands.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_PARTCOLTYPE)
public  class AlterTableAlterPartitionAnalyzer extends AbstractAlterTableAnalyzer {
  public AlterTableAlterPartitionAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    Table table = getTable(tableName);
    validateAlterTableType(table, AlterTableType.ALTERPARTITION, false);
    inputs.add(new ReadEntity(table));

    // Alter table ... partition column ( column newtype) only takes one column at a time.
    ASTNode colAst = (ASTNode) command.getChild(0);
    String name = colAst.getChild(0).getText().toLowerCase();
    String type = getTypeStringFromAST((ASTNode) (colAst.getChild(1)));
    String comment = (colAst.getChildCount() == 3) ? unescapeSQLString(colAst.getChild(2).getText()) : null;

    FieldSchema newCol = new FieldSchema(unescapeIdentifier(name), type, comment);

    boolean isDefined = false;
    for (FieldSchema col : table.getTTable().getPartitionKeys()) {
      if (col.getName().compareTo(newCol.getName()) == 0) {
        isDefined = true;
      }
    }
    if (!isDefined) {
      throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(newCol.getName()));
    }

    AlterTableAlterPartitionDesc desc = new AlterTableAlterPartitionDesc(tableName.getNotEmptyDbTable(), newCol);
    if (AcidUtils.isTransactionalTable(table)) {
      setAcidDdlDesc(desc);
    }

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }
}
