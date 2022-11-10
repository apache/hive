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

package org.apache.hadoop.hive.ql.ddl.table.storage.skewed;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ValidationUtility;

/**
 * Analyzer for skewed table commands.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_SKEWED)
public class AlterTableSkewedByAnalyzer extends AbstractAlterTableAnalyzer {
  public AlterTableSkewedByAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {

    Table table = getTable(tableName);
    validateAlterTableType(table, AlterTableType.SKEWED_BY, false);

    if (AcidUtils.isLocklessReadsEnabled(table, conf)) {
      throw new UnsupportedOperationException(command.getText());
    }
    inputs.add(new ReadEntity(table));
    outputs.add(new WriteEntity(table, WriteEntity.WriteType.DDL_EXCLUSIVE));

    DDLDescWithWriteId desc = null;
    if (command.getChildCount() == 0) {
      desc = new AlterTableNotSkewedDesc(tableName);
      setAcidDdlDesc(table, desc);
    } else {
      switch (((ASTNode) command.getChild(0)).getToken().getType()) {
      case HiveParser.TOK_TABLESKEWED:
        desc = handleAlterTableSkewedBy(command, tableName, table);
        setAcidDdlDesc(table, desc);
        break;
      case HiveParser.TOK_STOREDASDIRS:
        desc = handleAlterTableDisableStoredAsDirs(tableName, table);
        setAcidDdlDesc(table, desc);
        break;
      default:
        assert false;
      }
    }

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }

  private DDLDescWithWriteId handleAlterTableSkewedBy(ASTNode ast, TableName tableName, Table table) throws SemanticException {
    ASTNode skewedNode = (ASTNode) ast.getChild(0);
    List<String> skewedColumnNames = SkewedTableUtils.analyzeSkewedTableDDLColNames(skewedNode);
    List<List<String>> skewedColumnValues = SkewedTableUtils.analyzeDDLSkewedValues(skewedNode);
    boolean storedAsDirs = analyzeStoredAdDirs(skewedNode);

    if (table != null) {
      ValidationUtility.validateSkewedInformation(
          ParseUtils.validateColumnNameUniqueness(table.getCols()), skewedColumnNames, skewedColumnValues);
    }

    return new AlterTableSkewedByDesc(tableName, skewedColumnNames, skewedColumnValues, storedAsDirs);
  }

  private DDLDescWithWriteId handleAlterTableDisableStoredAsDirs(TableName tableName, Table table) throws SemanticException {
    List<String> skewedColumnNames = table.getSkewedColNames();
    List<List<String>> skewedColumnValues = table.getSkewedColValues();
    if (CollectionUtils.isEmpty(skewedColumnNames) || CollectionUtils.isEmpty(skewedColumnValues)) {
      throw new SemanticException(ErrorMsg.ALTER_TBL_STOREDASDIR_NOT_SKEWED.getMsg(tableName.getNotEmptyDbTable()));
    }

    return new AlterTableSkewedByDesc(tableName, skewedColumnNames, skewedColumnValues, false);
  }
}
