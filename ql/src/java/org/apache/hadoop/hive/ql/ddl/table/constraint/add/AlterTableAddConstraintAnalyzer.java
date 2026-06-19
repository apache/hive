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

package org.apache.hadoop.hive.ql.ddl.table.constraint.add;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.constraint.Constraints;
import org.apache.hadoop.hive.ql.ddl.table.constraint.ConstraintsUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for add constraint commands.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_ADDCONSTRAINT)
public class AlterTableAddConstraintAnalyzer extends AbstractAlterTableAnalyzer {
  public AlterTableAddConstraintAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    // TODO CAT - for now always use the default catalog.  Eventually will want to see if
    // the user specified a catalog
    List<SQLPrimaryKey> primaryKeys = new ArrayList<>();
    List<SQLForeignKey> foreignKeys = new ArrayList<>();
    List<SQLUniqueConstraint> uniqueConstraints = new ArrayList<>();
    List<SQLCheckConstraint> checkConstraints = new ArrayList<>();

    ASTNode constraintNode = (ASTNode) command.getChild(0);
    switch (constraintNode.getToken().getType()) {
    case HiveParser.TOK_UNIQUE:
      ConstraintsUtils.processUniqueConstraints(tableName, constraintNode, uniqueConstraints);
      break;
    case HiveParser.TOK_PRIMARY_KEY:
      ConstraintsUtils.processPrimaryKeys(tableName, constraintNode, primaryKeys);
      break;
    case HiveParser.TOK_FOREIGN_KEY:
      ConstraintsUtils.processForeignKeys(tableName, constraintNode, foreignKeys);
      break;
    case HiveParser.TOK_CHECK_CONSTRAINT:
      ConstraintsUtils.processCheckConstraints(tableName, constraintNode, null, checkConstraints, command,
          ctx.getTokenRewriteStream());
      break;
    default:
      throw new SemanticException(ErrorMsg.NOT_RECOGNIZED_CONSTRAINT.getMsg(constraintNode.getToken().getText()));
    }

    Constraints constraints =
        new Constraints(primaryKeys, foreignKeys, null, uniqueConstraints, null, checkConstraints);
    AlterTableAddConstraintDesc desc = new AlterTableAddConstraintDesc(tableName, null, constraints);

    Table table = getTable(tableName);
    if (AcidUtils.isTransactionalTable(table)) {
      setAcidDdlDesc(desc);
    }
    addInputsOutputsAlterTable(tableName, partitionSpec, desc, desc.getType(), false);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }
}
