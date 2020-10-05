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

package org.apache.hadoop.hive.ql.ddl.table.column.change;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
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

import com.google.common.collect.ImmutableList;

/**
 * Analyzer for change columns commands.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_RENAMECOL)
public class AlterTableChangeColumnAnalyzer extends AbstractAlterTableAnalyzer {
  public AlterTableChangeColumnAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    //col_old_name col_new_name column_type [COMMENT col_comment] [FIRST|AFTER column_name] [CASCADE|RESTRICT]
    String oldColumnName = command.getChild(0).getText().toLowerCase();
    String newColumnName = command.getChild(1).getText().toLowerCase();
    String newType = getTypeStringFromAST((ASTNode) command.getChild(2));

    Table table = getTable(tableName);
    SkewedInfo skewInfo = table.getTTable().getSd().getSkewedInfo();
    if ((null != skewInfo) && (null != skewInfo.getSkewedColNames()) &&
        skewInfo.getSkewedColNames().contains(oldColumnName)) {
      throw new SemanticException(oldColumnName + ErrorMsg.ALTER_TABLE_NOT_ALLOWED_RENAME_SKEWED_COLUMN.getMsg());
    }

    String newComment = null;
    boolean first = false;
    String flagCol = null;
    boolean isCascade = false;
    ASTNode constraintChild = null;
    for (int i = 3; i < command.getChildCount(); i++) {
      ASTNode child = (ASTNode)command.getChild(i);
      switch (child.getToken().getType()) {
      case HiveParser.StringLiteral:
        newComment = unescapeSQLString(child.getText());
        break;
      case HiveParser.TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION:
        flagCol = unescapeIdentifier(child.getChild(0).getText());
        break;
      case HiveParser.KW_FIRST:
        first = true;
        break;
      case HiveParser.TOK_CASCADE:
        isCascade = true;
        break;
      case HiveParser.TOK_RESTRICT:
        break;
      default:
        constraintChild = child;
      }
    }

    Constraints constraints = getConstraints(tableName, command, newColumnName, table, constraintChild);

    AlterTableChangeColumnDesc desc = new AlterTableChangeColumnDesc(tableName, partitionSpec, isCascade, constraints,
        unescapeIdentifier(oldColumnName), unescapeIdentifier(newColumnName), newType, newComment, first, flagCol);
    if (AcidUtils.isTransactionalTable(table)) {
      // Note: we might actually need it only when certain changes (e.g. name or type?) are made.
      setAcidDdlDesc(desc);
    }

    addInputsOutputsAlterTable(tableName, partitionSpec, desc, desc.getType(), false);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }

  private Constraints getConstraints(TableName tableName, ASTNode command, String newColumnName, Table table,
      ASTNode constraintChild) throws SemanticException {
    List<SQLPrimaryKey> primaryKeys = null;
    List<SQLForeignKey> foreignKeys = null;
    List<SQLUniqueConstraint> uniqueConstraints = null;
    List<SQLNotNullConstraint> notNullConstraints = null;
    List<SQLDefaultConstraint> defaultConstraints = null;
    List<SQLCheckConstraint> checkConstraints = null;
    if (constraintChild != null) {
      // Process column constraint
      switch (constraintChild.getToken().getType()) {
      case HiveParser.TOK_CHECK_CONSTRAINT:
        checkConstraints = new ArrayList<>();
        ConstraintsUtils.processCheckConstraints(tableName, constraintChild, ImmutableList.of(newColumnName),
            checkConstraints, (ASTNode) command.getChild(2), this.ctx.getTokenRewriteStream());
        break;
      case HiveParser.TOK_DEFAULT_VALUE:
        defaultConstraints = new ArrayList<>();
        ConstraintsUtils.processDefaultConstraints(tableName, constraintChild, ImmutableList.of(newColumnName),
            defaultConstraints, (ASTNode) command.getChild(2), this.ctx.getTokenRewriteStream());
        break;
      case HiveParser.TOK_NOT_NULL:
        notNullConstraints = new ArrayList<>();
        ConstraintsUtils.processNotNullConstraints(tableName, constraintChild, ImmutableList.of(newColumnName),
            notNullConstraints);
        break;
      case HiveParser.TOK_UNIQUE:
        uniqueConstraints = new ArrayList<>();
        ConstraintsUtils.processUniqueConstraints(tableName, constraintChild, ImmutableList.of(newColumnName),
            uniqueConstraints);
        break;
      case HiveParser.TOK_PRIMARY_KEY:
        primaryKeys = new ArrayList<>();
        ConstraintsUtils.processPrimaryKeys(tableName, constraintChild, ImmutableList.of(newColumnName), primaryKeys);
        break;
      case HiveParser.TOK_FOREIGN_KEY:
        foreignKeys = new ArrayList<>();
        ConstraintsUtils.processForeignKeys(tableName, constraintChild, foreignKeys);
        break;
      default:
        throw new SemanticException(ErrorMsg.NOT_RECOGNIZED_CONSTRAINT.getMsg(
            constraintChild.getToken().getText()));
      }
    }

    /* Validate the operation of renaming a column name. */
    if (checkConstraints != null && !checkConstraints.isEmpty()) {
      ConstraintsUtils.validateCheckConstraint(table.getCols(), checkConstraints, ctx.getConf());
    }

    if (table.getTableType() == TableType.EXTERNAL_TABLE &&
        ConstraintsUtils.hasEnabledOrValidatedConstraints(notNullConstraints, defaultConstraints, checkConstraints)) {
      throw new SemanticException(ErrorMsg.INVALID_CSTR_SYNTAX.getMsg(
          "Constraints are disallowed with External tables. Only RELY is allowed."));
    }

    return new Constraints(primaryKeys, foreignKeys, notNullConstraints, uniqueConstraints, defaultConstraints,
        checkConstraints);
  }
}
