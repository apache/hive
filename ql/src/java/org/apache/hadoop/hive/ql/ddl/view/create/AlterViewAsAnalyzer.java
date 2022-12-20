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

package org.apache.hadoop.hive.ql.ddl.view.create;

import java.util.ArrayList;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

/**
 * Analyzer for alter view ... as commands.
 */
@DDLType(types = HiveParser.TOK_ALTERVIEW_AS)
public class AlterViewAsAnalyzer extends AbstractCreateViewAnalyzer {
  public AlterViewAsAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
    queryState.setCommandType(HiveOperation.ALTERVIEW_AS);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    TableName viewName = getQualifiedTableName((ASTNode) root.getChild(0));
    String fqViewName = viewName.getNotEmptyDbTable();
    LOG.info("Altering the query of view " + fqViewName + " position=" + root.getCharPositionInLine());

    ASTNode select = (ASTNode) root.getChild(1).getChild(0);

    String originalText = ctx.getTokenRewriteStream().toString(select.getTokenStartIndex(), select.getTokenStopIndex());

    SemanticAnalyzer analyzer = analyzeQuery(select, fqViewName);

    schema = new ArrayList<FieldSchema>(analyzer.getResultSchema());
    ParseUtils.validateColumnNameUniqueness(
        analyzer.getOriginalResultSchema() == null ? schema : analyzer.getOriginalResultSchema());
    setColumnAccessInfo(analyzer.getColumnAccessInfo());
    String expandedText = ctx.getTokenRewriteStream().toString(select.getTokenStartIndex(), select.getTokenStopIndex());

    AlterViewAsDesc desc = new AlterViewAsDesc(fqViewName, schema, originalText, expandedText);
    validateCreateView(desc, analyzer);

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
    DDLUtils.addDbAndTableToOutputs(getDatabase(viewName.getDb()), viewName, TableType.VIRTUAL_VIEW, false,
        null, outputs);
  }

  private void validateCreateView(AlterViewAsDesc desc, SemanticAnalyzer analyzer) throws SemanticException {
    validateTablesUsed(analyzer);

    Table oldView = null;
    try {
      oldView = getTable(desc.getViewName(), false);
    } catch (HiveException e) {
      throw new SemanticException(e.getMessage(), e);
    }

    if (oldView == null) {
      String viewNotExistErrorMsg = "The following view does not exist: " + desc.getViewName();
      throw new SemanticException(ErrorMsg.ALTER_VIEW_AS_SELECT_NOT_EXIST.getMsg(viewNotExistErrorMsg));
    }

    validateReplaceWithPartitions(desc.getViewName(), oldView, null);
  }
}
