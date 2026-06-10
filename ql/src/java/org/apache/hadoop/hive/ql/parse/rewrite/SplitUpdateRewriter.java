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
package org.apache.hadoop.hive.ql.parse.rewrite;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.MultiInsertSqlGenerator;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SetClausePatcher;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.SqlGeneratorFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.metadata.RowLineageUtils.addRowLineageColumnsForUpdate;

public class SplitUpdateRewriter implements Rewriter<UpdateStatement> {

  private static final Context.Operation OPERATION = Context.Operation.UPDATE;

  private final HiveConf conf;
  protected final SqlGeneratorFactory sqlGeneratorFactory;
  private final SetClausePatcher setClausePatcher;

  public SplitUpdateRewriter(HiveConf conf, SqlGeneratorFactory sqlGeneratorFactory) {
    this.conf = conf;
    this.sqlGeneratorFactory = sqlGeneratorFactory;
    this.setClausePatcher = new SetClausePatcher();
  }

  @Override
  public ParseUtils.ReparseResult rewrite(Context context, UpdateStatement updateBlock)
      throws SemanticException {
    Map<Integer, ASTNode> setColExprs = new HashMap<>(updateBlock.getSetClauseTree().getChildCount());

    MultiInsertSqlGenerator sqlGenerator = sqlGeneratorFactory.createSqlGenerator();

    sqlGenerator.append("FROM\n");
    sqlGenerator.append("(SELECT ");

    sqlGenerator.appendAcidSelectColumns(OPERATION);
    List<String> deleteValues = sqlGenerator.getDeleteValues(OPERATION);
    int columnOffset = deleteValues.size();

    Table targetTable = updateBlock.getTargetTable();
    List<String> insertValues = new ArrayList<>(Collections.nCopies(targetTable.getAllCols().size(), null));
    boolean first = true;

    List<FieldSchema> nonPartCols = targetTable.getCols();
    for (int i = 0; i < nonPartCols.size(); i++) {
      appendUpdateColumn(updateBlock, sqlGenerator, insertValues, setColExprs,
          nonPartCols.get(i).getName(), i, columnOffset, true, !first);
      first = false;
    }
    List<FieldSchema> partCols = targetTable.getPartCols();
    for (int i = 0; i < partCols.size(); i++) {
      boolean appendToSelect = targetTable.hasNonNativePartitionSupport();
      appendUpdateColumn(updateBlock, sqlGenerator, insertValues, setColExprs,
          partCols.get(i).getName(), nonPartCols.size() + i, columnOffset, appendToSelect, !first);
      first = false;
    }
    addRowLineageColumnsForUpdate(targetTable, sqlGenerator, insertValues, conf);

    sqlGenerator.append(" FROM ").append(sqlGenerator.getTargetTableFullName()).append(") ");
    sqlGenerator.appendSubQueryAlias().append("\n");

    sqlGenerator.appendInsertBranch(null, insertValues);
    sqlGenerator.appendInsertBranch(null, deleteValues);

    sqlGenerator.appendSortKeys();

    ParseUtils.ReparseResult rr = ParseUtils.parseRewrittenQuery(context, sqlGenerator.toString());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    ASTNode rewrittenInsert = new CalcitePlanner.ASTSearcher().simpleBreadthFirstSearch(
        rewrittenTree, HiveParser.TOK_FROM, HiveParser.TOK_SUBQUERY, HiveParser.TOK_INSERT);

    rewrittenCtx.setOperation(Context.Operation.UPDATE, true);
    rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.INSERT);
    rewrittenCtx.addDeleteOfUpdateDestNamePrefix(2, Context.DestClausePrefix.DELETE);

    if (updateBlock.getWhereTree() != null) {
      rewrittenInsert.addChild(updateBlock.getWhereTree());
    }

    setClausePatcher.patchProjectionForUpdate(rewrittenInsert, setColExprs);

    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);

    return rr;
  }

  private void appendUpdateColumn(UpdateStatement updateBlock,
               MultiInsertSqlGenerator sqlGenerator, List<String> insertValues,
               Map<Integer, ASTNode> setColExprs, String columnName, int setColExprIndex, int columnOffset,
               boolean appendToSelect, boolean prependComma) {
    String identifier = HiveUtils.unparseIdentifier(columnName, conf);

    // The insert value is placed for every column (data and partition).
    int index = updateBlock.getTargetTable().getColumnIndexByName(columnName);
    insertValues.set(index, sqlGenerator.qualify(identifier));

    if (!appendToSelect) {
      return;
    }
    if (prependComma) {
      sqlGenerator.append(",");
    }
    ASTNode setCol = updateBlock.getSetCols().get(columnName);
    if (setCol != null) {
      if (setCol.getType() == HiveParser.TOK_TABLE_OR_COL &&
          setCol.getChildCount() == 1 && setCol.getChild(0).getType() == HiveParser.TOK_DEFAULT_VALUE) {
        sqlGenerator.append(updateBlock.getColNameToDefaultConstraint().get(columnName));
      } else {
        sqlGenerator.append(identifier);
        // This is one of the columns we're setting, record it's position so we can come back
        // later and patch it up. 0th is ROW_ID
        setColExprs.put(setColExprIndex + columnOffset, setCol);
      }
    } else {
      sqlGenerator.append(identifier);
    }
    sqlGenerator.append(" AS ");
    sqlGenerator.append(identifier);
  }
}
