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
package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.hadoop.hive.ql.Context.Operation.DELETE;

/**
 * A subclass of the {@link SemanticAnalyzer} that just handles
 * update statements. It works by rewriting the updates into multi insert
 * statements: one branch for delete delta and one branch for insert new values.
 * See also {@link UpdateDeleteSemanticAnalyzer}
 */
public class SplitUpdateSemanticAnalyzer extends RewriteSemanticAnalyzer {

  public static final String DELETE_PREFIX = "__d__";
  public static final String SUB_QUERY_ALIAS = "s";
  private Context.Operation operation = Context.Operation.OTHER;

  SplitUpdateSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  protected void analyze(ASTNode tree) throws SemanticException {
    if (tree.getToken().getType() != HiveParser.TOK_UPDATE_TABLE) {
      throw new RuntimeException("Asked to parse token " + tree.getName() + " in SplitUpdateSemanticAnalyzer");
    }

    analyzeUpdate(tree);
  }

  private void analyzeUpdate(ASTNode tree) throws SemanticException {
    operation = Context.Operation.UPDATE;

    List<? extends Node> children = tree.getChildren();

//    TOK_UPDATE_TABLE
//            TOK_TABNAME <- The first child should be the table we are updating
    ASTNode tabName = (ASTNode) children.get(0);
    assert tabName.getToken().getType() == HiveParser.TOK_TABNAME :
            "Expected tablename as first child of " + operation + " but found " + tabName.getName();
    Table mTable = getTargetTable(tabName);
    validateTargetTable(mTable);

    ASTNode where = null;
    int whereIndex = 2;
    if (children.size() > whereIndex) {
      where = (ASTNode) children.get(whereIndex);
      assert where.getToken().getType() == HiveParser.TOK_WHERE :
              "Expected where clause, but found " + where.getName();
    }

    Set<String> setRCols = new LinkedHashSet<>();
//    TOK_UPDATE_TABLE
//            TOK_TABNAME
//               ...
//            TOK_SET_COLUMNS_CLAUSE <- The set list from update should be the second child (index 1)
    assert children.size() >= 2 : "Expected update token to have at least two children";
    ASTNode setClause = (ASTNode) children.get(1);
    Map<String, ASTNode> setCols = collectSetColumnsAndExpressions(setClause, setRCols, mTable);
    Map<Integer, ASTNode> setColExprs = new HashMap<>(setClause.getChildCount());

    // save the operation type into the query state
    SessionStateUtil.addResource(conf, Context.Operation.class.getSimpleName(), Context.Operation.OTHER.name());

    StringBuilder rewrittenQueryStr = createRewrittenQueryStrBuilder();
    rewrittenQueryStr.append("(SELECT ");

    boolean nonNativeAcid = AcidUtils.isNonNativeAcidTable(mTable);
    int columnOffset;
    List<String> deleteValues;
    if (nonNativeAcid) {
      List<FieldSchema> acidSelectColumns = mTable.getStorageHandler().acidSelectColumns(mTable, operation);
      deleteValues = new ArrayList<>(acidSelectColumns.size());
      for (FieldSchema fieldSchema : acidSelectColumns) {
        String identifier = HiveUtils.unparseIdentifier(fieldSchema.getName(), this.conf);
        rewrittenQueryStr.append(identifier).append(" AS ");
        String prefixedIdentifier = HiveUtils.unparseIdentifier(DELETE_PREFIX + fieldSchema.getName(), this.conf);
        rewrittenQueryStr.append(prefixedIdentifier);
        rewrittenQueryStr.append(",");
        deleteValues.add(String.format("%s.%s", SUB_QUERY_ALIAS, prefixedIdentifier));
      }

      columnOffset = acidSelectColumns.size();
    } else {
      rewrittenQueryStr.append("ROW__ID,");
      deleteValues = new ArrayList<>(1 + mTable.getPartCols().size());
      deleteValues.add(SUB_QUERY_ALIAS + ".ROW__ID");
      for (FieldSchema fieldSchema : mTable.getPartCols()) {
        deleteValues.add(SUB_QUERY_ALIAS + "." + HiveUtils.unparseIdentifier(fieldSchema.getName(), conf));
      }
      columnOffset = 1;
    }

    List<FieldSchema> nonPartCols = mTable.getCols();
    Map<String, String> colNameToDefaultConstraint = getColNameToDefaultValueMap(mTable);
    List<String> insertValues = new ArrayList<>(mTable.getCols().size());
    boolean first = true;
    for (int i = 0; i < nonPartCols.size(); i++) {
      if (first) {
        first = false;
      } else {
        rewrittenQueryStr.append(",");
      }

      String name = nonPartCols.get(i).getName();
      ASTNode setCol = setCols.get(name);
      String identifier = HiveUtils.unparseIdentifier(name, this.conf);

      if (setCol != null) {
        if (setCol.getType() == HiveParser.TOK_TABLE_OR_COL &&
                setCol.getChildCount() == 1 && setCol.getChild(0).getType() == HiveParser.TOK_DEFAULT_VALUE) {
          rewrittenQueryStr.append(colNameToDefaultConstraint.get(name));
        } else {
          rewrittenQueryStr.append(identifier);
          // This is one of the columns we're setting, record it's position so we can come back
          // later and patch it up. 0th is ROW_ID
          setColExprs.put(i + columnOffset, setCol);
        }
      } else {
        rewrittenQueryStr.append(identifier);
      }
      rewrittenQueryStr.append(" AS ");
      rewrittenQueryStr.append(identifier);

      insertValues.add(SUB_QUERY_ALIAS + "." + identifier);
    }
    addPartitionColsToSelect(mTable.getPartCols(), rewrittenQueryStr);
    addPartitionColsAsValues(mTable.getPartCols(), SUB_QUERY_ALIAS, insertValues);
    rewrittenQueryStr.append(" FROM ").append(getFullTableNameForSQL(tabName)).append(") ");
    rewrittenQueryStr.append(SUB_QUERY_ALIAS).append("\n");

    appendInsertBranch(rewrittenQueryStr, null, insertValues);
    appendInsertBranch(rewrittenQueryStr, null, deleteValues);

    List<String> sortKeys;
    if (nonNativeAcid) {
      sortKeys = mTable.getStorageHandler().acidSortColumns(mTable, DELETE).stream()
              .map(fieldSchema -> String.format(
                      "%s.%s",
                      SUB_QUERY_ALIAS,
                      HiveUtils.unparseIdentifier(DELETE_PREFIX + fieldSchema.getName(), this.conf)))
              .collect(Collectors.toList());
    } else {
      sortKeys = singletonList(SUB_QUERY_ALIAS + ".ROW__ID ");
    }
    appendSortBy(rewrittenQueryStr, sortKeys);

    ReparseResult rr = parseRewrittenQuery(rewrittenQueryStr, ctx.getCmd());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    ASTNode rewrittenInsert = new ASTSearcher().simpleBreadthFirstSearch(
            rewrittenTree, HiveParser.TOK_FROM, HiveParser.TOK_SUBQUERY, HiveParser.TOK_INSERT);

    rewrittenCtx.setOperation(Context.Operation.UPDATE);
    rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.INSERT);
    rewrittenCtx.addDeleteOfUpdateDestNamePrefix(2, Context.DestClausePrefix.DELETE);

    if (where != null) {
      rewrittenInsert.addChild(where);
    }

    patchProjectionForUpdate(rewrittenInsert, setColExprs);

    try {
      useSuper = true;
      // Note: this will overwrite this.ctx with rewrittenCtx
      rewrittenCtx.setEnableUnparse(false);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Rewritten AST {}", rewrittenTree.dump());
      }
      super.analyze(rewrittenTree, rewrittenCtx);
    } finally {
      useSuper = false;
    }

    updateOutputs(mTable);

    setUpAccessControlInfoForUpdate(mTable, setCols);

    // Add the setRCols to the input list
    if (columnAccessInfo == null) { //assuming this means we are not doing Auth
      return;
    }

    for (String colName : setRCols) {
      columnAccessInfo.add(Table.getCompleteName(mTable.getDbName(), mTable.getTableName()), colName);
    }
  }
}
