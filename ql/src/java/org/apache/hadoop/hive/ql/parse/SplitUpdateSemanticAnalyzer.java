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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A subclass of the {@link SemanticAnalyzer} that just handles
 * update statements. It works by rewriting the updates into multi-insert
 * statements (since they are actually inserts).
 * One insert branch for inserting the new values of the updated records.
 * And another insert branch for inserting delete delta records of the updated records.
 *
 * From
 * UPDATE acidtlb SET b=350
 * WHERE a = 30
 *
 * To
 * FROM
 * (SELECT ROW__ID,`a` AS `a`,350 AS `b` FROM `default`.`acidtlb` WHERE a = 30) s
 * INSERT INTO `default`.`acidtlb`    -- insert delta
 * SELECT s.`a`,s.`b`
 * INSERT INTO `default`.`acidtlb`    -- delete delta
 * SELECT s.ROW__ID
 * SORT BY s.ROW__ID
 */
public class SplitUpdateSemanticAnalyzer extends RewriteSemanticAnalyzer {

  private Context.Operation operation = Context.Operation.OTHER;

  SplitUpdateSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected ASTNode getTargetTableNode(ASTNode tree) {
    // The first child should be the table we are updating / deleting from
    ASTNode tabName = (ASTNode)tree.getChild(0);
    assert tabName.getToken().getType() == HiveParser.TOK_TABNAME :
            "Expected tablename as first child of " + operation + " but found " + tabName.getName();
    return tabName;
  }

  protected void analyze(ASTNode tree, Table table, ASTNode tabNameNode) throws SemanticException {
    switch (tree.getToken().getType()) {
    case HiveParser.TOK_UPDATE_TABLE:
      analyzeUpdate(tree, table, tabNameNode);
      break;
    default:
      throw new RuntimeException("Asked to parse token " + tree.getName() + " in " +
          "SplitUpdateSemanticAnalyzer");
    }
  }

  private void analyzeUpdate(ASTNode tree, Table mTable, ASTNode tabNameNode) throws SemanticException {
    operation = Context.Operation.UPDATE;

    List<? extends Node> children = tree.getChildren();

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

    List<FieldSchema> nonPartCols = mTable.getCols();
    Map<String, String> colNameToDefaultConstraint = getColNameToDefaultValueMap(mTable);
    StringBuilder rewrittenQueryStr = createRewrittenQueryStrBuilder();
    rewrittenQueryStr.append("(SELECT ");

    ColumnAppender columnAppender = getColumnAppender(SUB_QUERY_ALIAS, DELETE_PREFIX);
    columnAppender.appendAcidSelectColumns(rewrittenQueryStr, operation);
    List<String> deleteValues = columnAppender.getDeleteValues(operation);
    int columnOffset = deleteValues.size();

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
    addPartitionColsAsValues(mTable.getPartCols(), SUB_QUERY_ALIAS, insertValues);
    rewrittenQueryStr.append(" FROM ").append(getFullTableNameForSQL(tabNameNode)).append(") ");
    rewrittenQueryStr.append(SUB_QUERY_ALIAS).append("\n");

    appendInsertBranch(rewrittenQueryStr, null, insertValues);
    appendInsertBranch(rewrittenQueryStr, null, deleteValues);

    List<String> sortKeys = columnAppender.getSortKeys();
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

    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);
    analyzeRewrittenTree(rewrittenTree, rewrittenCtx);

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

  @Override
  protected boolean allowOutputMultipleTimes() {
    return true;
  }

  @Override
  protected boolean enableColumnStatsCollecting() {
    return false;
  }
}
