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

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.runtime.CommonToken;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * A subclass of the {@link org.apache.hadoop.hive.ql.parse.SemanticAnalyzer} that just handles
 * update and delete statements. It works by rewriting the updates and deletes into insert
 * statements (since they are actually inserts) and then doing some patch up to make them work as
 * updates and deletes instead.
 */
public class UpdateDeleteSemanticAnalyzer extends RewriteSemanticAnalyzer {

  private Context.Operation operation = Context.Operation.OTHER;

  UpdateDeleteSemanticAnalyzer(QueryState queryState) throws SemanticException {
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
    case HiveParser.TOK_DELETE_FROM:
      operation = Context.Operation.DELETE;
      reparseAndSuperAnalyze(tree, table, tabNameNode);
      break;
    case HiveParser.TOK_UPDATE_TABLE:
      boolean nonNativeAcid = AcidUtils.isNonNativeAcidTable(table, true);
      if (nonNativeAcid) {
        throw new SemanticException(ErrorMsg.NON_NATIVE_ACID_UPDATE.getErrorCodedMsg());
      }
      operation = Context.Operation.UPDATE;
      reparseAndSuperAnalyze(tree, table, tabNameNode);
      break;
    default:
      throw new RuntimeException("Asked to parse token " + tree.getName() + " in " +
          "UpdateDeleteSemanticAnalyzer");
    }
  }

  /**
   * This supports update and delete statements
   * Rewrite the delete or update into an insert.  Crazy, but it works as deletes and update
   * actually are inserts into the delta file in Hive.  A delete
   * DELETE FROM _tablename_ [WHERE ...]
   * will be rewritten as
   * INSERT INTO TABLE _tablename_ [PARTITION (_partcols_)] SELECT ROW__ID[,
   * _partcols_] from _tablename_ SORT BY ROW__ID
   * An update
   * UPDATE _tablename_ SET x = _expr_ [WHERE...]
   * will be rewritten as
   * INSERT INTO TABLE _tablename_ [PARTITION (_partcols_)] SELECT _all_,
   * _partcols_from _tablename_ SORT BY ROW__ID
   * where _all_ is all the non-partition columns.  The expressions from the set clause will be
   * re-attached later.
   * The where clause will also be re-attached later.
   * The sort by clause is put in there so that records come out in the right order to enable
   * merge on read.
   */
  private void reparseAndSuperAnalyze(ASTNode tree, Table mTable, ASTNode tabNameNode) throws SemanticException {
    List<? extends Node> children = tree.getChildren();
    
    boolean shouldTruncate = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_OPTIMIZE_REPLACE_DELETE_WITH_TRUNCATE) 
      && children.size() == 1 && deleting();
    if (shouldTruncate) {
      StringBuilder rewrittenQueryStr = new StringBuilder("truncate ").append(getFullTableNameForSQL(tabNameNode));
      ReparseResult rr = parseRewrittenQuery(rewrittenQueryStr, ctx.getCmd());
      Context rewrittenCtx = rr.rewrittenCtx;
      ASTNode rewrittenTree = rr.rewrittenTree;

      BaseSemanticAnalyzer truncate = SemanticAnalyzerFactory.get(queryState, rewrittenTree);
      // Note: this will overwrite this.ctx with rewrittenCtx
      rewrittenCtx.setEnableUnparse(false);
      truncate.analyze(rewrittenTree, rewrittenCtx);
      
      rootTasks = truncate.getRootTasks();
      outputs = truncate.getOutputs();
      updateOutputs(mTable);
      return;
    }
    
    boolean shouldOverwrite = false;
    HiveStorageHandler storageHandler = mTable.getStorageHandler();
    if (storageHandler != null) {
      shouldOverwrite = storageHandler.shouldOverwrite(mTable, operation.name());
    }
    
    StringBuilder rewrittenQueryStr = new StringBuilder();
    if (shouldOverwrite) {
      rewrittenQueryStr.append("insert overwrite table ");
    } else {
      rewrittenQueryStr.append("insert into table ");
    }
    rewrittenQueryStr.append(getFullTableNameForSQL(tabNameNode));
    addPartitionColsToInsert(mTable.getPartCols(), rewrittenQueryStr);

    ColumnAppender columnAppender = getColumnAppender(null, DELETE_PREFIX);
    int columnOffset = columnAppender.getDeleteValues(operation).size();
    if (!shouldOverwrite) {
      rewrittenQueryStr.append(" select ");
      columnAppender.appendAcidSelectColumns(rewrittenQueryStr, operation);
      rewrittenQueryStr.setLength(rewrittenQueryStr.length() - 1);
    } else {
      rewrittenQueryStr.append(" select * ");
    }

    Map<Integer, ASTNode> setColExprs = null;
    Map<String, ASTNode> setCols = null;
    // Must be deterministic order set for consistent q-test output across Java versions
    Set<String> setRCols = new LinkedHashSet<String>();
    if (updating()) {
      // We won't write the set
      // expressions in the rewritten query.  We'll patch that up later.
      // The set list from update should be the second child (index 1)
      assert children.size() >= 2 : "Expected update token to have at least two children";
      ASTNode setClause = (ASTNode)children.get(1);
      setCols = collectSetColumnsAndExpressions(setClause, setRCols, mTable);
      setColExprs = new HashMap<>(setClause.getChildCount());

      List<FieldSchema> nonPartCols = mTable.getCols();
      for (int i = 0; i < nonPartCols.size(); i++) {
        rewrittenQueryStr.append(',');
        String name = nonPartCols.get(i).getName();
        ASTNode setCol = setCols.get(name);
        rewrittenQueryStr.append(HiveUtils.unparseIdentifier(name, this.conf));
        if (setCol != null) {
          // This is one of the columns we're setting, record it's position so we can come back
          // later and patch it up.
          // Add one to the index because the select has the ROW__ID as the first column.
          setColExprs.put(columnOffset + i, setCol);
        }
      }
    }

    rewrittenQueryStr.append(" from ");
    rewrittenQueryStr.append(getFullTableNameForSQL(tabNameNode));

    ASTNode where = null;
    int whereIndex = deleting() ? 1 : 2;
    if (children.size() > whereIndex) {
      where = (ASTNode)children.get(whereIndex);
      assert where.getToken().getType() == HiveParser.TOK_WHERE :
          "Expected where clause, but found " + where.getName();

      if (shouldOverwrite) {
        if (where.getChildCount() == 1) {

          // Add isNull check for the where clause condition, since null is treated as false in where condition and
          // not null also resolves to false, so we need to explicitly handle this case.
          ASTNode isNullFuncNodeExpr = new ASTNode(new CommonToken(HiveParser.TOK_FUNCTION, "TOK_FUNCTION"));
          isNullFuncNodeExpr.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, "isNull")));
          isNullFuncNodeExpr.addChild(where.getChild(0));

          ASTNode orNodeExpr = new ASTNode(new CommonToken(HiveParser.KW_OR, "OR"));
          orNodeExpr.addChild(isNullFuncNodeExpr);

          // Add the inverted where clause condition, since we want to hold the records which doesn't satisfy this
          // condition.
          ASTNode notNodeExpr = new ASTNode(new CommonToken(HiveParser.KW_NOT, "!"));
          notNodeExpr.addChild(where.getChild(0));
          orNodeExpr.addChild(notNodeExpr);
          where.setChild(0, orNodeExpr);
        } else if (where.getChildCount() > 1) {
          throw new SemanticException("Overwrite mode not supported with more than 1 children in where clause.");
        }
      }
    }

    if (!shouldOverwrite) {
      // Add a sort by clause so that the row ids come out in the correct order
      appendSortBy(rewrittenQueryStr, columnAppender.getSortKeys());
    }
    ReparseResult rr = parseRewrittenQuery(rewrittenQueryStr, ctx.getCmd());
    Context rewrittenCtx = rr.rewrittenCtx;
    ASTNode rewrittenTree = rr.rewrittenTree;

    ASTNode rewrittenInsert = (ASTNode)rewrittenTree.getChildren().get(1);
    assert rewrittenInsert.getToken().getType() == HiveParser.TOK_INSERT :
        "Expected TOK_INSERT as second child of TOK_QUERY but found " + rewrittenInsert.getName();

    if (updating()) {
      rewrittenCtx.setOperation(Context.Operation.UPDATE);
      rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.UPDATE);
    } else if (deleting()) {
      if (shouldOverwrite) {
        // We are now actually executing an Insert query, so set the modes accordingly.
        rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.INSERT);
      } else {
        rewrittenCtx.setOperation(Context.Operation.DELETE);
        rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.DELETE);
      }
    }

    if (where != null) {
      // The structure of the AST for the rewritten insert statement is:
      // TOK_QUERY -> TOK_FROM
      //          \-> TOK_INSERT -> TOK_INSERT_INTO
      //                        \-> TOK_SELECT
      //                        \-> TOK_SORTBY
      // Or
      // TOK_QUERY -> TOK_FROM
      //          \-> TOK_INSERT -> TOK_INSERT_INTO
      //                        \-> TOK_SELECT
      //
      // The following adds the TOK_WHERE and its subtree from the original query as a child of
      // TOK_INSERT, which is where it would have landed if it had been there originally in the
      // string.  We do it this way because it's easy then turning the original AST back into a
      // string and reparsing it.
      if (rewrittenInsert.getChildren().size() == 3) {
        // We have to move the SORT_BY over one, so grab it and then push it to the second slot,
        // and put the where in the first slot
        ASTNode sortBy = (ASTNode) rewrittenInsert.getChildren().get(2);
        assert sortBy.getToken().getType() == HiveParser.TOK_SORTBY :
            "Expected TOK_SORTBY to be third child of TOK_INSERT, but found " + sortBy.getName();
        rewrittenInsert.addChild(sortBy);
        rewrittenInsert.setChild(2, where);
      } else {
        ASTNode select = (ASTNode) rewrittenInsert.getChildren().get(1);
        assert select.getToken().getType() == HiveParser.TOK_SELECT :
            "Expected TOK_SELECT to be second child of TOK_INSERT, but found " + select.getName();
        rewrittenInsert.addChild(where);
      }
    }

    if (updating() && setColExprs != null) {
      patchProjectionForUpdate(rewrittenInsert, setColExprs);
    }

    // Note: this will overwrite this.ctx with rewrittenCtx
    rewrittenCtx.setEnableUnparse(false);
    analyzeRewrittenTree(rewrittenTree, rewrittenCtx);

    updateOutputs(mTable);


    if (updating()) {
      setUpAccessControlInfoForUpdate(mTable, setCols);

      // Add the setRCols to the input list
      for (String colName : setRCols) {
        if (columnAccessInfo != null) { //assuming this means we are not doing Auth
          columnAccessInfo.add(Table.getCompleteName(mTable.getDbName(), mTable.getTableName()),
              colName);
        }
      }
    }
  }

  private boolean updating() {
    return operation == Context.Operation.UPDATE;
  }
  private boolean deleting() {
    return operation == Context.Operation.DELETE;
  }

  @Override
  protected boolean enableColumnStatsCollecting() {
    return false;
  }
}
