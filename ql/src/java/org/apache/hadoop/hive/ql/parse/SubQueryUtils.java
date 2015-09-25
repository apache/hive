/**
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.parse.QBSubQuery.SubQueryType;
import org.apache.hadoop.hive.ql.parse.QBSubQuery.SubQueryTypeDef;

public class SubQueryUtils {

  static void extractConjuncts(ASTNode node, List<ASTNode> conjuncts) {
    if (node.getType() != HiveParser.KW_AND ) {
      conjuncts.add(node);
      return;
    }
    extractConjuncts((ASTNode)node.getChild(0), conjuncts);
    extractConjuncts((ASTNode)node.getChild(1), conjuncts);
  }

  /*
   * Remove the SubQuery from the Where Clause Tree.
   * return the remaining WhereClause.
   */
  static ASTNode rewriteParentQueryWhere(ASTNode whereCond, ASTNode subQuery)
      throws SemanticException {
    ParentQueryWhereClauseRewrite rewrite =
        new ParentQueryWhereClauseRewrite(whereCond, subQuery);
    return rewrite.remove();
  }

  static ASTNode constructTrueCond() {
    ASTNode eq = (ASTNode) ParseDriver.adaptor.create(HiveParser.EQUAL, "=");
    ASTNode lhs = (ASTNode) ParseDriver.adaptor.create(HiveParser.Number, "1");
    ASTNode rhs = (ASTNode) ParseDriver.adaptor.create(HiveParser.Number, "1");
    ParseDriver.adaptor.addChild(eq, lhs);
    ParseDriver.adaptor.addChild(eq, rhs);
    return eq;
  }

  static ASTNode andAST(ASTNode left, ASTNode right) {
    if ( left == null ) {
      return right;
    } else if ( right == null ) {
      return left;
    } else {
      Object o = ParseDriver.adaptor.create(HiveParser.KW_AND, "AND");
      ParseDriver.adaptor.addChild(o, left);
      ParseDriver.adaptor.addChild(o, right);
      return (ASTNode) o;
    }
  }

  static ASTNode orAST(ASTNode left, ASTNode right) {
    if ( left == null ) {
      return right;
    } else if ( right == null ) {
      return left;
    } else {
      Object o = ParseDriver.adaptor.create(HiveParser.KW_OR, "OR");
      ParseDriver.adaptor.addChild(o, left);
      ParseDriver.adaptor.addChild(o, right);
      return (ASTNode) o;
    }
  }

  static ASTNode isNull(ASTNode expr) {
    ASTNode node = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_FUNCTION, "TOK_FUNCTION");
    node.addChild((ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_ISNULL, "TOK_ISNULL"));
    node.addChild(expr);
    return node;
  }


  /*
   * Check that SubQuery is a top level conjuncts.
   * Remove it from the Where Clause AST.
   */
  static class ParentQueryWhereClauseRewrite {
    ASTNode root;
    ASTNode subQuery;

    ParentQueryWhereClauseRewrite(ASTNode root, ASTNode subQuery) {
      this.root = root;
      this.subQuery = subQuery;
    }

    ASTNode getParentInWhereClause(ASTNode node) {
      if (node == null || node == root) {
        return null;
      }
      return (ASTNode) node.getParent();
    }

    boolean removeSubQuery(ASTNode node) {
      if (node.getType() == HiveParser.KW_AND) {
        boolean r = removeSubQuery((ASTNode) node.getChild(0));
        if (!r) {
          r = removeSubQuery((ASTNode) node.getChild(1));
        }
        return r;
      } else if (node.getType() == HiveParser.KW_NOT) {
        ASTNode child = (ASTNode) node.getChild(0);
        if (child == subQuery) {
          ASTNode sqOpType = (ASTNode) subQuery.getChild(0).getChild(0);
          if (sqOpType.getType() == HiveParser.KW_EXISTS) {
            sqOpType.getToken().setType(HiveParser.TOK_SUBQUERY_OP_NOTEXISTS);
          } else {
            sqOpType.getToken().setType(HiveParser.TOK_SUBQUERY_OP_NOTIN);
          }
          ASTNode parent = getParentInWhereClause(node);
          if (parent == null) {
            root = subQuery;
          } else {
            int nodeIdx = node.getChildIndex();
            parent.setChild(nodeIdx, subQuery);
          }
          return removeSubQuery(subQuery);

        }
        return false;
      } else if (node == subQuery) {
        ASTNode parent = getParentInWhereClause(node);
        ASTNode gParent = getParentInWhereClause(parent);
        ASTNode sibling = null;

        if (parent != null) {
          if (subQuery.getChildIndex() == 0) {
            sibling = (ASTNode) parent.getChild(1);
          } else {
            sibling = (ASTNode) parent.getChild(0);
          }
        }

        /*
         * SubQuery was only condition in where clause
         */
        if (sibling == null) {
          root = constructTrueCond();
        } // SubQuery was just one conjunct
        else if (gParent == null) {
          root = sibling;
        } else {
          // otherwise replace parent by sibling.
          int pIdx = parent.getChildIndex();
          gParent.setChild(pIdx, sibling);
        }
        return true;
      } else {
        return false;
      }
    }

    ASTNode remove() throws SemanticException {
      boolean r = removeSubQuery(root);
      if (r) {
        return root;
      }
      /*
       *  Restriction.7.h :: SubQuery predicates can appear only as top level conjuncts.
       */
      throw new SemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
          subQuery, "Only SubQuery expressions that are top level conjuncts are allowed"));
    }
  }

  static List<ASTNode> findSubQueries(ASTNode node)
      throws SemanticException {
    List<ASTNode> subQueries = new ArrayList<ASTNode>();
    findSubQueries(node, subQueries);
    return subQueries;
  }

  private static void findSubQueries(ASTNode node, List<ASTNode> subQueries) {
    switch(node.getType()) {
    case HiveParser.TOK_SUBQUERY_EXPR:
      subQueries.add(node);
      break;
    default:
      int childCount = node.getChildCount();
      for(int i=0; i < childCount; i++) {
        findSubQueries((ASTNode) node.getChild(i), subQueries);
      }
    }
  }

  static QBSubQuery buildSubQuery(String outerQueryId,
      int sqIdx,
      ASTNode sqAST,
      ASTNode originalSQAST,
      Context ctx) throws SemanticException {
    ASTNode sqOp = (ASTNode) sqAST.getChild(0);
    ASTNode sq = (ASTNode) sqAST.getChild(1);
    ASTNode outerQueryExpr = (ASTNode) sqAST.getChild(2);

    /*
     * Restriction.8.m :: We allow only 1 SubQuery expression per Query.
     */
    if (outerQueryExpr != null && outerQueryExpr.getType() == HiveParser.TOK_SUBQUERY_EXPR ) {

      throw new SemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
          originalSQAST.getChild(1), "Only 1 SubQuery expression is supported."));
    }

   return new QBSubQuery(outerQueryId, sqIdx, sq, outerQueryExpr,
       buildSQOperator(sqOp),
       originalSQAST,
       ctx);
  }

  static SubQueryTypeDef buildSQOperator(ASTNode astSQOp) throws SemanticException {
    ASTNode opAST = (ASTNode) astSQOp.getChild(0);
    SubQueryType type = SubQueryType.get(opAST);
    return new SubQueryTypeDef(opAST, type);
  }

  /*
   * is this expr a UDAF invocation; does it imply windowing
   * @return
   * 0 if implies neither
   * 1 if implies aggregation
   * 2 if implies windowing
   */
  static int checkAggOrWindowing(ASTNode expressionTree) throws SemanticException {
    int exprTokenType = expressionTree.getToken().getType();
    if (exprTokenType == HiveParser.TOK_FUNCTION
        || exprTokenType == HiveParser.TOK_FUNCTIONDI
        || exprTokenType == HiveParser.TOK_FUNCTIONSTAR) {
      assert (expressionTree.getChildCount() != 0);
      if (expressionTree.getChild(expressionTree.getChildCount()-1).getType()
          == HiveParser.TOK_WINDOWSPEC) {
        return 2;
      }
      if (expressionTree.getChild(0).getType() == HiveParser.Identifier) {
        String functionName = SemanticAnalyzer.unescapeIdentifier(expressionTree.getChild(0)
            .getText());
        if (FunctionRegistry.getGenericUDAFResolver(functionName) != null) {
          return 1;
        }
      }
    }
    int r = 0;
    for (int i = 0; i < expressionTree.getChildCount(); i++) {
      int c = checkAggOrWindowing((ASTNode) expressionTree.getChild(i));
      r = Math.max(r, c);
    }
    return r;
  }

  static List<String> getTableAliasesInSubQuery(ASTNode fromClause) {
    List<String> aliases = new ArrayList<String>();
    getTableAliasesInSubQuery((ASTNode) fromClause.getChild(0), aliases);
    return aliases;
  }

  private static void getTableAliasesInSubQuery(ASTNode joinNode, List<String> aliases) {

    if ((joinNode.getToken().getType() == HiveParser.TOK_TABREF)
        || (joinNode.getToken().getType() == HiveParser.TOK_SUBQUERY)
        || (joinNode.getToken().getType() == HiveParser.TOK_PTBLFUNCTION)) {
      String tableName = SemanticAnalyzer.getUnescapedUnqualifiedTableName((ASTNode) joinNode.getChild(0))
          .toLowerCase();
      String alias = joinNode.getChildCount() == 1 ? tableName
          : SemanticAnalyzer.unescapeIdentifier(joinNode.getChild(joinNode.getChildCount() - 1)
          .getText().toLowerCase());
      alias = (joinNode.getToken().getType() == HiveParser.TOK_PTBLFUNCTION) ?
          SemanticAnalyzer.unescapeIdentifier(joinNode.getChild(1).getText().toLowerCase()) :
            alias;
      aliases.add(alias);
    } else {
      ASTNode left = (ASTNode) joinNode.getChild(0);
      ASTNode right = (ASTNode) joinNode.getChild(1);
      getTableAliasesInSubQuery(left, aliases);
      getTableAliasesInSubQuery(right, aliases);
    }
  }
  
  static ASTNode hasUnQualifiedColumnReferences(ASTNode ast) {
    int type = ast.getType();
    if ( type == HiveParser.DOT ) {
      return null;
    }
    else if ( type == HiveParser.TOK_TABLE_OR_COL ) {
      return ast;
    }
    
    for(int i=0; i < ast.getChildCount(); i++ ) {
      ASTNode c = hasUnQualifiedColumnReferences((ASTNode) ast.getChild(i));
      if ( c != null ) {
        return c;
      }
    }
    return null;
  }

  static ASTNode setQualifiedColumnReferences(ASTNode ast, String tableAlias) {
    int type = ast.getType();
    if (type == HiveParser.DOT) {
      return ast;
    }
    if (type == HiveParser.TOK_TABLE_OR_COL) {
      if (tableAlias == null) {
        return null;
      }
      String colName = SemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getText());
      return SubQueryUtils.createColRefAST(tableAlias, colName);
    }

    for (int i = 0; i < ast.getChildCount(); i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      ASTNode c = setQualifiedColumnReferences(child, tableAlias);
      if (c == null) {
        return null;
      }
      if (c != child) {
        ast.setChild(i, c);
      }
    }
    return ast;
  }
  
  static ASTNode subQueryWhere(ASTNode insertClause) {
    if (insertClause.getChildCount() > 2 &&
        insertClause.getChild(2).getType() == HiveParser.TOK_WHERE ) {
      return (ASTNode) insertClause.getChild(2);
    }
    return null;
  }

  /*
   * construct the ASTNode for the SQ column that will join with the OuterQuery Expression.
   * So for 'select ... from R1 where A in (select B from R2...)'
   * this will build (= outerQueryExpr 'ast returned by call to buildSQJoinExpr')
   */
  static ASTNode buildOuterQryToSQJoinCond(ASTNode outerQueryExpr,
      String sqAlias,
      RowResolver sqRR) {
    ASTNode node = (ASTNode) ParseDriver.adaptor.create(HiveParser.EQUAL, "=");
    node.addChild(outerQueryExpr);
    node.addChild(buildSQJoinExpr(sqAlias, sqRR));
    return node;
  }

  /*
   * construct the ASTNode for the SQ column that will join with the OuterQuery Expression.
   * So for 'select ... from R1 where A in (select B from R2...)'
   * this will build (. (TOK_TABLE_OR_COL Identifier[SQ_1]) Identifier[B])
   * where 'SQ_1' is the alias generated for the SubQuery.
   */
  static ASTNode buildSQJoinExpr(String sqAlias, RowResolver sqRR) {

    List<ColumnInfo> signature = sqRR.getRowSchema().getSignature();
    ColumnInfo joinColumn = signature.get(0);
    String[] joinColName = sqRR.reverseLookup(joinColumn.getInternalName());
    return createColRefAST(sqAlias, joinColName[1]);
  }

  static ASTNode buildOuterJoinPostCond(String sqAlias, RowResolver sqRR) {
    return isNull(buildSQJoinExpr(sqAlias, sqRR));
  }

  @SuppressWarnings("rawtypes")
  static String getAlias(Operator o, Map<String, Operator> aliasToOpInfo) {
    for(Map.Entry<String, Operator> e : aliasToOpInfo.entrySet()) {
      if ( e.getValue() == o) {
        return e.getKey();
      }
    }
    return null;
  }

  static ASTNode createColRefAST(String tabAlias, String colName) {
    ASTNode dot = (ASTNode) ParseDriver.adaptor.create(HiveParser.DOT, ".");
    ASTNode tabAst = createTabRefAST(tabAlias);
    ASTNode colAst = (ASTNode) ParseDriver.adaptor.create(HiveParser.Identifier, colName);
    dot.addChild(tabAst);
    dot.addChild(colAst);
    return dot;
  }

  static ASTNode createAliasAST(String colName) {
    return (ASTNode) ParseDriver.adaptor.create(HiveParser.Identifier, colName);
  }

  static ASTNode createTabRefAST(String tabAlias) {
    ASTNode tabAst = (ASTNode)
        ParseDriver.adaptor.create(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL");
    ASTNode tabName = (ASTNode) ParseDriver.adaptor.create(HiveParser.Identifier, tabAlias);
    tabAst.addChild(tabName);
    return tabAst;
  }

  static ASTNode buildSelectExpr(ASTNode expression) {
    ASTNode selAst = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_SELEXPR, "TOK_SELEXPR");
    selAst.addChild(expression);
    return selAst;
  }

  static ASTNode buildGroupBy() {
    ASTNode gBy = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_GROUPBY, "TOK_GROUPBY");
    return gBy;
  }

  static ASTNode createSelectItem(ASTNode expr, ASTNode alias) {
    ASTNode selectItem = (ASTNode)
        ParseDriver.adaptor.create(HiveParser.TOK_SELEXPR, "TOK_SELEXPR");
    selectItem.addChild(expr);
    selectItem.addChild(alias);
    return selectItem;
  }

  static ASTNode alterCorrelatedPredicate(ASTNode correlatedExpr, ASTNode sqAlias, boolean left) {
    if ( left ) {
      correlatedExpr.setChild(0, sqAlias);
    } else {
      correlatedExpr.setChild(1, sqAlias);
    }
    return correlatedExpr;
  }

  static void addGroupExpressionToFront(ASTNode gBy, ASTNode expr) {
    ASTNode grpExpr = (ASTNode)
        ParseDriver.adaptor.create(HiveParser.TOK_GROUPING_SETS_EXPRESSION,
            "TOK_GROUPING_SETS_EXPRESSION");
    grpExpr.addChild(expr);
    List<ASTNode> newChildren = new ArrayList<ASTNode>();
    newChildren.add(expr);
    int i = gBy.getChildCount() - 1;
    while ( i >= 0 ) {
      newChildren.add((ASTNode) gBy.deleteChild(i));
      i--;
    }
    for(ASTNode child : newChildren ) {
      gBy.addChild(child);
    }
  }

  static ASTNode buildPostJoinNullCheck(List<ASTNode> subQueryJoinAliasExprs) {
    ASTNode check = null;
    for(ASTNode expr : subQueryJoinAliasExprs) {
      check = orAST(check, isNull(expr));
    }
    return check;
  }

  /*
   * Set of functions to create the Null Check Query for Not-In SubQuery predicates.
   * For a SubQuery predicate like:
   *   a not in (select b from R2 where R2.y > 5)
   * The Not In null check query is:
   *   (select count(*) as c from R2 where R2.y > 5 and b is null)
   * This Subquery is joined with the Outer Query plan on the join condition 'c = 0'.
   * The join condition ensures that in case there are null values in the joining column
   * the Query returns no rows.
   * 
   * The AST tree for this is:
   * 
   * ^(TOK_QUERY
   *    ^(TOK FROM
   *        ^(TOK_SUBQUERY
   *            {the input SubQuery, with correlation removed}
   *            subQueryAlias 
   *          ) 
   *     )
   *     ^(TOK_INSERT
   *         ^(TOK_DESTINATION...)
   *         ^(TOK_SELECT
   *             ^(TOK_SELECTEXPR {ast tree for count *}
   *          )
   *          ^(TOK_WHERE
   *             {is null check for joining column} 
   *           )
   *      )
   * )
   */  
  static ASTNode buildNotInNullCheckQuery(ASTNode subQueryAST, 
      String subQueryAlias, 
      String cntAlias,
      List<ASTNode> corrExprs,
      RowResolver sqRR) {
    
    subQueryAST = (ASTNode) ParseDriver.adaptor.dupTree(subQueryAST);
    ASTNode qry = (ASTNode) 
        ParseDriver.adaptor.create(HiveParser.TOK_QUERY, "TOK_QUERY");
    
    qry.addChild(buildNotInNullCheckFrom(subQueryAST, subQueryAlias));
    ASTNode insertAST = buildNotInNullCheckInsert();
    qry.addChild(insertAST);
    insertAST.addChild(buildNotInNullCheckSelect(cntAlias));
    insertAST.addChild(buildNotInNullCheckWhere(subQueryAST, 
        subQueryAlias, corrExprs, sqRR));
    
    return qry;
  }
  
  /*
   * build:
   *    ^(TOK FROM
   *        ^(TOK_SUBQUERY
   *            {the input SubQuery, with correlation removed}
   *            subQueryAlias 
   *          ) 
   *     )

   */
  static ASTNode buildNotInNullCheckFrom(ASTNode subQueryAST, String subQueryAlias) {
    ASTNode from = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_FROM, "TOK_FROM");
    ASTNode sqExpr = (ASTNode) 
        ParseDriver.adaptor.create(HiveParser.TOK_SUBQUERY, "TOK_SUBQUERY");
    sqExpr.addChild(subQueryAST);
    sqExpr.addChild(createAliasAST(subQueryAlias));
    from.addChild(sqExpr);
    return from;
  }
  
  /*
   * build
   *     ^(TOK_INSERT
   *         ^(TOK_DESTINATION...)
   *      )
   */
  static ASTNode buildNotInNullCheckInsert() {
    ASTNode insert = (ASTNode) 
        ParseDriver.adaptor.create(HiveParser.TOK_INSERT, "TOK_INSERT");
    ASTNode dest = (ASTNode) 
        ParseDriver.adaptor.create(HiveParser.TOK_DESTINATION, "TOK_DESTINATION");
    ASTNode dir = (ASTNode) 
        ParseDriver.adaptor.create(HiveParser.TOK_DIR, "TOK_DIR");
    ASTNode tfile = (ASTNode) 
        ParseDriver.adaptor.create(HiveParser.TOK_TMP_FILE, "TOK_TMP_FILE");
    insert.addChild(dest);
    dest.addChild(dir);
    dir.addChild(tfile);
    
    return insert;
  }
  
  /*
   * build:
   *         ^(TOK_SELECT
   *             ^(TOK_SELECTEXPR {ast tree for count *}
   *          )
   */
  static ASTNode buildNotInNullCheckSelect(String cntAlias) {
    ASTNode select = (ASTNode) 
        ParseDriver.adaptor.create(HiveParser.TOK_SELECT, "TOK_SELECT");
    ASTNode selectExpr = (ASTNode) 
        ParseDriver.adaptor.create(HiveParser.TOK_SELEXPR, "TOK_SELEXPR");
    ASTNode countStar = (ASTNode) 
        ParseDriver.adaptor.create(HiveParser.TOK_FUNCTIONSTAR, "TOK_FUNCTIONSTAR");
    ASTNode alias = (createAliasAST(cntAlias));
    
    countStar.addChild((ASTNode) ParseDriver.adaptor.create(HiveParser.Identifier, "count"));
    select.addChild(selectExpr);
    selectExpr.addChild(countStar);
    selectExpr.addChild(alias);
    
    return select;
  }
  
  /*
   * build:
   *          ^(TOK_WHERE
   *             {is null check for joining column} 
   *           )
   */
  static ASTNode buildNotInNullCheckWhere(ASTNode subQueryAST, 
      String sqAlias, 
      List<ASTNode> corrExprs,
      RowResolver sqRR) {
    
    ASTNode sqSelect = (ASTNode) subQueryAST.getChild(1).getChild(1);
    ASTNode selExpr = (ASTNode) sqSelect.getChild(0);
    String colAlias = null;
    
    if ( selExpr.getChildCount() == 2 ) {
      colAlias = selExpr.getChild(1).getText();
    } else if (selExpr.getChild(0).getType() != HiveParser.TOK_ALLCOLREF) {
      colAlias = sqAlias + "_ninc_col0";
      selExpr.addChild((ASTNode)ParseDriver.adaptor.create(HiveParser.Identifier, colAlias));
    } else {
      List<ColumnInfo> signature = sqRR.getRowSchema().getSignature();
      ColumnInfo joinColumn = signature.get(0);
      String[] joinColName = sqRR.reverseLookup(joinColumn.getInternalName());
      colAlias = joinColName[1];
    }
    
    ASTNode searchCond = isNull(createColRefAST(sqAlias, colAlias));
    
    for(ASTNode e : corrExprs ) {
      ASTNode p = (ASTNode) ParseDriver.adaptor.dupTree(e);
      p = isNull(p);      
      searchCond = orAST(searchCond, p);      
    }
    
    ASTNode where = (ASTNode) ParseDriver.adaptor.create(HiveParser.TOK_WHERE, "TOK_WHERE");
    where.addChild(searchCond);
    return where;
  }
  
  static ASTNode buildNotInNullJoinCond(String subqueryAlias, String cntAlias) {
    
    ASTNode eq = (ASTNode) 
        ParseDriver.adaptor.create(HiveParser.EQUAL, "=");
    
    eq.addChild(createColRefAST(subqueryAlias, cntAlias));
    eq.addChild((ASTNode) 
        ParseDriver.adaptor.create(HiveParser.Number, "0"));
    
    return eq;
  }
  
  public static interface ISubQueryJoinInfo {
    public String getAlias();
    public JoinType getJoinType();
    public ASTNode getJoinConditionAST();
    public QBSubQuery getSubQuery();
    public ASTNode getSubQueryAST();
    public String getOuterQueryId();
  };

    
  /*
   * Using CommonTreeAdaptor because the Adaptor in ParseDriver doesn't carry
   * the token indexes when duplicating a Tree.
   */
  static final CommonTreeAdaptor adaptor = new CommonTreeAdaptor();
}




