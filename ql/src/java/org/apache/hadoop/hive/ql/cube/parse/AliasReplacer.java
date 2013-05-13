package org.apache.hadoop.hive.ql.cube.parse;

import java.util.Map;

import org.antlr.runtime.CommonToken;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class AliasReplacer implements ContextRewriter {

  public AliasReplacer(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql)  throws SemanticException {

    // Rewrite the all the columns in the query with table alias prefixed.
    // If col1 of table tab1 is accessed, it would be changed as tab1.col1.
    // If tab1 is already aliased say with t1, col1 is changed as t1.col1
    // replace the columns in select, groupby, having, orderby by
    // prepending the table alias to the col
    //sample select trees
    // 1: (TOK_SELECT (TOK_SELEXPR (TOK_TABLE_OR_COL key))
    // (TOK_SELEXPR (TOK_FUNCTION count (TOK_TABLE_OR_COL value))))
    // 2: (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) key))
    // (TOK_SELEXPR (TOK_FUNCTION count (. (TOK_TABLE_OR_COL src) value))))
    // 3: (TOK_SELECT (TOK_SELEXPR (. (TOK_TABLE_OR_COL src) key) srckey))))
    Map<String, String> colToTableAlias = cubeql.getColumnsToTableAlias();
    if (colToTableAlias == null) {
      return;
    }

    ASTNode selectAST = cubeql.getSelectAST();
    replaceAliases(selectAST, 0, colToTableAlias);
    String rewritSelect = HQLParser.getString(selectAST);
    if (StringUtils.isNotBlank(rewritSelect)) {
      cubeql.setSelectTree(rewritSelect);
    }

    ASTNode havingAST = cubeql.getHavingAST();
    replaceAliases(havingAST, 0, colToTableAlias);
    String rewritHaving = HQLParser.getString(havingAST);
    if (StringUtils.isNotBlank(rewritHaving)) {
      cubeql.setHavingTree(rewritHaving);
    }

    ASTNode orderByAST = cubeql.getOrderByAST();
    replaceAliases(orderByAST, 0, colToTableAlias);
    String rewritOrderby = HQLParser.getString(orderByAST);
    if (StringUtils.isNotBlank(rewritOrderby)) {
      cubeql.setOrderByTree(rewritOrderby);
    }

    ASTNode groupByAST = cubeql.getGroupByAST();
    replaceAliases(groupByAST, 0, colToTableAlias);
    String rewritGroupby = HQLParser.getString(groupByAST);
    if (StringUtils.isNotBlank(rewritGroupby)) {
      cubeql.setGroupByTree(rewritGroupby);
    }

    ASTNode whereAST = cubeql.getWhereAST();
    replaceAliases(whereAST, 0, colToTableAlias);
    String rewritWhere = HQLParser.getString(whereAST);
    if (StringUtils.isNotBlank(rewritWhere)) {
      cubeql.setWhereTree(rewritWhere);
    }
  }

  private void replaceAliases(ASTNode node, int nodePos, Map<String, String> colToTableAlias) {
    if (node == null) {
      return;
    }

    int nodeType = node.getToken().getType();
    if (nodeType == HiveParser.TOK_TABLE_OR_COL || nodeType == HiveParser.DOT) {
      String colName = HQLParser.getColName(node);
      String newAlias = colToTableAlias.get(colName.toLowerCase());

      if (StringUtils.isBlank(newAlias)) {
        return;
      }

      if (nodeType == HiveParser.DOT) {
        // No need to create a new node, just replace the table name ident
        ASTNode aliasNode = (ASTNode) node.getChild(0);
        ASTNode newAliasIdent = new ASTNode(new CommonToken(HiveParser.Identifier, newAlias));
        aliasNode.setChild(0, newAliasIdent);
        newAliasIdent.setParent(aliasNode);
      } else {
        // Just a column ref, we need to make it alias.col
        // '.' will become the parent node
        ASTNode dot = new ASTNode(new CommonToken(HiveParser.DOT, "."));
        ASTNode aliasIdentNode = new ASTNode(new CommonToken(HiveParser.Identifier, newAlias));
        ASTNode tabRefNode = new ASTNode(new CommonToken(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL"));

        tabRefNode.addChild(aliasIdentNode);
        aliasIdentNode.setParent(tabRefNode);
        dot.addChild(tabRefNode);
        tabRefNode.setParent(dot);

        ASTNode colIdentNode = new ASTNode(new CommonToken(HiveParser.Identifier, colName));

        dot.addChild(colIdentNode);

        ASTNode parent = (ASTNode) node.getParent();

        parent.setChild(nodePos, dot);
      }
    } else {
      // recurse down
      for (int i = 0; i < node.getChildCount(); i++) {
        ASTNode child = (ASTNode) node.getChild(i);
        replaceAliases(child, i, colToTableAlias);
      }
    }
  }

}
