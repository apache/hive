package org.apache.hadoop.hive.ql.cube.parse;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class AliasReplacer implements ContextRewriter {

  public AliasReplacer(Configuration conf) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) {

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

    Map<String, List<String>> tblToColumns = cubeql.getTblToColumns();
    for (Map.Entry<String, List<String>> entry : tblToColumns.entrySet()) {
      System.out.println("Table: " + entry.getKey() + "Columns: " + entry.getValue());
    }
    Map<String, String> colToTableAlias = cubeql.getColumnsToTableAlias();
    if (colToTableAlias == null) {
      return;
    }
    String selectTree = cubeql.getSelectTree();
    String whereTree = cubeql.getWhereTree();
    String havingTree = cubeql.getHavingTree();
    String orderByTree = cubeql.getOrderByTree();
    String groupByTree = cubeql.getGroupByTree();

    if (selectTree != null) {
      cubeql.setSelectTree(replaceCols(selectTree, colToTableAlias));
    }

    if (whereTree != null) {
      cubeql.setWhereTree(replaceCols(whereTree, colToTableAlias));
    }

    if (havingTree != null) {
      cubeql.setHavingTree(replaceCols(havingTree, colToTableAlias));
    }

    if (orderByTree != null) {
      cubeql.setOrderByTree(replaceCols(orderByTree, colToTableAlias));
    }

    if (groupByTree != null) {
      cubeql.setGroupByTree(replaceCols(groupByTree, colToTableAlias));
    }

  }

  private String replaceCols(String tree, Map<String, String> colToTableAlias) {
    String srcTree = new String(tree);
    for (Map.Entry<String, String> entry : colToTableAlias.entrySet()) {
      srcTree = srcTree.replaceAll(entry.getKey(),
          entry.getValue() + "." + entry.getKey());
    }
    return srcTree;
  }
}
