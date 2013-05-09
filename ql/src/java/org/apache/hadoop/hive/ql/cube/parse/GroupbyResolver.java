package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class GroupbyResolver implements ContextRewriter {

  public GroupbyResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    // Process Aggregations by making sure that all group by keys are projected;
    // and all projection fields are added to group by keylist;
    String groupByTree = cubeql.getGroupByTree();
    String selectTree = cubeql.getSelectTree();
    List<String> selectExprs = new ArrayList<String>();
    String[] sel = selectTree.split(",");
    for (String s : sel) {
      selectExprs.add(s.trim().toLowerCase());
    }
    List<String> groupByExprs = new ArrayList<String>();
    if (groupByTree != null) {
      String[] gby = groupByTree.split(",");
      for (String g : gby) {
        groupByExprs.add(g.trim().toLowerCase());
      }
    }
    // each selected column, if it is not a cube measure, and does not have
    // aggregation on the column, then it is added to group by columns.
    for (String expr : selectExprs) {
      if (cubeql.hasAggregates()) {
        String alias = cubeql.getAlias(expr);
        if (alias != null) {
          expr = expr.substring(0, (expr.length() - alias.length())).trim();
        }
        if (!groupByExprs.contains(expr)) {
          if (!cubeql.isAggregateExpr(expr)) {
            String groupbyExpr = expr;
            if (groupByTree != null) {
              groupByTree += ", ";
              groupByTree += groupbyExpr;
            } else {
              groupByTree = new String();
              groupByTree += groupbyExpr;
            }
          }
        }
      }
    }
    if (groupByTree != null) {
      cubeql.setGroupByTree(groupByTree);
    }

    for (String expr : groupByExprs) {
      if (!selectExprs.contains(expr)) {
        selectTree = expr + ", " + selectTree;
      }
    }
    cubeql.setSelectTree(selectTree);
  }

}
