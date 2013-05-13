package org.apache.hadoop.hive.ql.cube.parse;

import static org.apache.hadoop.hive.ql.parse.HiveParser.Identifier;
import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_TABLE_OR_COL;

import org.antlr.runtime.CommonToken;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMeasure;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * <p>
 * Replace select and having columns with default aggregate functions on them,
 * if default aggregate is defined and if there isn't already an aggregate
 * function specified on the columns.
 * </p>
 *
 * <p>
 * Expressions which already contain aggregate sub-expressions will not be
 * changed.
 * </p>
 *
 * <p>
 * At this point it's assumed that aliases have been added to all columns.
 * </p>
 */
public class AggregateResolver implements ContextRewriter {
  public static final Log LOG = LogFactory.getLog(
      AggregateResolver.class.getName());

  private final Configuration conf;

  public AggregateResolver(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    if (cubeql.getCube() == null) {
      return;
    }

    validateAggregates(cubeql, cubeql.getSelectAST(), false, false, false);
    validateAggregates(cubeql, cubeql.getHavingAST(), false, false, false);
    String rewritSelect = resolveForSelect(cubeql, cubeql.getSelectTree());
    cubeql.setSelectTree(rewritSelect);

    String rewritHaving = resolveForHaving(cubeql);
    if (StringUtils.isNotBlank(rewritHaving)) {
      cubeql.setHavingTree(rewritHaving);
    }
  }

  private void validateAggregates(CubeQueryContext cubeql, ASTNode node,
      boolean insideAggregate,
      boolean insideArithExpr, boolean insideNonAggrFn)
          throws SemanticException {
    if (node == null) {
      return;
    }

    int nodeType = node.getToken().getType();
    if (nodeType == HiveParser.TOK_TABLE_OR_COL || nodeType == HiveParser.DOT) {
      // Found a column ref. If this is a measure, it should be inside an
      // aggregate if its part of
      // an arithmetic expression or an argument of a non-aggregate function
      String msrname = getColName(node);
      if (cubeql.isCubeMeasure(msrname) &&
          !insideAggregate
          && (insideArithExpr || insideNonAggrFn)) {
        throw new SemanticException("Not inside aggregate " + msrname);
      }
    } else if (HQLParser.isArithmeticOp(nodeType)) {
      // Allowed - sum ( msr1 * msr2 + msr3)
      // Not allowed - msr1 + msr2 * msr3 <- Not inside aggregate
      // Not allowed - sum(msr1) + msr2 <- Aggregate only on one measure
      // count of measures within aggregates must be equal to count of measures
      // if both counts are equal and zero, then this node should be inside
      // aggregate
      int measuresInAggregates = countMeasuresInAggregates(cubeql, node, false);
      int measuresInTree = countMeasures(cubeql, node);

      if (measuresInAggregates == measuresInTree) {
        if (measuresInAggregates == 0 && !insideAggregate) {
          // (msr1 + msr2)
          throw new SemanticException("Invalid projection expression: "
              + HQLParser.getString(node));
        } else if (insideAggregate) {
          // sum(sum(msr1) + sum(msr2))
          throw new SemanticException("Invalid projection expression: "
              + HQLParser.getString(node));
        }
      } else {
        throw new SemanticException("Invalid projection expression: "
            + HQLParser.getString(node));
      }
    } else {
      boolean isArithmetic = HQLParser.isArithmeticOp(nodeType);
      boolean isAggregate = isAggregateAST(node);
      boolean isNonAggrFn = nodeType == HiveParser.TOK_FUNCTION && !isAggregate;
      for (int i = 0; i < node.getChildCount(); i++) {
        validateAggregates(cubeql, (ASTNode) node.getChild(i), isAggregate,
            isArithmetic, isNonAggrFn);
      }
    }
  }

  private int countMeasures(CubeQueryContext cubeql, ASTNode node) {
    int nodeType = node.getToken().getType();
    if (nodeType == HiveParser.TOK_TABLE_OR_COL || nodeType == HiveParser.DOT) {
      String msrname = getColName(node);
      if (cubeql.isCubeMeasure(msrname)) {
        return 1;
      } else {
        return 0;
      }
    } else {
      int count = 0;
      for (int i = 0; i < node.getChildCount(); i++) {
        count += countMeasures(cubeql, (ASTNode) node.getChild(i));
      }
      return count;
    }
  }

  private int countMeasuresInAggregates(CubeQueryContext cubeql, ASTNode node,
      boolean inAggregate) {
    int nodeType = node.getToken().getType();
    if (nodeType == HiveParser.TOK_TABLE_OR_COL || nodeType == HiveParser.DOT) {
      String msrname = getColName(node);
      if (cubeql.isCubeMeasure(msrname) && inAggregate) {
        return 1;
      } else {
        return 0;
      }
    } else {
      int count = 0;
      for (int i = 0; i < node.getChildCount(); i++) {
        boolean isAggr = isAggregateAST((ASTNode) node.getChild(i));
        count += countMeasuresInAggregates(cubeql, (ASTNode) node.getChild(i),
            isAggr);
      }
      return count;
    }
  }

  private String resolveForSelect(CubeQueryContext cubeql, String exprTree)
      throws SemanticException {
    // Aggregate resolver needs cube to be resolved first
    assert cubeql.getCube() != null;

    if (StringUtils.isBlank(exprTree)) {
      return "";
    }

    String exprTokens[] = StringUtils.split(exprTree, ",");
    for (int i = 0; i < exprTokens.length; i++) {
      String token = exprTokens[i].trim();
      String tokenAlias = cubeql.getAlias(token);
      boolean hasAlias = false;
      if (StringUtils.isNotBlank(tokenAlias)) {
        token = token.substring(0, exprTree.lastIndexOf(tokenAlias)).trim();
        hasAlias = true;
      }

      if (!cubeql.isAggregateExpr(token)) {
        if (cubeql.isCubeMeasure(token)) {
          // Take care of brackets added around col names
          // in HQLParsrer.getString
          if (token.startsWith("(") && token.endsWith(")")
              && token.length() > 2) {
            token = token.substring(1, token.length() - 1);
          }

          String splits[] = StringUtils.split(token, ".");
          for (int j = 0; j < splits.length; j++) {
            splits[j] = splits[j].trim();
          }

          String msrName = (splits.length <= 1) ? splits[0] : splits[1];
          CubeMeasure measure = cubeql.getCube().getMeasureByName(msrName);
          if (measure != null) {
            String msrAggregate = measure.getAggregate();

            if (StringUtils.isNotBlank(msrAggregate)) {
              exprTokens[i] = msrAggregate + "( " + token + ")" + (hasAlias ?
                  " " + tokenAlias : "");
              exprTokens[i] = exprTokens[i].toLowerCase();
              // Add this expression to aggregate expr set so that group by
              // resolver can skip
              // over expressions changed during aggregate resolver.
              cubeql.addAggregateExpr(exprTokens[i]);
            } else {
              throw new SemanticException("Default aggregate is not set for" +
                  " measure: " + msrName);
            }
          } else {
            // should not be here, since if it is a measure, we should get a
            // cube measure object
            throw new SemanticException("Measure not found for " + msrName);
          }
        }
      }
    }

    return StringUtils.join(exprTokens, ", ");
  }

  // We need to traverse the AST for Having clause.
  // We need to skip any columns that are inside an aggregate UDAF or
  // inside an arithmetic expression
  private String resolveForHaving(CubeQueryContext cubeql)
      throws SemanticException {
    ASTNode havingTree = cubeql.getHavingAST();
    String havingTreeStr = cubeql.getHavingTree();

    if (StringUtils.isBlank(havingTreeStr) || havingTree == null) {
      return null;
    }

    for (int i = 0; i < havingTree.getChildCount(); i++) {
      transform(cubeql, havingTree, (ASTNode) havingTree.getChild(i), i);
    }

    return HQLParser.getString(havingTree);
  }

  private void transform(CubeQueryContext cubeql, ASTNode parent, ASTNode node,
      int nodePos) throws SemanticException {
    if (parent == null || node == null) {
      return;
    }
    int nodeType = node.getToken().getType();

    if (!(isAggregateAST(node) || HQLParser.isArithmeticOp(nodeType))) {
      if (nodeType == HiveParser.TOK_TABLE_OR_COL ||
          nodeType == HiveParser.DOT) {
        // Leaf node
        ASTNode wrapped = wrapAggregate(cubeql, node);
        if (wrapped != node) {
          parent.setChild(nodePos, wrapped);
        }
      } else {
        // Dig deeper in non-leaf nodes
        for (int i = 0; i < node.getChildCount(); i++) {
          transform(cubeql, node, (ASTNode) node.getChild(i), i);
        }
      }
    }
  }

  private boolean isAggregateAST(ASTNode node) {
    int exprTokenType = node.getToken().getType();
    if (exprTokenType == HiveParser.TOK_FUNCTION
        || exprTokenType == HiveParser.TOK_FUNCTIONDI
        || exprTokenType == HiveParser.TOK_FUNCTIONSTAR) {
      assert (node.getChildCount() != 0);
      if (node.getChild(0).getType() == HiveParser.Identifier) {
        String functionName = BaseSemanticAnalyzer.unescapeIdentifier(
            node.getChild(0).getText());
        if (FunctionRegistry.getGenericUDAFResolver(functionName) != null) {
          return true;
        }
      }
    }

    return false;
  }

  // Wrap an aggregate function around the node if its a measure, leave it
  // unchanged otherwise
  private ASTNode wrapAggregate(CubeQueryContext cubeql, ASTNode node)
      throws SemanticException {

    String tabname = null;
    String colname = null;

    if (node.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
      colname = ((ASTNode) node.getChild(0)).getText();
    } else {
      // node in 'alias.column' format
      ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL,
          Identifier);
      ASTNode colIdent = (ASTNode) node.getChild(1);

      colname = colIdent.getText();
      tabname = tabident.getText();
    }

    String msrname = StringUtils.isBlank(tabname) ? colname : tabname + "."
        + colname;

    if (cubeql.isCubeMeasure(msrname)) {
      CubeMeasure measure = cubeql.getCube().getMeasureByName(colname);
      String aggregateFn = measure.getAggregate();

      if (StringUtils.isBlank(aggregateFn)) {
        throw new SemanticException("Default aggregate is not set for measure: "
            + colname);
      }
      ASTNode fnroot = new ASTNode(new CommonToken(HiveParser.TOK_FUNCTION));
      fnroot.setParent(node.getParent());

      ASTNode fnIdentNode = new ASTNode(new CommonToken(HiveParser.Identifier,
          aggregateFn));
      fnIdentNode.setParent(fnroot);
      fnroot.addChild(fnIdentNode);

      node.setParent(fnroot);
      fnroot.addChild(node);

      return fnroot;
    } else {
      return node;
    }
  }

  private String getColName(ASTNode node) {
    String tabname = null;
    String colname = null;
    int nodeType = node.getToken().getType();
    if (nodeType == HiveParser.TOK_TABLE_OR_COL) {
      colname = ((ASTNode) node.getChild(0)).getText();
    } else {
      // node in 'alias.column' format
      ASTNode tabident = HQLParser.findNodeByPath(node, TOK_TABLE_OR_COL,
          Identifier);
      ASTNode colIdent = (ASTNode) node.getChild(1);

      colname = colIdent.getText();
      tabname = tabident.getText();
    }

    return StringUtils.isBlank(tabname) ? colname : tabname + "." + colname;
  }
}
