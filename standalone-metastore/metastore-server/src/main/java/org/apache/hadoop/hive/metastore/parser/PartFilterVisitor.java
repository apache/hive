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
package org.apache.hadoop.hive.metastore.parser;

import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;

import static org.apache.hadoop.hive.metastore.parser.ExpressionTree.LeafNode;
import static org.apache.hadoop.hive.metastore.parser.ExpressionTree.LogicalOperator;
import static org.apache.hadoop.hive.metastore.parser.ExpressionTree.Operator;
import static org.apache.hadoop.hive.metastore.parser.ExpressionTree.TreeNode;

public class PartFilterVisitor extends PartitionFilterBaseVisitor<Object> {

  /**
   * Override the default behavior for all visit methods. This will only return a non-null result
   * when the context has only one child. This is done because there is no generic method to
   * combine the results of the context children. In all other cases null is returned.
   */
  @Override
  public Object visitChildren(RuleNode node) {
    if (node.getChildCount() == 1) {
      return node.getChild(0).accept(this);
    }
    return null;
  }

  @Override
  public ExpressionTree visitFilter(PartitionFilterParser.FilterContext ctx) {
    ExpressionTree tree = new ExpressionTree();
    TreeNode treeNode = (TreeNode) visit(ctx.orExpression());
    tree.setRoot(treeNode);
    return tree;
  }

  @Override
  public TreeNode visitOrExpression(PartitionFilterParser.OrExpressionContext ctx) {
    List<TreeNode> nodes = ctx.andExprs.stream()
        .map(this::visitAndExpression)
        .collect(Collectors.toList());
    return buildTreeFromNodes(nodes, LogicalOperator.OR);
  }

  @Override
  public TreeNode visitAndExpression(PartitionFilterParser.AndExpressionContext ctx) {
    List<TreeNode> nodes = ctx.exprs.stream()
        .map(this::visitExpression)
        .collect(Collectors.toList());
    return buildTreeFromNodes(nodes, LogicalOperator.AND);
  }

  private TreeNode buildTreeFromNodes(List<? extends TreeNode> nodes, LogicalOperator operator) {
    // The 'nodes' list is expected to have at least one element.
    // If the list if empty, the lexer parse would have failed.
    if (nodes.size() == 1) {
      return nodes.get(0);
    }
    TreeNode root = new TreeNode(nodes.get(0), operator, nodes.get(1));
    for (int i = 2; i < nodes.size(); ++i) {
      TreeNode tmp = new TreeNode(root, operator, nodes.get(i));
      root = tmp;
    }
    return root;
  }

  @Override
  public TreeNode visitExpression(PartitionFilterParser.ExpressionContext ctx) {
    if (ctx.orExpression() != null) {
      return visitOrExpression(ctx.orExpression());
    }
    return (TreeNode) visit(ctx.conditionExpression());
  }

  @Override
  public TreeNode visitComparison(PartitionFilterParser.ComparisonContext ctx) {
    LeafNode leafNode = new LeafNode();
    leafNode.keyName = (String) visit(ctx.key);
    leafNode.value = visit(ctx.value);
    leafNode.operator = visitComparisonOperator(ctx.comparisonOperator());
    return leafNode;
  }

  @Override
  public Object visitReverseComparison(PartitionFilterParser.ReverseComparisonContext ctx) {
    LeafNode leafNode = new LeafNode();
    leafNode.keyName = (String) visit(ctx.key);
    leafNode.value = visit(ctx.value);
    leafNode.operator = visitComparisonOperator(ctx.comparisonOperator());
    leafNode.isReverseOrder = true;
    return leafNode;
  }

  @Override
  public TreeNode visitBetweenCondition(PartitionFilterParser.BetweenConditionContext ctx) {
    LeafNode left = new LeafNode();
    LeafNode right = new LeafNode();
    left.keyName = right.keyName = (String) visit(ctx.key);
    left.value = visit(ctx.lower);
    right.value = visit(ctx.upper);

    boolean isPositive = ctx.NOT() == null;
    left.operator = isPositive ? Operator.GREATERTHANOREQUALTO : Operator.LESSTHAN;
    right.operator = isPositive ? Operator.LESSTHANOREQUALTO : Operator.GREATERTHAN;
    LogicalOperator rootOperator = isPositive ? LogicalOperator.AND : LogicalOperator.OR;

    TreeNode treeNode = new TreeNode(left, rootOperator, right);
    return treeNode;
  }

  @Override
  public TreeNode visitInCondition(PartitionFilterParser.InConditionContext ctx) {
    List<Object> values = visitConstantSeq(ctx.constantSeq());
    boolean isPositive = ctx.NOT() == null;
    String keyName = (String) visit(ctx.key);
    return buildInCondition(keyName, values, isPositive);
  }

  private TreeNode buildInCondition(String keyName, List<Object> values, boolean isPositive) {
    List<LeafNode> nodes = values.stream()
        .map(value -> {
          LeafNode leafNode = new LeafNode();
          leafNode.keyName = keyName;
          leafNode.value = value;
          leafNode.operator = isPositive ? Operator.EQUALS : Operator.NOTEQUALS2;
          return leafNode; })
        .collect(Collectors.toList());
    return buildTreeFromNodes(nodes, isPositive ? LogicalOperator.OR : LogicalOperator.AND);
  }

  @Override
  public TreeNode visitMultiColInExpression(PartitionFilterParser.MultiColInExpressionContext ctx) {
    List<String> keyNames = visitIdentifierList(ctx.identifierList());
    List<List<Object>> structs = visitConstStructList(ctx.constStructList());
    boolean isPositive = ctx.NOT() == null;

    List<TreeNode> treeNodes = new ArrayList<>(structs.size());
    for (int i = 0; i < structs.size(); ++i) {
      List<Object> struct = structs.get(i);
      if (keyNames.size() != struct.size()) {
        throw new ParseCancellationException("Struct key " + keyNames + " and value " + struct + " sizes do not match.");
      }
      List<LeafNode> nodes = new ArrayList<>(struct.size());
      for (int j = 0; j < struct.size(); ++j) {
        LeafNode leafNode = new LeafNode();
        leafNode.keyName = keyNames.get(j);
        leafNode.value = struct.get(j);
        leafNode.operator = isPositive ? Operator.EQUALS : Operator.NOTEQUALS2;
        nodes.add(leafNode);
      }
      treeNodes.add(buildTreeFromNodes(nodes, isPositive ? LogicalOperator.AND : LogicalOperator.OR));
    }
    return buildTreeFromNodes(treeNodes, isPositive ? LogicalOperator.OR : LogicalOperator.AND);
  }

  @Override
  public Operator visitComparisonOperator(PartitionFilterParser.ComparisonOperatorContext ctx) {
    TerminalNode node = (TerminalNode) ctx.getChild(0);
    switch (node.getSymbol().getType()) {
      case PartitionFilterParser.EQ: return Operator.EQUALS;
      case PartitionFilterParser.NEQ: return Operator.NOTEQUALS;
      case PartitionFilterParser.NEQJ: return Operator.NOTEQUALS2;
      case PartitionFilterParser.LT: return Operator.LESSTHAN;
      case PartitionFilterParser.LTE: return Operator.LESSTHANOREQUALTO;
      case PartitionFilterParser.GT: return Operator.GREATERTHAN;
      case PartitionFilterParser.GTE: return Operator.GREATERTHANOREQUALTO;
      case PartitionFilterParser.LIKE: return Operator.LIKE;
      default:
        throw new ParseCancellationException("Unsupported comparison operator: " + node.getSymbol().getText());
    }
  }

  @Override
  public List<Object> visitConstantSeq(PartitionFilterParser.ConstantSeqContext ctx) {
    return ctx.values.stream().map(this::visit).collect(Collectors.toList());
  }

  @Override
  public List<Object> visitConstStruct(PartitionFilterParser.ConstStructContext ctx) {
    return visitConstantSeq(ctx.constantSeq());
  }

  @Override
  public List<List<Object>> visitConstStructList(PartitionFilterParser.ConstStructListContext ctx) {
    return ctx.structs.stream().map(this::visitConstStruct).collect(Collectors.toList());
  }

  @Override
  public List<String> visitIdentifierList(PartitionFilterParser.IdentifierListContext ctx) {
    return ctx.ident.stream().map(i -> i.getText()).collect(Collectors.toList());
  }

  @Override
  public String visitStringLiteral(PartitionFilterParser.StringLiteralContext ctx) {
    return unquoteString(ctx.getText());
  }

  private String unquoteString(String string) {
    if (string.length() > 1) {
      if ((string.charAt(0) == '\'' && string.charAt(string.length() -1 ) == '\'')
           || (string.charAt(0) == '"' && string.charAt(string.length() - 1) == '"')) {
        return string.substring(1, string.length() - 1);
      }
    }
    return string;
  }

  @Override
  public Long visitIntegerLiteral(PartitionFilterParser.IntegerLiteralContext ctx) {
    return Long.parseLong(ctx.getText());
  }

  @Override
  public String visitDateLiteral(PartitionFilterParser.DateLiteralContext ctx) {
    PartitionFilterParser.DateContext date = ctx.date();
    String dateValue = unquoteString(date.value.getText());
    try {
      MetaStoreUtils.convertStringToDate(dateValue);
    } catch (DateTimeParseException e) {
      throw new ParseCancellationException(e.getMessage());
    }
    return dateValue;
  }

  @Override
  public String visitTimestampLiteral(PartitionFilterParser.TimestampLiteralContext ctx) {
    PartitionFilterParser.TimestampContext timestamp = ctx.timestamp();
    String timestampValue = unquoteString(timestamp.value.getText());
    try {
      MetaStoreUtils.convertStringToTimestamp(timestampValue);
    } catch (DateTimeParseException e) {
      throw new ParseCancellationException(e.getMessage());
    }
    return timestampValue;
  }

  @Override
  public String visitUnquotedIdentifer(PartitionFilterParser.UnquotedIdentiferContext ctx) {
    return ctx.getText();
  }

  @Override
  public String visitQuotedIdentifier(PartitionFilterParser.QuotedIdentifierContext ctx) {
    return StringUtils.replace(ctx.getText().substring(1, ctx.getText().length() -1 ), "``", "`");
  }

  @Override
  public TreeNode visitBooleanCondition(PartitionFilterParser.BooleanConditionContext ctx) {
    TreeNode exprNode = (TreeNode) visit(ctx.expression());
    boolean isNegated = ctx.NOT() != null;  // Check for negation (NOT)
    boolean parsedBoolean = Boolean.parseBoolean(ctx.booleanLiteral().getText());

    // For TRUE case: return expression directly if not negated, otherwise negate it
    // For FALSE case: return negated expression if not negated, otherwise return as is
    return (parsedBoolean != isNegated) ? exprNode : negateTree(exprNode);
  }

  private Operator invertOperator(Operator operator) {
    switch (operator) {
      case EQUALS:
        return Operator.NOTEQUALS;
      case NOTEQUALS:
      case NOTEQUALS2:
        return Operator.EQUALS;
      case GREATERTHAN:
        return Operator.LESSTHANOREQUALTO;
      case LESSTHAN:
        return Operator.GREATERTHANOREQUALTO;
      case GREATERTHANOREQUALTO:
        return Operator.LESSTHAN;
      case LESSTHANOREQUALTO:
        return Operator.GREATERTHAN;
      case LIKE:
        throw new UnsupportedOperationException("LIKE operator inversion is not supported.");
      default:
        throw new IllegalArgumentException("Unsupported operator for inversion: " + operator.getOp());
    }
  }

  @Override
  public TreeNode visitBooleanWrappedExpression(PartitionFilterParser.BooleanWrappedExpressionContext ctx) {
    // Visit the inner expression and check if "NOT" is used
    TreeNode innerNode = (TreeNode) visit(ctx.orExpression());
    boolean isNot = ctx.NOT() != null;
    boolean parsedBoolean = Boolean.parseBoolean(ctx.booleanLiteral().getText());

    // Return the node based on the expected boolean value (negated or not)
    return parsedBoolean != isNot ? innerNode : negateTree(innerNode);
  }

  private TreeNode negateTree(TreeNode node) {
    if (node instanceof LeafNode) {
      // Negate leaf nodes directly
      return negateLeafNode((LeafNode) node);
    } else if (node != null) {
      // Negate logical nodes (AND/OR) recursively
      TreeNode negatedLeft = negateTree(node.getLhs());
      TreeNode negatedRight = negateTree(node.getRhs());
      LogicalOperator negatedOperator = (node.getAndOr() == LogicalOperator.AND)
              ? LogicalOperator.OR
              : LogicalOperator.AND;
      return new TreeNode(negatedLeft, negatedOperator, negatedRight);
    }
    throw new IllegalArgumentException("Unknown TreeNode type");
  }

  private LeafNode negateLeafNode(LeafNode leaf) {
    LeafNode negatedLeaf = new LeafNode();
    negatedLeaf.keyName = leaf.keyName;

    // Invert the operator for the leaf node
    negatedLeaf.operator = invertOperator(leaf.operator);
    negatedLeaf.value = leaf.value;
    return negatedLeaf;
  }

  @Override
  public TreeNode visitInConditionWithBoolean(PartitionFilterParser.InConditionWithBooleanContext ctx) {
    List<Object> values = visitConstantSeq(ctx.constantSeq());
    String keyName = (String) visit(ctx.key);
    // Determine if the condition is a NOT IN by checking the presence of the NOT keyword
    boolean isNotIn = !ctx.NOT().isEmpty();
    boolean parsedBoolean = Boolean.parseBoolean(ctx.booleanLiteral().getText());

    // If the boolean literal is TRUE, the condition should be IN, otherwise NOT IN
    return buildInCondition(keyName, values, parsedBoolean != isNotIn);
  }
}
