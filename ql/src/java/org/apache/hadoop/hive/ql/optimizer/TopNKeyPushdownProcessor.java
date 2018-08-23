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
package org.apache.hadoop.hive.ql.optimizer;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TopNKeyOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import static org.apache.hadoop.hive.ql.optimizer.TopNKeyProcessor.copyDown;

public class TopNKeyPushdownProcessor implements NodeProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(TopNKeyPushdownProcessor.class);

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
      Object... nodeOutputs) throws SemanticException {
    pushdown((TopNKeyOperator) nd);
    return null;
  }

  private void pushdown(TopNKeyOperator topNKey) throws SemanticException {

    final Operator<? extends OperatorDesc> parent =
        topNKey.getParentOperators().get(0);

    switch (parent.getType()) {
      case SELECT:
        pushdownThroughSelect(topNKey);
        break;

      case FORWARD:
        moveDown(topNKey);
        pushdown(topNKey);
        break;

      case GROUPBY:
        pushdownThroughGroupBy(topNKey);
        break;

      case REDUCESINK:
        pushdownThroughReduceSink(topNKey);
        break;

      case MERGEJOIN:
      case JOIN:
        {
          final CommonJoinOperator<? extends JoinDesc> join =
              (CommonJoinOperator<? extends JoinDesc>) parent;
          final JoinCondDesc[] joinConds = join.getConf().getConds();
          final JoinCondDesc firstJoinCond = joinConds[0];
          for (JoinCondDesc joinCond : joinConds) {
            if (!firstJoinCond.equals(joinCond)) {
              return;
            }
          }
          switch (firstJoinCond.getType()) {
            case JoinDesc.FULL_OUTER_JOIN:
              pushdownThroughFullOuterJoin(topNKey);
              break;

            case JoinDesc.LEFT_OUTER_JOIN:
              pushdownThroughLeftOuterJoin(topNKey);
              break;

            case JoinDesc.RIGHT_OUTER_JOIN:
              pushdownThroughRightOuterJoin(topNKey);
              break;

            default:
              break;
          }
        }
        break;

      case TOPNKEY:
        if (hasSameTopNKeyDesc(parent, topNKey.getConf())) {
          parent.removeChildAndAdoptItsChildren(topNKey);
        }
        break;

      default:
        break;
    }
  }

  private void pushdownThroughSelect(TopNKeyOperator topNKey) throws SemanticException {

    final SelectOperator select = (SelectOperator) topNKey.getParentOperators().get(0);
    final TopNKeyDesc topNKeyDesc = topNKey.getConf();

    // Map columns
    final List<ExprNodeDesc> mappedColumns = mapColumns(topNKeyDesc.getKeyColumns(),
        select.getColumnExprMap());
    if (mappedColumns.isEmpty()) {
      return;
    }

    // Move down
    topNKeyDesc.setColumnSortOrder(topNKeyDesc.getColumnSortOrder());
    topNKeyDesc.setKeyColumns(mappedColumns);
    moveDown(topNKey);
    pushdown(topNKey);
  }

  private void pushdownThroughGroupBy(TopNKeyOperator topNKey) throws SemanticException {
    /*
     * Push through GroupBy. No grouping sets. If TopNKey expression is same as GroupBy expression,
     * we can push it and remove it from above GroupBy. If expression in TopNKey shared common
     * prefix with GroupBy, TopNKey could be pushed through GroupBy using that prefix and kept above
     * it.
     */
    final GroupByOperator groupBy = (GroupByOperator) topNKey.getParentOperators().get(0);
    final GroupByDesc groupByDesc = groupBy.getConf();
    final TopNKeyDesc topNKeyDesc = topNKey.getConf();

    // Check grouping sets
    if (groupByDesc.isGroupingSetsPresent()) {
      return;
    }

    // Map columns
    final List<ExprNodeDesc> mappedColumns = mapColumns(topNKeyDesc.getKeyColumns(),
        groupByDesc.getColumnExprMap());
    // If TopNKey expression is same as GroupBy expression
    if (!ExprNodeDescUtils.isSame(groupByDesc.getKeys(), mappedColumns)) {
      return;
    }

    // We can push it and remove it from above GroupBy.
    final TopNKeyDesc newTopNKeyDesc = new TopNKeyDesc(topNKeyDesc.getTopN(),
        topNKeyDesc.getColumnSortOrder(), mappedColumns);
    groupBy.removeChildAndAdoptItsChildren(topNKey);
    pushdown(copyDown(groupBy, newTopNKeyDesc));
  }

  private void pushdownThroughReduceSink(TopNKeyOperator topNKey) throws SemanticException {
    /*
     * Push through ReduceSink. If TopNKey expression is same as ReduceSink expression and order is
     * the same, we can push it and remove it from above ReduceSink. If expression in TopNKey shared
     * common prefix with ReduceSink including same order, TopNKey could be pushed through
     * ReduceSink using that prefix and kept above it.
     */
    final ReduceSinkOperator reduceSink = (ReduceSinkOperator) topNKey.getParentOperators().get(0);
    final ReduceSinkDesc reduceSinkDesc = reduceSink.getConf();
    final TopNKeyDesc topNKeyDesc = topNKey.getConf();

    // Check orders
    if (!reduceSinkDesc.getOrder().equals(topNKeyDesc.getColumnSortOrder())) {
      return;
    }

    // Map columns
    final List<ExprNodeDesc> mappedColumns = mapColumns(topNKeyDesc.getKeyColumns(),
        reduceSinkDesc.getColumnExprMap());
    // If TopNKey expression is same as ReduceSink expression
    if (!ExprNodeDescUtils.isSame(reduceSinkDesc.getKeyCols(), mappedColumns)) {
      return;
    }

    // We can push it and remove it from above ReduceSink.
    final TopNKeyDesc newTopNKeyDesc = new TopNKeyDesc(topNKeyDesc.getTopN(),
        topNKeyDesc.getColumnSortOrder(), mappedColumns);
    reduceSink.removeChildAndAdoptItsChildren(topNKey);
    pushdown(copyDown(reduceSink, newTopNKeyDesc));
  }

  private void pushdownThroughFullOuterJoin(TopNKeyOperator topNKey) throws SemanticException {
    /*
     * Push through FOJ. Push TopNKey expression without keys to largest input. Keep on top of FOJ.
     */
    final CommonJoinOperator<? extends JoinDesc> join =
        (CommonJoinOperator<? extends JoinDesc>) topNKey.getParentOperators().get(0);
    final TopNKeyDesc topNKeyDesc = topNKey.getConf();
    final ReduceSinkOperator leftInput = (ReduceSinkOperator) join.getParentOperators().get(0);
    final ReduceSinkOperator rightInput = (ReduceSinkOperator) join.getParentOperators().get(1);

    // Check null orders
    if (!checkNullOrder(leftInput.getConf())) {
      return;
    }
    if (!checkNullOrder(rightInput.getConf())) {
      return;
    }

    // Map columns
    final ReduceSinkOperator joinInput;
    final List<ExprNodeDesc> mappedColumns;
    if (leftInput.getStatistics().getDataSize() > rightInput.getStatistics().getDataSize()) {
      joinInput = rightInput;
      mappedColumns = new ArrayList<>(joinInput.getConf().getKeyCols());
      for (JoinCondDesc cond : join.getConf().getConds()) {
        mappedColumns.remove(cond.getRight());
      }
    } else {
      joinInput = leftInput;
      mappedColumns = new ArrayList<>(joinInput.getConf().getKeyCols());
      for (JoinCondDesc cond : join.getConf().getConds()) {
        mappedColumns.remove(cond.getLeft());
      }
    }
    if (mappedColumns.isEmpty()) {
      return;
    }

    // Copy down
    final String mappedOrder = mapOrder(topNKeyDesc.getColumnSortOrder(),
        joinInput.getConf().getKeyCols(), mappedColumns);
    final TopNKeyDesc newTopNKeyDesc = new TopNKeyDesc(topNKeyDesc.getTopN(), mappedOrder,
        mappedColumns);
    pushdown(copyDown(joinInput, newTopNKeyDesc));
  }

  private void pushdownThroughLeftOuterJoin(TopNKeyOperator topNKey) throws SemanticException {
    pushdownThroughLeftOrRightOuterJoin(topNKey, 0);
  }

  private void pushdownThroughRightOuterJoin(TopNKeyOperator topNKey) throws SemanticException {
    pushdownThroughLeftOrRightOuterJoin(topNKey, 1);
  }

  private void pushdownThroughLeftOrRightOuterJoin(TopNKeyOperator topNKey, int position)
      throws SemanticException {
    /*
     * Push through LOJ. If TopNKey expression refers fully to expressions from left input, push
     * with rewriting of expressions and remove from top of LOJ. If TopNKey expression has a prefix
     * that refers to expressions from left input, push with rewriting of those expressions and keep
     * on top of LOJ.
     */
    final TopNKeyDesc topNKeyDesc = topNKey.getConf();
    final CommonJoinOperator<? extends JoinDesc> join =
        (CommonJoinOperator<? extends JoinDesc>) topNKey.getParentOperators().get(0);
    final List<Operator<? extends OperatorDesc>> joinInputs = join.getParentOperators();
    final ReduceSinkOperator reduceSinkOperator = (ReduceSinkOperator) joinInputs.get(position);
    final ReduceSinkDesc reduceSinkDesc = reduceSinkOperator.getConf();

    // Check null order
    if (!checkNullOrder(reduceSinkDesc)) {
      return;
    }

    // Map columns
    final List<ExprNodeDesc> mappedColumns = mapColumns(mapColumns(topNKeyDesc.getKeyColumns(),
        join.getColumnExprMap()), reduceSinkOperator.getColumnExprMap());
    if (mappedColumns.isEmpty()) {
      return;
    }

    // Copy down
    final String mappedOrder = mapOrder(topNKeyDesc.getColumnSortOrder(),
        reduceSinkDesc.getKeyCols(), mappedColumns);
    final TopNKeyDesc newTopNKeyDesc = new TopNKeyDesc(topNKeyDesc.getTopN(), mappedOrder,
        mappedColumns);
    pushdown(copyDown(reduceSinkOperator, newTopNKeyDesc));

    // If all columns are mapped, remove from top
    if (topNKeyDesc.getKeyColumns().size() == mappedColumns.size()) {
      join.removeChildAndAdoptItsChildren(topNKey);
    }
  }

  private static boolean hasSameTopNKeyDesc(Operator<? extends OperatorDesc> operator,
      TopNKeyDesc desc) {

    if (operator instanceof TopNKeyOperator) {
      final TopNKeyOperator topNKey = (TopNKeyOperator) operator;
      final TopNKeyDesc opDesc = topNKey.getConf();
      if (opDesc.isSame(desc)) {
        return true;
      }
    }
    return false;
  }

  private static String mapOrder(String order, List<ExprNodeDesc> parentCols, List<ExprNodeDesc>
      mappedCols) {

    final StringBuilder builder = new StringBuilder();
    int index = 0;
    for (ExprNodeDesc mappedCol : mappedCols) {
      if (parentCols.contains(mappedCol)) {
        builder.append(order.charAt(index++));
      } else {
        builder.append("+");
      }
    }
    return builder.toString();
  }

  private static List<ExprNodeDesc> mapColumns(List<ExprNodeDesc> columns, Map<String, ExprNodeDesc>
      colExprMap) {

    if (colExprMap == null) {
      return columns;
    }
    final List<ExprNodeDesc> mappedColumns = new ArrayList<>();
    for (ExprNodeDesc column : columns) {
      final String columnName = column.getExprString();
      if (colExprMap.containsKey(columnName)) {
        mappedColumns.add(colExprMap.get(columnName));
      }
    }
    return mappedColumns;
  }

  private static void moveDown(TopNKeyOperator topNKey) throws SemanticException {

    assert topNKey.getNumParent() == 1;
    final Operator<? extends OperatorDesc> parent = topNKey.getParentOperators().get(0);
    final List<Operator<? extends OperatorDesc>> grandParents = parent.getParentOperators();
    parent.removeChildAndAdoptItsChildren(topNKey);
    for (Operator<? extends OperatorDesc> grandParent : grandParents) {
      grandParent.replaceChild(parent, topNKey);
    }
    topNKey.setParentOperators(new ArrayList<>(grandParents));
    topNKey.setChildOperators(new ArrayList<>(Collections.singletonList(parent)));
    parent.setParentOperators(new ArrayList<>(Collections.singletonList(topNKey)));
  }

  private static boolean checkNullOrder(ReduceSinkDesc reduceSinkDesc) {

    final String order = reduceSinkDesc.getOrder();
    final String nullOrder = reduceSinkDesc.getNullOrder();
    if (nullOrder == null) {
      for (int i = 0; i < order.length(); i++) {
        if (order.charAt(i) != '+') {
          return false;
        }
      }
    } else {
      for (int i = 0; i < nullOrder.length(); i++) {
        if (nullOrder.charAt(i) != 'a') {
          return false;
        }
      }
    }
    return true;
  }
}
