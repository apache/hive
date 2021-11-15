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
package org.apache.hadoop.hive.ql.optimizer.topnkey;

import static org.apache.hadoop.hive.ql.optimizer.topnkey.TopNKeyProcessor.copyDown;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TopNKeyOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of TopNKey operator pushdown.
 */
public class TopNKeyPushdownProcessor implements SemanticNodeProcessor {
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
      pushdownThroughParent(topNKey);
      break;

    case GROUPBY:
      pushdownThroughGroupBy(topNKey);
      break;

    case REDUCESINK:
      pushdownThroughReduceSink(topNKey);
      break;

    case MERGEJOIN:
    case JOIN:
      pushDownThroughJoin(topNKey);
      break;

    case TOPNKEY:
      pushdownThroughTopNKey(topNKey);
      break;

    default:
      break;
    }
  }

  /**
   * Push through Project if expression(s) in TopNKey can be mapped to expression(s) based on Project input.
   *
   * @param topNKey TopNKey operator to push
   * @throws SemanticException when removeChildAndAdoptItsChildren was not successful in the method pushdown
   */
  private void pushdownThroughSelect(TopNKeyOperator topNKey) throws SemanticException {

    final SelectOperator select = (SelectOperator) topNKey.getParentOperators().get(0);
    final TopNKeyDesc topNKeyDesc = topNKey.getConf();

    final List<ExprNodeDesc> mappedColumns = mapColumns(topNKeyDesc.getKeyColumns(), select.getColumnExprMap());
    if (mappedColumns.size() != topNKeyDesc.getKeyColumns().size()) {
      return;
    }

    LOG.debug("Pushing {} through {}", topNKey.getName(), select.getName());
    topNKeyDesc.setKeyColumns(mappedColumns);
    topNKeyDesc.setPartitionKeyColumns(mappedColumns.subList(0, topNKeyDesc.getPartitionKeyColumns().size()));
    moveDown(topNKey);
    pushdown(topNKey);
  }

  private static List<ExprNodeDesc> mapColumns(List<ExprNodeDesc> columns, Map<String, ExprNodeDesc>
          colExprMap) {

    if (colExprMap == null) {
      return new ArrayList<>(0);
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

  private void pushdownThroughParent(TopNKeyOperator topNKey) throws SemanticException {
    Operator<? extends OperatorDesc> parent = topNKey.getParentOperators().get(0);
    LOG.debug("Pushing {} through {}", topNKey.getName(), parent.getName());
    moveDown(topNKey);
    pushdown(topNKey);
  }

  /**
   * Push through GroupBy. No grouping sets. If TopNKey expression is same as GroupBy expression,
   * we can push it and remove it from above GroupBy. If expression in TopNKey shared common
   * prefix with GroupBy, TopNKey could be pushed through GroupBy using that prefix and kept above
   * it.
   *
   * @param topNKey TopNKey operator to push
   * @throws SemanticException when removeChildAndAdoptItsChildren was not successful
   */
  private void pushdownThroughGroupBy(TopNKeyOperator topNKey) throws SemanticException {
    final GroupByOperator groupBy = (GroupByOperator) topNKey.getParentOperators().get(0);
    final GroupByDesc groupByDesc = groupBy.getConf();
    final TopNKeyDesc topNKeyDesc = topNKey.getConf();

    CommonKeyPrefix commonKeyPrefix = CommonKeyPrefix.map(topNKeyDesc, groupByDesc);
    if (commonKeyPrefix.isEmpty() || commonKeyPrefix.size() == topNKeyDesc.getPartitionKeyColumns().size()) {
      return;
    }

    LOG.debug("Pushing a copy of {} through {}", topNKey.getName(), groupBy.getName());
    final TopNKeyDesc newTopNKeyDesc = topNKeyDesc.combine(commonKeyPrefix);
    pushdown((TopNKeyOperator) copyDown(groupBy, newTopNKeyDesc));

    if (topNKeyDesc.getKeyColumns().size() == commonKeyPrefix.size()) {
      LOG.debug("Removing {} above {}", topNKey.getName(), groupBy.getName());
      groupBy.removeChildAndAdoptItsChildren(topNKey);
    }
  }

  /**
   * Push through ReduceSink. If TopNKey expression is same as ReduceSink expression and order is
   * the same, we can push it and remove it from above ReduceSink. If expression in TopNKey shared
   * common prefix with ReduceSink including same order, TopNKey could be pushed through
   * ReduceSink using that prefix and kept above it.
   *
   * @param topNKey TopNKey operator to push
   * @throws SemanticException when removeChildAndAdoptItsChildren was not successful
   */
  private void pushdownThroughReduceSink(TopNKeyOperator topNKey) throws SemanticException {
    ReduceSinkOperator reduceSink = (ReduceSinkOperator) topNKey.getParentOperators().get(0);
    final ReduceSinkDesc reduceSinkDesc = reduceSink.getConf();
    final TopNKeyDesc topNKeyDesc = topNKey.getConf();

    CommonKeyPrefix commonKeyPrefix = CommonKeyPrefix.map(topNKeyDesc, reduceSinkDesc);
    if (commonKeyPrefix.isEmpty() || commonKeyPrefix.size() == topNKeyDesc.getPartitionKeyColumns().size()) {
      return;
    }

    LOG.debug("Pushing a copy of {} through {}", topNKey.getName(), reduceSink.getName());
    final TopNKeyDesc newTopNKeyDesc = topNKeyDesc.combine(commonKeyPrefix);
    pushdown((TopNKeyOperator) copyDown(reduceSink, newTopNKeyDesc));

    if (topNKeyDesc.getKeyColumns().size() == commonKeyPrefix.size()) {
      LOG.debug("Removing {} above {}", topNKey.getName(), reduceSink.getName());
      reduceSink.removeChildAndAdoptItsChildren(topNKey);
    }
  }

  // Only push down through Left Outer Join is supported.
  // Right and Full Outer Join support will be added in a follow up patch.
  private void pushDownThroughJoin(TopNKeyOperator topNKey)
          throws SemanticException {
    CommonJoinOperator<? extends JoinDesc> parent =
            (CommonJoinOperator<? extends JoinDesc>) topNKey.getParentOperators().get(0);
    JoinDesc joinDesc = parent.getConf();
    JoinCondDesc[] joinConds = joinDesc.getConds();
    JoinCondDesc firstJoinCond = joinConds[0];
    for (JoinCondDesc joinCond : joinConds) {
      if (!firstJoinCond.equals(joinCond)) {
        return;
      }
    }
    if (firstJoinCond.getType() == JoinDesc.LEFT_OUTER_JOIN) {
      pushdownThroughLeftOuterJoin(topNKey);
    } else if (firstJoinCond.getType() == JoinDesc.INNER_JOIN && joinDesc.isPkFkJoin()) {
      pushdownInnerJoin(topNKey, joinDesc.getFkJoinTableIndex(), joinDesc.isNonFkSideIsFiltered());
    }
  }

  /**
   * Push through LOJ. If TopNKey expression refers fully to expressions from left input, push
   * with rewriting of expressions and remove from top of LOJ. If TopNKey expression has a prefix
   * that refers to expressions from left input, push with rewriting of those expressions and keep
   * on top of LOJ.
   *
   * @param topNKey TopNKey operator to push
   * @throws SemanticException when removeChildAndAdoptItsChildren was not successful
   */
  private void pushdownThroughLeftOuterJoin(TopNKeyOperator topNKey) throws SemanticException {
    final TopNKeyDesc topNKeyDesc = topNKey.getConf();
    final CommonJoinOperator<? extends JoinDesc> join =
            (CommonJoinOperator<? extends JoinDesc>) topNKey.getParentOperators().get(0);
    final List<Operator<? extends OperatorDesc>> joinInputs = join.getParentOperators();
    final ReduceSinkOperator reduceSinkOperator = (ReduceSinkOperator) joinInputs.get(0);
    final ReduceSinkDesc reduceSinkDesc = reduceSinkOperator.getConf();

    CommonKeyPrefix commonKeyPrefix = CommonKeyPrefix.map(
            mapUntilColumnEquals(topNKeyDesc.getKeyColumns(), join.getColumnExprMap()),
            topNKeyDesc.getColumnSortOrder(),
            topNKeyDesc.getNullOrder(),
            reduceSinkDesc.getKeyCols(),
            reduceSinkDesc.getColumnExprMap(),
            reduceSinkDesc.getOrder(),
            reduceSinkDesc.getNullOrder());
    if (commonKeyPrefix.isEmpty() || commonKeyPrefix.size() == topNKeyDesc.getPartitionKeyColumns().size()) {
      return;
    }

    LOG.debug("Pushing a copy of {} through {} and {}",
            topNKey.getName(), join.getName(), reduceSinkOperator.getName());
    final TopNKeyDesc newTopNKeyDesc = topNKeyDesc.combine(commonKeyPrefix);
    pushdown((TopNKeyOperator) copyDown(reduceSinkOperator, newTopNKeyDesc));

    if (topNKeyDesc.getKeyColumns().size() == commonKeyPrefix.size()) {
      LOG.debug("Removing {} above {}", topNKey.getName(), join.getName());
      join.removeChildAndAdoptItsChildren(topNKey);
    }
  }

  /**
   * Tries to push the TopNKeyFilter through an inner join:
   *  requirements:
   *    - being PK-FK join
   *    - PK side is not filtered
   *    - First n TopNKey key columns (Order By) are originated from the FK side.
   * @throws SemanticException
   */
  private void pushdownInnerJoin(TopNKeyOperator topNKey, int fkJoinInputIndex, boolean nonFkSideIsFiltered) throws SemanticException {
    TopNKeyDesc topNKeyDesc = topNKey.getConf();
    CommonJoinOperator<? extends JoinDesc> join =
            (CommonJoinOperator<? extends JoinDesc>) topNKey.getParentOperators().get(0);
    List<Operator<? extends OperatorDesc>> joinInputs = join.getParentOperators();
    ReduceSinkOperator fkJoinInput = (ReduceSinkOperator) joinInputs.get(fkJoinInputIndex);
    if (nonFkSideIsFiltered) {
      LOG.debug("Not pushing {} through {} as non FK side of the join is filtered", topNKey.getName(), join.getName());
      return;
    }
    CommonKeyPrefix commonKeyPrefix = CommonKeyPrefix.map(
            mapUntilColumnEquals(topNKeyDesc.getKeyColumns(), join.getColumnExprMap()),
            topNKeyDesc.getColumnSortOrder(),
            topNKeyDesc.getNullOrder(),
            fkJoinInput.getConf().getKeyCols(),
            fkJoinInput.getConf().getColumnExprMap(),
            fkJoinInput.getConf().getOrder(),
            fkJoinInput.getConf().getNullOrder());
    if (commonKeyPrefix.isEmpty() || commonKeyPrefix.size() == topNKeyDesc.getPartitionKeyColumns().size()) {
      return;
    }
    LOG.debug("Pushing a copy of {} through {} and {}",
            topNKey.getName(), join.getName(), fkJoinInput.getName());
    final TopNKeyDesc newTopNKeyDesc = topNKeyDesc.combine(commonKeyPrefix);
    pushdown((TopNKeyOperator) copyDown(fkJoinInput, newTopNKeyDesc));

    if (topNKeyDesc.getKeyColumns().size() == commonKeyPrefix.size()) {
      LOG.debug("Removing {} above {}", topNKey.getName(), join.getName());
      join.removeChildAndAdoptItsChildren(topNKey);
    }
  }

  private List<ExprNodeDesc> mapUntilColumnEquals(List<ExprNodeDesc> columns, Map<String,
          ExprNodeDesc> colExprMap) {
    if (colExprMap == null) {
      return new ArrayList<>(0);
    }
    final List<ExprNodeDesc> mappedColumns = new ArrayList<>();
    for (ExprNodeDesc column : columns) {
      final String columnName = column.getExprString();
      if (colExprMap.containsKey(columnName)) {
        mappedColumns.add(colExprMap.get(columnName));
      } else {
        return mappedColumns;
      }
    }
    return mappedColumns;
  }

  /**
   * Push through another Top N Key operator.
   * If the TNK operators are the same one of them will be removed. See {@link TopNKeyDesc#isSame}
   * else If expression in <code>topnKey</code> is a common prefix in it's parent TNK op and topN property is same
   * then <code>topnkey</code> could be pushed through parent.
   * If the Top N Key operator can not be pushed through this method tries to remove one of them:
   * - if topN property is the same and the keys of one of the operators are subset of the other then the operator
   *   can be removed
   * - if the keys are the same operator with higher topN value can be removed
   * @param topNKey TopNKey operator to push
   * @throws SemanticException when removeChildAndAdoptItsChildren was not successful
   */
  private void pushdownThroughTopNKey(TopNKeyOperator topNKey) throws SemanticException {
    TopNKeyOperator parent = (TopNKeyOperator) topNKey.getParentOperators().get(0);
    if (hasSameTopNKeyDesc(parent, topNKey.getConf())) {
      LOG.debug("Removing {} above same operator: {}", topNKey.getName(), parent.getName());
      parent.removeChildAndAdoptItsChildren(topNKey);
      return;
    }

    TopNKeyDesc topNKeyDesc = topNKey.getConf();
    TopNKeyDesc parentTopNKeyDesc = parent.getConf();
    CommonKeyPrefix commonKeyPrefix = CommonKeyPrefix.map(
            topNKeyDesc.getKeyColumns(), topNKeyDesc.getColumnSortOrder(), topNKeyDesc.getNullOrder(),
            parentTopNKeyDesc.getKeyColumns(), parentTopNKeyDesc.getColumnSortOrder(),
            parentTopNKeyDesc.getNullOrder());

    if (topNKeyDesc.getTopN() == parentTopNKeyDesc.getTopN()) {
      if (topNKeyDesc.getKeyColumns().size() == commonKeyPrefix.size()) {
        // TNK keys are subset of the parent TNK keys
        pushdownThroughParent(topNKey);
        if (topNKey.getChildOperators().get(0).getType() == OperatorType.TOPNKEY) {
          LOG.debug("Removing {} since child {} supersedes it", parent.getName(), topNKey.getName());
          topNKey.getParentOperators().get(0).removeChildAndAdoptItsChildren(topNKey);
        }
      } else if (parentTopNKeyDesc.getKeyColumns().size() == commonKeyPrefix.size()) {
        // parent TNK keys are subset of TNK keys
        LOG.debug("Removing parent of {} since it supersedes", topNKey.getName());
        parent.getParentOperators().get(0).removeChildAndAdoptItsChildren(parent);
      }
    } else if (topNKeyDesc.getKeyColumns().size() == commonKeyPrefix.size() &&
            parentTopNKeyDesc.getKeyColumns().size() == commonKeyPrefix.size()) {
      if (topNKeyDesc.getTopN() > parentTopNKeyDesc.getTopN()) {
        LOG.debug("Removing {}. Parent {} has same keys but lower topN {} > {}",
                topNKey.getName(), parent.getName(), topNKeyDesc.getTopN(), parentTopNKeyDesc.getTopN());
        topNKey.getParentOperators().get(0).removeChildAndAdoptItsChildren(topNKey);
      } else {
        LOG.debug("Removing parent {}. {} has same keys but lower topN {} < {}",
                parent.getName(), topNKey.getName(), topNKeyDesc.getTopN(), parentTopNKeyDesc.getTopN());
        parent.getParentOperators().get(0).removeChildAndAdoptItsChildren(parent);
      }
    }
  }

  private static boolean hasSameTopNKeyDesc(Operator<? extends OperatorDesc> operator, TopNKeyDesc desc) {
    if (!(operator instanceof TopNKeyOperator)) {
      return false;
    }

    final TopNKeyOperator topNKey = (TopNKeyOperator) operator;
    final TopNKeyDesc opDesc = topNKey.getConf();
    return opDesc.isSame(desc);
  }

  public static void moveDown(Operator<? extends OperatorDesc> operator) throws SemanticException {

    assert operator.getNumParent() == 1;
    final Operator<? extends OperatorDesc> parent = operator.getParentOperators().get(0);
    final List<Operator<? extends OperatorDesc>> grandParents = parent.getParentOperators();
    parent.removeChildAndAdoptItsChildren(operator);
    for (Operator<? extends OperatorDesc> grandParent : grandParents) {
      grandParent.replaceChild(parent, operator);
    }
    operator.getParentOperators().clear();
    operator.getParentOperators().addAll(grandParents);

    operator.getChildOperators().clear();
    operator.getChildOperators().add(parent);

    parent.getParentOperators().clear();
    parent.getParentOperators().add(operator);
  }
}
