/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.FilterSelectivityEstimator;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Sorts conditions in a filter predicate to accelerate query processing
 * based on selectivity and compute cost. Currently it is not applied recursively,
 * i.e., it is only applied to top predicates in the condition.
 */
public class HiveFilterSortPredicates extends RelHomogeneousShuttle {

  private static final Logger LOG = LoggerFactory.getLogger(HiveFilterSortPredicates.class);

  private final AtomicInteger noColsMissingStats;

  public HiveFilterSortPredicates(AtomicInteger noColsMissingStats) {
    this.noColsMissingStats = noColsMissingStats;
  }

  @Override
  public RelNode visit(RelNode other) {
    RelNode visitedNode = super.visit(other);
    if (visitedNode instanceof Filter) {
      return rewriteFilter((Filter) visitedNode);
    }

    return visitedNode;
  }

  private RelNode rewriteFilter(Filter matchNode) {
    try {
      final Filter filter = matchNode;
      final RelNode input = filter.getInput();

      final RexNode originalCond = filter.getCondition();
      final RexSortPredicatesShuttle sortPredicatesShuttle = new RexSortPredicatesShuttle(
          input, filter.getCluster().getMetadataQuery());
      final RexNode newCond = originalCond.accept(sortPredicatesShuttle);
      if (!sortPredicatesShuttle.modified) {
        // We are done, bail out
        return matchNode;
      }

      // We register the new filter so we do not fire the rule on it again
      final Filter newFilter = filter.copy(filter.getTraitSet(), input, newCond);
      return newFilter;
    }
    catch (Exception e) {
      if (noColsMissingStats.get() > 0) {
        LOG.warn("Missing column stats (see previous messages), skipping sort predicates in filter expressions in CBO");
        noColsMissingStats.set(0);
      } else {
        throw e;
      }
    }
    return matchNode;
  }

  /**
   * If the expression is an AND/OR, it will sort predicates accordingly
   * to maximize performance.
   */
  private static class RexSortPredicatesShuttle extends RexShuttle {

    private FilterSelectivityEstimator selectivityEstimator;
    private boolean modified;

    private RexSortPredicatesShuttle(RelNode inputRel, RelMetadataQuery mq) {
      selectivityEstimator = new FilterSelectivityEstimator(inputRel, mq);
      modified = false;
    }

    @Override
    public RexNode visitCall(final RexCall call) {
      switch (call.getKind()) {
        case AND:
          List<RexNode> newAndOperands = call.getOperands()
              .stream()
              .map(pred -> new Pair<>(pred, rankingAnd(pred)))
              .sorted(Comparator.comparing(Pair::getValue, Comparator.nullsLast(Double::compare)))
              .map(Pair::getKey)
              .collect(Collectors.toList());
          if (!call.getOperands().equals(newAndOperands)) {
            modified = true;
            return call.clone(call.getType(), newAndOperands);
          }
          break;
        case OR:
          List<RexNode> newOrOperands = call.getOperands()
              .stream()
              .map(pred -> new Pair<>(pred, rankingOr(pred)))
              .sorted(Comparator.comparing(Pair::getValue, Comparator.nullsLast(Double::compare)))
              .map(Pair::getKey)
              .collect(Collectors.toList());
          if (!call.getOperands().equals(newOrOperands)) {
            modified = true;
            return call.clone(call.getType(), newOrOperands);
          }
          break;
      }
      return call;
    }

    /**
     * Nodes in an AND clause are sorted by a rank value calculated as:
     * rank = (selectivity - 1) / cost per tuple
     * The intuition is that more selective/cheaper conditions should be evaluated
     * first in the AND clause, since FALSE will end the evaluation.
     */
    private Double rankingAnd(RexNode e) {
      Double selectivity = selectivityEstimator.estimateSelectivity(e);
      if (selectivity == null) {
        return null;
      }
      Double costPerTuple = costPerTuple(e);
      if (costPerTuple == null) {
        return null;
      }
      return (selectivity - 1d) / costPerTuple;
    }

    /**
     * Nodes in an OR clause are sorted by a rank value calculated as:
     * rank = (-selectivity) / cost per tuple
     * The intuition is that less selective/cheaper conditions should be evaluated
     * first in the OR clause, since TRUE will end the evaluation.
     */
    private Double rankingOr(RexNode e) {
      Double selectivity = selectivityEstimator.estimateSelectivity(e);
      if (selectivity == null) {
        return null;
      }
      Double costPerTuple = costPerTuple(e);
      if (costPerTuple == null) {
        return null;
      }
      return -selectivity / costPerTuple;
    }

    private Double costPerTuple(RexNode e) {
      return e.accept(new RexFunctionCost());
    }

  }

  /**
   * The cost of a call expression e is computed as:
   * cost(e) = functionCost + sum_1..n(byteSize(o_i) + cost(o_i))
   * with the call having operands i in 1..n.
   */
  private static class RexFunctionCost extends RexVisitorImpl<Double> {

    private RexFunctionCost() {
      super(true);
    }

    @Override
    public Double visitCall(RexCall call) {
      if (!deep) {
        return null;
      }

      Double cost = 0.d;
      for (RexNode operand : call.operands) {
        Double operandCost = operand.accept(this);
        if (operandCost == null) {
          return null;
        }
        cost += operandCost;
        Double size;
        if (operand.isA(SqlKind.LITERAL)) {
          size = HiveRelMdSize.INSTANCE.typeValueSize(operand.getType(),
              ((RexLiteral) operand).getValueAs(Comparable.class));
        } else {
          size = HiveRelMdSize.INSTANCE.averageTypeValueSize(operand.getType());
        }
        if (size == null) {
          return null;
        }
        cost += size;
      }

      return cost + functionCost(call);
    }

    private static Double functionCost(RexCall call) {
      switch (call.getKind()) {
        case EQUALS:
        case NOT_EQUALS:
        case LESS_THAN:
        case GREATER_THAN:
        case LESS_THAN_OR_EQUAL:
        case GREATER_THAN_OR_EQUAL:
        case IS_NOT_NULL:
        case IS_NULL:
        case IS_TRUE:
        case IS_NOT_TRUE:
        case IS_FALSE:
        case IS_NOT_FALSE:
          return 1d;

        case BETWEEN:
          return 3d;

        case IN:
          return 2d * (call.getOperands().size() - 1);

        case AND:
        case OR:
          return 1d * call.getOperands().size();

        case CAST:
          // This heuristic represents that CAST operation is 8 times more expensive
          // than a comparison operation such as EQUALS, NOT_EQUALS, etc.
          return 8d;

        default:
          // By default, we give this heuristic value to unrecognized functions.
          // The idea is that those functions will be more expensive to evaluate
          // than the simple functions considered above.
          // TODO: Add more functions/improve the heuristic after running additional experiments.
          return 32d;
      }
    }

    @Override
    public Double visitInputRef(RexInputRef inputRef) {
      return 0d;
    }

    @Override
    public Double visitFieldAccess(RexFieldAccess fieldAccess) {
      return 0d;
    }

    @Override
    public Double visitLiteral(RexLiteral literal) {
      return 0d;
    }

    @Override
    public Double visitDynamicParam(RexDynamicParam dynamicParam) {
      return 0d;
    }

  }
}
