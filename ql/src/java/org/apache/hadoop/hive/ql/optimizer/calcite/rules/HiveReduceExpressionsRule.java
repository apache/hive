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

import java.util.List;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Collection of planner rules that apply various simplifying transformations on
 * RexNode trees. Currently, there are two transformations:
 *
 * <ul>
 * <li>Constant reduction, which evaluates constant subtrees, replacing them
 * with a corresponding RexLiteral
 * <li>Removal of redundant casts, which occurs when the argument into the cast
 * is the same as the type of the resulting cast expression
 * </ul>
 */
public abstract class HiveReduceExpressionsRule extends ReduceExpressionsRule {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveReduceExpressionsRule.class);

  //~ Static fields/initializers ---------------------------------------------

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter}.
   */
  public static final ReduceExpressionsRule FILTER_INSTANCE =
          new FilterReduceExpressionsRule(HiveFilter.class, HiveRelFactories.HIVE_BUILDER);

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject}.
   */
  public static final ReduceExpressionsRule PROJECT_INSTANCE =
      new ProjectReduceExpressionsRule(HiveProject.class, HiveRelFactories.HIVE_BUILDER);

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin}.
   */
  public static final ReduceExpressionsRule JOIN_INSTANCE =
      new JoinReduceExpressionsRule(HiveJoin.class, false, HiveRelFactories.HIVE_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a HiveReduceExpressionsRule.
   *
   * @param clazz class of rels to which this rule should apply
   */
  protected HiveReduceExpressionsRule(Class<? extends RelNode> clazz,
      RelBuilderFactory relBuilderFactory, String desc) {
    super(clazz, relBuilderFactory, desc);
  }

  /**
   * Rule that reduces constants inside a {@link org.apache.calcite.rel.core.Filter}.
   * If the condition is a constant, the filter is removed (if TRUE) or replaced with
   * an empty {@link org.apache.calcite.rel.core.Values} (if FALSE or NULL).
   */
  public static class FilterReduceExpressionsRule extends ReduceExpressionsRule {

    public FilterReduceExpressionsRule(Class<? extends Filter> filterClass,
        RelBuilderFactory relBuilderFactory) {
      super(filterClass, relBuilderFactory, "ReduceExpressionsRule(Filter)");
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      final List<RexNode> expList =
          Lists.newArrayList(filter.getCondition());
      RexNode newConditionExp;
      boolean reduced;
      final RelMetadataQuery mq = call.getMetadataQuery();
      final RelOptPredicateList predicates =
          mq.getPulledUpPredicates(filter.getInput());
      if (reduceExpressions(filter, expList, predicates, true, false)) {
        assert expList.size() == 1;
        newConditionExp = expList.get(0);
        reduced = true;
      } else {
        // No reduction, but let's still test the original
        // predicate to see if it was already a constant,
        // in which case we don't need any runtime decision
        // about filtering.
        newConditionExp = filter.getCondition();
        reduced = false;
      }

      // Even if no reduction, let's still test the original
      // predicate to see if it was already a constant,
      // in which case we don't need any runtime decision
      // about filtering.
      if (newConditionExp.isAlwaysTrue()) {
        call.transformTo(
            filter.getInput());
      } else if (reduced) {
        if (RexUtil.isNullabilityCast(filter.getCluster().getTypeFactory(),
            newConditionExp)) {
          newConditionExp = ((RexCall) newConditionExp).getOperands().get(0);
        }
        // reduce might end up creating an expression with null type
        // e.g condition(null = null) is reduced to condition (null) with null type
        // since this is a condition which will always be boolean type we cast it to
        // boolean type
        if(newConditionExp.getType().getSqlTypeName() == SqlTypeName.NULL) {
          newConditionExp = call.builder().cast(newConditionExp, SqlTypeName.BOOLEAN);
        }
        call.transformTo(call.builder().
            push(filter.getInput()).filter(newConditionExp).build());
      } else {
        if (newConditionExp instanceof RexCall) {
          RexCall rexCall = (RexCall) newConditionExp;
          boolean reverse = rexCall.getKind() == SqlKind.NOT;
          if (reverse) {
            if (!(rexCall.getOperands().get(0) instanceof RexCall)) {
              // If child is not a RexCall instance, we can bail out
              return;
            }
            rexCall = (RexCall) rexCall.getOperands().get(0);
          }
          reduceNotNullableFilter(call, filter, rexCall, reverse);
        }
        return;
      }

      // New plan is absolutely better than old plan.
      call.getPlanner().setImportance(filter, 0.0);
    }

    /**
     * For static schema systems, a filter that is always false or null can be
     * replaced by a values operator that produces no rows, as the schema
     * information can just be taken from the input Rel. In dynamic schema
     * environments, the filter might have an unknown input type, in these cases
     * they must define a system specific alternative to a Values operator, such
     * as inserting a limit 0 instead of a filter on top of the original input.
     *
     * <p>The default implementation of this method is to call
     * {@link RelBuilder#empty}, which for the static schema will be optimized
     * to an empty
     * {@link org.apache.calcite.rel.core.Values}.
     *
     * @param input rel to replace, assumes caller has already determined
     *              equivalence to Values operation for 0 records or a
     *              false filter.
     * @return equivalent but less expensive replacement rel
     */
    protected RelNode createEmptyRelOrEquivalent(RelOptRuleCall call, Filter input) {
      return call.builder().push(input).empty().build();
    }

    private void reduceNotNullableFilter(
        RelOptRuleCall call,
        Filter filter,
        RexCall rexCall,
        boolean reverse) {
      // If the expression is a IS [NOT] NULL on a non-nullable
      // column, then we can either remove the filter or replace
      // it with an Empty.
      boolean alwaysTrue;
      switch (rexCall.getKind()) {
      case IS_NULL:
      case IS_UNKNOWN:
        alwaysTrue = false;
        break;
      case IS_NOT_NULL:
        alwaysTrue = true;
        break;
      default:
        return;
      }
      if (reverse) {
        alwaysTrue = !alwaysTrue;
      }
      RexNode operand = rexCall.getOperands().get(0);
      if (operand instanceof RexInputRef) {
        RexInputRef inputRef = (RexInputRef) operand;
        if (!inputRef.getType().isNullable()) {
          if (alwaysTrue) {
            call.transformTo(filter.getInput());
          } else {
            call.transformTo(createEmptyRelOrEquivalent(call, filter));
          }
        }
      }
    }
  }

}

// End HiveReduceExpressionsRule.java
