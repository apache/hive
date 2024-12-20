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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;

import com.google.common.collect.ImmutableList;

/**
 * Planner rule that pulls up constant keys through a SortLimit or SortExchange operator.
 * <p>
 * This rule is only applied on SortLimit operators that are not the root
 * of the plan tree. This is done because the interaction of this rule
 * with the AST conversion may cause some optimizations to not kick in
 * e.g. SimpleFetchOptimizer. Nevertheless, this will not have any
 * performance impact in the resulting plans.
 */
public final class HiveSortPullUpConstantsRule {

  private HiveSortPullUpConstantsRule() {}

  public static final HiveSortPullUpConstantsRuleBase<HiveSortLimit> SORT_LIMIT_INSTANCE =
      new HiveSortLimitPullUpConstantsRule();

  private static final class HiveSortLimitPullUpConstantsRule
      extends HiveSortPullUpConstantsRuleBase<HiveSortLimit> {

    private HiveSortLimitPullUpConstantsRule() {
      super(HiveSortLimit.class);
    }

    @Override
    protected void buildSort(RelBuilder relBuilder, HiveSortLimit sortNode, Mappings.TargetMapping mapping) {
      List<RelFieldCollation> fieldCollations = applyToFieldCollations(sortNode.getCollation(), mapping);
      final ImmutableList<RexNode> sortFields =
          relBuilder.fields(RelCollations.of(fieldCollations));
      relBuilder.sortLimit(sortNode.offset == null ? -1 : RexLiteral.intValue(sortNode.offset),
          sortNode.fetch == null ? -1 : RexLiteral.intValue(sortNode.fetch), sortFields);
    }
  }

  public static final HiveSortExchangePullUpConstantsRule SORT_EXCHANGE_INSTANCE =
      new HiveSortExchangePullUpConstantsRule();

  private static final class HiveSortExchangePullUpConstantsRule
      extends HiveSortPullUpConstantsRuleBase<HiveSortExchange> {

    private HiveSortExchangePullUpConstantsRule() {
      super(HiveSortExchange.class);
    }

    @Override
    protected void buildSort(RelBuilder relBuilder, HiveSortExchange sortNode, Mappings.TargetMapping mapping) {
      List<RelFieldCollation> fieldCollations = applyToFieldCollations(sortNode.getCollation(), mapping);
      RelDistribution distribution = sortNode.getDistribution().apply(mapping);
      relBuilder.sortExchange(distribution, RelCollations.of(fieldCollations));
    }
  }


  private abstract static class HiveSortPullUpConstantsRuleBase<T extends SingleRel> extends RelOptRule {

    protected HiveSortPullUpConstantsRuleBase(Class<T> sortClass) {
      super(operand(RelNode.class, unordered(operand(sortClass, any()))), HiveRelFactories.HIVE_BUILDER, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final RelNode parent = call.rel(0);
      final T sortNode = call.rel(1);

      final int count = sortNode.getInput().getRowType().getFieldCount();
      if (count == 1) {
        // No room for optimization since we cannot convert to an empty
        // Project operator.
        return;
      }

      final RexBuilder rexBuilder = sortNode.getCluster().getRexBuilder();
      final RelMetadataQuery mq = call.getMetadataQuery();
      final RelOptPredicateList predicates = mq.getPulledUpPredicates(sortNode.getInput());
      if (predicates == null) {
        return;
      }

      Map<RexNode, RexNode> conditionsExtracted =
          RexUtil.predicateConstants(RexNode.class, rexBuilder, predicates.pulledUpPredicates);
      Map<RexNode, RexNode> constants = new HashMap<>();
      for (int i = 0; i < count; i++) {
        RexNode expr = rexBuilder.makeInputRef(sortNode.getInput(), i);
        if (conditionsExtracted.containsKey(expr)) {
          constants.put(expr, conditionsExtracted.get(expr));
        }
      }

      // None of the expressions are constant. Nothing to do.
      if (constants.isEmpty()) {
        return;
      }

      if (count == constants.size()) {
        // At least a single item in project is required.
        constants.remove(constants.keySet().iterator().next());
      }

      // Create expressions for Project operators before and after the Sort
      List<RelDataTypeField> fields = sortNode.getInput().getRowType().getFieldList();
      List<Pair<RexNode, String>> newChildExprs = new ArrayList<>();
      List<RexNode> topChildExprs = new ArrayList<>();
      List<String> topChildExprsFields = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        RexNode expr = rexBuilder.makeInputRef(sortNode.getInput(), i);
        RelDataTypeField field = fields.get(i);
        if (constants.containsKey(expr)) {
          if (constants.get(expr).getType().equals(field.getType())) {
            topChildExprs.add(constants.get(expr));
          } else {
            topChildExprs.add(rexBuilder.makeCast(field.getType(), constants.get(expr), true));
          }
          topChildExprsFields.add(field.getName());
        } else {
          newChildExprs.add(Pair.of(expr, field.getName()));
          topChildExprs.add(expr);
          topChildExprsFields.add(field.getName());
        }
      }

      // Update field collations
      final Mappings.TargetMapping mapping =
          RelOptUtil.permutation(Pair.left(newChildExprs), sortNode.getInput().getRowType()).inverse();

      // Update top Project positions
      topChildExprs = ImmutableList.copyOf(RexUtil.apply(mapping, topChildExprs));

      // Create new Project-Sort-Project sequence
      final RelBuilder relBuilder = call.builder();
      relBuilder.push(sortNode.getInput());
      relBuilder.project(Pair.left(newChildExprs), Pair.right(newChildExprs));
      buildSort(relBuilder, sortNode, mapping);
      // Create top Project fixing nullability of fields
      relBuilder.project(topChildExprs, topChildExprsFields);
      relBuilder.convert(sortNode.getRowType(), false);

      List<RelNode> inputs = new ArrayList<>();
      for (RelNode child : parent.getInputs()) {
        if (!((HepRelVertex) child).getCurrentRel().equals(sortNode)) {
          inputs.add(child);
        } else {
          inputs.add(relBuilder.build());
        }
      }
      call.transformTo(parent.copy(parent.getTraitSet(), inputs));
    }

    protected List<RelFieldCollation> applyToFieldCollations(
        RelCollation relCollation, Mappings.TargetMapping mapping) {
      List<RelFieldCollation> fieldCollations = new ArrayList<>();
      for (RelFieldCollation fc : relCollation.getFieldCollations()) {
        final int target = mapping.getTargetOpt(fc.getFieldIndex());
        if (target < 0) {
          // It is a constant, we can ignore it
          continue;
        }
        fieldCollations.add(fc.withFieldIndex(target));
      }
      return fieldCollations;
    }

    protected abstract void buildSort(RelBuilder relBuilder, T sortNode, Mappings.TargetMapping mapping);
  }
}
