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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * Planner rule that pulls up constant keys through a SortLimit operator.
 * 
 * This rule is only applied on SortLimit operators that are not the root
 * of the plan tree. This is done because the interaction of this rule
 * with the AST conversion may cause some optimizations to not kick in
 * e.g. SimpleFetchOptimizer. Nevertheless, this will not have any
 * performance impact in the resulting plans.
 */
public class HiveSortLimitPullUpConstantsRule extends RelOptRule {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveSortLimitPullUpConstantsRule.class);


  public static final HiveSortLimitPullUpConstantsRule INSTANCE =
          new HiveSortLimitPullUpConstantsRule(HiveSortLimit.class,
                  HiveRelFactories.HIVE_BUILDER);

  private HiveSortLimitPullUpConstantsRule(Class<? extends Sort> sortClass,
      RelBuilderFactory relBuilderFactory) {
    super(operand(RelNode.class, unordered(operand(sortClass, any()))), relBuilderFactory, null);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final RelNode parent = call.rel(0);
    final Sort sort = call.rel(1);

    final int count = sort.getInput().getRowType().getFieldCount();
    if (count == 1) {
      // No room for optimization since we cannot convert to an empty
      // Project operator.
      return;
    }

    final RexBuilder rexBuilder = sort.getCluster().getRexBuilder();
    final RelMetadataQuery mq = call.getMetadataQuery();
    final RelOptPredicateList predicates = mq.getPulledUpPredicates(sort.getInput());
    if (predicates == null) {
      return;
    }

    Map<RexNode, RexNode> conditionsExtracted = HiveReduceExpressionsRule.predicateConstants(
            RexNode.class, rexBuilder, predicates);
    Map<RexNode, RexNode> constants = new HashMap<>();
    for (int i = 0; i < count ; i++) {
      RexNode expr = rexBuilder.makeInputRef(sort.getInput(), i);
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
    List<RelDataTypeField> fields = sort.getInput().getRowType().getFieldList();
    List<Pair<RexNode, String>> newChildExprs = new ArrayList<>();
    List<RexNode> topChildExprs = new ArrayList<>();
    List<String> topChildExprsFields = new ArrayList<>();
    for (int i = 0; i < count ; i++) {
      RexNode expr = rexBuilder.makeInputRef(sort.getInput(), i);
      RelDataTypeField field = fields.get(i);
      if (constants.containsKey(expr)) {
        topChildExprs.add(constants.get(expr));
        topChildExprsFields.add(field.getName());
      } else {
        newChildExprs.add(Pair.<RexNode,String>of(expr, field.getName()));
        topChildExprs.add(expr);
        topChildExprsFields.add(field.getName());
      }
    }

    // Update field collations
    final Mappings.TargetMapping mapping =
            RelOptUtil.permutation(Pair.left(newChildExprs), sort.getInput().getRowType()).inverse();
    List<RelFieldCollation> fieldCollations = new ArrayList<>();
    for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
      final int target = mapping.getTargetOpt(fc.getFieldIndex());
      if (target < 0) {
        // It is a constant, we can ignore it
        continue;
      }
      fieldCollations.add(fc.copy(target));
    }

    // Update top Project positions
    topChildExprs = ImmutableList.copyOf(RexUtil.apply(mapping, topChildExprs));

    // Create new Project-Sort-Project sequence
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(sort.getInput());
    relBuilder.project(Pair.left(newChildExprs), Pair.right(newChildExprs));
    final ImmutableList<RexNode> sortFields =
            relBuilder.fields(RelCollations.of(fieldCollations));
    relBuilder.sortLimit(sort.offset == null ? -1 : RexLiteral.intValue(sort.offset),
            sort.fetch == null ? -1 : RexLiteral.intValue(sort.fetch), sortFields);
    // Create top Project fixing nullability of fields
    relBuilder.project(topChildExprs, topChildExprsFields);
    relBuilder.convert(sort.getRowType(), false);

    List<RelNode> inputs = new ArrayList<>();
    for (RelNode child : parent.getInputs()) {
      if (!((HepRelVertex) child).getCurrentRel().equals(sort)) {
        inputs.add(child);
      } else {
        inputs.add(relBuilder.build());
      }
    }
    call.transformTo(parent.copy(parent.getTraitSet(), inputs));
  }

}
