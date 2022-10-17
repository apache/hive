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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.Bug;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * Planner rule that pulls up constants through a Union operator.
 */
public class HiveUnionPullUpConstantsRule extends RelOptRule {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveUnionPullUpConstantsRule.class);


  public static final HiveUnionPullUpConstantsRule INSTANCE =
          new HiveUnionPullUpConstantsRule(HiveUnion.class,
                  HiveRelFactories.HIVE_BUILDER);

  private HiveUnionPullUpConstantsRule(
      Class<? extends Union> unionClass,
      RelBuilderFactory relBuilderFactory) {
    super(operand(unionClass, any()),
            relBuilderFactory, null);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (Bug.CALCITE_5337_FIXED) {
      throw new IllegalStateException("Class redundant when the fix for CALCITE-5337 is merged into Calcite");
    }

    final Union union = call.rel(0);

    final int count = union.getRowType().getFieldCount();
    if (count == 1) {
      // No room for optimization since we cannot create an empty
      // Project operator.
      return;
    }

    final RexBuilder rexBuilder = union.getCluster().getRexBuilder();
    final RelMetadataQuery mq = call.getMetadataQuery();
    final RelOptPredicateList predicates = mq.getPulledUpPredicates(union);
    if (predicates == null) {
      return;
    }

    Map<RexNode, RexNode> constants = new HashMap<>();
    for (int i = 0; i < count ; i++) {
      RexNode expr = rexBuilder.makeInputRef(union, i);
      if (predicates.constantMap.containsKey(expr)) {
        constants.put(expr, predicates.constantMap.get(expr));
      }
    }

    // None of the expressions are constant. Nothing to do.
    if (constants.isEmpty()) {
      return;
    }

    // Create expressions for Project operators before and after the Union
    List<RelDataTypeField> fields = union.getRowType().getFieldList();
    List<RexNode> topChildExprs = new ArrayList<>();
    List<String> topChildExprsFields = new ArrayList<>();
    List<RexNode> refs = new ArrayList<>();
    ImmutableBitSet.Builder refsIndexBuilder = ImmutableBitSet.builder();
    for (int i = 0; i < count ; i++) {
      RexNode expr = rexBuilder.makeInputRef(union, i);
      RelDataTypeField field = fields.get(i);
      if (constants.containsKey(expr)) {
        if (constants.get(expr).getType().equals(field.getType())) {
          topChildExprs.add(constants.get(expr));
        } else {
          topChildExprs.add(rexBuilder.makeCast(field.getType(), constants.get(expr), true));
        }
        topChildExprsFields.add(field.getName());
      } else {
        topChildExprs.add(expr);
        topChildExprsFields.add(field.getName());
        refs.add(expr);
        refsIndexBuilder.set(i);
      }
    }
    ImmutableBitSet refsIndex = refsIndexBuilder.build();

    // Update top Project positions
    final Mappings.TargetMapping mapping =
            RelOptUtil.permutation(refs, union.getInput(0).getRowType()).inverse();
    topChildExprs = ImmutableList.copyOf(RexUtil.apply(mapping, topChildExprs));

    // Create new Project-Union-Project sequences
    final RelBuilder relBuilder = call.builder();
    for (int i = 0; i < union.getInputs().size() ; i++) {
      RelNode input = union.getInput(i);
      List<Pair<RexNode, String>> newChildExprs = new ArrayList<>();
      for (int j = 0; j < refsIndex.cardinality(); j++) {
        int pos = refsIndex.nth(j);
        newChildExprs.add(Pair.of(rexBuilder.makeInputRef(input, pos),
            input.getRowType().getFieldList().get(pos).getName()));
      }
      if (newChildExprs.isEmpty()) {
        // At least a single item in project is required.
        newChildExprs.add(Pair.of(topChildExprs.get(0), topChildExprsFields.get(0)));
      }
      // Add the input with project on top
      relBuilder.push(input);
      relBuilder.project(Pair.left(newChildExprs), Pair.right(newChildExprs));
    }
    relBuilder.union(union.all, union.getInputs().size());
    // Create top Project fixing nullability of fields
    relBuilder.project(topChildExprs, topChildExprsFields);
    relBuilder.convert(union.getRowType(), false);

    call.transformTo(relBuilder.build());
  }

}
