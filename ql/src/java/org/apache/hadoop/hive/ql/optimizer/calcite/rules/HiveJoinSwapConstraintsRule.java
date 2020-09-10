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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil.RewritablePKFKJoinInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Planner rule that pushes a Join down in a tree past a Join in order
 * to leave non-filtering column appending joins at the top of the plan.
 *
 * <p>Join(Join(X, Y), Z) &rarr; Join(Join(X, Z), Y)
 */
public class HiveJoinSwapConstraintsRule extends RelOptRule {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveJoinSwapConstraintsRule.class);

  public static final HiveJoinSwapConstraintsRule INSTANCE =
      new HiveJoinSwapConstraintsRule(HiveRelFactories.HIVE_BUILDER);


  protected HiveJoinSwapConstraintsRule(RelBuilderFactory relBuilder) {
    super(
        operand(Join.class,
            operand(Join.class, any()),
            operand(RelNode.class, any())),
        relBuilder, "HiveJoinSwapConstraintsRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Join topJoin = call.rel(0);
    final Join bottomJoin = call.rel(1);
    final RexBuilder rexBuilder = topJoin.getCluster().getRexBuilder();

    // 1) Check whether these joins can be swapped.
    if (topJoin.getJoinType().generatesNullsOnLeft()
        || bottomJoin.getJoinType().generatesNullsOnLeft()
        || bottomJoin.isSemiJoin()) {
      // Nothing to do
      return;
    }

    // 2) Check whether the bottom is a non-filtering column appending join.
    // - If the top one is a non-filtering column appending join, we do not
    // trigger the optimization, since we do not want to swap this type of
    // joins.
    // - If the bottom one is not a non-filtering column appending join,
    // we cannot trigger the optimization.
    RewritablePKFKJoinInfo topInfo = HiveRelOptUtil.isRewritablePKFKJoin(
        topJoin, topJoin.getLeft(), topJoin.getRight(), call.getMetadataQuery());
    RewritablePKFKJoinInfo bottomInfo = HiveRelOptUtil.isRewritablePKFKJoin(
        bottomJoin, bottomJoin.getLeft(), bottomJoin.getRight(), call.getMetadataQuery());
    if (topInfo.rewritable || !bottomInfo.rewritable) {
      // Nothing to do
      return;
    }

    // 3) Rewrite.
    // X is the left child of the join below
    // Y is the right child of the join below
    // Z is the right child of the top join
    int nFieldsX = bottomJoin.getLeft().getRowType().getFieldList().size();
    int nFieldsY = bottomJoin.getRight().getRowType().getFieldList().size();
    int nFieldsZ = topJoin.getRight().getRowType().getFieldList().size();
    int nTotalFields = nFieldsX + nFieldsY + nFieldsZ;
    List<RelDataTypeField> fields = new ArrayList<>();

    // create a list of fields for the full join result; note that
    // we can't simply use the fields because the row-type of a
    // semi-join would only include the left hand side fields
    List<RelDataTypeField> joinFields =
        topJoin.getRowType().getFieldList();
    for (int i = 0; i < (nFieldsX + nFieldsY); i++) {
      fields.add(joinFields.get(i));
    }
    joinFields = topJoin.getRight().getRowType().getFieldList();
    for (int i = 0; i < nFieldsZ; i++) {
      fields.add(joinFields.get(i));
    }

    // determine which operands below the join are the actual
    // rels that participate in it
    final Set<Integer> leftKeys = HiveCalciteUtil.getInputRefs(topJoin.getCondition());
    leftKeys.removeIf(i -> i >= topJoin.getLeft().getRowType().getFieldCount());
    int nKeysFromX = 0;
    for (int leftKey : leftKeys) {
      if (leftKey < nFieldsX) {
        nKeysFromX++;
      }
    }
    // the keys must all originate from the left
    if (nKeysFromX != leftKeys.size()) {
      // Nothing to do
      return;
    }

    // need to convert the conditions
    // (X, Y, Z) --> (X, Z, Y)
    int[] adjustments = new int[nTotalFields];
    setJoinAdjustments(
        adjustments,
        nFieldsX,
        nFieldsY,
        nFieldsZ,
        nFieldsZ,
        -nFieldsY);
    final RexNode newBottomCondition =
        topJoin.getCondition().accept(
            new RelOptUtil.RexInputConverter(
                rexBuilder,
                fields,
                adjustments));
    // create the new joins
    final Join newBottomJoin =
        topJoin.copy(
            topJoin.getTraitSet(),
            newBottomCondition,
            bottomJoin.getLeft(),
            topJoin.getRight(),
            topJoin.getJoinType(),
            topJoin.isSemiJoinDone());
    final RexNode newTopCondition;
    if (newBottomJoin.isSemiJoin()) {
      newTopCondition = bottomJoin.getCondition();
    } else {
      newTopCondition =
          bottomJoin.getCondition().accept(
              new RelOptUtil.RexInputConverter(
                  rexBuilder,
                  fields,
                  adjustments));
    }
    final Join newTopJoin =
        bottomJoin.copy(
            bottomJoin.getTraitSet(),
            newTopCondition,
            newBottomJoin,
            bottomJoin.getRight(),
            bottomJoin.getJoinType(),
            bottomJoin.isSemiJoinDone());

    if (newBottomJoin.isSemiJoin()) {
      call.transformTo(newTopJoin);
    } else {
      // need to swap the columns to match the original join
      // (X, Y, Z) --> (X, Z, Y)
      List<RexNode> exprs = new ArrayList<>();
      for (int i = 0; i < nFieldsX; i++) {
        exprs.add(rexBuilder.makeInputRef(newTopJoin, i));
      }
      for (int i = nFieldsX + nFieldsZ; i < topJoin.getRowType().getFieldCount(); i++) {
        exprs.add(rexBuilder.makeInputRef(newTopJoin, i));
      }
      for (int i = nFieldsX; i < nFieldsX + nFieldsZ; i++) {
        exprs.add(rexBuilder.makeInputRef(newTopJoin, i));
      }
      call.transformTo(
          call.builder()
              .push(newTopJoin)
              .project(exprs)
              .build());
    }
  }

  /**
   * Sets an array to reflect how much each index corresponding to a field
   * needs to be adjusted. The array corresponds to fields in a 3-way join
   * between (X, Y, and Z). X remains unchanged, but Y and Z need to be
   * adjusted by some fixed amount as determined by the input.
   *
   * @param adjustments array to be filled out
   * @param nFieldsX    number of fields in X
   * @param nFieldsY    number of fields in Y
   * @param nFieldsZ    number of fields in Z
   * @param adjustY     the amount to adjust Y by
   * @param adjustZ     the amount to adjust Z by
   */
  private void setJoinAdjustments(
      int[] adjustments,
      int nFieldsX,
      int nFieldsY,
      int nFieldsZ,
      int adjustY,
      int adjustZ) {
    for (int i = 0; i < nFieldsX; i++) {
      adjustments[i] = 0;
    }
    for (int i = nFieldsX; i < (nFieldsX + nFieldsY); i++) {
      adjustments[i] = adjustY;
    }
    for (int i = nFieldsX + nFieldsY;
         i < (nFieldsX + nFieldsY + nFieldsZ);
         i++) {
      adjustments[i] = adjustZ;
    }
  }

}
