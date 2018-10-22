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

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The rule can perform two different optimizations.
 * 1) Removes a join if is does not alter the cardinality of the one of its inputs.
 * In particular, this rule is triggered if:
 * - it is a join on PK-FK/UK-FK,
 * - project on top only references columns from the FK side, and
 * - PK/UK side is not filtered
 * It optionally adds an IS NOT NULL filter if any FK column can be nullable
 * 2) Transforms a left/right outer join into an inner join if:
 * - it is a join on PK-FK/UK-FK,
 * - FK is not nullable
 * - PK/UK side is not filtered
 */
public class HiveJoinConstraintsRule extends RelOptRule {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveJoinConstraintsRule.class);

  public static final HiveJoinConstraintsRule INSTANCE =
      new HiveJoinConstraintsRule(HiveRelFactories.HIVE_BUILDER);


  protected HiveJoinConstraintsRule(RelBuilderFactory relBuilder) {
    super(
        operand(Project.class,
            some(operand(Join.class, any()))),
        relBuilder, "HiveJoinConstraintsRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
    List<RexNode> topProjExprs = project.getChildExps();
    Join join = call.rel(1);
    final JoinRelType joinType = join.getJoinType();
    final RelNode leftInput = join.getLeft();
    final RelNode rightInput = join.getRight();
    final RexNode cond = join.getCondition();

    // 1) If it is an inner, check whether project only uses columns from one side.
    // That side will need to be the FK side.
    // If it is a left outer, left will be the FK side.
    // If it is a right outer, right will be the FK side.
    final RelNode fkInput;
    final ImmutableBitSet topRefs =
        RelOptUtil.InputFinder.bits(topProjExprs, null);
    final ImmutableBitSet leftBits =
        ImmutableBitSet.range(leftInput.getRowType().getFieldCount());
    final ImmutableBitSet rightBits =
        ImmutableBitSet.range(leftInput.getRowType().getFieldCount(),
            join.getRowType().getFieldCount());
    // These boolean values represent corresponding left, right input which is potential FK
    boolean leftInputPotentialFK = topRefs.intersects(leftBits);
    boolean rightInputPotentialFK = topRefs.intersects(rightBits);
    if (leftInputPotentialFK && rightInputPotentialFK && joinType == JoinRelType.INNER) {
      // Both inputs are referenced. Before making a decision, try to swap
      // references in join condition if it is an inner join, i.e. if a join
      // condition column is referenced above the join, then we can just
      // reference the column from the other side.
      // For example, given two relations R(a1,a2), S(b1) :
      // SELECT a2, b1 FROM R, S ON R.a1=R.b1 =>
      // SELECT a2, a1 FROM R, S ON R.a1=R.b1
      int joinFieldCount = join.getRowType().getFieldCount();
      Mapping mappingLR = Mappings.create(MappingType.PARTIAL_FUNCTION, joinFieldCount, joinFieldCount);
      Mapping mappingRL = Mappings.create(MappingType.PARTIAL_FUNCTION, joinFieldCount, joinFieldCount);
      for (RexNode conj : RelOptUtil.conjunctions(cond)) {
        if (!conj.isA(SqlKind.EQUALS)) {
          continue;
        }
        RexCall eq = (RexCall) conj;
        RexNode op1 = eq.getOperands().get(0);
        RexNode op2 = eq.getOperands().get(1);
        if (op1 instanceof RexInputRef && op2 instanceof RexInputRef) {
          // Check references
          int ref1 = ((RexInputRef) op1).getIndex();
          int ref2 = ((RexInputRef) op2).getIndex();
          int leftRef = -1;
          int rightRef = -1;
          if (leftBits.get(ref1) && rightBits.get(ref2)) {
            leftRef = ref1;
            rightRef = ref2;
          } else if (rightBits.get(ref1) && leftBits.get(ref2)) {
            leftRef = ref2;
            rightRef = ref1;
          }
          if (leftRef != -1 && rightRef != -1) {
            // We do not add more than one mapping per source
            // as it is useless
            if (mappingLR.getTargetOpt(leftRef) == -1) {
              mappingLR.set(leftRef, rightRef);
            }
            if (mappingRL.getTargetOpt(rightRef) == -1) {
              mappingRL.set(rightRef, leftRef);
            }
          }
        }
      }
      if (mappingLR.size() != 0) {
        // First insert missing elements into the mapping as identity mappings
        for (int i = 0; i < joinFieldCount; i++) {
          if (mappingLR.getTargetOpt(i) == -1) {
            mappingLR.set(i, i);
          }
          if (mappingRL.getTargetOpt(i) == -1) {
            mappingRL.set(i, i);
          }
        }
        // Then, we start by trying to reference only left side in top projections
        List<RexNode> swappedTopProjExprs = topProjExprs.stream()
            .map(projExpr -> projExpr.accept(new RexPermuteInputsShuttle(mappingRL, call.rel(1))))
            .collect(Collectors.toList());
        rightInputPotentialFK = RelOptUtil.InputFinder.bits(swappedTopProjExprs, null).intersects(rightBits);
        if (!rightInputPotentialFK) {
          topProjExprs = swappedTopProjExprs;
        } else {
          // If it did not work, we try to reference only right side in top projections
          swappedTopProjExprs = topProjExprs.stream()
              .map(projExpr -> projExpr.accept(new RexPermuteInputsShuttle(mappingLR, call.rel(1))))
              .collect(Collectors.toList());
          leftInputPotentialFK = RelOptUtil.InputFinder.bits(swappedTopProjExprs, null).intersects(leftBits);
          if (!leftInputPotentialFK) {
            topProjExprs = swappedTopProjExprs;
          }
        }
      }
    } else if (!leftInputPotentialFK && !rightInputPotentialFK) {
      // TODO: There are no references in the project operator above.
      // In this case, we should probably do two passes, one for
      // left as FK and one for right as FK, although it may be expensive.
      // Currently we only assume left as FK
      leftInputPotentialFK = true;
    }

    final Mode mode;
    switch (joinType) {
    case INNER:
      if (leftInputPotentialFK && rightInputPotentialFK) {
        // Bails out as it references columns from both sides (or no columns)
        // and there is nothing to transform
        return;
      }
      fkInput = leftInputPotentialFK ? leftInput : rightInput;
      mode = Mode.REMOVE;
      break;
    case LEFT:
      fkInput = leftInput;
      mode = leftInputPotentialFK && !rightInputPotentialFK ? Mode.REMOVE : Mode.TRANSFORM;
      break;
    case RIGHT:
      fkInput = rightInput;
      mode = !leftInputPotentialFK && rightInputPotentialFK ? Mode.REMOVE : Mode.TRANSFORM;
      break;
    default:
      // Other type, bail out
      return;
    }

    // 2) Check whether this join can be rewritten or removed
    Pair<Boolean, List<RexNode>> r = HiveRelOptUtil.isRewritablePKFKJoin(call.builder(),
        join, leftInput == fkInput, call.getMetadataQuery());

    // 3) If it is the only condition, we can trigger the rewriting
    if (r.left) {
      List<RexNode> nullableNodes = r.right;
      // If we reach here, we trigger the transform
      if (mode == Mode.REMOVE) {
        if (rightInputPotentialFK) {
          // First, if FK is the right input, we need to shift
          nullableNodes = nullableNodes.stream()
              .map(node -> RexUtil.shift(node, 0, -leftInput.getRowType().getFieldCount()))
              .collect(Collectors.toList());
          topProjExprs = topProjExprs.stream()
              .map(node -> RexUtil.shift(node, 0, -leftInput.getRowType().getFieldCount()))
              .collect(Collectors.toList());
        }
        // Fix nullability in references to the input node
        topProjExprs = RexUtil.fixUp(rexBuilder, topProjExprs, RelOptUtil.getFieldTypeList(fkInput.getRowType()));
        // Trigger transformation
        if (nullableNodes.isEmpty()) {
          call.transformTo(call.builder()
              .push(fkInput)
              .project(topProjExprs)
              .convert(project.getRowType(), false)
              .build());
        } else {
          RexNode newFilterCond;
          if (nullableNodes.size() == 1) {
            newFilterCond = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, nullableNodes.get(0));
          } else {
            List<RexNode> isNotNullConds = new ArrayList<>();
            for (RexNode nullableNode : nullableNodes) {
              isNotNullConds.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, nullableNode));
            }
            newFilterCond = rexBuilder.makeCall(SqlStdOperatorTable.AND, isNotNullConds);
          }
          call.transformTo(call.builder()
              .push(fkInput)
              .filter(newFilterCond)
              .project(topProjExprs)
              .convert(project.getRowType(), false)
              .build());
        }
      } else { // Mode.TRANSFORM
        // Trigger transformation
        call.transformTo(call.builder()
            .push(leftInput).push(rightInput)
            .join(JoinRelType.INNER, join.getCondition())
            .convert(call.rel(1).getRowType(), false) // Preserve nullability
            .project(project.getChildExps())
            .build());
      }
    }
  }

  private enum Mode {
    // Removes join operator from the plan
    REMOVE,
    // Transforms LEFT/RIGHT outer join into INNER join
    TRANSFORM
  }
}