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
    final RelNode nonFkInput;
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
      nonFkInput = leftInputPotentialFK ? rightInput : leftInput;
      mode = Mode.REMOVE;
      break;
    case LEFT:
      fkInput = leftInput;
      nonFkInput = rightInput;
      mode = leftInputPotentialFK && !rightInputPotentialFK ? Mode.REMOVE : Mode.TRANSFORM;
      break;
    case RIGHT:
      fkInput = rightInput;
      nonFkInput = leftInput;
      mode = !leftInputPotentialFK && rightInputPotentialFK ? Mode.REMOVE : Mode.TRANSFORM;
      break;
    default:
      // Other type, bail out
      return;
    }

    // 2) Check whether there is any filtering condition on the
    // non-FK side. Basically we check whether the operators
    // below altered the PK cardinality in any way
    final RelMetadataQuery mq = call.getMetadataQuery();
    if (HiveRelOptUtil.isRowFilteringPlan(mq, nonFkInput)) {
      return;
    }

    // 3) Check whether there is an FK relationship
    if (join.getJoinType() != JoinRelType.INNER) {
      // If it is not an inner, we transform it as the metadata
      // providers for expressions do not pull information through
      // outer join (as it would not be correct)
      join = (Join) call.builder()
          .push(leftInput).push(rightInput)
          .join(JoinRelType.INNER, cond)
          .build();
    }
    final Map<RexTableInputRef, RexNode> refToRex = new HashMap<>();
    final EquivalenceClasses ec = new EquivalenceClasses();
    for (RexNode conj : RelOptUtil.conjunctions(cond)) {
      if (!conj.isA(SqlKind.EQUALS)) {
        // Not an equality, we bail out
        return;
      }
      RexCall equiCond = (RexCall) conj;
      RexNode eqOp1 = equiCond.getOperands().get(0);
      Set<RexNode> eqOp1ExprsLineage = mq.getExpressionLineage(join, eqOp1);
      if (eqOp1ExprsLineage == null) {
        // Cannot be mapped, bail out
        return;
      }
      RexNode eqOp2 = equiCond.getOperands().get(1);
      Set<RexNode> eqOp2ExprsLineage = mq.getExpressionLineage(join, eqOp2);
      if (eqOp2ExprsLineage == null) {
        // Cannot be mapped, bail out
        return;
      }
      List<RexTableInputRef> eqOp2ExprsFiltered = null;
      for (RexNode eqOpExprLineage1 : eqOp1ExprsLineage) {
        RexTableInputRef inputRef1 = extractTableInputRef(eqOpExprLineage1);
        if (inputRef1 == null) {
          // Bail out as this condition could not be map into an input reference
          return;
        }
        refToRex.put(inputRef1, eqOp1);
        if (eqOp2ExprsFiltered == null) {
          // First iteration
          eqOp2ExprsFiltered = new ArrayList<>();
          for (RexNode eqOpExprLineage2 : eqOp2ExprsLineage) {
            RexTableInputRef inputRef2 = extractTableInputRef(eqOpExprLineage2);
            if (inputRef2 == null) {
              // Bail out as this condition could not be map into an input reference
              return;
            }
            // Add to list of expressions for follow-up iterations
            eqOp2ExprsFiltered.add(inputRef2);
            // Add to equivalence classes and backwards mapping
            ec.addEquivalenceClass(inputRef1, inputRef2);
            refToRex.put(inputRef2, eqOp2);
          }
        } else {
          // Rest of iterations, only adding, no checking
          for (RexTableInputRef inputRef2 : eqOp2ExprsFiltered) {
            ec.addEquivalenceClass(inputRef1, inputRef2);
          }
        }
      }
    }
    if (ec.getEquivalenceClassesMap().isEmpty()) {
      // This may be a cartesian product, we bail out
      return;
    }

    // 4) Gather all tables from the FK side and the table from the
    // non-FK side
    final Set<RelTableRef> leftTables = mq.getTableReferences(leftInput);
    final Set<RelTableRef> rightTables =
        Sets.difference(mq.getTableReferences(join), mq.getTableReferences(leftInput));
    final Set<RelTableRef> fkTables = leftInputPotentialFK ? leftTables : rightTables;
    final Set<RelTableRef> nonFkTables = leftInputPotentialFK ? rightTables : leftTables;
    assert nonFkTables.size() == 1;
    final RelTableRef nonFkTable = nonFkTables.iterator().next();
    final List<String> nonFkTableQName = nonFkTable.getQualifiedName();

    // 5) For each table, check whether there is a matching on the non-FK side.
    // If there is and it is the only condition, we are ready to transform
    boolean canBeRewritten = false;
    List<RexNode> nullableNodes = new ArrayList<>();
    for (RelTableRef tRef : fkTables) {
      List<RelReferentialConstraint> constraints = tRef.getTable().getReferentialConstraints();
      for (RelReferentialConstraint constraint : constraints) {
        if (constraint.getTargetQualifiedName().equals(nonFkTableQName)) {
          EquivalenceClasses ecT = EquivalenceClasses.copy(ec);
          boolean allContained = true;
          for (int pos = 0; pos < constraint.getNumColumns(); pos++) {
            int foreignKeyPos = constraint.getColumnPairs().get(pos).source;
            RelDataType foreignKeyColumnType =
                tRef.getTable().getRowType().getFieldList().get(foreignKeyPos).getType();
            RexTableInputRef foreignKeyColumnRef =
                RexTableInputRef.of(tRef, foreignKeyPos, foreignKeyColumnType);
            if (foreignKeyColumnType.isNullable()) {
              if (joinType == JoinRelType.INNER) {
                // If it is nullable and it is an INNER, we just need a IS NOT NULL filter
                RexNode originalCondOp = refToRex.get(foreignKeyColumnRef);
                assert originalCondOp != null;
                nullableNodes.add(originalCondOp);
              } else {
                // If it is nullable and this is not an INNER, we cannot execute any transformation
                allContained = false;
                break;
              }
            }
            int uniqueKeyPos = constraint.getColumnPairs().get(pos).target;
            RexTableInputRef uniqueKeyColumnRef = RexTableInputRef.of(nonFkTable, uniqueKeyPos,
                nonFkTable.getTable().getRowType().getFieldList().get(uniqueKeyPos).getType());
            if (ecT.getEquivalenceClassesMap().containsKey(uniqueKeyColumnRef) &&
                ecT.getEquivalenceClassesMap().get(uniqueKeyColumnRef).contains(foreignKeyColumnRef)) {
              // Remove this condition from eq classes as we have checked that it is present
              // in the join condition
              ecT.getEquivalenceClassesMap().get(uniqueKeyColumnRef).remove(foreignKeyColumnRef);
              if (ecT.getEquivalenceClassesMap().get(uniqueKeyColumnRef).size() == 1) { // self
                ecT.getEquivalenceClassesMap().remove(uniqueKeyColumnRef);
              }
              ecT.getEquivalenceClassesMap().get(foreignKeyColumnRef).remove(uniqueKeyColumnRef);
              if (ecT.getEquivalenceClassesMap().get(foreignKeyColumnRef).size() == 1) { // self
                ecT.getEquivalenceClassesMap().remove(foreignKeyColumnRef);
              }
            } else {
              // No relationship, we cannot do anything
              allContained = false;
              break;
            }
          }
          if (allContained && ecT.getEquivalenceClassesMap().isEmpty()) {
            // We made it
            canBeRewritten = true;
            break;
          }
        }
      }
    }

    // 6) If it is the only condition, we can trigger the rewriting
    if (canBeRewritten) {
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
            .project(project.getChildExps())
            .build());
      }
    }
  }

  private static RexTableInputRef extractTableInputRef(RexNode node) {
    RexTableInputRef ref = null;
    if (node instanceof RexTableInputRef) {
      ref = (RexTableInputRef) node;
    } else if (RexUtil.isLosslessCast(node) &&
        ((RexCall) node).getOperands().get(0) instanceof RexTableInputRef) {
      ref = (RexTableInputRef) ((RexCall) node).getOperands().get(0);
    }
    return ref;
  }

  /**
   * Class representing an equivalence class, i.e., a set of equivalent columns
   *
   * TODO: This is a subset of a private class in materialized view rewriting
   * in Calcite. It should be moved to its own class in Calcite so it can be
   * accessible here.
   */
  private static class EquivalenceClasses {

    private final Map<RexTableInputRef, Set<RexTableInputRef>> nodeToEquivalenceClass;

    protected EquivalenceClasses() {
      nodeToEquivalenceClass = new HashMap<>();
    }

    protected void addEquivalenceClass(RexTableInputRef p1, RexTableInputRef p2) {
      Set<RexTableInputRef> c1 = nodeToEquivalenceClass.get(p1);
      Set<RexTableInputRef> c2 = nodeToEquivalenceClass.get(p2);
      if (c1 != null && c2 != null) {
        // Both present, we need to merge
        if (c1.size() < c2.size()) {
          // We swap them to merge
          Set<RexTableInputRef> c2Temp = c2;
          c2 = c1;
          c1 = c2Temp;
        }
        for (RexTableInputRef newRef : c2) {
          c1.add(newRef);
          nodeToEquivalenceClass.put(newRef, c1);
        }
      } else if (c1 != null) {
        // p1 present, we need to merge into it
        c1.add(p2);
        nodeToEquivalenceClass.put(p2, c1);
      } else if (c2 != null) {
        // p2 present, we need to merge into it
        c2.add(p1);
        nodeToEquivalenceClass.put(p1, c2);
      } else {
        // None are present, add to same equivalence class
        Set<RexTableInputRef> equivalenceClass = new LinkedHashSet<>();
        equivalenceClass.add(p1);
        equivalenceClass.add(p2);
        nodeToEquivalenceClass.put(p1, equivalenceClass);
        nodeToEquivalenceClass.put(p2, equivalenceClass);
      }
    }

    protected Map<RexTableInputRef, Set<RexTableInputRef>> getEquivalenceClassesMap() {
      return nodeToEquivalenceClass;
    }

    protected static EquivalenceClasses copy(EquivalenceClasses ec) {
      final EquivalenceClasses newEc = new EquivalenceClasses();
      for (Entry<RexTableInputRef, Set<RexTableInputRef>> e : ec.nodeToEquivalenceClass.entrySet()) {
        newEc.nodeToEquivalenceClass.put(e.getKey(), Sets.newLinkedHashSet(e.getValue()));
      }
      return newEc;
    }
  }

  private enum Mode {
    // Removes join operator from the plan
    REMOVE,
    // Transforms LEFT/RIGHT outer join into INNER join
    TRANSFORM
  }
}