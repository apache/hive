/**
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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;

import com.google.common.collect.ImmutableList;


/**
 * This class infers the order in Aggregate columns and the order of conjuncts
 * in a Join condition that might be more beneficial to avoid additional sort
 * stages. The only visible change is that order of join conditions might change.
 * Further, Aggregate operators might get annotated with order in which Aggregate
 * columns should be generated when we transform the operator tree into AST or
 * Hive operator tree.
 */
public class HiveRelColumnsAlignment implements ReflectiveVisitor {

  private final ReflectUtil.MethodDispatcher<RelNode> alignDispatcher;
  private final RelBuilder relBuilder;


  /**
   * Creates a HiveRelColumnsAlignment.
   */
  public HiveRelColumnsAlignment(RelBuilder relBuilder) {
    this.relBuilder = relBuilder;
    this.alignDispatcher =
        ReflectUtil.createMethodDispatcher(
            RelNode.class,
            this,
            "align",
            RelNode.class,
            List.class);
  }


  /**
   * Execute the logic in this class. In particular, make a top-down traversal of the tree
   * and annotate and recreate appropiate operators.
   */
  public RelNode align(RelNode root) {
    final RelNode newRoot = dispatchAlign(root, ImmutableList.<RelFieldCollation>of());
    return newRoot;
  }

  protected final RelNode dispatchAlign(RelNode node, List<RelFieldCollation> collations) {
    return alignDispatcher.invoke(node, collations);
  }

  public RelNode align(Aggregate rel, List<RelFieldCollation> collations) {
    // 1) We extract the group by positions that are part of the collations and
    // sort them so they respect it
    LinkedHashSet<Integer> aggregateColumnsOrder = new LinkedHashSet<>();
    ImmutableList.Builder<RelFieldCollation> propagateCollations = ImmutableList.builder();
    if (!rel.indicator && !collations.isEmpty()) {
      for (RelFieldCollation c : collations) {
        if (c.getFieldIndex() < rel.getGroupCount()) {
          // Group column found
          if (aggregateColumnsOrder.add(c.getFieldIndex())) {
            propagateCollations.add(c.copy(rel.getGroupSet().nth(c.getFieldIndex())));
          }
        }
      }
    }
    for (int i = 0; i < rel.getGroupCount(); i++) {
      if (!aggregateColumnsOrder.contains(i)) {
        // Not included in the input collations, but can be propagated as this Aggregate
        // will enforce it
        propagateCollations.add(new RelFieldCollation(rel.getGroupSet().nth(i)));
      }
    }

    // 2) We propagate
    final RelNode child = dispatchAlign(rel.getInput(), propagateCollations.build());

    // 3) We annotate the Aggregate operator with this info
    final HiveAggregate newAggregate = (HiveAggregate) rel.copy(rel.getTraitSet(),
            ImmutableList.of(child));
    newAggregate.setAggregateColumnsOrder(aggregateColumnsOrder);
    return newAggregate;
  }

  public RelNode align(Join rel, List<RelFieldCollation> collations) {
    ImmutableList.Builder<RelFieldCollation> propagateCollationsLeft = ImmutableList.builder();
    ImmutableList.Builder<RelFieldCollation> propagateCollationsRight = ImmutableList.builder();
    final int nLeftColumns = rel.getLeft().getRowType().getFieldList().size();
    Map<Integer,RexNode> idxToConjuncts = new HashMap<>();
    Map<Integer,Integer> refToRef = new HashMap<>();
    // 1) We extract the conditions that can be useful
    List<RexNode> conjuncts = new ArrayList<>();
    List<RexNode> otherConjuncts = new ArrayList<>();
    for (RexNode conj : RelOptUtil.conjunctions(rel.getCondition())) {
      if (conj.getKind() != SqlKind.EQUALS) {
        otherConjuncts.add(conj);
        continue;
      }
      // TODO: Currently we only support EQUAL operator on two references.
      // We might extend the logic to support other (order-preserving)
      // UDFs here.
      RexCall equals = (RexCall) conj;
      if (!(equals.getOperands().get(0) instanceof RexInputRef) ||
              !(equals.getOperands().get(1) instanceof RexInputRef)) {
        otherConjuncts.add(conj);
        continue;
      }
      RexInputRef ref0 = (RexInputRef) equals.getOperands().get(0);
      RexInputRef ref1 = (RexInputRef) equals.getOperands().get(1);
      if ((ref0.getIndex() < nLeftColumns && ref1.getIndex() >= nLeftColumns) ||
              (ref1.getIndex() < nLeftColumns && ref0.getIndex() >= nLeftColumns)) {
        // We made sure the references are for different join inputs
        idxToConjuncts.put(ref0.getIndex(), equals);
        idxToConjuncts.put(ref1.getIndex(), equals);
        refToRef.put(ref0.getIndex(), ref1.getIndex());
        refToRef.put(ref1.getIndex(), ref0.getIndex());
      } else {
        otherConjuncts.add(conj);
      }
    }

    // 2) We extract the collation for this operator and the collations
    // that we will propagate to the inputs of the join
    for (RelFieldCollation c : collations) {
      RexNode equals = idxToConjuncts.get(c.getFieldIndex());
      if (equals != null) {
        conjuncts.add(equals);
        idxToConjuncts.remove(c.getFieldIndex());
        idxToConjuncts.remove(refToRef.get(c.getFieldIndex()));
        if (c.getFieldIndex() < nLeftColumns) {
          propagateCollationsLeft.add(c.copy(c.getFieldIndex()));
          propagateCollationsRight.add(c.copy(refToRef.get(c.getFieldIndex()) - nLeftColumns));
        } else {
          propagateCollationsLeft.add(c.copy(refToRef.get(c.getFieldIndex())));
          propagateCollationsRight.add(c.copy(c.getFieldIndex() - nLeftColumns));
        }
      }
    }
    final Set<RexNode> visited = new HashSet<>();
    for (Entry<Integer,RexNode> e : idxToConjuncts.entrySet()) {
      if (visited.add(e.getValue())) {
        // Not included in the input collations, but can be propagated as this Join
        // might enforce it
        conjuncts.add(e.getValue());
        if (e.getKey() < nLeftColumns) {
          propagateCollationsLeft.add(new RelFieldCollation(e.getKey()));
          propagateCollationsRight.add(new RelFieldCollation(refToRef.get(e.getKey()) - nLeftColumns));
        } else {
          propagateCollationsLeft.add(new RelFieldCollation(refToRef.get(e.getKey())));
          propagateCollationsRight.add(new RelFieldCollation(e.getKey() - nLeftColumns));
        }
      }
    }
    conjuncts.addAll(otherConjuncts);

    // 3) We propagate
    final RelNode newLeftInput = dispatchAlign(rel.getLeft(), propagateCollationsLeft.build());
    final RelNode newRightInput = dispatchAlign(rel.getRight(), propagateCollationsRight.build());

    // 4) We change the Join operator to reflect this info
    final RelNode newJoin = rel.copy(rel.getTraitSet(), RexUtil.composeConjunction(
            relBuilder.getRexBuilder(), conjuncts, false), newLeftInput, newRightInput,
            rel.getJoinType(), rel.isSemiJoinDone());
    return newJoin;
  }

  public RelNode align(SetOp rel, List<RelFieldCollation> collations) {
    ImmutableList.Builder<RelNode> newInputs = new ImmutableList.Builder<>();
    for (RelNode input : rel.getInputs()) {
      newInputs.add(dispatchAlign(input, collations));
    }
    return rel.copy(rel.getTraitSet(), newInputs.build());
  }

  public RelNode align(Project rel, List<RelFieldCollation> collations) {
    // 1) We extract the collations indices
    boolean containsWindowing = false;
    for (RexNode childExp : rel.getChildExps()) {
      if (childExp instanceof RexOver) {
        // TODO: support propagation for partitioning/ordering in windowing
        containsWindowing = true;
        break;
      }
    }
    ImmutableList.Builder<RelFieldCollation> propagateCollations = ImmutableList.builder();
    if (!containsWindowing) {
      for (RelFieldCollation c : collations) {
        RexNode rexNode = rel.getChildExps().get(c.getFieldIndex());
        if (rexNode instanceof RexInputRef) {
          int newIdx = ((RexInputRef) rexNode).getIndex();
          propagateCollations.add(c.copy((newIdx)));
        }
      }
    }
    // 2) We propagate
    final RelNode child = dispatchAlign(rel.getInput(), propagateCollations.build());
    // 3) Return new Project
    return rel.copy(rel.getTraitSet(), ImmutableList.of(child));
  }

  public RelNode align(Filter rel, List<RelFieldCollation> collations) {
    final RelNode child = dispatchAlign(rel.getInput(), collations);
    return rel.copy(rel.getTraitSet(), ImmutableList.of(child));
  }

  public RelNode align(Sort rel, List<RelFieldCollation> collations) {
    final RelNode child = dispatchAlign(rel.getInput(), rel.collation.getFieldCollations());
    return rel.copy(rel.getTraitSet(), ImmutableList.of(child));
  }

  // Catch-all rule when none of the others apply.
  public RelNode align(RelNode rel, List<RelFieldCollation> collations) {
    ImmutableList.Builder<RelNode> newInputs = new ImmutableList.Builder<>();
    for (RelNode input : rel.getInputs()) {
      newInputs.add(dispatchAlign(input, ImmutableList.<RelFieldCollation>of()));
    }
    return rel.copy(rel.getTraitSet(), newInputs.build());
  }

}
