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
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveMultiJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Rule that merges a join with multijoin/join children if
 * the equi compared the same set of input columns.
 */
public class HiveJoinToMultiJoinRule extends RelOptRule {

  public static final HiveJoinToMultiJoinRule INSTANCE =
      new HiveJoinToMultiJoinRule(HiveJoin.class, HiveRelFactories.HIVE_PROJECT_FACTORY);

  private final ProjectFactory projectFactory;

  private static transient final Logger LOG = LoggerFactory.getLogger(HiveJoinToMultiJoinRule.class);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a JoinToMultiJoinRule.
   */
  public HiveJoinToMultiJoinRule(Class<? extends Join> clazz, ProjectFactory projectFactory) {
    super(operand(clazz,
            operand(RelNode.class, any()),
            operand(RelNode.class, any())));
    this.projectFactory = projectFactory;
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public void onMatch(RelOptRuleCall call) {
    final HiveJoin join = call.rel(0);
    final RelNode left = call.rel(1);
    final RelNode right = call.rel(2);

    // 1. We try to merge this join with the left child
    RelNode multiJoin = mergeJoin(join, left, right);
    if (multiJoin != null) {
      call.transformTo(multiJoin);
      return;
    }

    // 2. If we cannot, we swap the inputs so we can try
    //    to merge it with its right child
    RelNode swapped = JoinCommuteRule.swap(join, true);
    assert swapped != null;

    //    The result of the swapping operation is either
    //    i)  a Project or,
    //    ii) if the project is trivial, a raw join
    final HiveJoin newJoin;
    Project topProject = null;
    if (swapped instanceof HiveJoin) {
      newJoin = (HiveJoin) swapped;
    } else {
      topProject = (Project) swapped;
      newJoin = (HiveJoin) swapped.getInput(0);
    }

    // 3. We try to merge the join with the right child
    multiJoin = mergeJoin(newJoin, right, left);
    if (multiJoin != null) {
      if (topProject != null) {
        multiJoin = projectFactory.createProject(multiJoin,
                topProject.getChildExps(),
                topProject.getRowType().getFieldNames());
      }
      call.transformTo(multiJoin);
      return;
    }
  }

  // This method tries to merge the join with its left child. The left
  // child should be a join for this to happen.
  private static RelNode mergeJoin(HiveJoin join, RelNode left, RelNode right) {
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();

    // We check whether the join can be combined with any of its children
    final List<RelNode> newInputs = Lists.newArrayList();
    final List<RexNode> newJoinCondition = Lists.newArrayList();
    final List<Pair<Integer,Integer>> joinInputs = Lists.newArrayList();
    final List<JoinRelType> joinTypes = Lists.newArrayList();
    final List<RexNode> joinFilters = Lists.newArrayList();

    // Left child
    if (left instanceof HiveJoin || left instanceof HiveMultiJoin) {
      final RexNode leftCondition;
      final List<Pair<Integer,Integer>> leftJoinInputs;
      final List<JoinRelType> leftJoinTypes;
      final List<RexNode> leftJoinFilters;
      boolean combinable;
      if (left instanceof HiveJoin) {
        HiveJoin hj = (HiveJoin) left;
        leftCondition = hj.getCondition();
        leftJoinInputs = ImmutableList.of(Pair.of(0, 1));
        leftJoinTypes = ImmutableList.of(hj.getJoinType());
        leftJoinFilters = ImmutableList.of(hj.getJoinFilter());
        try {
          combinable = isCombinableJoin(join, hj);
        } catch (CalciteSemanticException e) {
          LOG.trace("Failed to merge join-join", e);
          combinable = false;
        }
      } else {
        HiveMultiJoin hmj = (HiveMultiJoin) left;
        leftCondition = hmj.getCondition();
        leftJoinInputs = hmj.getJoinInputs();
        leftJoinTypes = hmj.getJoinTypes();
        leftJoinFilters = hmj.getJoinFilters();
        try {
          combinable = isCombinableJoin(join, hmj);
        } catch (CalciteSemanticException e) {
          LOG.trace("Failed to merge join-multijoin", e);
          combinable = false;
        }
      }

      if (combinable) {
        newJoinCondition.add(leftCondition);
        for (int i = 0; i < leftJoinInputs.size(); i++) {
          joinInputs.add(leftJoinInputs.get(i));
          joinTypes.add(leftJoinTypes.get(i));
          joinFilters.add(leftJoinFilters.get(i));
        }
        newInputs.addAll(left.getInputs());
      } else { // The join operation in the child is not on the same keys
        return null;
      }
    } else { // The left child is not a join or multijoin operator
      return null;
    }
    final int numberLeftInputs = newInputs.size();

    // Right child
    newInputs.add(right);

    // If we cannot combine any of the children, we bail out
    newJoinCondition.add(join.getCondition());
    if (newJoinCondition.size() == 1) {
      return null;
    }

    final List<RelDataTypeField> systemFieldList = ImmutableList.of();
    List<List<RexNode>> joinKeyExprs = new ArrayList<List<RexNode>>();
    List<Integer> filterNulls = new ArrayList<Integer>();
    for (int i=0; i<newInputs.size(); i++) {
      joinKeyExprs.add(new ArrayList<RexNode>());
    }
    RexNode filters;
    try {
      filters = HiveRelOptUtil.splitHiveJoinCondition(systemFieldList, newInputs,
          join.getCondition(), joinKeyExprs, filterNulls, null);
    } catch (CalciteSemanticException e) {
        LOG.trace("Failed to merge joins", e);
        return null;
    }
    ImmutableBitSet.Builder keysInInputsBuilder = ImmutableBitSet.builder();
    for (int i=0; i<newInputs.size(); i++) {
      List<RexNode> partialCondition = joinKeyExprs.get(i);
      if (!partialCondition.isEmpty()) {
        keysInInputsBuilder.set(i);
      }
    }
    // If we cannot merge, we bail out
    ImmutableBitSet keysInInputs = keysInInputsBuilder.build();
    ImmutableBitSet leftReferencedInputs =
            keysInInputs.intersect(ImmutableBitSet.range(numberLeftInputs));
    ImmutableBitSet rightReferencedInputs =
            keysInInputs.intersect(ImmutableBitSet.range(numberLeftInputs, newInputs.size()));
    if (join.getJoinType() != JoinRelType.INNER &&
            (leftReferencedInputs.cardinality() > 1 || rightReferencedInputs.cardinality() > 1)) {
      return null;
    }
    // Otherwise, we add to the join specs
    if (join.getJoinType() != JoinRelType.INNER) {
      int leftInput = keysInInputs.nextSetBit(0);
      int rightInput = keysInInputs.nextSetBit(numberLeftInputs);
      joinInputs.add(Pair.of(leftInput, rightInput));
      joinTypes.add(join.getJoinType());
      joinFilters.add(filters);
    } else {
      for (int i : leftReferencedInputs) {
        for (int j : rightReferencedInputs) {
          joinInputs.add(Pair.of(i, j));
          joinTypes.add(join.getJoinType());
          joinFilters.add(filters);
        }
      }
    }

    // We can now create a multijoin operator
    RexNode newCondition = RexUtil.flatten(rexBuilder,
            RexUtil.composeConjunction(rexBuilder, newJoinCondition, false));
    List<RelNode> newInputsArray = Lists.newArrayList(newInputs);
    JoinPredicateInfo joinPredInfo = null;
    try {
      joinPredInfo = HiveCalciteUtil.JoinPredicateInfo.constructJoinPredicateInfo(newInputsArray, systemFieldList, newCondition);
    } catch (CalciteSemanticException e) {
      throw new RuntimeException(e);
    }

    // If the number of joins < number of input tables-1, this is not a star join.
    if (joinPredInfo.getEquiJoinPredicateElements().size() < newInputs.size()-1) {
      return null;
    }
    // Validate that the multi-join is a valid star join before returning it.
    for (int i=0; i<newInputs.size(); i++) {
      List<RexNode> joinKeys = null;
      for (int j = 0; j < joinPredInfo.getEquiJoinPredicateElements().size(); j++) {
        List<RexNode> currJoinKeys = joinPredInfo.
          getEquiJoinPredicateElements().get(j).getJoinExprs(i);
        if (currJoinKeys.isEmpty()) {
          continue;
        }
        if (joinKeys == null) {
          joinKeys = currJoinKeys;
        } else {
          // If we join on different keys on different tables, we can no longer apply
          // multi-join conversion as this is no longer a valid star join.
          // Bail out if this is the case.
          if (!joinKeys.containsAll(currJoinKeys) || !currJoinKeys.containsAll(joinKeys)) {
            return null;
          }
        }
      }
    }

    return new HiveMultiJoin(
            join.getCluster(),
            newInputsArray,
            newCondition,
            join.getRowType(),
            joinInputs,
            joinTypes,
            joinFilters,
            joinPredInfo);
  }

  /*
   * Returns true if the join conditions execute over the same keys
   */
  private static boolean isCombinableJoin(HiveJoin join, HiveJoin leftChildJoin)
          throws CalciteSemanticException {
    final JoinPredicateInfo joinPredInfo = HiveCalciteUtil.JoinPredicateInfo.
            constructJoinPredicateInfo(join, join.getCondition());
    final JoinPredicateInfo leftChildJoinPredInfo = HiveCalciteUtil.JoinPredicateInfo.
            constructJoinPredicateInfo(leftChildJoin, leftChildJoin.getCondition());
    return isCombinablePredicate(joinPredInfo, leftChildJoinPredInfo, leftChildJoin.getInputs().size());
  }

  /*
   * Returns true if the join conditions execute over the same keys
   */
  private static boolean isCombinableJoin(HiveJoin join, HiveMultiJoin leftChildJoin)
          throws CalciteSemanticException {
    final JoinPredicateInfo joinPredInfo = HiveCalciteUtil.JoinPredicateInfo.
            constructJoinPredicateInfo(join, join.getCondition());
    final JoinPredicateInfo leftChildJoinPredInfo = HiveCalciteUtil.JoinPredicateInfo.
            constructJoinPredicateInfo(leftChildJoin, leftChildJoin.getCondition());
    return isCombinablePredicate(joinPredInfo, leftChildJoinPredInfo, leftChildJoin.getInputs().size());
  }

  /*
   * To be able to combine a parent join and its left input join child,
   * the left keys over which the parent join is executed need to be the same
   * than those of the child join.
   * Thus, we iterate over the different inputs of the child, checking if the
   * keys of the parent are the same
   */
  private static boolean isCombinablePredicate(JoinPredicateInfo joinPredInfo,
          JoinPredicateInfo leftChildJoinPredInfo, int noLeftChildInputs) throws CalciteSemanticException {
    Set<Integer> keys = joinPredInfo.getProjsJoinKeysInChildSchema(0);
    if (keys.isEmpty()) {
      return false;
    }
    for (int i = 0; i < noLeftChildInputs; i++) {
      if (keys.equals(leftChildJoinPredInfo.getProjsJoinKeysInJoinSchema(i))) {
        return true;
      }
    }
    return false;
  }
}
