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
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories.JoinFactory;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCostModel.JoinAlgorithm;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveDefaultCostModel.DefaultJoinAlgorithm;

import com.google.common.collect.ImmutableList;

//TODO: Should we convert MultiJoin to be a child of HiveJoin
public class HiveJoin extends Join implements HiveRelNode {
  
  public static final JoinFactory HIVE_JOIN_FACTORY = new HiveJoinFactoryImpl();

  public enum MapJoinStreamingRelation {
    NONE, LEFT_RELATION, RIGHT_RELATION
  }

  private final boolean leftSemiJoin;
  private final JoinPredicateInfo joinPredInfo;
  private JoinAlgorithm joinAlgorithm;
  private RelOptCost joinCost;


  public static HiveJoin getJoin(RelOptCluster cluster, RelNode left, RelNode right,
      RexNode condition, JoinRelType joinType, boolean leftSemiJoin) {
    try {
      Set<String> variablesStopped = Collections.emptySet();
      HiveJoin join = new HiveJoin(cluster, null, left, right, condition, joinType, variablesStopped,
              DefaultJoinAlgorithm.INSTANCE, leftSemiJoin);
      return join;
    } catch (InvalidRelException e) {
      throw new RuntimeException(e);
    }
  }

  protected HiveJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right,
      RexNode condition, JoinRelType joinType, Set<String> variablesStopped,
      JoinAlgorithm joinAlgo, boolean leftSemiJoin) throws InvalidRelException {
    super(cluster, TraitsUtil.getDefaultTraitSet(cluster), left, right, condition, joinType,
        variablesStopped);
    this.joinPredInfo = HiveCalciteUtil.JoinPredicateInfo.constructJoinPredicateInfo(this);
    this.joinAlgorithm = joinAlgo;
    this.leftSemiJoin = leftSemiJoin;
  }

  @Override
  public void implement(Implementor implementor) {
  }

  @Override
  public final HiveJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
      RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    try {
      Set<String> variablesStopped = Collections.emptySet();
      return new HiveJoin(getCluster(), traitSet, left, right, conditionExpr, joinType,
          variablesStopped, joinAlgorithm, leftSemiJoin);
    } catch (InvalidRelException e) {
      // Semantic error not possible. Must be a bug. Convert to
      // internal error.
      throw new AssertionError(e);
    }
  }

  public JoinPredicateInfo getJoinPredicateInfo() {
    return joinPredInfo;
  }

  public void setJoinAlgorithm(JoinAlgorithm joinAlgorithm) {
    this.joinAlgorithm = joinAlgorithm;
  }

  public JoinAlgorithm getJoinAlgorithm() {
    return this.joinAlgorithm;
  }

  public ImmutableList<RelCollation> getCollation() {
    return joinAlgorithm.getCollation(this);
  }

  public RelDistribution getDistribution() {
    return joinAlgorithm.getDistribution(this);
  }

  public Double getMemory() {
    return joinAlgorithm.getMemory(this);
  }

  public Double getCumulativeMemoryWithinPhaseSplit() {
    return joinAlgorithm.getCumulativeMemoryWithinPhaseSplit(this);
  }

  public Boolean isPhaseTransition() {
    return joinAlgorithm.isPhaseTransition(this);
  }

  public Integer getSplitCount() {
    return joinAlgorithm.getSplitCount(this);
  }

  public MapJoinStreamingRelation getStreamingSide() {
    Double leftInputSize = RelMetadataQuery.memory(left);
    Double rightInputSize = RelMetadataQuery.memory(right);
    if (leftInputSize == null && rightInputSize == null) {
      return MapJoinStreamingRelation.NONE;
    } else if (leftInputSize != null &&
            (rightInputSize == null ||
            (leftInputSize < rightInputSize))) {
      return MapJoinStreamingRelation.RIGHT_RELATION;
    } else if (rightInputSize != null &&
            (leftInputSize == null ||
            (rightInputSize <= leftInputSize))) {
      return MapJoinStreamingRelation.LEFT_RELATION;
    }
    return MapJoinStreamingRelation.NONE;
  }

  public RelNode getStreamingInput() {
    MapJoinStreamingRelation mapJoinStreamingSide = getStreamingSide();
    RelNode smallInput;
    if (mapJoinStreamingSide == MapJoinStreamingRelation.LEFT_RELATION) {
      smallInput = this.getRight();
    } else if (mapJoinStreamingSide == MapJoinStreamingRelation.RIGHT_RELATION) {
      smallInput = this.getLeft();
    } else {
      smallInput = null;
    }
    return smallInput;
  }

  public ImmutableBitSet getSortedInputs() {
    ImmutableBitSet.Builder sortedInputsBuilder = new ImmutableBitSet.Builder();
    JoinPredicateInfo joinPredInfo = HiveCalciteUtil.JoinPredicateInfo.
            constructJoinPredicateInfo(this);
    List<ImmutableIntList> joinKeysInChildren = new ArrayList<ImmutableIntList>();
    joinKeysInChildren.add(
            ImmutableIntList.copyOf(
                    joinPredInfo.getProjsFromLeftPartOfJoinKeysInChildSchema()));
    joinKeysInChildren.add(
            ImmutableIntList.copyOf(
                    joinPredInfo.getProjsFromRightPartOfJoinKeysInChildSchema()));

    for (int i=0; i<this.getInputs().size(); i++) {
      boolean correctOrderFound = RelCollations.contains(
              RelMetadataQuery.collations(this.getInputs().get(i)),
              joinKeysInChildren.get(i));
      if (correctOrderFound) {
        sortedInputsBuilder.set(i);
      }
    }
    return sortedInputsBuilder.build();
  }

  public void setJoinCost(RelOptCost joinCost) {
    this.joinCost = joinCost;
  }

  public boolean isLeftSemiJoin() {
    return leftSemiJoin;
  }

  /**
   * Model cost of join as size of Inputs.
   */
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return RelMetadataQuery.getNonCumulativeCost(this);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("algorithm", joinAlgorithm == null ?
                "none" : joinAlgorithm)
        .item("cost", joinCost == null ?
                "not available" : joinCost);
  }

  /**
   * @return returns rowtype representing only the left join input
   */
  @Override
  public RelDataType deriveRowType() {
    if (leftSemiJoin) {
      return deriveJoinRowType(left.getRowType(), null, JoinRelType.INNER,
          getCluster().getTypeFactory(), null,
          Collections.<RelDataTypeField> emptyList());
    }
    return super.deriveRowType();
  }

  private static class HiveJoinFactoryImpl implements JoinFactory {
    /**
     * Creates a join.
     *
     * @param left
     *          Left input
     * @param right
     *          Right input
     * @param condition
     *          Join condition
     * @param joinType
     *          Join type
     * @param variablesStopped
     *          Set of names of variables which are set by the LHS and used by
     *          the RHS and are not available to nodes above this JoinRel in the
     *          tree
     * @param semiJoinDone
     *          Whether this join has been translated to a semi-join
     */
    @Override
    public RelNode createJoin(RelNode left, RelNode right, RexNode condition, JoinRelType joinType,
        Set<String> variablesStopped, boolean semiJoinDone) {
      return getJoin(left.getCluster(), left, right, condition, joinType, false);
    }
  }

}
