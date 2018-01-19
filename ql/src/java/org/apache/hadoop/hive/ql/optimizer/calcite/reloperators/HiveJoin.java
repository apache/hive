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
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttle;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCostModel.JoinAlgorithm;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveDefaultCostModel.DefaultJoinAlgorithm;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRulesRegistry;

import com.google.common.collect.ImmutableList;

//TODO: Should we convert MultiJoin to be a child of HiveJoin
public class HiveJoin extends Join implements HiveRelNode {

  public enum MapJoinStreamingRelation {
    NONE, LEFT_RELATION, RIGHT_RELATION
  }

  private final RexNode joinFilter;
  private final JoinPredicateInfo joinPredInfo;
  private JoinAlgorithm joinAlgorithm;
  private RelOptCost joinCost;


  public static HiveJoin getJoin(RelOptCluster cluster, RelNode left, RelNode right,
      RexNode condition, JoinRelType joinType) {
    try {
      Set<String> variablesStopped = Collections.emptySet();
      HiveJoin join = new HiveJoin(cluster, null, left, right, condition, joinType, variablesStopped,
              DefaultJoinAlgorithm.INSTANCE);
      return join;
    } catch (InvalidRelException | CalciteSemanticException e) {
      throw new RuntimeException(e);
    }
  }

  protected HiveJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right,
      RexNode condition, JoinRelType joinType, Set<String> variablesStopped,
      JoinAlgorithm joinAlgo) throws InvalidRelException, CalciteSemanticException {
    super(cluster, TraitsUtil.getDefaultTraitSet(cluster), left, right, condition, joinType,
        variablesStopped);
    final List<RelDataTypeField> systemFieldList = ImmutableList.of();
    List<List<RexNode>> joinKeyExprs = new ArrayList<List<RexNode>>();
    List<Integer> filterNulls = new ArrayList<Integer>();
    for (int i=0; i<this.getInputs().size(); i++) {
      joinKeyExprs.add(new ArrayList<RexNode>());
    }
    this.joinFilter = HiveRelOptUtil.splitHiveJoinCondition(systemFieldList, this.getInputs(),
            this.getCondition(), joinKeyExprs, filterNulls, null);
    this.joinPredInfo = HiveCalciteUtil.JoinPredicateInfo.constructJoinPredicateInfo(this);
    this.joinAlgorithm = joinAlgo;
  }

  @Override
  public void implement(Implementor implementor) {
  }

  @Override
  public final HiveJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
      RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    try {
      Set<String> variablesStopped = Collections.emptySet();
      HiveJoin join = new HiveJoin(getCluster(), traitSet, left, right, conditionExpr, joinType,
          variablesStopped, joinAlgorithm);
      // If available, copy state to registry for optimization rules
      HiveRulesRegistry registry = join.getCluster().getPlanner().getContext().unwrap(HiveRulesRegistry.class);
      if (registry != null) {
        registry.copyPushedPredicates(this, join);
      }
      return join;
    } catch (InvalidRelException | CalciteSemanticException e) {
      // Semantic error not possible. Must be a bug. Convert to
      // internal error.
      throw new AssertionError(e);
    }
  }

  public RexNode getJoinFilter() {
    return joinFilter;
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
    RelMetadataQuery mq = left.getCluster().getMetadataQuery();
    Double leftInputSize = mq.memory(left);
    Double rightInputSize = mq.memory(right);
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

  public ImmutableBitSet getSortedInputs() throws CalciteSemanticException {
    ImmutableBitSet.Builder sortedInputsBuilder = ImmutableBitSet.builder();
    JoinPredicateInfo joinPredInfo = HiveCalciteUtil.JoinPredicateInfo.
            constructJoinPredicateInfo(this);
    List<ImmutableIntList> joinKeysInChildren = new ArrayList<ImmutableIntList>();
    joinKeysInChildren.add(
            ImmutableIntList.copyOf(
                    joinPredInfo.getProjsFromLeftPartOfJoinKeysInChildSchema()));
    joinKeysInChildren.add(
            ImmutableIntList.copyOf(
                    joinPredInfo.getProjsFromRightPartOfJoinKeysInChildSchema()));

    final RelMetadataQuery mq = this.left.getCluster().getMetadataQuery();
    for (int i=0; i<this.getInputs().size(); i++) {
      boolean correctOrderFound = RelCollations.contains(
          mq.collations(this.getInputs().get(i)),
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

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("algorithm", joinAlgorithm == null ?
                "none" : joinAlgorithm)
        .item("cost", joinCost == null ?
                "not available" : joinCost);
  }

  //required for HiveRelDecorrelator
  public RelNode accept(RelShuttle shuttle) {
    if (shuttle instanceof HiveRelShuttle) {
      return ((HiveRelShuttle)shuttle).visit(this);
    }
    return shuttle.visit(this);
  }
}
