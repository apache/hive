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
import java.util.List;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * A HiveMultiJoin represents a succession of binary joins.
 */
public final class HiveMultiJoin extends AbstractRelNode {

  private final List<RelNode> inputs;
  private final RexNode condition;
  private final RelDataType rowType;
  private final ImmutableList<Pair<Integer,Integer>> joinInputs;
  private final ImmutableList<JoinRelType> joinTypes;
  private final ImmutableList<RexNode> filters;

  private final boolean outerJoin;
  private final JoinPredicateInfo joinPredInfo;


  /**
   * Constructs a MultiJoin.
   *
   * @param cluster               cluster that join belongs to
   * @param inputs                inputs into this multi-join
   * @param condition             join filter applicable to this join node
   * @param rowType               row type of the join result of this node
   * @param joinInputs
   * @param joinTypes             the join type corresponding to each input; if
   *                              an input is null-generating in a left or right
   *                              outer join, the entry indicates the type of
   *                              outer join; otherwise, the entry is set to
   *                              INNER
   * @param filters               filters associated with each join
   *                              input
   * @param joinPredicateInfo     join predicate information
   */
  public HiveMultiJoin(
      RelOptCluster cluster,
      List<RelNode> inputs,
      RexNode condition,
      RelDataType rowType,
      List<Pair<Integer,Integer>> joinInputs,
      List<JoinRelType> joinTypes,
      List<RexNode> filters,
      JoinPredicateInfo joinPredicateInfo) {
    super(cluster, TraitsUtil.getDefaultTraitSet(cluster));
    this.inputs = inputs;
    this.condition = condition;
    this.rowType = rowType;

    assert joinInputs.size() == joinTypes.size();
    this.joinInputs = ImmutableList.copyOf(joinInputs);
    this.joinTypes = ImmutableList.copyOf(joinTypes);
    this.filters = ImmutableList.copyOf(filters);
    this.outerJoin = containsOuter();
    if (joinPredicateInfo == null) {
      try {
        this.joinPredInfo = HiveCalciteUtil.JoinPredicateInfo.constructJoinPredicateInfo(this);
      } catch (CalciteSemanticException e) {
        throw new RuntimeException(e);
      }
    } else {
      this.joinPredInfo = joinPredicateInfo;
    }
  }

  public HiveMultiJoin(
    RelOptCluster cluster,
    List<RelNode> inputs,
    RexNode condition,
    RelDataType rowType,
    List<Pair<Integer,Integer>> joinInputs,
    List<JoinRelType> joinTypes,
    List<RexNode> filters) {
    this(cluster, Lists.newArrayList(inputs), condition, rowType, joinInputs, joinTypes, filters, null);
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p) {
    inputs.set(ordinalInParent, p);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(HiveRelNode.CONVENTION);
    return new HiveMultiJoin(
        getCluster(),
        inputs,
        condition,
        rowType,
        joinInputs,
        joinTypes,
        filters);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    List<String> joinsString = new ArrayList<String>();
    for (int i = 0; i < joinInputs.size(); i++) {
      final StringBuilder sb = new StringBuilder();
      sb.append(joinInputs.get(i).left).append(" - ").append(joinInputs.get(i).right)
          .append(" : ").append(joinTypes.get(i).name());
      joinsString.add(sb.toString());
    }

    super.explainTerms(pw);
    for (Ord<RelNode> ord : Ord.zip(inputs)) {
      pw.input("input#" + ord.i, ord.e);
    }
    return pw.item("condition", condition)
        .item("joinsDescription", joinsString);
  }

  @Override
  public RelDataType deriveRowType() {
    return rowType;
  }

  @Override
  public List<RelNode> getInputs() {
    return inputs;
  }

  @Override public List<RexNode> getChildExps() {
    return ImmutableList.of(condition);
  }

  @Override
  public RelNode accept(RexShuttle shuttle) {
    RexNode joinFilter = shuttle.apply(this.condition);

    if (joinFilter == this.condition) {
      return this;
    }

    return new HiveMultiJoin(
        getCluster(),
        inputs,
        joinFilter,
        rowType,
        joinInputs,
        joinTypes,
        filters);
  }

  /**
   * @return join filters associated with this MultiJoin
   */
  public RexNode getCondition() {
    return condition;
  }

  /**
   * @return true if the MultiJoin contains a (partial) outer join.
   */
  public boolean isOuterJoin() {
    return outerJoin;
  }

  /**
   * @return join relationships between inputs
   */
  public List<Pair<Integer,Integer>> getJoinInputs() {
    return joinInputs;
  }

  /**
   * @return join types of each input
   */
  public List<JoinRelType> getJoinTypes() {
    return joinTypes;
  }

  /**
   * @return join conditions filters
   */
  public List<RexNode> getJoinFilters() {
    return filters;
  }

  /**
   * @return the join predicate information
   */
  public JoinPredicateInfo getJoinPredicateInfo() {
    return joinPredInfo;
  }

  private boolean containsOuter() {
    for (JoinRelType joinType : joinTypes) {
      if (joinType != JoinRelType.INNER) {
        return true;
      }
    }
    return false;
  }
}

// End MultiJoin.java
