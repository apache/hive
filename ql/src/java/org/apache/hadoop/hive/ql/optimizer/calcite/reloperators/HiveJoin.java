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

import java.util.Collections;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories.JoinFactory;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCost;

//TODO: Should we convert MultiJoin to be a child of HiveJoin
public class HiveJoin extends Join implements HiveRelNode {
  // NOTE: COMMON_JOIN & SMB_JOIN are Sort Merge Join (in case of COMMON_JOIN
  // each parallel computation handles multiple splits where as in case of SMB
  // each parallel computation handles one bucket). MAP_JOIN and BUCKET_JOIN is
  // hash joins where MAP_JOIN keeps the whole data set of non streaming tables
  // in memory where as BUCKET_JOIN keeps only the b
  public enum JoinAlgorithm {
    NONE, COMMON_JOIN, MAP_JOIN, BUCKET_JOIN, SMB_JOIN
  }

  public enum MapJoinStreamingRelation {
    NONE, LEFT_RELATION, RIGHT_RELATION
  }

  public static final JoinFactory HIVE_JOIN_FACTORY = new HiveJoinFactoryImpl();

  private final boolean leftSemiJoin;
  private final JoinAlgorithm      joinAlgorithm;
  //This will be used once we do Join Algorithm selection
  @SuppressWarnings("unused")
  private final MapJoinStreamingRelation mapJoinStreamingSide = MapJoinStreamingRelation.NONE;

  public static HiveJoin getJoin(RelOptCluster cluster, RelNode left, RelNode right,
      RexNode condition, JoinRelType joinType, boolean leftSemiJoin) {
    try {
      Set<String> variablesStopped = Collections.emptySet();
      return new HiveJoin(cluster, null, left, right, condition, joinType, variablesStopped,
          JoinAlgorithm.NONE, null, leftSemiJoin);
    } catch (InvalidRelException e) {
      throw new RuntimeException(e);
    }
  }

  protected HiveJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right,
      RexNode condition, JoinRelType joinType, Set<String> variablesStopped,
      JoinAlgorithm joinAlgo, MapJoinStreamingRelation streamingSideForMapJoin, boolean leftSemiJoin)
      throws InvalidRelException {
    super(cluster, TraitsUtil.getDefaultTraitSet(cluster), left, right, condition, joinType,
        variablesStopped);
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
          variablesStopped, JoinAlgorithm.NONE, null, leftSemiJoin);
    } catch (InvalidRelException e) {
      // Semantic error not possible. Must be a bug. Convert to
      // internal error.
      throw new AssertionError(e);
    }
  }

  public JoinAlgorithm getJoinAlgorithm() {
    return joinAlgorithm;
  }

  public boolean isLeftSemiJoin() {
    return leftSemiJoin;
  }

  /**
   * Model cost of join as size of Inputs.
   */
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    double leftRCount = RelMetadataQuery.getRowCount(getLeft());
    double rightRCount = RelMetadataQuery.getRowCount(getRight());
    return HiveCost.FACTORY.makeCost(leftRCount + rightRCount, 0.0, 0.0);
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
