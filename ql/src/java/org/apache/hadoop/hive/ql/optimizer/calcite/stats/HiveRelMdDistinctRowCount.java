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
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NumberUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCost;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.plan.ColStatistics;

import com.google.common.collect.ImmutableList;

public class HiveRelMdDistinctRowCount extends RelMdDistinctRowCount {

  private static final HiveRelMdDistinctRowCount INSTANCE =
      new HiveRelMdDistinctRowCount();

  public static final RelMetadataProvider SOURCE = ChainedRelMetadataProvider
      .of(ImmutableList.of(

      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.DISTINCT_ROW_COUNT.method, INSTANCE),

      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.CUMULATIVE_COST.method, INSTANCE)));

  private HiveRelMdDistinctRowCount() {
  }

  public Double getDistinctRowCount(HiveTableScan htRel, RelMetadataQuery mq, ImmutableBitSet groupKey,
      RexNode predicate) {
    List<Integer> projIndxLst = HiveCalciteUtil
        .translateBitSetToProjIndx(groupKey);
    List<ColStatistics> colStats = htRel.getColStat(projIndxLst);
    Double noDistinctRows = 1.0;
    for (ColStatistics cStat : colStats) {
      noDistinctRows *= cStat.getCountDistint();
    }

    return Math.min(noDistinctRows, mq.getRowCount(htRel));
  }

  public static Double getDistinctRowCount(RelNode r, RelMetadataQuery mq, int indx) {
    ImmutableBitSet bitSetOfRqdProj = ImmutableBitSet.of(indx);
    return mq.getDistinctRowCount(r, bitSetOfRqdProj, r
        .getCluster().getRexBuilder().makeLiteral(true));
  }

  @Override
  public Double getDistinctRowCount(Join rel, RelMetadataQuery mq, ImmutableBitSet groupKey,
      RexNode predicate) {
    if (rel instanceof HiveSemiJoin) {
      return super.getDistinctRowCount(rel, mq, groupKey, predicate);
    }

    if (rel instanceof HiveJoin) {
      return getJoinDistinctRowCount(mq, rel, rel.getJoinType(),
          groupKey, predicate, true);
    }

    return mq.getDistinctRowCount(rel, groupKey, predicate);
  }

  /**
   * TODO: This method is a copy of {@link RelMdUtil#getJoinDistinctRowCount}.
   * We will remove it once we replace Math.max with a null-safe method in
   * Calcite.
   *
   * Computes the number of distinct rows for a set of keys returned from a
   * join. Also known as NDV (number of distinct values).
   *
   * @param joinRel   RelNode representing the join
   * @param joinType  type of join
   * @param groupKey  keys that the distinct row count will be computed for
   * @param predicate join predicate
   * @param useMaxNdv If true use formula <code>max(left NDV, right NDV)</code>,
   *                  otherwise use <code>left NDV * right NDV</code>.
   * @return number of distinct rows
   */
  private static Double getJoinDistinctRowCount(RelMetadataQuery mq,
      RelNode joinRel, JoinRelType joinType, ImmutableBitSet groupKey,
      RexNode predicate, boolean useMaxNdv) {
    Double distRowCount = null;
    ImmutableBitSet.Builder leftMask = ImmutableBitSet.builder();
    ImmutableBitSet.Builder rightMask = ImmutableBitSet.builder();
    RelNode left = joinRel.getInputs().get(0);
    RelNode right = joinRel.getInputs().get(1);

    RelMdUtil.setLeftRightBitmaps(
        groupKey,
        leftMask,
        rightMask,
        left.getRowType().getFieldCount());

    // determine which filters apply to the left vs right
    RexNode leftPred = null;
    RexNode rightPred = null;
    if (predicate != null) {
      final List<RexNode> leftFilters = new ArrayList<>();
      final List<RexNode> rightFilters = new ArrayList<>();
      final List<RexNode> joinFilters = new ArrayList<>();
      final List<RexNode> predList = RelOptUtil.conjunctions(predicate);

      RelOptUtil.classifyFilters(
          joinRel,
          predList,
          joinType,
          joinType == JoinRelType.INNER,
          !joinType.generatesNullsOnLeft(),
          !joinType.generatesNullsOnRight(),
          joinFilters,
          leftFilters,
          rightFilters);

      RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
      leftPred =
          RexUtil.composeConjunction(rexBuilder, leftFilters, true);
      rightPred =
          RexUtil.composeConjunction(rexBuilder, rightFilters, true);
    }

    Double leftDistRowCount = mq.getDistinctRowCount(left, leftMask.build(), leftPred);
    Double rightDistRowCount = mq.getDistinctRowCount(right, rightMask.build(), rightPred);
    if (leftDistRowCount != null && rightDistRowCount != null) {
      if (useMaxNdv) {
        distRowCount = Math.max(leftDistRowCount, rightDistRowCount);
      } else {
        distRowCount =
            NumberUtil.multiply(leftDistRowCount, rightDistRowCount);
      }
    }

    return RelMdUtil.numDistinctVals(distRowCount, mq.getRowCount(joinRel));
  }

  /*
   * Favor Broad Plans over Deep Plans.
   */
  public RelOptCost getCumulativeCost(HiveJoin rel, RelMetadataQuery mq) {
    RelOptCost cost = mq.getNonCumulativeCost(rel);
    List<RelNode> inputs = rel.getInputs();
    RelOptCost maxICost = HiveCost.ZERO;
    for (RelNode input : inputs) {
      RelOptCost iCost = mq.getCumulativeCost(input);
      if (maxICost.isLt(iCost)) {
        maxICost = iCost;
      }
    }
    return cost.plus(maxICost);
  }
}
