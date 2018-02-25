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

import java.util.List;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdDistinctRowCount;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCost;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
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

  // Catch-all rule when none of the others apply.
  @Override
  public Double getDistinctRowCount(RelNode rel, RelMetadataQuery mq, ImmutableBitSet groupKey,
      RexNode predicate) {
    if (rel instanceof HiveTableScan) {
      return getDistinctRowCount((HiveTableScan) rel, mq, groupKey, predicate);
    }
    /*
     * For now use Calcite' default formulas for propagating NDVs up the Query
     * Tree.
     */
    return super.getDistinctRowCount(rel, mq, groupKey, predicate);
  }

  private Double getDistinctRowCount(HiveTableScan htRel, RelMetadataQuery mq, ImmutableBitSet groupKey,
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
    if (rel instanceof HiveJoin) {
      HiveJoin hjRel = (HiveJoin) rel;
      //TODO: Improve this
      if (rel instanceof SemiJoin) {
        return mq.getDistinctRowCount(hjRel.getLeft(), groupKey,
            rel.getCluster().getRexBuilder().makeLiteral(true));
      } else {
        return RelMdUtil.getJoinDistinctRowCount(mq, rel, rel.getJoinType(),
            groupKey, predicate, true);
      }
    }

    return mq.getDistinctRowCount(rel, groupKey, predicate);
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
