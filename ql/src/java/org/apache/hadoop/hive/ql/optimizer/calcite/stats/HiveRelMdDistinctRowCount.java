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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.Converter;
import org.apache.calcite.rel.core.JoinRelType;
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
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAntiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc.JdbcHiveTableScan;
import org.apache.hadoop.hive.ql.plan.ColStatistics;

import java.util.ArrayList;
import java.util.List;

public class HiveRelMdDistinctRowCount extends RelMdDistinctRowCount {

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.DISTINCT_ROW_COUNT.method, new HiveRelMdDistinctRowCount());

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

  public Double getDistinctRowCount(HiveSemiJoin rel, RelMetadataQuery mq, ImmutableBitSet groupKey,
      RexNode predicate) {
    return super.getDistinctRowCount(rel, mq, groupKey, predicate);
  }

  public Double getDistinctRowCount(HiveAntiJoin rel, RelMetadataQuery mq, ImmutableBitSet groupKey,
                                    RexNode predicate) {
    //TODO : Currently calcite does not support this.
    // https://issues.apache.org/jira/browse/HIVE-23933
    return super.getDistinctRowCount(rel, mq, groupKey, predicate);
  }

  public Double getDistinctRowCount(HiveJoin rel, RelMetadataQuery mq, ImmutableBitSet groupKey,
      RexNode predicate) {
    return getJoinDistinctRowCount(mq, rel, rel.getJoinType(),
           groupKey, predicate, true);
  }

  /**
   * Currently Calcite doesn't handle return row counts for Converters and JdbcHiveTableScan. These
   * method checks for these objects and calls appropriate methods.
   * https://issues.apache.org/jira/browse/HIVE-25364
   *
   */
  public Double getDistinctRowCount(JdbcHiveTableScan rel, RelMetadataQuery mq, ImmutableBitSet groupKey,
                                    RexNode predicate) {
    return getDistinctRowCount(rel.getHiveTableScan(), mq, groupKey, predicate);
  }

  public Double getDistinctRowCount(Converter r, RelMetadataQuery mq, ImmutableBitSet groupKey,
                                    RexNode predicate) {
    return mq.getDistinctRowCount(r.getInput(0), groupKey, predicate);
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

}
