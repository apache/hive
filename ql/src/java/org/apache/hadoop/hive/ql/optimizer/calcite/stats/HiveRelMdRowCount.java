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
import java.util.Set;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil.PKFKJoinInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

public class HiveRelMdRowCount extends RelMdRowCount {

  protected static final Logger LOG  = LoggerFactory.getLogger(HiveRelMdRowCount.class.getName());


  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
      .reflectiveSource(BuiltInMethod.ROW_COUNT.method, new HiveRelMdRowCount());

  protected HiveRelMdRowCount() {
    super();
  }

  public Double getRowCount(Join join, RelMetadataQuery mq) {
    // Try to infer from constraints first
    final Pair<PKFKRelationInfo, RexNode> constraintBasedResult =
        constraintsBasedAnalyzeJoinForPKFK(join, mq);
    if (constraintBasedResult != null) {
      // We succeeded, we calculate the selectivity based on the inferred information
      // and any residual predicate
      double joinSelectivity = Math.min(1.0,
          constraintBasedResult.left.pkInfo.selectivity * constraintBasedResult.left.ndvScalingFactor);
      double residualSelectivity = RelMdUtil.guessSelectivity(constraintBasedResult.right);
      double rowCount = constraintBasedResult.left.fkInfo.rowCount * joinSelectivity * residualSelectivity;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Identified Primary - Foreign Key relation from constraints:\n {} {} Row count for join: {}\n" +
            " Join selectivity: {}\n Residual selectivity: {}\n", RelOptUtil.toString(join), constraintBasedResult.left,
            rowCount, joinSelectivity, residualSelectivity);
      }
      return rowCount;
    }
    // Otherwise, try to infer from stats
    final PKFKRelationInfo pkfk = analyzeJoinForPKFK(join, mq);
    if (pkfk != null) {
      double selectivity = pkfk.pkInfo.selectivity * pkfk.ndvScalingFactor;
      selectivity = Math.min(1.0, selectivity);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Identified Primary - Foreign Key relation: {} {}", RelOptUtil.toString(join), pkfk);
      }
      return pkfk.fkInfo.rowCount * selectivity;
    }
    // If we cannot infer anything, then we just go to join.estimateRowCount(mq).
    // Do not call mq.getRowCount(join), will trigger CyclicMetadataException
    final Double rowCount = join.estimateRowCount(mq);
    if (LOG.isDebugEnabled()) {
      LOG.debug("No Primary - Foreign Key relation: \n{} Row count for join: {}\n",
          RelOptUtil.toString(join), rowCount);
    }
    return rowCount;
  }

  @Override
  public Double getRowCount(SemiJoin rel, RelMetadataQuery mq) {
    PKFKRelationInfo pkfk = analyzeJoinForPKFK(rel, mq);
    if (pkfk != null) {
      double selectivity = pkfk.pkInfo.selectivity * pkfk.ndvScalingFactor;
      selectivity = Math.min(1.0, selectivity);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Identified Primary - Foreign Key relation: {} {}", RelOptUtil.toString(rel), pkfk);
      }
      return pkfk.fkInfo.rowCount * selectivity;
    }
    return super.getRowCount(rel, mq);
  }

  @Override
  public Double getRowCount(Sort rel, RelMetadataQuery mq) {
    final Double rowCount = mq.getRowCount(rel.getInput());
    if (rowCount != null && rel.fetch != null) {
      final int offset = rel.offset == null ? 0 : RexLiteral.intValue(rel.offset);
      final int limit = RexLiteral.intValue(rel.fetch);
      final Double offsetLimit = new Double(offset + limit);
      // offsetLimit is smaller than rowCount of the input operator
      // thus, we return the offsetLimit
      if (offsetLimit < rowCount) {
        return offsetLimit;
      }
    }
    return rowCount;
  }

  static class PKFKRelationInfo {
    public final int fkSide;
    public final double ndvScalingFactor;
    public final FKSideInfo fkInfo;
    public final PKSideInfo pkInfo;
    public final boolean isPKSideSimple;

    PKFKRelationInfo(int fkSide,
        FKSideInfo fkInfo,
        PKSideInfo pkInfo,
        double ndvScalingFactor,
        boolean isPKSideSimple) {
      this.fkSide = fkSide;
      this.fkInfo = fkInfo;
      this.pkInfo = pkInfo;
      this.ndvScalingFactor = ndvScalingFactor;
      this.isPKSideSimple = isPKSideSimple;
    }

    @Override
    public String toString() {
      return String.format(
          "Primary - Foreign Key join:\n\tfkSide = %d\n\tFKInfo:%s\n" +
          "\tPKInfo:%s\n\tisPKSideSimple:%s\n\tNDV Scaling Factor:%.2f\n",
          fkSide,
          fkInfo,
          pkInfo,
          isPKSideSimple,
          ndvScalingFactor);
    }
  }

  static class FKSideInfo {
    public final double rowCount;
    public final double distinctCount;
    public FKSideInfo(double rowCount, double distinctCount) {
      this.rowCount = rowCount;
      this.distinctCount = distinctCount;
    }

    @Override
    public String toString() {
      return String.format("FKInfo(rowCount=%.2f,ndv=%.2f)", rowCount, distinctCount);
    }
  }

  static class PKSideInfo extends FKSideInfo {
    public final double selectivity;
    public PKSideInfo(double rowCount, double distinctCount, double selectivity) {
      super(rowCount, distinctCount);
      this.selectivity = selectivity;
    }

    @Override
    public String toString() {
      return String.format("PKInfo(rowCount=%.2f,ndv=%.2f,selectivity=%.2f)", rowCount, distinctCount,selectivity);
    }
  }

  /*
   * For T1 join T2 on T1.x = T2.y if we identify 'y' s a key of T2 then we can
   * infer the join cardinality as: rowCount(T1) * selectivity(T2) i.e this is
   * like a SemiJoin where the T1(Fact side/FK side) is filtered by a factor
   * based on the Selectivity of the PK/Dim table side.
   *
   * 1. If both T1.x and T2.y are keys then use the larger one as the PK side.
   * 2. In case of outer Joins: a) The FK side should be the Null Preserving
   * side. It doesn't make sense to apply this heuristic in case of Dim loj Fact
   * or Fact roj Dim b) The selectivity factor applied on the Fact Table should
   * be 1.
   */
  public static PKFKRelationInfo analyzeJoinForPKFK(Join joinRel, RelMetadataQuery mq) {

    RelNode left = joinRel.getInputs().get(0);
    RelNode right = joinRel.getInputs().get(1);

    final List<RexNode> initJoinFilters = RelOptUtil.conjunctions(joinRel
        .getCondition());

    /*
     * No joining condition.
     */
    if (initJoinFilters.isEmpty()) {
      return null;
    }

    List<RexNode> leftFilters = new ArrayList<RexNode>();
    List<RexNode> rightFilters = new ArrayList<RexNode>();
    List<RexNode> joinFilters = new ArrayList<RexNode>(initJoinFilters);

    // @todo: remove this. 8/28/14 hb
    // for now adding because RelOptUtil.classifyFilters has an assertion about
    // column counts that is not true for semiJoins.
    if (joinRel instanceof SemiJoin) {
      return null;
    }

    RelOptUtil.classifyFilters(joinRel, joinFilters, joinRel.getJoinType(),
        false, !joinRel.getJoinType().generatesNullsOnRight(), !joinRel
            .getJoinType().generatesNullsOnLeft(), joinFilters, leftFilters,
        rightFilters);

    Pair<Integer, Integer> joinCols = canHandleJoin(joinRel, leftFilters,
        rightFilters, joinFilters);
    if (joinCols == null) {
      return null;
    }
    int leftColIdx = joinCols.left;
    int rightColIdx = joinCols.right;

    RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
    RexNode leftPred = RexUtil.composeConjunction(
        rexBuilder, leftFilters, true);
    RexNode rightPred = RexUtil.composeConjunction(
        rexBuilder, rightFilters, true);
    ImmutableBitSet lBitSet = ImmutableBitSet.of(leftColIdx);
    ImmutableBitSet rBitSet = ImmutableBitSet.of(rightColIdx);

    /*
     * If the form is Dim loj F or Fact roj Dim or Dim semij Fact then return
     * null.
     */
    boolean leftIsKey = (joinRel.getJoinType() == JoinRelType.INNER || joinRel.getJoinType() == JoinRelType.RIGHT)
        && isKey(lBitSet, left, mq);
    boolean rightIsKey = (joinRel.getJoinType() == JoinRelType.INNER || joinRel.getJoinType() == JoinRelType.LEFT)
        && isKey(rBitSet, right, mq);

    if (!leftIsKey && !rightIsKey) {
      return null;
    }

    double leftRowCount = mq.getRowCount(left);
    double rightRowCount = mq.getRowCount(right);

    if (leftIsKey && rightIsKey) {
      if (rightRowCount < leftRowCount) {
        leftIsKey = false;
      }
    }

    int pkSide = leftIsKey ? 0 : 1;
    boolean isPKSideSimpleTree = leftIsKey ? SimpleTreeOnJoinKey.check(false, left, lBitSet, mq) :
        SimpleTreeOnJoinKey.check(false, right, rBitSet, mq);
    double leftNDV = isPKSideSimpleTree ? mq.getDistinctRowCount(left, lBitSet, leftPred) : -1;
    double rightNDV = isPKSideSimpleTree ? mq.getDistinctRowCount(right, rBitSet, rightPred) : -1;

    /*
     * If the ndv of the PK - FK side don't match, and the PK side is a filter
     * on the Key column then scale the NDV on the FK side.
     *
     * As described by Peter Boncz: http://databasearchitects.blogspot.com/
     * in such cases we can be off by a large margin in the Join cardinality
     * estimate. The e.g. he provides is on the join of StoreSales and DateDim
     * on the TPCDS dataset. Since the DateDim is populated for 20 years into
     * the future, while the StoreSales only has 5 years worth of data, there
     * are 40 times fewer distinct dates in StoreSales.
     *
     * In general it is hard to infer the range for the foreign key on an
     * arbitrary expression. For e.g. the NDV for DayofWeek is the same
     * irrespective of NDV on the number of unique days, whereas the
     * NDV of Quarters has the same ratio as the NDV on the keys.
     *
     * But for expressions that apply only on columns that have the same NDV
     * as the key (implying that they are alternate keys) we can apply the
     * ratio. So in the case of StoreSales - DateDim joins for predicate on the
     * d_date column we can apply the scaling factor.
     */
    double ndvScalingFactor = 1.0;
    if ( isPKSideSimpleTree ) {
      ndvScalingFactor = pkSide == 0 ? leftNDV/rightNDV : rightNDV / leftNDV;
    }

    if (pkSide == 0) {
      FKSideInfo fkInfo = new FKSideInfo(rightRowCount,
          rightNDV);
      double pkSelectivity = pkSelectivity(joinRel, mq, true, left, leftRowCount);
      PKSideInfo pkInfo = new PKSideInfo(leftRowCount,
          leftNDV,
          joinRel.getJoinType().generatesNullsOnRight() ? 1.0 :
            pkSelectivity);

      return new PKFKRelationInfo(1, fkInfo, pkInfo, ndvScalingFactor, isPKSideSimpleTree);
    } else { // pkSide == 1
      FKSideInfo fkInfo = new FKSideInfo(leftRowCount,
          leftNDV);
      double pkSelectivity = pkSelectivity(joinRel, mq, false, right, rightRowCount);
      PKSideInfo pkInfo = new PKSideInfo(rightRowCount,
          rightNDV,
          joinRel.getJoinType().generatesNullsOnLeft() ? 1.0 :
            pkSelectivity);

      return new PKFKRelationInfo(0, fkInfo, pkInfo, ndvScalingFactor, isPKSideSimpleTree);
    }
  }

  /*
   *
   */
  public static Pair<PKFKRelationInfo, RexNode> constraintsBasedAnalyzeJoinForPKFK(Join join, RelMetadataQuery mq) {

    if (join instanceof SemiJoin) {
      // TODO: Support semijoin
      return null;
    }

    final RelNode left = join.getInputs().get(0);
    final RelNode right = join.getInputs().get(1);

    // 1) Split filters in conjuncts
    final List<RexNode> condConjs = RelOptUtil.conjunctions(
        join.getCondition());

    if (condConjs.isEmpty()) {
      // Bail out
      return null;
    }

    // 2) Classify filters depending on their provenance
    final List<RexNode> joinFilters = new ArrayList<>(condConjs);
    final List<RexNode> leftFilters = new ArrayList<>();
    final List<RexNode> rightFilters = new ArrayList<>();
    RelOptUtil.classifyFilters(join, joinFilters, join.getJoinType(),false,
        !join.getJoinType().generatesNullsOnRight(), !join.getJoinType().generatesNullsOnLeft(),
        joinFilters, leftFilters, rightFilters);

    // 3) Check if we are joining on PK-FK
    final PKFKJoinInfo leftInputResult =
        HiveRelOptUtil.extractPKFKJoin(join, joinFilters, false, mq);
    final PKFKJoinInfo rightInputResult =
        HiveRelOptUtil.extractPKFKJoin(join, joinFilters, true, mq);
    if (leftInputResult == null && rightInputResult == null) {
      // Nothing to do here, bail out
      return null;
    }

    boolean leftIsKey = (join.getJoinType() == JoinRelType.INNER || join.getJoinType() == JoinRelType.RIGHT)
        && leftInputResult.isPkFkJoin;
    boolean rightIsKey = (join.getJoinType() == JoinRelType.INNER || join.getJoinType() == JoinRelType.LEFT)
        && rightInputResult.isPkFkJoin;
    if (!leftIsKey && !rightIsKey) {
      // Nothing to do here, bail out
      return null;
    }
    final double leftRowCount = mq.getRowCount(left);
    final double rightRowCount = mq.getRowCount(right);
    if (leftIsKey && rightIsKey) {
      if (rightRowCount < leftRowCount) {
        leftIsKey = false;
      }
    }
    final ImmutableBitSet lBitSet = leftIsKey ? leftInputResult.pkFkJoinColumns.left : rightInputResult.pkFkJoinColumns.left;
    final ImmutableBitSet rBitSet = leftIsKey ? leftInputResult.pkFkJoinColumns.right : rightInputResult.pkFkJoinColumns.right;
    final List<RexNode> residualFilters = leftIsKey ? leftInputResult.additionalPredicates : rightInputResult.additionalPredicates;

    // 4) Extract additional information on the PK-FK relationship
    int pkSide = leftIsKey ? 0 : 1;
    boolean isPKSideSimpleTree = leftIsKey ? SimpleTreeOnJoinKey.check(true, left, lBitSet, mq) :
        SimpleTreeOnJoinKey.check(true, right, rBitSet, mq);
    RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    RexNode leftPred = RexUtil.composeConjunction(
        rexBuilder, leftFilters, true);
    RexNode rightPred = RexUtil.composeConjunction(
        rexBuilder, rightFilters, true);
    double leftNDV = isPKSideSimpleTree ? mq.getDistinctRowCount(left, lBitSet, leftPred) : -1;
    double rightNDV = isPKSideSimpleTree ? mq.getDistinctRowCount(right, rBitSet, rightPred) : -1;

    // 5) Add the rest of operators back to the join filters
    // and create residual condition
    RexNode residualCond = residualFilters.isEmpty() ? null :
        residualFilters.size() == 1 ? residualFilters.get(0) :
            rexBuilder.makeCall(SqlStdOperatorTable.AND, residualFilters);

    // 6) Return result
    if (pkSide == 0) {
      FKSideInfo fkInfo = new FKSideInfo(rightRowCount,
          rightNDV);
      double pkSelectivity = pkSelectivity(join, mq, true, left, leftRowCount);
      PKSideInfo pkInfo = new PKSideInfo(leftRowCount,
          leftNDV,
          join.getJoinType().generatesNullsOnRight() ? 1.0 :
              pkSelectivity);
      double ndvScalingFactor = isPKSideSimpleTree ? leftNDV/rightNDV : 1.0;
      if (isPKSideSimpleTree) {
        ndvScalingFactor = leftNDV/rightNDV;
      }
      return Pair.of(new PKFKRelationInfo(1, fkInfo, pkInfo, ndvScalingFactor, isPKSideSimpleTree),
          residualCond);
    } else { // pkSide == 1
      FKSideInfo fkInfo = new FKSideInfo(leftRowCount,
          leftNDV);
      double pkSelectivity = pkSelectivity(join, mq, false, right, rightRowCount);
      PKSideInfo pkInfo = new PKSideInfo(rightRowCount,
          rightNDV,
          join.getJoinType().generatesNullsOnLeft() ? 1.0 :
              pkSelectivity);
      double ndvScalingFactor = isPKSideSimpleTree ? rightNDV/leftNDV : 1.0;
      return Pair.of(new PKFKRelationInfo(0, fkInfo, pkInfo, ndvScalingFactor, isPKSideSimpleTree),
          residualCond);
    }
  }

  private static double pkSelectivity(Join joinRel, RelMetadataQuery mq, boolean leftChild,
      RelNode child,
      double childRowCount) {
    if ((leftChild && joinRel.getJoinType().generatesNullsOnRight()) ||
        (!leftChild && joinRel.getJoinType().generatesNullsOnLeft())) {
      return 1.0;
    } else {
      HiveTableScan tScan = EstimateUniqueKeys.getTableScan(child, true);
      if (tScan != null) {
        double tRowCount = mq.getRowCount(tScan);
        return childRowCount / tRowCount;
      } else {
        return 1.0;
      }
    }
  }

  private static boolean isKey(ImmutableBitSet c, RelNode rel, RelMetadataQuery mq) {
    boolean isKey = false;
    //EstimateUniqueKeys doesn't go through metadata providers anymore, which means we will not be
    // taking advantage of metadata caching anymore. This could potential increase query compile time
    // Leaving a note here in case we see increase in timings
    Set<ImmutableBitSet> keys = EstimateUniqueKeys.getUniqueKeys(rel);
    if (keys != null) {
      for (ImmutableBitSet key : keys) {
        if (key.equals(c)) {
          isKey = true;
          break;
        }
      }
    }
    return isKey;
  }

  /*
   * 1. Join condition must be an Equality Predicate.
   * 2. both sides must reference 1 column.
   * 3. If needed flip the columns.
   */
  private static Pair<Integer, Integer> canHandleJoin(Join joinRel,
      List<RexNode> leftFilters, List<RexNode> rightFilters,
      List<RexNode> joinFilters) {

    /*
     * If after classifying filters there is more than 1 joining predicate, we
     * don't handle this. Return null.
     */
    if (joinFilters.size() != 1) {
      return null;
    }

    RexNode joinCond = joinFilters.get(0);

    int leftColIdx;
    int rightColIdx;

    if (!(joinCond instanceof RexCall)) {
      return null;
    }

    if (((RexCall) joinCond).getOperator() != SqlStdOperatorTable.EQUALS) {
      return null;
    }

    ImmutableBitSet leftCols = RelOptUtil.InputFinder.bits(((RexCall) joinCond).getOperands().get(0));
    ImmutableBitSet rightCols = RelOptUtil.InputFinder.bits(((RexCall) joinCond).getOperands().get(1));

    if (leftCols.cardinality() != 1 || rightCols.cardinality() != 1 ) {
      return null;
    }

    int nFieldsLeft = joinRel.getLeft().getRowType().getFieldList().size();
    int nFieldsRight = joinRel.getRight().getRowType().getFieldList().size();
    int nSysFields = joinRel.getSystemFieldList().size();
    ImmutableBitSet rightFieldsBitSet = ImmutableBitSet.range(nSysFields + nFieldsLeft,
        nSysFields + nFieldsLeft + nFieldsRight);
    /*
     * flip column references if join condition specified in reverse order to
     * join sources.
     */
    if (rightFieldsBitSet.contains(leftCols)) {
      ImmutableBitSet t = leftCols;
      leftCols = rightCols;
      rightCols = t;
    }

    leftColIdx = leftCols.nextSetBit(0) - nSysFields;
    rightColIdx = rightCols.nextSetBit(0) - (nSysFields + nFieldsLeft);

    return new Pair<Integer, Integer>(leftColIdx, rightColIdx);
  }

  private static class SimpleTreeOnJoinKey extends RelVisitor {

    boolean constraintsBased;
    ImmutableBitSet joinKey;
    boolean simpleTree;
    RelMetadataQuery mq;

    static boolean check(boolean constraintsBased, RelNode r, ImmutableBitSet joinKey, RelMetadataQuery mq) {
      SimpleTreeOnJoinKey v = new SimpleTreeOnJoinKey(constraintsBased, joinKey, mq);
      v.go(r);
      return v.simpleTree;
    }

    SimpleTreeOnJoinKey(boolean constraintsBased, ImmutableBitSet joinKey, RelMetadataQuery mq) {
      super();
      this.constraintsBased = constraintsBased;
      this.joinKey = joinKey;
      this.mq = mq;
      simpleTree = true;
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {

      if (node instanceof HepRelVertex) {
        node = ((HepRelVertex) node).getCurrentRel();
      }

      if (node instanceof TableScan) {
        simpleTree = true;
      } else if (node instanceof Project) {
        simpleTree = isSimple((Project) node);
      } else if (node instanceof Filter) {
        simpleTree = isSimple((Filter) node, mq);
      } else {
        simpleTree = false;
      }

      if (simpleTree) {
        super.visit(node, ordinal, parent);
      }
    }

    private boolean isSimple(Project project) {
      ImmutableBitSet.Builder b = ImmutableBitSet.builder();
      for (int pos : joinKey) {
        RexNode r = project.getProjects().get(pos);
        if (!(r instanceof RexInputRef)) {
          return false;
        }
        b.set(((RexInputRef) r).getIndex());
      }
      joinKey = b.build();
      return true;
    }

    private boolean isSimple(Filter filter, RelMetadataQuery mq) {
      ImmutableBitSet condBits = RelOptUtil.InputFinder.bits(filter.getCondition());
      if (constraintsBased) {
        return mq.areColumnsUnique(filter, condBits);
      }
      return isKey(condBits, filter, mq);
    }

  }

}
