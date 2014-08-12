package org.apache.hadoop.hive.ql.optimizer.optiq.stats;

import java.util.BitSet;
import java.util.List;

import net.hydromatic.optiq.BuiltinMethod;

import org.apache.hadoop.hive.ql.optimizer.optiq.HiveOptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCost;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveTableScanRel;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.ChainedRelMetadataProvider;
import org.eigenbase.rel.metadata.ReflectiveRelMetadataProvider;
import org.eigenbase.rel.metadata.RelMdDistinctRowCount;
import org.eigenbase.rel.metadata.RelMdUtil;
import org.eigenbase.rel.metadata.RelMetadataProvider;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.rex.RexNode;

import com.google.common.collect.ImmutableList;

public class HiveRelMdDistinctRowCount extends RelMdDistinctRowCount {

  private static final HiveRelMdDistinctRowCount INSTANCE =
      new HiveRelMdDistinctRowCount();

  public static final RelMetadataProvider SOURCE = ChainedRelMetadataProvider
      .of(ImmutableList.of(

      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltinMethod.DISTINCT_ROW_COUNT.method, INSTANCE),

      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltinMethod.CUMULATIVE_COST.method, INSTANCE)));

  private HiveRelMdDistinctRowCount() {
  }

  // Catch-all rule when none of the others apply.
  @Override
  public Double getDistinctRowCount(RelNode rel, BitSet groupKey,
      RexNode predicate) {
    if (rel instanceof HiveTableScanRel) {
      return getDistinctRowCount((HiveTableScanRel) rel, groupKey, predicate);
    }
    /*
     * For now use Optiq' default formulas for propagating NDVs up the Query
     * Tree.
     */
    return super.getDistinctRowCount(rel, groupKey, predicate);
  }

  private Double getDistinctRowCount(HiveTableScanRel htRel, BitSet groupKey,
      RexNode predicate) {
    List<Integer> projIndxLst = HiveOptiqUtil
        .translateBitSetToProjIndx(groupKey);
    List<ColStatistics> colStats = htRel.getColStat(projIndxLst);
    Double noDistinctRows = 1.0;
    for (ColStatistics cStat : colStats) {
      noDistinctRows *= cStat.getCountDistint();
    }

    return Math.min(noDistinctRows, htRel.getRows());
  }

  public static Double getDistinctRowCount(RelNode r, int indx) {
    BitSet bitSetOfRqdProj = new BitSet();
    bitSetOfRqdProj.set(indx);
    return RelMetadataQuery.getDistinctRowCount(r, bitSetOfRqdProj, r
        .getCluster().getRexBuilder().makeLiteral(true));
  }

  @Override
  public Double getDistinctRowCount(JoinRelBase rel, BitSet groupKey,
      RexNode predicate) {
    if (rel instanceof HiveJoinRel) {
      HiveJoinRel hjRel = (HiveJoinRel) rel;
      //TODO: Improve this
      if (hjRel.isLeftSemiJoin()) {
        return RelMetadataQuery.getDistinctRowCount(hjRel.getLeft(), groupKey,
            rel.getCluster().getRexBuilder().makeLiteral(true));
      } else {
        return RelMdUtil.getJoinDistinctRowCount(rel, rel.getJoinType(),
            groupKey, predicate, true);
      }
    }

    return RelMetadataQuery.getDistinctRowCount(rel, groupKey, predicate);
  }

  /*
   * Favor Broad Plans over Deep Plans.
   */
  public RelOptCost getCumulativeCost(HiveJoinRel rel) {
    RelOptCost cost = RelMetadataQuery.getNonCumulativeCost(rel);
    List<RelNode> inputs = rel.getInputs();
    RelOptCost maxICost = HiveCost.ZERO;
    for (RelNode input : inputs) {
      RelOptCost iCost = RelMetadataQuery.getCumulativeCost(input);
      if (maxICost.isLt(iCost)) {
        maxICost = iCost;
      }
    }
    return cost.plus(maxICost);
  }
}
