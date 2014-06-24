package org.apache.hadoop.hive.ql.optimizer.optiq.stats;

import java.util.BitSet;
import java.util.List;

import net.hydromatic.optiq.BuiltinMethod;

import org.apache.hadoop.hive.ql.optimizer.optiq.HiveOptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveTableScanRel;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.ReflectiveRelMetadataProvider;
import org.eigenbase.rel.metadata.RelMdDistinctRowCount;
import org.eigenbase.rel.metadata.RelMdUtil;
import org.eigenbase.rel.metadata.RelMetadataProvider;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util14.NumberUtil;

public class HiveRelMdDistinctRowCount extends RelMdDistinctRowCount {
  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
                                                     BuiltinMethod.DISTINCT_ROW_COUNT.method,
                                                     new HiveRelMdDistinctRowCount());

  private HiveRelMdDistinctRowCount() {
  }

  // Catch-all rule when none of the others apply.
  @Override
  public Double getDistinctRowCount(RelNode rel, BitSet groupKey, RexNode predicate) {
    if (rel instanceof HiveTableScanRel) {
      return getDistinctRowCount((HiveTableScanRel) rel, groupKey, predicate);
    }

    return NumberUtil.multiply(RelMetadataQuery.getRowCount(rel),
        RelMetadataQuery.getSelectivity(rel, predicate));
  }

  private Double getDistinctRowCount(HiveTableScanRel htRel, BitSet groupKey, RexNode predicate) {
    List<Integer> projIndxLst = HiveOptiqUtil.translateBitSetToProjIndx(groupKey);
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
    return RelMetadataQuery.getDistinctRowCount(r, bitSetOfRqdProj, r.getCluster().getRexBuilder()
        .makeLiteral(true));
  }

  @Override
  public Double getDistinctRowCount(JoinRelBase rel, BitSet groupKey, RexNode predicate) {
    return RelMdUtil.getJoinDistinctRowCount(rel, rel.getJoinType(), groupKey, predicate, true);
  }
}
