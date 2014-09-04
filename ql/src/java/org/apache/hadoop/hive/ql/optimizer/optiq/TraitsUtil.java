package org.apache.hadoop.hive.ql.optimizer.optiq;

import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;

public class TraitsUtil {

  public static RelTraitSet getSelectTraitSet(RelOptCluster cluster, RelNode child) {
    return cluster.traitSetOf(HiveRel.CONVENTION, RelCollationImpl.EMPTY);
  }

  public static RelTraitSet getSortTraitSet(RelOptCluster cluster, RelTraitSet traitSet,
      RelCollation collation) {
    return traitSet.plus(collation);
  }

  public static RelTraitSet getFilterTraitSet(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child) {
    return cluster.traitSetOf(HiveRel.CONVENTION, RelCollationImpl.EMPTY);
  }

  public static RelTraitSet getLimitTraitSet(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child) {
    return cluster.traitSetOf(HiveRel.CONVENTION, RelCollationImpl.EMPTY);
  }

  public static RelTraitSet getAggregateTraitSet(RelOptCluster cluster, RelTraitSet traitSet,
      List<Integer> gbCols, List<AggregateCall> aggCalls, RelNode child) {
    return cluster.traitSetOf(HiveRel.CONVENTION, RelCollationImpl.EMPTY);
  }

  public static RelTraitSet getTableScanTraitSet(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptHiveTable table, RelDataType rowtype) {
    return cluster.traitSetOf(HiveRel.CONVENTION, RelCollationImpl.EMPTY);
  }

  public static RelTraitSet getJoinTraitSet(RelOptCluster cluster, RelTraitSet traitSet) {
    return cluster.traitSetOf(HiveRel.CONVENTION, RelCollationImpl.EMPTY);
  }

  public static RelTraitSet getUnionTraitSet(RelOptCluster cluster, RelTraitSet traitSet) {
    return cluster.traitSetOf(HiveRel.CONVENTION, RelCollationImpl.EMPTY);
  }
}
