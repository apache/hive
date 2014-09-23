package org.apache.hadoop.hive.ql.optimizer.optiq;


import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;

public class TraitsUtil {
  public static RelTraitSet getSortTraitSet(RelOptCluster cluster, RelTraitSet traitSet,
      RelCollation collation) {
    return traitSet.plus(collation);
  }

  public static RelTraitSet getDefaultTraitSet(RelOptCluster cluster) {
    return cluster.traitSetOf(HiveRel.CONVENTION, RelCollationImpl.EMPTY);
  }
}
