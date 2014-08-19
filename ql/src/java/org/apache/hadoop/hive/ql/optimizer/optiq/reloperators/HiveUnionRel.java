package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel.Implementor;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SetOpRel;
import org.eigenbase.rel.UnionRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;

public class HiveUnionRel extends UnionRelBase {

  public HiveUnionRel(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs) {
    super(cluster, traits, inputs, true);
  }

  @Override
  public SetOpRel copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    return new HiveUnionRel(this.getCluster(), traitSet, inputs);
  }

  public void implement(Implementor implementor) {
  }
}
