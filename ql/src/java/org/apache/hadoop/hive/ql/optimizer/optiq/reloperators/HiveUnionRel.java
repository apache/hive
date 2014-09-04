package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel.Implementor;
import org.eigenbase.rel.RelFactories;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SetOpRel;
import org.eigenbase.rel.UnionRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.sql.SqlKind;

public class HiveUnionRel extends UnionRelBase {

  public static final HiveUnionRelFactory UNION_REL_FACTORY = new HiveUnionRelFactory();

  public HiveUnionRel(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs) {
    super(cluster, traits, inputs, true);
  }

  @Override
  public SetOpRel copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    return new HiveUnionRel(this.getCluster(), traitSet, inputs);
  }

  public void implement(Implementor implementor) {
  }

  private static class HiveUnionRelFactory implements RelFactories.SetOpFactory {

    @Override
    public RelNode createSetOp(SqlKind kind, List<RelNode> inputs, boolean all) {
      if (kind != SqlKind.UNION) {
        throw new IllegalStateException("Expected to get Set operator of type Union. Found : " + kind);
      }
      return new HiveUnionRel(inputs.get(0).getCluster(), inputs.get(0).getTraitSet(), inputs);
    }
  }
}
