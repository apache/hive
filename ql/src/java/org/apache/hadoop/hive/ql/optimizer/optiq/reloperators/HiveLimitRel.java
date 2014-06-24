package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCost;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

public class HiveLimitRel extends SingleRel implements HiveRel {
  private final RexNode offset;
  private final RexNode fetch;

  HiveLimitRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode offset,
      RexNode fetch) {
    super(cluster, TraitsUtil.getLimitTraitSet(cluster, traitSet, child), child);
    this.offset = offset;
    this.fetch = fetch;
    assert getConvention() instanceof HiveRel;
    assert getConvention() == child.getConvention();
  }

  @Override
  public HiveLimitRel copy(RelTraitSet traitSet, List<RelNode> newInputs) {
    return new HiveLimitRel(getCluster(), traitSet, sole(newInputs), offset, fetch);
  }

  public void implement(Implementor implementor) {
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return HiveCost.FACTORY.makeZeroCost();
  }
}
