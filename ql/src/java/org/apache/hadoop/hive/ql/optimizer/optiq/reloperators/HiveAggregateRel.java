package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.BitSet;
import java.util.List;

import net.hydromatic.optiq.util.BitSets;

import org.apache.hadoop.hive.ql.optimizer.optiq.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCost;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

public class HiveAggregateRel extends AggregateRelBase implements HiveRel {

  public HiveAggregateRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
      BitSet groupSet, List<AggregateCall> aggCalls) throws InvalidRelException {
    super(cluster, TraitsUtil.getAggregateTraitSet(cluster, traitSet, BitSets.toList(groupSet),
        aggCalls, child), child, groupSet, aggCalls);
  }

  public AggregateRelBase copy(RelTraitSet traitSet, RelNode input, BitSet groupSet,
      List<AggregateCall> aggCalls) {
    try {
      return new HiveAggregateRel(getCluster(), traitSet, input, groupSet, aggCalls);
    } catch (InvalidRelException e) {
      // Semantic error not possible. Must be a bug. Convert to
      // internal error.
      throw new AssertionError(e);
    }
  }

  public void implement(Implementor implementor) {
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return HiveCost.FACTORY.makeZeroCost();
  }

  @Override
  public double getRows() {
    return RelMetadataQuery.getDistinctRowCount(this, groupSet, getCluster().getRexBuilder()
        .makeLiteral(true));
  }
}
