package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCost;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.RelFactories.FilterFactory;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

public class HiveFilterRel extends FilterRelBase implements HiveRel {

  public static final FilterFactory DEFAULT_FILTER_FACTORY = new HiveFilterFactoryImpl();

  public HiveFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, TraitsUtil.getFilterTraitSet(cluster, traits, child), child, condition);
  }

  @Override
  public FilterRelBase copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    assert traitSet.containsIfApplicable(HiveRel.CONVENTION);
    return new HiveFilterRel(getCluster(), traitSet, input, getCondition());
  }

  @Override
  public void implement(Implementor implementor) {
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return HiveCost.FACTORY.makeZeroCost();
  }

  /**
   * Implementation of {@link FilterFactory} that returns
   * {@link org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel}
   * .
   */
  private static class HiveFilterFactoryImpl implements FilterFactory {
    @Override
    public RelNode createFilter(RelNode child, RexNode condition) {
      RelOptCluster cluster = child.getCluster();
      HiveFilterRel filter = new HiveFilterRel(cluster, TraitsUtil.getFilterTraitSet(cluster, null,
          child), child, condition);
      return filter;
    }
  }
}
