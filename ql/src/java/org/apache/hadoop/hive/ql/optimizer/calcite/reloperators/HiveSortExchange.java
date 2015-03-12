package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SortExchange;

public class HiveSortExchange extends SortExchange {

  private HiveSortExchange(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RelDistribution distribution, RelCollation collation) {
    super(cluster, traitSet, input, distribution, collation);
  }

  public HiveSortExchange(RelInput input) {
    super(input);
  }
  
  /**
   * Creates a HiveSortExchange.
   *
   * @param input     Input relational expression
   * @param distribution Distribution specification
   * @param collation Collation specification
   */
  public static HiveSortExchange create(RelNode input,
      RelDistribution distribution, RelCollation collation) {
    RelOptCluster cluster = input.getCluster();
    distribution = RelDistributionTraitDef.INSTANCE.canonize(distribution);
    RelTraitSet traitSet =
        input.getTraitSet().replace(Convention.NONE).replace(distribution);
    collation = RelCollationTraitDef.INSTANCE.canonize(collation);
    return new HiveSortExchange(cluster, traitSet, input, distribution, collation);
  }

  @Override
  public SortExchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution,
          RelCollation newCollation) {
    return new HiveSortExchange(getCluster(), traitSet, newInput,
            newDistribution, newCollation);
  }

}
