package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SortExchange;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import com.google.common.collect.ImmutableList;

public class HiveSortExchange extends SortExchange {
  private ImmutableList<RexNode> joinKeys;
  private ExprNodeDesc[] joinExpressions;

  private HiveSortExchange(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RelDistribution distribution, RelCollation collation, ImmutableList<RexNode> joinKeys) {
    super(cluster, traitSet, input, distribution, collation);
    this.joinKeys = new ImmutableList.Builder<RexNode>().addAll(joinKeys).build();
  }

  /**
   * Creates a HiveSortExchange.
   *
   * @param input     Input relational expression
   * @param distribution Distribution specification
   * @param collation Collation specification
   * @param joinKeys Join Keys specification
   */
  public static HiveSortExchange create(RelNode input,
      RelDistribution distribution, RelCollation collation, ImmutableList<RexNode> joinKeys) {
    RelOptCluster cluster = input.getCluster();
    distribution = RelDistributionTraitDef.INSTANCE.canonize(distribution);
    collation = RelCollationTraitDef.INSTANCE.canonize(collation);
    RelTraitSet traitSet = RelTraitSet.createEmpty().plus(distribution).plus(collation);
    return new HiveSortExchange(cluster, traitSet, input, distribution, collation, joinKeys);
  }

  @Override
  public SortExchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution,
          RelCollation newCollation) {
    return new HiveSortExchange(getCluster(), traitSet, newInput,
            newDistribution, newCollation, joinKeys);
  }

  public ImmutableList<RexNode> getJoinKeys() {
    return joinKeys;
  }

  public void setJoinKeys(ImmutableList<RexNode> joinKeys) {
    this.joinKeys = joinKeys;
  }

  public ExprNodeDesc[] getJoinExpressions() {
    return joinExpressions;
  }

  public void setJoinExpressions(ExprNodeDesc[] joinExpressions) {
    this.joinExpressions = joinExpressions;
  }

}
