package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel.JoinAlgorithm;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;

import org.eigenbase.rel.rules.SwapJoinRule;
import org.eigenbase.relopt.RelOptRuleCall;

public class HiveSwapJoinRule extends SwapJoinRule {
  public static final HiveSwapJoinRule INSTANCE = new HiveSwapJoinRule();

  private HiveSwapJoinRule() {
    super(HiveJoinRel.class, HiveProjectRel.DEFAULT_PROJECT_FACTORY);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.<HiveJoinRel> rel(0).isLeftSemiJoin())
      return false;
    else
      return super.matches(call)
          && call.<HiveJoinRel> rel(0).getJoinAlgorithm() == JoinAlgorithm.NONE;
  }
}
