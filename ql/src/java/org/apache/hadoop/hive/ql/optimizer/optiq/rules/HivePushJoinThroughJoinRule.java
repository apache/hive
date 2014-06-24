package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel.JoinAlgorithm;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.rules.PushJoinThroughJoinRule;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;

public class HivePushJoinThroughJoinRule extends PushJoinThroughJoinRule {
  public static final RelOptRule RIGHT = new HivePushJoinThroughJoinRule(
                                           "Hive PushJoinThroughJoinRule:right", true,
                                           HiveJoinRel.class);
  public static final RelOptRule LEFT  = new HivePushJoinThroughJoinRule(
                                           "Hive PushJoinThroughJoinRule:left", false,
                                           HiveJoinRel.class);

  private HivePushJoinThroughJoinRule(String description, boolean right,
      Class<? extends JoinRelBase> clazz) {
    super(description, right, clazz, HiveProjectRel.DEFAULT_PROJECT_FACTORY);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    boolean isAMatch = false;
    final HiveJoinRel topJoin = call.rel(0);
    final HiveJoinRel bottomJoin = call.rel(1);

    if (!topJoin.isLeftSemiJoin() && topJoin.getJoinAlgorithm() == JoinAlgorithm.NONE
        && bottomJoin.getJoinAlgorithm() == JoinAlgorithm.NONE) {
      isAMatch = true;
    }

    return isAMatch;
  }
}
