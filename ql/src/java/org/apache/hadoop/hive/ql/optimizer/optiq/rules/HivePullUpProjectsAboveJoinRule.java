package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.rules.PullUpProjectsAboveJoinRule;
import org.eigenbase.relopt.RelOptRuleOperand;

public class HivePullUpProjectsAboveJoinRule extends PullUpProjectsAboveJoinRule {

  public static final HivePullUpProjectsAboveJoinRule BOTH_PROJECT  = new HivePullUpProjectsAboveJoinRule(
                                                                        operand(
                                                                            HiveJoinRel.class,
                                                                            operand(
                                                                                ProjectRelBase.class,
                                                                                any()),
                                                                            operand(
                                                                                ProjectRelBase.class,
                                                                                any())),
                                                                        "HivePullUpProjectsAboveJoinRule: with two HiveProjectRel children");

  public static final HivePullUpProjectsAboveJoinRule LEFT_PROJECT  = new HivePullUpProjectsAboveJoinRule(
                                                                        operand(
                                                                            HiveJoinRel.class,
                                                                            some(operand(
                                                                                ProjectRelBase.class,
                                                                                any()))),
                                                                        "HivePullUpProjectsAboveJoinRule: with HiveProjectRel on left");

  public static final HivePullUpProjectsAboveJoinRule RIGHT_PROJECT = new HivePullUpProjectsAboveJoinRule(
                                                                        operand(
                                                                            HiveJoinRel.class,
                                                                            operand(RelNode.class,
                                                                                any()),
                                                                            operand(
                                                                                ProjectRelBase.class,
                                                                                any())),
                                                                        "HivePullUpProjectsAboveJoinRule: with HiveProjectRel on right");

  public HivePullUpProjectsAboveJoinRule(RelOptRuleOperand operand, String description) {
    super(operand, description, HiveProjectRel.DEFAULT_PROJECT_FACTORY);
  }
}
