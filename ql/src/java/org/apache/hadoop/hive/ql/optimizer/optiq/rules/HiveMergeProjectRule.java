package org.apache.hadoop.hive.ql.optimizer.optiq.rules;

import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.eigenbase.rel.rules.MergeProjectRule;

public class HiveMergeProjectRule extends MergeProjectRule {
  public static final HiveMergeProjectRule INSTANCE = new HiveMergeProjectRule();

  public HiveMergeProjectRule() {
    super(true, HiveProjectRel.DEFAULT_PROJECT_FACTORY);
  }
}
