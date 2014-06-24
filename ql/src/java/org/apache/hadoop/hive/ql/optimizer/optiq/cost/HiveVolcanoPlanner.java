package org.apache.hadoop.hive.ql.optimizer.optiq.cost;

import org.eigenbase.rel.RelCollationTraitDef;
import org.eigenbase.relopt.ConventionTraitDef;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.volcano.VolcanoPlanner;

/**
 * Refinement of {@link org.eigenbase.relopt.volcano.VolcanoPlanner} for Hive.
 * 
 * <p>
 * It uses {@link org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCost} as
 * its cost model.
 */
public class HiveVolcanoPlanner extends VolcanoPlanner {
  private static final boolean ENABLE_COLLATION_TRAIT = true;

  /** Creates a HiveVolcanoPlanner. */
  public HiveVolcanoPlanner() {
    super(HiveCost.FACTORY);
  }

  public static RelOptPlanner createPlanner() {
    final VolcanoPlanner planner = new HiveVolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    if (ENABLE_COLLATION_TRAIT) {
      planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
      planner.registerAbstractRelationalRules();
    }
    return planner;
  }
}
