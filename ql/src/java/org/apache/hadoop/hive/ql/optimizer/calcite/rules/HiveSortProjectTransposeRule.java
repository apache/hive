package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;

import com.google.common.collect.ImmutableList;

public class HiveSortProjectTransposeRule extends RelOptRule {

  public static final HiveSortProjectTransposeRule INSTANCE =
      new HiveSortProjectTransposeRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a HiveSortProjectTransposeRule.
   */
  private HiveSortProjectTransposeRule() {
    super(
        operand(
            HiveSortLimit.class,
            operand(HiveProject.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveSortLimit sortLimit = call.rel(0);

    // If does not contain a limit operation, we bail out
    if (!HiveCalciteUtil.limitRelNode(sortLimit)) {
      return false;
    }

    return true;
  }

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    final HiveSortLimit sort = call.rel(0);
    final HiveProject project = call.rel(1);

    // Determine mapping between project input and output fields. If sort
    // relies on non-trivial expressions, we can't push.
    final Mappings.TargetMapping map =
        RelOptUtil.permutation(
            project.getProjects(), project.getInput().getRowType());
    for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
      if (map.getTargetOpt(fc.getFieldIndex()) < 0) {
        return;
      }
    }

    // Create new collation
    final RelCollation newCollation =
        RelCollationTraitDef.INSTANCE.canonize(
            RexUtil.apply(map, sort.getCollation()));

    // New operators
    final HiveSortLimit newSort = sort.copy(sort.getTraitSet().replace(newCollation),
            project.getInput(), newCollation, sort.offset, sort.fetch);
    final RelNode newProject = project.copy(sort.getTraitSet(),
            ImmutableList.<RelNode>of(newSort));

    call.transformTo(newProject);
  }

}
