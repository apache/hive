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
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;

import com.google.common.collect.ImmutableList;

public class HiveProjectSortTransposeRule extends RelOptRule {

  public static final HiveProjectSortTransposeRule INSTANCE =
      new HiveProjectSortTransposeRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a HiveProjectSortTransposeRule.
   */
  private HiveProjectSortTransposeRule() {
    super(
        operand(
            HiveProject.class,
            operand(HiveSortLimit.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    final HiveProject project = call.rel(0);
    final HiveSortLimit sort = call.rel(1);

    // Determine mapping between project input and output fields. If sort
    // relies on non-trivial expressions, we can't push.
    final Mappings.TargetMapping map =
        RelOptUtil.permutation(
            project.getProjects(), project.getInput().getRowType()).inverse();
    for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
      if (map.getTarget(fc.getFieldIndex()) < 0) {
        return;
      }
    }

    // Create new collation
    final RelCollation newCollation =
        RelCollationTraitDef.INSTANCE.canonize(
            RexUtil.apply(map, sort.getCollation()));

    // New operators
    final RelNode newProject = project.copy(sort.getInput().getTraitSet(),
            ImmutableList.<RelNode>of(sort.getInput()));
    final HiveSortLimit newSort = sort.copy(newProject.getTraitSet(),
            newProject, newCollation, sort.offset, sort.fetch);

    call.transformTo(newSort);
  }

}
