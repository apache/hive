package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.hadoop.hive.ql.optimizer.optiq.HiveOptiqUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCost;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelFactories.ProjectFactory;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;

public class HiveProjectRel extends ProjectRelBase implements HiveRel {

  public static final ProjectFactory DEFAULT_PROJECT_FACTORY = new HiveProjectFactoryImpl();

  private final List<Integer>        m_virtualCols;

  /**
   * Creates a HiveProjectRel.
   * 
   * @param cluster
   *          Cluster this relational expression belongs to
   * @param child
   *          input relational expression
   * @param exps
   *          List of expressions for the input columns
   * @param rowType
   *          output row type
   * @param flags
   *          values as in {@link ProjectRelBase.Flags}
   */
  public HiveProjectRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
      List<RexNode> exps, RelDataType rowType, int flags) {
    super(cluster, traitSet, child, exps, rowType, flags);
    m_virtualCols = ImmutableList.copyOf(HiveOptiqUtil.getVirtualCols(exps));
  }

  /**
   * Creates a HiveProjectRel with no sort keys.
   * 
   * @param child
   *          input relational expression
   * @param exps
   *          set of expressions for the input columns
   * @param fieldNames
   *          aliases of the expressions
   */
  public static HiveProjectRel create(RelNode child, List<RexNode> exps, List<String> fieldNames) {
    RelOptCluster cluster = child.getCluster();
    RelDataType rowType = RexUtil.createStructType(cluster.getTypeFactory(), exps, fieldNames);
    return create(cluster, child, exps, rowType, Collections.<RelCollation> emptyList());
  }

  /**
   * Creates a HiveProjectRel.
   */
  public static HiveProjectRel create(RelOptCluster cluster, RelNode child, List<RexNode> exps,
      RelDataType rowType, final List<RelCollation> collationList) {
    RelTraitSet traitSet = TraitsUtil.getSelectTraitSet(cluster, exps, child);
    return new HiveProjectRel(cluster, traitSet, child, exps, rowType, Flags.BOXED);
  }

  public ProjectRelBase copy(RelTraitSet traitSet, RelNode input, List<RexNode> exps,
      RelDataType rowType) {
    assert traitSet.containsIfApplicable(HiveRel.CONVENTION);
    return new HiveProjectRel(getCluster(), traitSet, input, exps, rowType, getFlags());
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return HiveCost.FACTORY.makeZeroCost();
  }

  public void implement(Implementor implementor) {
  }

  public List<Integer> getVirtualCols() {
    return m_virtualCols;
  }

  /**
   * Implementation of {@link ProjectFactory} that returns
   * {@link org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel}
   * .
   */
  private static class HiveProjectFactoryImpl implements ProjectFactory {
    @Override
    public RelNode createProject(RelNode input, List<RexNode> exps, List<String> fieldNames) {
      RelNode project = HiveProjectRel.create(input, exps, fieldNames);

      // Make sure extra traits are carried over from the original rel
      project = RelOptRule.convert(project, input.getTraitSet());
      return project;
    }
  }
}
