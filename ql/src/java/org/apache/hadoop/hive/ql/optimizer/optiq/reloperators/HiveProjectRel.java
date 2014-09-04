package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.ArrayList;
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
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.util.mapping.Mapping;
import org.eigenbase.util.mapping.MappingType;

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
      List<? extends RexNode> exps, RelDataType rowType, int flags) {
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
  public static HiveProjectRel create(RelNode child, List<? extends RexNode> exps, List<String> fieldNames) {
    RelOptCluster cluster = child.getCluster();
    RelDataType rowType = RexUtil.createStructType(cluster.getTypeFactory(), exps, fieldNames);
    return create(cluster, child, exps, rowType, Collections.<RelCollation> emptyList());
  }

  /**
   * Creates a HiveProjectRel.
   */
  public static HiveProjectRel create(RelOptCluster cluster, RelNode child, List<? extends RexNode> exps,
      RelDataType rowType, final List<RelCollation> collationList) {
    RelTraitSet traitSet = TraitsUtil.getSelectTraitSet(cluster, child);
    return new HiveProjectRel(cluster, traitSet, child, exps, rowType, Flags.BOXED);
  }

  /**
   * Creates a relational expression which projects the output fields of a
   * relational expression according to a partial mapping.
   *
   * <p>
   * A partial mapping is weaker than a permutation: every target has one
   * source, but a source may have 0, 1 or more than one targets. Usually the
   * result will have fewer fields than the source, unless some source fields
   * are projected multiple times.
   *
   * <p>
   * This method could optimize the result as {@link #permute} does, but does
   * not at present.
   *
   * @param rel
   *          Relational expression
   * @param mapping
   *          Mapping from source fields to target fields. The mapping type must
   *          obey the constraints {@link MappingType#isMandatorySource()} and
   *          {@link MappingType#isSingleSource()}, as does
   *          {@link MappingType#INVERSE_FUNCTION}.
   * @param fieldNames
   *          Field names; if null, or if a particular entry is null, the name
   *          of the permuted field is used
   * @return relational expression which projects a subset of the input fields
   */
  public static RelNode projectMapping(RelNode rel, Mapping mapping, List<String> fieldNames) {
    assert mapping.getMappingType().isSingleSource();
    assert mapping.getMappingType().isMandatorySource();

    if (mapping.isIdentity()) {
      return rel;
    }

    final List<String> outputNameList = new ArrayList<String>();
    final List<RexNode> outputProjList = new ArrayList<RexNode>();
    final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

    for (int i = 0; i < mapping.getTargetCount(); i++) {
      int source = mapping.getSource(i);
      final RelDataTypeField sourceField = fields.get(source);
      outputNameList
          .add(((fieldNames == null) || (fieldNames.size() <= i) || (fieldNames.get(i) == null)) ? sourceField
              .getName() : fieldNames.get(i));
      outputProjList.add(rexBuilder.makeInputRef(rel, source));
    }

    return create(rel, outputProjList, outputNameList);
  }

  @Override
  public ProjectRelBase copy(RelTraitSet traitSet, RelNode input, List<RexNode> exps,
      RelDataType rowType) {
    assert traitSet.containsIfApplicable(HiveRel.CONVENTION);
    return new HiveProjectRel(getCluster(), traitSet, input, exps, rowType, getFlags());
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return HiveCost.FACTORY.makeZeroCost();
  }

  @Override
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
    public RelNode createProject(RelNode child,
        List<? extends RexNode> childExprs, List<String> fieldNames) {
      RelNode project = HiveProjectRel.create(child, childExprs, fieldNames);

      // Make sure extra traits are carried over from the original rel
      project = RelOptRule.convert(project, child.getTraitSet());
      return project;
    }
  }
}
