package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import org.apache.hadoop.hive.ql.optimizer.optiq.TraitsUtil;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelFactories;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

public class HiveSortRel extends SortRel implements HiveRel {

  public static final HiveSortRelFactory HIVE_SORT_REL_FACTORY = new HiveSortRelFactory();

  public HiveSortRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
      RelCollation collation, RexNode offset, RexNode fetch) {
    super(cluster, TraitsUtil.getSortTraitSet(cluster, traitSet, collation), child, collation,
        offset, fetch);

    assert getConvention() == child.getConvention();
  }

  @Override
  public HiveSortRel copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation,
      RexNode offset, RexNode fetch) {
    // TODO: can we blindly copy sort trait? What if inputs changed and we
    // are now sorting by different cols
    RelCollation canonizedCollation = traitSet.canonize(newCollation);
    return new HiveSortRel(getCluster(), traitSet, newInput, canonizedCollation, offset, fetch);
  }

  public RexNode getFetchExpr() {
    return fetch;
  }

  @Override
  public void implement(Implementor implementor) {
  }

  private static class HiveSortRelFactory implements RelFactories.SortFactory {

    @Override
    public RelNode createSort(RelTraitSet traits, RelNode child,
        RelCollation collation, RexNode offset, RexNode fetch) {
      return new HiveSortRel(child.getCluster(), traits, child, collation, offset, fetch);
    }
  }
}
