package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;

public interface HiveRel extends RelNode {
  void implement(Implementor implementor);

  /** Calling convention for relational operations that occur in Hive. */
  final Convention CONVENTION = new Convention.Impl("HIVE", HiveRel.class);

  class Implementor {

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((HiveRel) input).implement(this);
    }
  }
}
