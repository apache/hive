/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
