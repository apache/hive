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
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import java.util.Map;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;

import com.google.common.collect.ImmutableMap;

public class HiveSort extends Sort implements HiveRelNode {

  public static final HiveSortRelFactory HIVE_SORT_REL_FACTORY = new HiveSortRelFactory();

  // NOTE: this is to work around Hive Calcite Limitations w.r.t OB.
  // 1. Calcite can not accept expressions in OB; instead it needs to be expressed
  // as VC in input Select.
  // 2. Hive can not preserve ordering through select boundaries.
  // 3. This map is used for outermost OB to migrate the VC corresponding OB
  // expressions from input select.
  // 4. This is used by ASTConverter after we are done with Calcite Planning
  private ImmutableMap<Integer, RexNode> mapOfInputRefToRexCall;

  public HiveSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
      RelCollation collation, RexNode offset, RexNode fetch) {
    super(cluster, TraitsUtil.getSortTraitSet(cluster, traitSet, collation), child, collation,
        offset, fetch);
  }

  @Override
  public HiveSort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation,
      RexNode offset, RexNode fetch) {
    // TODO: can we blindly copy sort trait? What if inputs changed and we
    // are now sorting by different cols
    RelCollation canonizedCollation = traitSet.canonize(newCollation);
    return new HiveSort(getCluster(), traitSet, newInput, canonizedCollation, offset, fetch);
  }

  public RexNode getFetchExpr() {
    return fetch;
  }

  public void setInputRefToCallMap(ImmutableMap<Integer, RexNode> refToCall) {
    this.mapOfInputRefToRexCall = refToCall;
  }

  public Map<Integer, RexNode> getInputRefToCallMap() {
    return this.mapOfInputRefToRexCall;
  }

  @Override
  public void implement(Implementor implementor) {
  }

  private static class HiveSortRelFactory implements RelFactories.SortFactory {

    @Override
    public RelNode createSort(RelTraitSet traits, RelNode child, RelCollation collation,
        RexNode offset, RexNode fetch) {
      return new HiveSort(child.getCluster(), traits, child, collation, offset, fetch);
    }
  }
}
