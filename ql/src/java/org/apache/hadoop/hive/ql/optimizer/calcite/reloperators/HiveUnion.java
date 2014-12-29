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

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode.Implementor;

public class HiveUnion extends Union {

  public static final HiveUnionRelFactory UNION_REL_FACTORY = new HiveUnionRelFactory();

  public HiveUnion(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs) {
    super(cluster, traits, inputs, true);
  }

  @Override
  public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    return new HiveUnion(this.getCluster(), traitSet, inputs);
  }

  public void implement(Implementor implementor) {
  }

  private static class HiveUnionRelFactory implements RelFactories.SetOpFactory {

    @Override
    public RelNode createSetOp(SqlKind kind, List<RelNode> inputs, boolean all) {
      if (kind != SqlKind.UNION) {
        throw new IllegalStateException("Expected to get Set operator of type Union. Found : " + kind);
      }
      return new HiveUnion(inputs.get(0).getCluster(), inputs.get(0).getTraitSet(), inputs);
    }
  }
}
