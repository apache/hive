/*
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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SortExchange;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import com.google.common.collect.ImmutableList;

/**
 * Hive extension of calcite SortExchange.
 * Add support of keys used when sorting or joining.
 */
public final class HiveSortExchange extends SortExchange implements HiveRelNode {
  private final ImmutableList<RexNode> keys;
  private ExprNodeDesc[] keyExpressions;

  private HiveSortExchange(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RelDistribution distribution, RelCollation collation, ImmutableList<RexNode> keys) {
    super(cluster, traitSet, input, distribution, collation);
    this.keys = new ImmutableList.Builder<RexNode>().addAll(keys).build();
  }

  /**
   * Creates a HiveSortExchange.
   *
   * @param input     Input relational expression
   * @param distribution Distribution specification
   * @param collation Collation specification
   * @param keys Keys specification
   */
  public static HiveSortExchange create(RelNode input,
      RelDistribution distribution, RelCollation collation, ImmutableList<RexNode> keys) {
    RelOptCluster cluster = input.getCluster();
    distribution = RelDistributionTraitDef.INSTANCE.canonize(distribution);
    collation = RelCollationTraitDef.INSTANCE.canonize(collation);
    RelTraitSet traitSet = getTraitSet(cluster, collation, distribution);
    return new HiveSortExchange(cluster, traitSet, input, distribution, collation, keys);
  }

  private static RelTraitSet getTraitSet(
      RelOptCluster cluster, RelCollation collation, RelDistribution distribution) {
    return TraitsUtil.getDefaultTraitSet(cluster).replace(collation).replace(distribution);
  }

  public static HiveSortExchange create(RelNode input,
      RelDistribution distribution, RelCollation collation) {
    RelOptCluster cluster = input.getCluster();
    distribution = RelDistributionTraitDef.INSTANCE.canonize(distribution);
    collation = RelCollationTraitDef.INSTANCE.canonize(collation);
    RelTraitSet traitSet = getTraitSet(cluster, collation, distribution);
    RelCollation canonizedCollation = traitSet.canonize(RelCollationImpl.of(collation.getFieldCollations()));

    ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
    for (RelFieldCollation relFieldCollation : canonizedCollation.getFieldCollations()) {
      int index = relFieldCollation.getFieldIndex();
      builder.add(cluster.getRexBuilder().makeInputRef(input, index));
    }

    return new HiveSortExchange(cluster, traitSet, input, distribution, collation, builder.build());
  }

  @Override
  public SortExchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution,
          RelCollation newCollation) {
    return new HiveSortExchange(getCluster(), traitSet, newInput,
            newDistribution, newCollation, keys);
  }

  public ImmutableList<RexNode> getKeys() {
    return keys;
  }

  public ExprNodeDesc[] getKeyExpressions() {
    return keyExpressions;
  }

  public void setKeyExpressions(ExprNodeDesc[] keyExpressions) {
    this.keyExpressions = keyExpressions;
  }
}
