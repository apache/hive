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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttleImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTezModelRelMetadataProvider;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveDefaultCostModel;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonList;

/**
 * Rule to trigger {@link HiveCardinalityPreservingJoinOptimization} on top of the plan.
 */
public class HiveCardinalityPreservingJoinRule extends HiveFieldTrimmerRule {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCardinalityPreservingJoinRule.class);

  private final double factor;

  public HiveCardinalityPreservingJoinRule(double factor) {
    super(false, "HiveCardinalityPreservingJoinRule");
    this.factor = Math.max(factor, 0.0);
  }

  @Override
  protected RelNode trim(RelOptRuleCall call, RelNode root, RelNode input) {
    final HiveCardinalityPreservingJoinOptimization optimizer =
        new HiveCardinalityPreservingJoinOptimization();
    final RelNode optimized;
    if (input instanceof Sort) {
      // If the input is a sort-limit, we need to target that node instead of the root.
      // Otherwise, we can end up rewriting the join on top of the sort, which could lead
      // to incorrect results
      RelNode optimizedInput = optimizer.trim(call.builder(), input);
      if (optimizedInput == input) {
        // Not possible to apply optimization
        return root;
      }
      optimized = root.copy(root.getTraitSet(), singletonList(optimizedInput));
    } else {
      optimized = optimizer.trim(call.builder(), root);
      if (optimized == root) {
        // Not possible to apply optimization
        return root;
      }
    }

    RelNode chosen = choosePlan(root, optimized);
    new JoinAlgorithmSetter().visit(chosen);
    return chosen;
  }

  private RelNode choosePlan(RelNode node, RelNode optimized) {
    JaninoRelMetadataProvider original = RelMetadataQuery.THREAD_PROVIDERS.get();
    try {
      RelMetadataQuery.THREAD_PROVIDERS.set(
          HiveTezModelRelMetadataProvider.DEFAULT);
      node.getCluster().invalidateMetadataQuery();
      RelMetadataQuery metadataQuery = RelMetadataQuery.instance();

      RelOptCost optimizedCost = metadataQuery.getCumulativeCost(optimized);
      RelOptCost originalCost = metadataQuery.getCumulativeCost(node);
      originalCost = originalCost.multiplyBy(factor);
      LOG.debug("Original plan cost {} vs Optimized plan cost {}", originalCost, optimizedCost);
      if (optimizedCost.isLt(originalCost)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Plan before:\n" + RelOptUtil.toString(node));
          LOG.debug("Plan after:\n" + RelOptUtil.toString(optimized));
        }
        return optimized;
      }

      return node;
    }
    finally {
      node.getCluster().invalidateMetadataQuery();
      RelMetadataQuery.THREAD_PROVIDERS.set(original);
    }
  }

  private static class JoinAlgorithmSetter extends HiveRelShuttleImpl {
    @Override
    public RelNode visit(HiveJoin join) {
      join.setJoinAlgorithm(HiveDefaultCostModel.DefaultJoinAlgorithm.INSTANCE);
      return super.visit(join);
    }
  }
}
