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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveDefaultTezModelRelMetadataProvider;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rule to trigger {@link HiveCardinalityPreservingJoinOptimization} on top of the plan.
 */
public class HiveCardinalityPreservingJoinRule {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCardinalityPreservingJoinRule.class);

  private final double factor;
  private final RelOptCluster cluster;

  public HiveCardinalityPreservingJoinRule(RelOptCluster cluster, double factor) {
    this.cluster = cluster;
    this.factor = Math.max(factor, 0.0);
  }

  public RelNode trim(RelNode node) {
    RelBuilder relBuilder = HiveRelFactories.HIVE_BUILDER.create(cluster, null);
    RelNode optimized = new HiveCardinalityPreservingJoinOptimization(cluster).trim(relBuilder, node);
    if (optimized == node) {
      return node;
    }

    JaninoRelMetadataProvider original = RelMetadataQuery.THREAD_PROVIDERS.get();
    try {
      RelMetadataQuery.THREAD_PROVIDERS.set(getJaninoRelMetadataProvider());
      RelMetadataQuery metadataQuery = RelMetadataQuery.instance();

      RelOptCost optimizedCost = metadataQuery.getCumulativeCost(optimized);
      RelOptCost originalCost = metadataQuery.getCumulativeCost(node);
      originalCost = originalCost.multiplyBy(factor);
      LOG.debug("Original plan cost {} vs Optimized plan cost {}", originalCost, optimizedCost);
      if (optimizedCost.isLt(originalCost)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Plan after:\n" + RelOptUtil.toString(optimized));
        }
        return optimized;
      }

      return node;
    }
    finally {
      RelMetadataQuery.THREAD_PROVIDERS.set(original);
    }
  }

  private JaninoRelMetadataProvider getJaninoRelMetadataProvider() {
    return new HiveDefaultTezModelRelMetadataProvider().getMetadataProvider();
  }
}
