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

package org.apache.hadoop.hive.ql.optimizer.calcite.cost;

import com.google.common.collect.Multimap;
import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveConfPlannerContext;
import org.apache.hadoop.hive.ql.optimizer.calcite.HivePlannerContext;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveDruidRules;

/**
 * Refinement of {@link org.apache.calcite.plan.volcano.VolcanoPlanner} for Hive.
 * 
 * <p>
 * It uses {@link org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCost} as
 * its cost model.
 */
public class HiveVolcanoPlanner extends VolcanoPlanner {
  private static final boolean ENABLE_COLLATION_TRAIT = true;

  private final boolean isHeuristic;


  /** Creates a HiveVolcanoPlanner. */
  public HiveVolcanoPlanner(HivePlannerContext conf) {
    super(HiveCost.FACTORY, conf);
    isHeuristic = conf.unwrap(HiveConfPlannerContext.class).isHeuristicMaterializationStrategy();
  }

  public static RelOptPlanner createPlanner(HivePlannerContext conf) {
    final VolcanoPlanner planner = new HiveVolcanoPlanner(conf);
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    if (ENABLE_COLLATION_TRAIT) {
      planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    }
    return planner;
  }

  @Override
  public void registerClass(RelNode node) {
    if (node instanceof DruidQuery) {
      // Special handling for Druid rules here as otherwise
      // planner will add Druid rules with logical builder
      addRule(HiveDruidRules.FILTER);
      addRule(HiveDruidRules.PROJECT_FILTER_TRANSPOSE);
      addRule(HiveDruidRules.AGGREGATE_FILTER_TRANSPOSE);
      addRule(HiveDruidRules.AGGREGATE_PROJECT);
      addRule(HiveDruidRules.PROJECT);
      addRule(HiveDruidRules.AGGREGATE);
      addRule(HiveDruidRules.POST_AGGREGATION_PROJECT);
      addRule(HiveDruidRules.FILTER_AGGREGATE_TRANSPOSE);
      addRule(HiveDruidRules.FILTER_PROJECT_TRANSPOSE);
      addRule(HiveDruidRules.SORT_PROJECT_TRANSPOSE);
      addRule(HiveDruidRules.SORT);
      addRule(HiveDruidRules.PROJECT_SORT_TRANSPOSE);
      return;
    }
    super.registerClass(node);
  }

  /**
   * The method extends the logic of the super method to decrease
   * the cost of the plan if it contains materialized views
   * (heuristic).
   */
  public RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {
    assert rel != null : "pre-condition: rel != null";
    if (rel instanceof RelSubset) {
      // Get cost of the subset, best rel may have been chosen or not
      RelSubset subset = (RelSubset) rel;
      if (subset.getBest() != null) {
        return getCost(subset.getBest(), mq);
      }
      return costFactory.makeInfiniteCost();
    }
    if (rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE)
        == Convention.NONE) {
      return costFactory.makeInfiniteCost();
    }
    // We get the cost of the operator
    RelOptCost cost = mq.getNonCumulativeCost(rel);
    if (!costFactory.makeZeroCost().isLt(cost)) {
      // cost must be positive, so nudge it
      cost = costFactory.makeTinyCost();
    }
    // If this operator has a materialized view below,
    // we make its cost tiny and adjust the cost of its
    // inputs
    boolean usesMaterializedViews = false;
    Multimap<Class<? extends RelNode>, RelNode> nodeTypes =
        mq.getNodeTypes(rel);
    for (RelNode scan : nodeTypes.get(TableScan.class)) {
      if (((RelOptHiveTable) scan.getTable()).getHiveTableMD().isMaterializedView()) {
        usesMaterializedViews = true;
        break;
      }
    }
    if (isHeuristic && usesMaterializedViews) {
      // If a child of this expression uses a materialized view,
      // then we decrease its cost by a certain factor. This is
      // useful for e.g. partial rewritings, where a part of plan
      // does not use the materialization, but we still want to
      // decrease its cost so it is chosen instead of the original
      // plan
      cost = cost.multiplyBy(RelOptUtil.EPSILON);
      if (!costFactory.makeZeroCost().isLt(cost)) {
        // cost must be positive, so nudge it
        cost = costFactory.makeTinyCost();
      }
      for (RelNode input : rel.getInputs()) {
        cost = cost.plus(getCost(input, mq));
      }
    } else {
      // No materialized view or not heuristic approach, normal costing
      for (RelNode input : rel.getInputs()) {
        cost = cost.plus(getCost(input, mq));
      }
    }
    return cost;
  }
}
