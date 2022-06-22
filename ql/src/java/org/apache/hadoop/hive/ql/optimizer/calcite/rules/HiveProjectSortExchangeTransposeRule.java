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

import static org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil.getNewRelDistributionKeys;
import static org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil.getNewRelFieldCollations;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SortExchange;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelDistribution;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;

import com.google.common.collect.ImmutableList;

/**
 * Push down Projection above SortExchange.
 * HiveProject
 *   HiveSortExchange
 *     ...
 *
 * =&gt;
 *
 * HiveSortExchange
 *   HiveProject
 *     ...
 */
public final class HiveProjectSortExchangeTransposeRule extends RelOptRule {
  public static final HiveProjectSortExchangeTransposeRule INSTANCE = new HiveProjectSortExchangeTransposeRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a HiveProjectSortTransposeRule.
   */
  private HiveProjectSortExchangeTransposeRule() {
    super(
        operand(
            HiveProject.class,
            operand(HiveSortExchange.class, any())));
  }

  protected HiveProjectSortExchangeTransposeRule(RelOptRuleOperand operand) {
    super(operand);
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    final HiveProject project = call.rel(0);
    final HiveSortExchange sortExchange = call.rel(1);
    final RelOptCluster cluster = project.getCluster();

    List<RelFieldCollation> fieldCollations = getNewRelFieldCollations(project, sortExchange.getCollation(), cluster);
    if (fieldCollations == null) {
      return;
    }

    RelCollation newCollation = RelCollationTraitDef.INSTANCE.canonize(RelCollationImpl.of(fieldCollations));
    List<Integer> newDistributionKeys = getNewRelDistributionKeys(project, sortExchange.getDistribution());
    RelDistribution newDistribution = RelDistributionTraitDef.INSTANCE.canonize(
        new HiveRelDistribution(sortExchange.getDistribution().getType(), newDistributionKeys));
    RelTraitSet newTraitSet = TraitsUtil.getDefaultTraitSet(sortExchange.getCluster())
        .replace(newCollation).replace(newDistribution);

    // New operators
    final RelNode newProject = project.copy(sortExchange.getInput().getTraitSet(),
        ImmutableList.of(sortExchange.getInput()));
    final SortExchange newSort = sortExchange.copy(newTraitSet, newProject, newDistribution, newCollation);

    call.transformTo(newSort);
  }
}
