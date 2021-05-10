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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;


/**
 * Rule that triggers the field trimmer on the root of a plan.
 */
public class HiveFieldTrimmerRule  extends RelOptRule {

  private final boolean fetchStats;
  private boolean triggered;

  public HiveFieldTrimmerRule(boolean fetchStats) {
    this(fetchStats, "HiveFieldTrimmerRule");
  }

  protected HiveFieldTrimmerRule(boolean fetchStats, String description) {
    super(operand(RelNode.class, any()),
        HiveRelFactories.HIVE_BUILDER, description);
    this.fetchStats = fetchStats;
    this.triggered = false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (triggered) {
      // Bail out
      return;
    }

    RelNode node = call.rel(0);
    final HepRelVertex root = (HepRelVertex) call.getPlanner().getRoot();
    if (root.getCurrentRel() != node) {
      // Bail out
      return;
    }
    // The node is the root, release the kraken!
    node = HiveHepExtractRelNodeRule.execute(node);
    call.transformTo(trim(call, node));
    triggered = true;
  }

  protected RelNode trim(RelOptRuleCall call, RelNode node) {
    return HiveRelFieldTrimmer.get(fetchStats).trim(call.builder(), node);
  }
}
