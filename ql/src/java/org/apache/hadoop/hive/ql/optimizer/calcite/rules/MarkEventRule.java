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
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.engine.EngineEventSequence;

public class MarkEventRule extends RelOptRule {

  private final String event;
  private boolean triggered;

  public MarkEventRule(String event) {
    super(operand(RelNode.class, any()),
        HiveRelFactories.HIVE_BUILDER, null);
    this.event = event;
    this.triggered = false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (triggered) {
      // Bail out
      return;
    }
    final EngineEventSequence timeline =
        call.getPlanner().getContext().unwrap(EngineEventSequence.class);
    if (timeline != null) {
      // NOOP if this is not an external engine plan
      timeline.markEvent(event);
    }
    triggered = true;
  }

}
