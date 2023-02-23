/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

/**
 * A rule that matches a relation that contains expressions with the SEARCH operator
 * and expands them.
 * 
 * Many places in Hive do not know how to handle the SEARCH operator, so we use
 * this rule to remove it from the plan.
 */
public final class HiveSearchExpandRule extends RelRule<RelRule.Config> {
  private HiveSearchExpandRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(final RelOptRuleCall call) {
    RelNode n = call.rel(0);
    final boolean[] result = new boolean[] { false };
    n.accept(new RexShuttle() {
      @Override
      public RexNode visitCall(final RexCall call) {
        if (SqlKind.SEARCH.equals(call.getKind())) {
          result[0] = true;
          return call;
        }
        return super.visitCall(call);
      }
    });
    return result[0];
  }

  @Override
  public void onMatch(final RelOptRuleCall call) {
    RelNode n = call.rel(0);
    RelNode t = n.accept(RexUtil.searchShuttle(n.getCluster().getRexBuilder(), null, -1));
    call.transformTo(t);
  }

  public static final class HiveSearchExpandRuleConfig extends BaseMutableHiveConfig {
    @Override
    public RelOptRule toRule() {
      return new HiveSearchExpandRule(this);
    }
  }
}
