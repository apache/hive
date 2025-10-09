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
import org.apache.hadoop.hive.ql.optimizer.calcite.SearchTransformer;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

import static org.apache.calcite.rex.RexUnknownAs.FALSE;
import static org.apache.calcite.rex.RexUnknownAs.UNKNOWN;

/**
 * A holder class for rules related to the SEARCH operator.
 */
public final class HiveSearchRules {
  private HiveSearchRules() {
    throw new IllegalStateException();
  }

  public static final RelOptRule PROJECT_SEARCH_EXPAND =
      new HiveRexShuttleTransformRule.Config().withRexShuttle(x -> new SearchTransformer.Shuttle(x, UNKNOWN))
          .withDescription("HiveProjectSearchExpandRule")
          .withOperandSupplier(o -> o.operand(HiveProject.class).anyInputs())
          .toRule();
  public static final RelOptRule FILTER_SEARCH_EXPAND =
      new HiveRexShuttleTransformRule.Config().withRexShuttle(x -> new SearchTransformer.Shuttle(x, FALSE))
          .withDescription("HiveFilterSearchExpandRule")
          .withOperandSupplier(o -> o.operand(HiveFilter.class).anyInputs())
          .toRule();
  public static final RelOptRule JOIN_SEARCH_EXPAND =
      new HiveRexShuttleTransformRule.Config().withRexShuttle(x -> new SearchTransformer.Shuttle(x, FALSE))
          .withDescription("HiveJoinSearchExpandRule")
          .withOperandSupplier(o -> o.operand(HiveJoin.class).anyInputs())
          .toRule();

}
