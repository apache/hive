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
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FIXME
 */
public class HiveAggregatedPrimaryRule extends RelOptRule {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveAggregatedPrimaryRule.class);

  public static final HiveAggregatedPrimaryRule INSTANCE = new HiveAggregatedPrimaryRule(HiveRelFactories.HIVE_BUILDER);

  protected HiveAggregatedPrimaryRule(RelBuilderFactory relBuilder) {
    super(operand(Aggregate.class, any()), relBuilder, null);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate agg = call.rel(0);


  }

}
