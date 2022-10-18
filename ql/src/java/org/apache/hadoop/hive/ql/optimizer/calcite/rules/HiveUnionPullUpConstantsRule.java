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
import org.apache.calcite.rel.rules.UnionPullUpConstantsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Planner rule that pulls up constants through a Union operator.
 */
public class HiveUnionPullUpConstantsRule {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveUnionPullUpConstantsRule.class);


  public static final RelOptRule INSTANCE = UnionPullUpConstantsRule.Config.DEFAULT
      .withOperandFor(HiveUnion.class)
      .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
      .as(UnionPullUpConstantsRule.Config.class)
      .toRule();

  private HiveUnionPullUpConstantsRule() {
    throw new IllegalStateException("Instantiation not allowed");
  }
}
