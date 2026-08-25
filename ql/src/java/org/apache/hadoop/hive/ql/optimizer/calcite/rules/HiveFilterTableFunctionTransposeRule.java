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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.FilterTableFunctionTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;

/**
 * Rule to transpose Filter and TableFunctionScan RelNodes
 */
public class HiveFilterTableFunctionTransposeRule {

  private HiveFilterTableFunctionTransposeRule() {
    throw new IllegalStateException("Instantiation not allowed");
  }

  public static final RelOptRule INSTANCE =
      FilterTableFunctionTransposeRule.Config.DEFAULT
          .withOperandSupplier(b0 ->
              b0.operand(HiveFilter.class).oneInput(
                  b1 -> b1.operand(HiveTableFunctionScan.class).anyInputs()))
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .toRule();

}
