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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import java.util.List;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFieldTrimmerRule;

public class HiveFetchDeletedRowsRule extends HiveFieldTrimmerRule {

  public static HiveFetchDeletedRowsRule fetchFrom(List<String> tableNames) {
    return new HiveFetchDeletedRowsRule(tableNames);
  }

  private final List<String> tableNames;

  private HiveFetchDeletedRowsRule(List<String> tableNames) {
    super(false, "HiveFetchDeletedRowsPropagatorRule");
    this.tableNames = tableNames;
  }

  @Override
  protected RelNode trim(RelOptRuleCall call, RelNode node) {
    return new HiveFetchDeletedRowsPropagator(call.builder(), tableNames).propagate(node);
  }
}
