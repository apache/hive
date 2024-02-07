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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.cte;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.TableSpool;

import java.util.Map;

/**
 * Rule to remove trivial spool operators.
 * <p>A spool is trivial if it is not reference anywhere in the plan.</p>
 */
public class SpoolRemoveRule extends RelOptRule {
  /**
   * Mappping between table name and number of occurrences in the whole plan.
   */
  private final Map<String, Long> tableOccurences;

  public SpoolRemoveRule(Map<String, Long> tableOccurrences) {
    super(operand(TableSpool.class, any()));
    this.tableOccurences = tableOccurrences;
  }

  @Override public void onMatch(RelOptRuleCall relOptRuleCall) {
    TableSpool spool = relOptRuleCall.rel(0);
    if (tableOccurences.getOrDefault(spool.getTable().getQualifiedName().toString(), 0L) < 1) {
      relOptRuleCall.transformTo(spool.getInput());
    }
  }
}
