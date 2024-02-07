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

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableSpool;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TableScanToSpoolRule extends RelOptRule {
  /**
   * Track created spools to avoid introducing more than one.
   */
  private final Set<String> spools = new HashSet<>();
  /**
   * Available CTEs from which we can create spool operators.
   */
  private final Map<String, RelOptMaterialization> ctes;

  public TableScanToSpoolRule(Map<String, RelOptMaterialization> ctes) {
    super(operand(TableScan.class, none()));
    this.ctes = ctes;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    TableScan scan = call.rel(0);
    String tableName = scan.getTable().getQualifiedName().toString();
    RelOptMaterialization cte = ctes.get(tableName);
    if (cte != null && spools.add(tableName)) {
      // The Spool types are not used at the moment so choice between LAZY/EAGER does not affect anything
      RelOptTableImpl cteTable =
          RelOptTableImpl.create(null, scan.getRowType(), scan.getTable().getQualifiedName(), null);
      call.transformTo(
          new LogicalTableSpool(scan.getCluster(), scan.getCluster().traitSet(), cte.queryRel, Spool.Type.LAZY,
              Spool.Type.LAZY, cteTable));
    }
  }
}
