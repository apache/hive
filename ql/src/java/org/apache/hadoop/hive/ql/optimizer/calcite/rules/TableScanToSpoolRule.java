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

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.TableScan;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories.HIVE_SPOOL_FACTORY;

public class TableScanToSpoolRule extends RelRule<CteRuleConfig> {
  /**
   * Track created spools to avoid introducing more than one.
   */
  private final Set<String> spools = new HashSet<>();
  public TableScanToSpoolRule(CteRuleConfig config) {
    super(config);
    if (config.referenceThreshold() <= 0) {
      throw new IllegalArgumentException("Invalid reference threshold:" + config.referenceThreshold());
    }
  }

  @Override
  public boolean matches(final RelOptRuleCall call) {
    TableScan scan = call.rel(0);
    List<String> tableName = scan.getTable().getQualifiedName();
    if (spools.contains(tableName.toString())) {
      return false;
    }
    return config.getTableOccurrences().getOrDefault(tableName, 0) > config.referenceThreshold();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    TableScan scan = call.rel(0);
    String tableName = scan.getTable().getQualifiedName().toString();
    for (RelOptMaterialization cte : call.getPlanner().getMaterializations()) {
      if (tableName.equals(cte.qualifiedTableName.toString()) && spools.add(tableName)) {
        RelOptTableImpl cteTable =
            RelOptTableImpl.create(null, scan.getRowType(), scan.getTable().getQualifiedName(), null);
        // The Spool types are not used at the moment so choice between LAZY/EAGER does not affect anything
        call.transformTo(HIVE_SPOOL_FACTORY.createTableSpool(cte.queryRel, Spool.Type.LAZY, Spool.Type.LAZY, cteTable));
      }
    }
  }
}
