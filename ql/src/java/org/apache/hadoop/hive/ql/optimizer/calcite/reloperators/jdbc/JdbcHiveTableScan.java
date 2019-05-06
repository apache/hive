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
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.jdbc;

import java.util.List;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import org.apache.calcite.rel.RelWriter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
/**
 * Relational expression representing a scan of a HiveDB collection.
 *
 * <p>
 * Additional operations might be applied, using the "find" or "aggregate" methods.
 * </p>
 */
public class JdbcHiveTableScan extends JdbcTableScan {

  private final HiveTableScan hiveTableScan;

  public JdbcHiveTableScan(RelOptCluster cluster, RelOptTable table, JdbcTable jdbcTable,
      JdbcConvention jdbcConvention, HiveTableScan hiveTableScan) {
    super(cluster, table, jdbcTable, jdbcConvention);
    this.hiveTableScan= hiveTableScan;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new JdbcHiveTableScan(
        getCluster(), table, jdbcTable, (JdbcConvention) getConvention(), this.hiveTableScan);
  }

  public HiveTableScan getHiveTableScan() {
    return hiveTableScan;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return hiveTableScan.explainTerms(pw);
  }
}
