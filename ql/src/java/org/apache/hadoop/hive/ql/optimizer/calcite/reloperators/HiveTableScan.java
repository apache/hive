/**
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
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCost;
import org.apache.hadoop.hive.ql.plan.ColStatistics;


/**
 * Relational expression representing a scan of a HiveDB collection.
 *
 * <p>
 * Additional operations might be applied, using the "find" or "aggregate"
 * methods.
 * </p>
 */
public class HiveTableScan extends TableScan implements HiveRelNode {

  /**
   * Creates a HiveTableScan.
   *
   * @param cluster
   *          Cluster
   * @param traitSet
   *          Traits
   * @param table
   *          Table
   * @param table
   *          HiveDB table
   */
  public HiveTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptHiveTable table,
      RelDataType rowtype) {
    super(cluster, TraitsUtil.getDefaultTraitSet(cluster), table);
    assert getConvention() == HiveRelNode.CONVENTION;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return this;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return HiveCost.FACTORY.makeZeroCost();
  }

  @Override
  public void register(RelOptPlanner planner) {

  }

  @Override
  public void implement(Implementor implementor) {

  }

  @Override
  public double getRows() {
    return ((RelOptHiveTable) table).getRowCount();
  }

  public List<ColStatistics> getColStat(List<Integer> projIndxLst) {
    return ((RelOptHiveTable) table).getColStat(projIndxLst);
  }
}