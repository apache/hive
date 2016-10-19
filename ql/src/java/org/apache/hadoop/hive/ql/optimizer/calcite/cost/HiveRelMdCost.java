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
package org.apache.hadoop.hive.ql.optimizer.calcite.cost;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import com.google.common.collect.ImmutableList;

/**
 * HiveRelMdCost supplies the implementation of cost model.
 */
public class HiveRelMdCost implements MetadataHandler<BuiltInMetadata.NonCumulativeCost> {

  private final HiveCostModel hiveCostModel;

  public HiveRelMdCost(HiveCostModel hiveCostModel) {
    this.hiveCostModel = hiveCostModel;
  }

  public RelMetadataProvider getMetadataProvider() {
    return ChainedRelMetadataProvider.of(
               ImmutableList.of(
                   ReflectiveRelMetadataProvider.reflectiveSource(this,
                       BuiltInMethod.NON_CUMULATIVE_COST.method),
                   RelMdPercentageOriginalRows.SOURCE));
  }

  @Override
  public MetadataDef<BuiltInMetadata.NonCumulativeCost> getDef() {
    return BuiltInMetadata.NonCumulativeCost.DEF;
  }

  public RelOptCost getNonCumulativeCost(HiveAggregate aggregate, RelMetadataQuery mq) {
    return hiveCostModel.getAggregateCost(aggregate);
  }

  public RelOptCost getNonCumulativeCost(HiveJoin join, RelMetadataQuery mq) {
    return hiveCostModel.getJoinCost(join);
  }

  public RelOptCost getNonCumulativeCost(HiveTableScan ts, RelMetadataQuery mq) {
    return hiveCostModel.getScanCost(ts);
  }

  // Default case
  public RelOptCost getNonCumulativeCost(RelNode rel, RelMetadataQuery mq) {
    return hiveCostModel.getDefaultCost();
  }

}

// End HiveRelMdCost.java
