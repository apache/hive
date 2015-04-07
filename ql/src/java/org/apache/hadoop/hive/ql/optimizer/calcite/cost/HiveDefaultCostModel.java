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

import java.util.EnumSet;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;

/**
 * Default implementation of the cost model.
 * Currently used by MR and Spark execution engines.
 */
public class HiveDefaultCostModel extends HiveCostModel {

  @Override
  public RelOptCost getDefaultCost() {
    return HiveCost.FACTORY.makeZeroCost();
  }

  @Override
  public RelOptCost getAggregateCost(HiveAggregate aggregate) {
    return HiveCost.FACTORY.makeZeroCost();
  }

  @Override
  protected EnumSet<JoinAlgorithm> getExecutableJoinAlgorithms(HiveJoin join) {
    return EnumSet.of(JoinAlgorithm.NONE);
  }

  @Override
  protected RelOptCost getJoinCost(HiveJoin join, JoinAlgorithm algorithm) {
    RelOptCost algorithmCost;
    switch (algorithm) {
      case NONE:
        algorithmCost = computeJoinCardinalityCost(join);
        break;
      default:
        algorithmCost = null;
    }
    return algorithmCost;
  }

  private static RelOptCost computeJoinCardinalityCost(HiveJoin join) {
    double leftRCount = RelMetadataQuery.getRowCount(join.getLeft());
    double rightRCount = RelMetadataQuery.getRowCount(join.getRight());
    return HiveCost.FACTORY.makeCost(leftRCount + rightRCount, 0.0, 0.0);
  }

}
