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
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import java.util.List;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCost;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;


public class HiveRelMdCumulativeCost implements MetadataHandler<BuiltInMetadata.CumulativeCost> {

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.CUMULATIVE_COST.method, new HiveRelMdCumulativeCost());

  //~ Constructors -----------------------------------------------------------

  private HiveRelMdCumulativeCost() {
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public MetadataDef<BuiltInMetadata.CumulativeCost> getDef() {
    return BuiltInMetadata.CumulativeCost.DEF;
  }

  /*
   * Favor Broad Plans over Deep Plans.
   */
  public RelOptCost getCumulativeCost(HiveJoin rel, RelMetadataQuery mq) {
    RelOptCost cost = mq.getNonCumulativeCost(rel);
    List<RelNode> inputs = rel.getInputs();
    RelOptCost maxICost = HiveCost.ZERO;
    for (RelNode input : inputs) {
      RelOptCost iCost = mq.getCumulativeCost(input);
      if (maxICost.isLt(iCost)) {
        maxICost = iCost;
      }
    }
    return cost.plus(maxICost);
  }
}
