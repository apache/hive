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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCostModel;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveDefaultCostModel;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveOnTezCostModel;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveRelMdCost;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdCollation;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdDistinctRowCount;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdDistribution;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdMemory;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdParallelism;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdPredicates;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdRowCount;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdSelectivity;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdSize;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdUniqueKeys;

import com.google.common.collect.ImmutableList;

public class HiveDefaultRelMetadataProvider {

  private final HiveConf hiveConf;


  public HiveDefaultRelMetadataProvider(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  public RelMetadataProvider getMetadataProvider() {

    // Create cost metadata provider
    final HiveCostModel cm;
    if (HiveConf.getVar(this.hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")
            && HiveConf.getBoolVar(this.hiveConf, HiveConf.ConfVars.HIVE_CBO_EXTENDED_COST_MODEL)) {
      cm = HiveOnTezCostModel.getCostModel(hiveConf);
    } else {
      cm = HiveDefaultCostModel.getCostModel();
    }

    // Get max split size for HiveRelMdParallelism
    final Double maxSplitSize = (double) HiveConf.getLongVar(
            this.hiveConf,
            HiveConf.ConfVars.MAPREDMAXSPLITSIZE);

    // Return MD provider
    return ChainedRelMetadataProvider.of(ImmutableList
            .of(
                    HiveRelMdDistinctRowCount.SOURCE,
                    new HiveRelMdCost(cm).getMetadataProvider(),
                    HiveRelMdSelectivity.SOURCE,
                    HiveRelMdRowCount.SOURCE,
                    HiveRelMdUniqueKeys.SOURCE,
                    HiveRelMdSize.SOURCE,
                    HiveRelMdMemory.SOURCE,
                    new HiveRelMdParallelism(maxSplitSize).getMetadataProvider(),
                    HiveRelMdDistribution.SOURCE,
                    HiveRelMdCollation.SOURCE,
                    HiveRelMdPredicates.SOURCE,
                    DefaultRelMetadataProvider.INSTANCE));
  }

}
