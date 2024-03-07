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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveDefaultCostModel;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveOnTezCostModel;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveRelMdCost;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdColumnUniqueness;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdCollation;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdCumulativeCost;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdDistinctRowCount;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdDistribution;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdExpressionLineage;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdMemory;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdParallelism;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdPredicates;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdRowCount;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdRuntimeRowCount;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdSelectivity;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdSize;
import org.apache.hadoop.hive.ql.optimizer.calcite.stats.HiveRelMdUniqueKeys;

import com.google.common.collect.ImmutableList;

public class HiveDefaultRelMetadataProvider {

  /**
   * The default metadata provider can be instantiated statically since
   * it does not need any parameter specified by user (hive conf).
   */
  private static final JaninoRelMetadataProvider DEFAULT =
      JaninoRelMetadataProvider.of(
          ChainedRelMetadataProvider.of(
              ImmutableList.of(
                  HiveRelMdDistinctRowCount.SOURCE,
                  HiveRelMdCumulativeCost.SOURCE,
                  new HiveRelMdCost(HiveDefaultCostModel.getCostModel()).getMetadataProvider(),
                  HiveRelMdSelectivity.SOURCE,
                  HiveRelMdRuntimeRowCount.SOURCE,
                  HiveRelMdUniqueKeys.SOURCE,
                  HiveRelMdColumnUniqueness.SOURCE,
                  HiveRelMdExpressionLineage.SOURCE,
                  HiveRelMdSize.SOURCE,
                  HiveRelMdMemory.SOURCE,
                  HiveRelMdDistribution.SOURCE,
                  HiveRelMdCollation.SOURCE,
                  HiveRelMdPredicates.SOURCE,
                  JaninoRelMetadataProvider.DEFAULT)));

  private final RelMetadataProvider metadataProvider;


  public HiveDefaultRelMetadataProvider(HiveConf hiveConf, List<Class<? extends RelNode>> nodeClasses) {
    this.metadataProvider = init(hiveConf, nodeClasses);
  }

  private RelMetadataProvider init(HiveConf hiveConf, List<Class<? extends RelNode>> nodeClasses) {
    // Create cost metadata provider
    if (HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")
        && HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_CBO_EXTENDED_COST_MODEL)) {
      // Get max split size for HiveRelMdParallelism
      final Double maxSplitSize = (double) HiveConf.getLongVar(
          hiveConf, HiveConf.ConfVars.MAPRED_MAX_SPLIT_SIZE);

      // Create and return metadata provider
      JaninoRelMetadataProvider metadataProvider = JaninoRelMetadataProvider.of(
          ChainedRelMetadataProvider.of(
              ImmutableList.of(
                  HiveRelMdDistinctRowCount.SOURCE,
                  HiveRelMdCumulativeCost.SOURCE,
                  new HiveRelMdCost(HiveOnTezCostModel.getCostModel(hiveConf)).getMetadataProvider(),
                  HiveRelMdSelectivity.SOURCE,
                  HiveRelMdRowCount.SOURCE,
                  HiveRelMdUniqueKeys.SOURCE,
                  HiveRelMdColumnUniqueness.SOURCE,
                  HiveRelMdExpressionLineage.SOURCE,
                  HiveRelMdSize.SOURCE,
                  HiveRelMdMemory.SOURCE,
                  new HiveRelMdParallelism(maxSplitSize).getMetadataProvider(),
                  HiveRelMdDistribution.SOURCE,
                  HiveRelMdCollation.SOURCE,
                  HiveRelMdPredicates.SOURCE,
                  JaninoRelMetadataProvider.DEFAULT)));

      if (nodeClasses != null) {
        // If classes were passed, pre-register them
        metadataProvider.register(nodeClasses);
      }

      return metadataProvider;
    }

    return DEFAULT;
  }

  public RelMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /**
   * This method can be called at startup time to pre-register all the
   * additional Hive classes (compared to Calcite core classes) that may
   * be visited during the planning phase.
   */
  public static void initializeMetadataProviderClass(List<Class<? extends RelNode>> nodeClasses) {
    // This will register the classes in the default Hive implementation
    DEFAULT.register(nodeClasses);
  }
}
