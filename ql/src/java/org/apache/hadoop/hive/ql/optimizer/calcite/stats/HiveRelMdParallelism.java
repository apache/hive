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
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdParallelism;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin.JoinAlgorithm;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin.MapJoinStreamingRelation;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSort;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

public class HiveRelMdParallelism extends RelMdParallelism {

  private final Double maxSplitSize;

  //~ Constructors -----------------------------------------------------------

  public HiveRelMdParallelism(Double maxSplitSize) {
    this.maxSplitSize = maxSplitSize;
  }

  public RelMetadataProvider getMetadataProvider() {
    return ReflectiveRelMetadataProvider.reflectiveSource(this,
                BuiltInMethod.IS_PHASE_TRANSITION.method,
                BuiltInMethod.SPLIT_COUNT.method);
  }

  //~ Methods ----------------------------------------------------------------

  public Boolean isPhaseTransition(HiveJoin join) {
    // As Exchange operator is introduced later on, we make a
    // common join operator create a new stage for the moment
    if (join.getJoinAlgorithm() == JoinAlgorithm.COMMON_JOIN) {
      return true;
    }
    return false;
  }

  public Boolean isPhaseTransition(HiveSort sort) {
    // As Exchange operator is introduced later on, we make a
    // sort operator create a new stage for the moment
    return true;
  }

  public Integer splitCount(HiveJoin join) {
    if (join.getJoinAlgorithm() == JoinAlgorithm.COMMON_JOIN) {
      return splitCountRepartition(join);
    }
    else if (join.getJoinAlgorithm() == JoinAlgorithm.MAP_JOIN ||
              join.getJoinAlgorithm() == JoinAlgorithm.BUCKET_JOIN ||
              join.getJoinAlgorithm() == JoinAlgorithm.SMB_JOIN) {
      RelNode largeInput;
      if (join.getMapJoinStreamingSide() == MapJoinStreamingRelation.LEFT_RELATION) {
        largeInput = join.getLeft();
      } else if (join.getMapJoinStreamingSide() == MapJoinStreamingRelation.RIGHT_RELATION) {
        largeInput = join.getRight();
      } else {
        return null;
      }
      return splitCount(largeInput);
    }
    return null;
  }

  public Integer splitCount(HiveTableScan scan) {
    RelOptHiveTable table = (RelOptHiveTable) scan.getTable();
    return table.getHiveTableMD().getNumBuckets();
  }

  public Integer splitCount(RelNode rel) {
    Boolean newPhase = RelMetadataQuery.isPhaseTransition(rel);

    if (newPhase == null) {
      return null;
    }

    if (newPhase) {
      // We repartition: new number of splits
      return splitCountRepartition(rel);
    }

    // We do not repartition: take number of splits from children
    Integer splitCount = 0;
    for (RelNode input : rel.getInputs()) {
      splitCount += RelMetadataQuery.splitCount(input);
    }
    return splitCount;
  }

  public Integer splitCountRepartition(RelNode rel) {
    // We repartition: new number of splits
    final Double averageRowSize = RelMetadataQuery.getAverageRowSize(rel);
    final Double rowCount = RelMetadataQuery.getRowCount(rel);
    if (averageRowSize == null || rowCount == null) {
      return null;
    }
    final Double totalSize = averageRowSize * rowCount;
    final Double splitCount = totalSize / maxSplitSize;
    return splitCount.intValue();
  }
  
}

// End RelMdParallelism.java