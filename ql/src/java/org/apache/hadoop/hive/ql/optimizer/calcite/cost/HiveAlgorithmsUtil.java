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
package org.apache.hadoop.hive.ql.optimizer.calcite.cost;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinLeafPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelCollation;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelDistribution;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin.MapJoinStreamingRelation;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;

import com.google.common.collect.ImmutableList;

public class HiveAlgorithmsUtil {

  private final double cpuCost;
  private final double netCost;
  private final double localFSWrite;
  private final double localFSRead;
  private final double hdfsWrite;
  private final double hdfsRead;

  HiveAlgorithmsUtil(HiveConf conf) {
    cpuCost = Double.parseDouble(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_CBO_COST_MODEL_CPU));
    netCost = cpuCost
        * Double.parseDouble(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_CBO_COST_MODEL_NET));
    localFSWrite = netCost
        * Double.parseDouble(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_CBO_COST_MODEL_LFS_WRITE));
    localFSRead = netCost
        * Double.parseDouble(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_CBO_COST_MODEL_LFS_READ));
    hdfsWrite = localFSWrite
        * Double.parseDouble(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_CBO_COST_MODEL_HDFS_WRITE));
    hdfsRead = localFSRead
        * Double.parseDouble(HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_CBO_COST_MODEL_HDFS_READ));
  }

  public static RelOptCost computeCardinalityBasedCost(HiveRelNode hr, RelMetadataQuery mq) {
    return new HiveCost(mq.getRowCount(hr), 0, 0);
  }

  public HiveCost computeScanCost(double cardinality, double avgTupleSize) {
    return new HiveCost(cardinality, 0, hdfsRead * cardinality * avgTupleSize);
  }

  public double computeSortMergeCPUCost(
          ImmutableList<Double> cardinalities,
          ImmutableBitSet sorted) {
    // Sort-merge join
    double cpuCost = 0.0;
    for (int i=0; i<cardinalities.size(); i++) {
      double cardinality = cardinalities.get(i);
      if (!sorted.get(i)) {
        // Sort cost
        cpuCost += computeSortCPUCost(cardinality);
      }
      // Merge cost
      cpuCost += cardinality * cpuCost;
    }
    return cpuCost;
  }

  public double computeSortCPUCost(Double cardinality) {
    return cardinality * Math.log(cardinality) * cpuCost;
  }

  public double computeSortMergeIOCost(
          ImmutableList<Pair<Double, Double>> relationInfos) {
    // Sort-merge join
    double ioCost = 0.0;
    for (Pair<Double,Double> relationInfo : relationInfos) {
      ioCost += computeSortIOCost(relationInfo);
    }
    return ioCost;
  }

  public double computeSortIOCost(Pair<Double, Double> relationInfo) {
    // Sort-merge join
    double ioCost = 0.0;
    double cardinality = relationInfo.left;
    double averageTupleSize = relationInfo.right;
    // Write cost
    ioCost += cardinality * averageTupleSize * localFSWrite;
    // Read cost
    ioCost += cardinality * averageTupleSize * localFSRead;
    // Net transfer cost
    ioCost += cardinality * averageTupleSize * netCost;
    return ioCost;
  }

  public static double computeMapJoinCPUCost(
          ImmutableList<Double> cardinalities,
          ImmutableBitSet streaming) {
    // Hash-join
    double cpuCost = 0.0;
    for (int i=0; i<cardinalities.size(); i++) {
      double cardinality = cardinalities.get(i);
      if (!streaming.get(i)) {
        cpuCost += cardinality;
      }
      cpuCost += cardinality * cpuCost;
    }
    return cpuCost;
  }

  public double computeMapJoinIOCost(
          ImmutableList<Pair<Double, Double>> relationInfos,
          ImmutableBitSet streaming, int parallelism) {
    // Hash-join
    double ioCost = 0.0;
    for (int i=0; i<relationInfos.size(); i++) {
      double cardinality = relationInfos.get(i).left;
      double averageTupleSize = relationInfos.get(i).right;
      if (!streaming.get(i)) {
        ioCost += cardinality * averageTupleSize * netCost * parallelism;
      }
    }
    return ioCost;
  }

  public double computeBucketMapJoinCPUCost(
          ImmutableList<Double> cardinalities,
          ImmutableBitSet streaming) {
    // Hash-join
    double cpuCost = 0.0;
    for (int i=0; i<cardinalities.size(); i++) {
      double cardinality = cardinalities.get(i);
      if (!streaming.get(i)) {
        cpuCost += cardinality * cpuCost;
      }
      cpuCost += cardinality * cpuCost;
    }
    return cpuCost;
  }

  public double computeBucketMapJoinIOCost(
          ImmutableList<Pair<Double, Double>> relationInfos,
          ImmutableBitSet streaming, int parallelism) {
    // Hash-join
    double ioCost = 0.0;
    for (int i=0; i<relationInfos.size(); i++) {
      double cardinality = relationInfos.get(i).left;
      double averageTupleSize = relationInfos.get(i).right;
      if (!streaming.get(i)) {
        ioCost += cardinality * averageTupleSize * netCost * parallelism;
      }
    }
    return ioCost;
  }

  public static double computeSMBMapJoinCPUCost(
          ImmutableList<Double> cardinalities) {
    // Hash-join
    double cpuCost = 0.0;
    for (int i=0; i<cardinalities.size(); i++) {
      cpuCost += cardinalities.get(i) * cpuCost;
    }
    return cpuCost;
  }

  public double computeSMBMapJoinIOCost(
          ImmutableList<Pair<Double, Double>> relationInfos,
          ImmutableBitSet streaming, int parallelism) {
    // Hash-join
    double ioCost = 0.0;
    for (int i=0; i<relationInfos.size(); i++) {
      double cardinality = relationInfos.get(i).left;
      double averageTupleSize = relationInfos.get(i).right;
      if (!streaming.get(i)) {
        ioCost += cardinality * averageTupleSize * netCost * parallelism;
      }
    }
    return ioCost;
  }

  public static boolean isFittingIntoMemory(Double maxSize, RelNode input, int buckets) {
    final RelMetadataQuery mq = input.getCluster().getMetadataQuery();
    Double currentMemory = mq.cumulativeMemoryWithinPhase(input);
    if (currentMemory != null) {
      if(currentMemory / buckets > maxSize) {
        return false;
      }
      return true;
    }
    return false;
  }

  public static ImmutableList<RelCollation> getJoinCollation(JoinPredicateInfo joinPredInfo,
          MapJoinStreamingRelation streamingRelation) {
    // Compute collations
    ImmutableList.Builder<RelFieldCollation> collationListBuilder =
            new ImmutableList.Builder<RelFieldCollation>();
    ImmutableList.Builder<RelFieldCollation> leftCollationListBuilder =
            new ImmutableList.Builder<RelFieldCollation>();
    ImmutableList.Builder<RelFieldCollation> rightCollationListBuilder =
            new ImmutableList.Builder<RelFieldCollation>();
    for (int i = 0; i < joinPredInfo.getEquiJoinPredicateElements().size(); i++) {
      JoinLeafPredicateInfo joinLeafPredInfo = joinPredInfo.
          getEquiJoinPredicateElements().get(i);
      for (int leftPos : joinLeafPredInfo.getProjsFromLeftPartOfJoinKeysInJoinSchema()) {
        final RelFieldCollation leftFieldCollation = new RelFieldCollation(leftPos);
        collationListBuilder.add(leftFieldCollation);
        leftCollationListBuilder.add(leftFieldCollation);        
      }
      for (int rightPos : joinLeafPredInfo.getProjsFromRightPartOfJoinKeysInJoinSchema()) {
        final RelFieldCollation rightFieldCollation = new RelFieldCollation(rightPos);
        collationListBuilder.add(rightFieldCollation);
        rightCollationListBuilder.add(rightFieldCollation);        
      }
    }

    // Return join collations
    final ImmutableList<RelCollation> collation;
    switch (streamingRelation) {
      case LEFT_RELATION:
        collation = ImmutableList.of(
                RelCollationTraitDef.INSTANCE.canonize(
                        new HiveRelCollation(leftCollationListBuilder.build())));
        break;
      case RIGHT_RELATION:
        collation = ImmutableList.of(
                RelCollationTraitDef.INSTANCE.canonize(
                        new HiveRelCollation(rightCollationListBuilder.build())));
        break;
      default:
        collation = ImmutableList.of(
                RelCollationTraitDef.INSTANCE.canonize(
                        new HiveRelCollation(collationListBuilder.build())));
        break;
    }
    return collation;
  }

  public static RelDistribution getJoinRedistribution(JoinPredicateInfo joinPredInfo) {
    // Compute distribution
    ImmutableList.Builder<Integer> keysListBuilder =
            new ImmutableList.Builder<Integer>();
    for (int i = 0; i < joinPredInfo.getEquiJoinPredicateElements().size(); i++) {
      JoinLeafPredicateInfo joinLeafPredInfo = joinPredInfo.
          getEquiJoinPredicateElements().get(i);
      for (int leftPos : joinLeafPredInfo.getProjsFromLeftPartOfJoinKeysInJoinSchema()) {
        keysListBuilder.add(leftPos);
      }
      for (int rightPos : joinLeafPredInfo.getProjsFromRightPartOfJoinKeysInJoinSchema()) {
        keysListBuilder.add(rightPos);
      }
    }
    return new HiveRelDistribution(
                RelDistribution.Type.HASH_DISTRIBUTED, keysListBuilder.build());
  }

  public static RelDistribution getJoinDistribution(JoinPredicateInfo joinPredInfo,
          MapJoinStreamingRelation streamingRelation) {
    // Compute distribution
    ImmutableList.Builder<Integer> leftKeysListBuilder =
            new ImmutableList.Builder<Integer>();
    ImmutableList.Builder<Integer> rightKeysListBuilder =
            new ImmutableList.Builder<Integer>();
    for (int i = 0; i < joinPredInfo.getEquiJoinPredicateElements().size(); i++) {
      JoinLeafPredicateInfo joinLeafPredInfo = joinPredInfo.
          getEquiJoinPredicateElements().get(i);
      for (int leftPos : joinLeafPredInfo.getProjsFromLeftPartOfJoinKeysInJoinSchema()) {
        leftKeysListBuilder.add(leftPos);        
      }
      for (int rightPos : joinLeafPredInfo.getProjsFromRightPartOfJoinKeysInJoinSchema()) {
        rightKeysListBuilder.add(rightPos);        
      }
    }

    RelDistribution distribution = null;
    // Keep buckets from the streaming relation
    if (streamingRelation == MapJoinStreamingRelation.LEFT_RELATION) {
      distribution = new HiveRelDistribution(
              RelDistribution.Type.HASH_DISTRIBUTED, leftKeysListBuilder.build());
    } else if (streamingRelation == MapJoinStreamingRelation.RIGHT_RELATION) {
      distribution = new HiveRelDistribution(
              RelDistribution.Type.HASH_DISTRIBUTED, rightKeysListBuilder.build());
    }

    return distribution;
  }

  public static Double getJoinMemory(HiveJoin join) {
    return getJoinMemory(join, join.getStreamingSide());
  }

  public static Double getJoinMemory(HiveJoin join, MapJoinStreamingRelation streamingSide) {
    Double memory = 0.0;
    final RelMetadataQuery mq = join.getCluster().getMetadataQuery();
    if (streamingSide == MapJoinStreamingRelation.NONE ||
            streamingSide == MapJoinStreamingRelation.RIGHT_RELATION) {
      // Left side
      final Double leftAvgRowSize = mq.getAverageRowSize(join.getLeft());
      final Double leftRowCount = mq.getRowCount(join.getLeft());
      if (leftAvgRowSize == null || leftRowCount == null) {
        return null;
      }
      memory += leftAvgRowSize * leftRowCount;
    }
    if (streamingSide == MapJoinStreamingRelation.NONE ||
            streamingSide == MapJoinStreamingRelation.LEFT_RELATION) {
      // Right side
      final Double rightAvgRowSize = mq.getAverageRowSize(join.getRight());
      final Double rightRowCount = mq.getRowCount(join.getRight());
      if (rightAvgRowSize == null || rightRowCount == null) {
        return null;
      }
      memory += rightAvgRowSize * rightRowCount;
    }
    return memory;
  }

  public static Integer getSplitCountWithRepartition(HiveJoin join) {
    final Double maxSplitSize = join.getCluster().getPlanner().getContext().
            unwrap(HiveAlgorithmsConf.class).getMaxSplitSize();
    // We repartition: new number of splits
    final RelMetadataQuery mq = join.getCluster().getMetadataQuery();
    final Double averageRowSize = mq.getAverageRowSize(join);
    final Double rowCount = mq.getRowCount(join);
    if (averageRowSize == null || rowCount == null) {
      return null;
    }
    final Double totalSize = averageRowSize * rowCount;
    final Double splitCount = totalSize / maxSplitSize;
    return splitCount.intValue();
  }

  public static Integer getSplitCountWithoutRepartition(HiveJoin join) {
    RelNode largeInput;
    if (join.getStreamingSide() == MapJoinStreamingRelation.LEFT_RELATION) {
      largeInput = join.getLeft();
    } else if (join.getStreamingSide() == MapJoinStreamingRelation.RIGHT_RELATION) {
      largeInput = join.getRight();
    } else {
      return null;
    }
    final RelMetadataQuery mq = join.getCluster().getMetadataQuery();
    return mq.splitCount(largeInput);
  }

}
