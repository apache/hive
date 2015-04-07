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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistribution.Type;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin.MapJoinStreamingRelation;

import com.google.common.collect.ImmutableList;

/**
 * Cost model for Tez execution engine.
 */
public class HiveOnTezCostModel extends HiveCostModel {

  private final Double maxMemory;


  public HiveOnTezCostModel(Double maxMemory) {
    this.maxMemory = maxMemory;
  }

  @Override
  public RelOptCost getDefaultCost() {
    return HiveCost.FACTORY.makeZeroCost();
  }

  @Override
  public RelOptCost getAggregateCost(HiveAggregate aggregate) {
    if (aggregate.isBucketedInput()) {
      return HiveCost.FACTORY.makeZeroCost();
    } else {
      // 1. Sum of input cardinalities
      final Double rCount = RelMetadataQuery.getRowCount(aggregate.getInput());
      if (rCount == null) {
        return null;
      }
      // 2. CPU cost = sorting cost
      final double cpuCost = HiveCostUtil.computeSortCPUCost(rCount);
      // 3. IO cost = cost of writing intermediary results to local FS +
      //              cost of reading from local FS for transferring to GBy +
      //              cost of transferring map outputs to GBy operator
      final Double rAverageSize = RelMetadataQuery.getAverageRowSize(aggregate.getInput());
      if (rAverageSize == null) {
        return null;
      }
      final double ioCost = HiveCostUtil.computeSortIOCost(new Pair<Double,Double>(rCount,rAverageSize));
      // 4. Result
      return HiveCost.FACTORY.makeCost(rCount, cpuCost, ioCost);
    }
  }

  @Override
  protected EnumSet<JoinAlgorithm> getExecutableJoinAlgorithms(HiveJoin join) {
    Set<JoinAlgorithm> possibleAlgorithms = new HashSet<JoinAlgorithm>();

    // Check streaming side
    RelNode smallInput;
    if (join.getMapJoinStreamingSide() == MapJoinStreamingRelation.LEFT_RELATION) {
      smallInput = join.getRight();
    } else if (join.getMapJoinStreamingSide() == MapJoinStreamingRelation.RIGHT_RELATION) {
      smallInput = join.getLeft();
    } else {
      smallInput = null;
    }

    if (smallInput != null) {
      // Requirements:
      // - For SMB, sorted by their keys on both sides and bucketed.
      // - For Bucket, bucketed by their keys on both sides. / Fitting in memory
      // - For Map, no additional requirement. / Fitting in memory

      // Get key columns
      JoinPredicateInfo joinPredInfo = HiveCalciteUtil.JoinPredicateInfo.
              constructJoinPredicateInfo(join);
      List<ImmutableIntList> joinKeysInChildren = new ArrayList<ImmutableIntList>();
      joinKeysInChildren.add(
              ImmutableIntList.copyOf(
                      joinPredInfo.getProjsFromLeftPartOfJoinKeysInChildSchema()));
      joinKeysInChildren.add(
              ImmutableIntList.copyOf(
                      joinPredInfo.getProjsFromRightPartOfJoinKeysInChildSchema()));

      // Obtain number of buckets
      Integer buckets = RelMetadataQuery.splitCount(smallInput);
      // Obtain map algorithms for which smallest input fits in memory
      boolean bucketFitsMemory = false;
      boolean inputFitsMemory = false;
      if (buckets != null) {
        bucketFitsMemory = isFittingIntoMemory(maxMemory, smallInput, buckets);
      }
      inputFitsMemory = bucketFitsMemory ?
              isFittingIntoMemory(maxMemory, smallInput, 1) : false;
      boolean orderedBucketed = true;
      boolean bucketed = true;
      for (int i=0; i<join.getInputs().size(); i++) {
        RelNode input = join.getInputs().get(i);
        // Is smbJoin possible? We need correct order
        if (orderedBucketed) {
          boolean orderFound = join.getSortedInputs().get(i);
          if (!orderFound) {
            orderedBucketed = false;
          }
        }
        // Is smbJoin or bucketJoin possible? We need correct bucketing
        if (bucketFitsMemory && bucketed) {
          RelDistribution distribution = RelMetadataQuery.distribution(input);
          if (distribution.getType() != Type.HASH_DISTRIBUTED) {
            orderedBucketed = false;
            bucketed = false;
          }
          if (!distribution.getKeys().containsAll(joinKeysInChildren.get(i))) {
            orderedBucketed = false;
            bucketed = false;
          }
        }
      }

      // Add chosen map join algorithm
      if (orderedBucketed) {
        possibleAlgorithms.add(JoinAlgorithm.SMB_JOIN);
      }
      if (bucketFitsMemory && bucketed) {
        possibleAlgorithms.add(JoinAlgorithm.BUCKET_JOIN);
      }
      if (inputFitsMemory) {
        possibleAlgorithms.add(JoinAlgorithm.MAP_JOIN);
      }
    }
    // A reduce side (common) join does not have special
    // requirements.
    possibleAlgorithms.add(JoinAlgorithm.COMMON_JOIN);
    return EnumSet.copyOf(possibleAlgorithms);
  }

  private static boolean isFittingIntoMemory(Double maxSize, RelNode input, int buckets) {
    Double currentMemory = RelMetadataQuery.cumulativeMemoryWithinPhase(input);
    if (currentMemory != null) {
      if(currentMemory / buckets > maxSize) {
        return false;
      }
      return true;
    }
    return false;
  }

  @Override
  protected RelOptCost getJoinCost(HiveJoin join, JoinAlgorithm algorithm) {
    RelOptCost algorithmCost;
    switch (algorithm) {
      case COMMON_JOIN:
        algorithmCost = computeCostCommonJoin(join);
        break;
      case MAP_JOIN:
        algorithmCost = computeCostMapJoin(join);
        break;
      case BUCKET_JOIN:
        algorithmCost = computeCostBucketJoin(join);
        break;
      case SMB_JOIN:
        algorithmCost = computeCostSMBJoin(join);
        break;
      default:
        algorithmCost = null;
    }
    return algorithmCost;
  }

  private static RelOptCost computeCostCommonJoin(HiveJoin join) {
    // 1. Sum of input cardinalities
    final Double leftRCount = RelMetadataQuery.getRowCount(join.getLeft());
    final Double rightRCount = RelMetadataQuery.getRowCount(join.getRight());
    if (leftRCount == null || rightRCount == null) {
      return null;
    }
    final double rCount = leftRCount + rightRCount;
    // 2. CPU cost = sorting cost (for each relation) +
    //               total merge cost
    ImmutableList<Double> cardinalities = new ImmutableList.Builder<Double>().
            add(leftRCount).
            add(rightRCount).
            build();
    final double cpuCost = HiveCostUtil.computeSortMergeCPUCost(cardinalities, join.getSortedInputs());
    // 3. IO cost = cost of writing intermediary results to local FS +
    //              cost of reading from local FS for transferring to join +
    //              cost of transferring map outputs to Join operator
    final Double leftRAverageSize = RelMetadataQuery.getAverageRowSize(join.getLeft());
    final Double rightRAverageSize = RelMetadataQuery.getAverageRowSize(join.getRight());
    if (leftRAverageSize == null || rightRAverageSize == null) {
      return null;
    }
    ImmutableList<Pair<Double,Double>> relationInfos = new ImmutableList.Builder<Pair<Double,Double>>().
            add(new Pair<Double,Double>(leftRCount,leftRAverageSize)).
            add(new Pair<Double,Double>(rightRCount,rightRAverageSize)).
            build();
    final double ioCost = HiveCostUtil.computeSortMergeIOCost(relationInfos);
    // 4. Result
    return HiveCost.FACTORY.makeCost(rCount, cpuCost, ioCost);
  }

  private static RelOptCost computeCostMapJoin(HiveJoin join) {
    // 1. Sum of input cardinalities
    final Double leftRCount = RelMetadataQuery.getRowCount(join.getLeft());
    final Double rightRCount = RelMetadataQuery.getRowCount(join.getRight());
    if (leftRCount == null || rightRCount == null) {
      return null;
    }
    final double rCount = leftRCount + rightRCount;
    // 2. CPU cost = HashTable  construction  cost  +
    //               join cost
    ImmutableList<Double> cardinalities = new ImmutableList.Builder<Double>().
            add(leftRCount).
            add(rightRCount).
            build();
    ImmutableBitSet.Builder streamingBuilder = new ImmutableBitSet.Builder();
    switch (join.getMapJoinStreamingSide()) {
      case LEFT_RELATION:
        streamingBuilder.set(0);
        break;
      case RIGHT_RELATION:
        streamingBuilder.set(1);
        break;
      default:
        return null;
    }
    ImmutableBitSet streaming = streamingBuilder.build();
    final double cpuCost = HiveCostUtil.computeMapJoinCPUCost(cardinalities, streaming);
    // 3. IO cost = cost of transferring small tables to join node *
    //              degree of parallelism
    final Double leftRAverageSize = RelMetadataQuery.getAverageRowSize(join.getLeft());
    final Double rightRAverageSize = RelMetadataQuery.getAverageRowSize(join.getRight());
    if (leftRAverageSize == null || rightRAverageSize == null) {
      return null;
    }
    ImmutableList<Pair<Double,Double>> relationInfos = new ImmutableList.Builder<Pair<Double,Double>>().
            add(new Pair<Double,Double>(leftRCount,leftRAverageSize)).
            add(new Pair<Double,Double>(rightRCount,rightRAverageSize)).
            build();
    final int parallelism = RelMetadataQuery.splitCount(join) == null
            ? 1 : RelMetadataQuery.splitCount(join);
    final double ioCost = HiveCostUtil.computeMapJoinIOCost(relationInfos, streaming, parallelism);
    // 4. Result
    return HiveCost.FACTORY.makeCost(rCount, cpuCost, ioCost);
  }

  private static RelOptCost computeCostBucketJoin(HiveJoin join) {
    // 1. Sum of input cardinalities
    final Double leftRCount = RelMetadataQuery.getRowCount(join.getLeft());
    final Double rightRCount = RelMetadataQuery.getRowCount(join.getRight());
    if (leftRCount == null || rightRCount == null) {
      return null;
    }
    final double rCount = leftRCount + rightRCount;
    // 2. CPU cost = HashTable  construction  cost  +
    //               join cost
    ImmutableList<Double> cardinalities = new ImmutableList.Builder<Double>().
            add(leftRCount).
            add(rightRCount).
            build();
    ImmutableBitSet.Builder streamingBuilder = new ImmutableBitSet.Builder();
    switch (join.getMapJoinStreamingSide()) {
      case LEFT_RELATION:
        streamingBuilder.set(0);
        break;
      case RIGHT_RELATION:
        streamingBuilder.set(1);
        break;
      default:
        return null;
    }
    ImmutableBitSet streaming = streamingBuilder.build();
    final double cpuCost = HiveCostUtil.computeBucketMapJoinCPUCost(cardinalities, streaming);
    // 3. IO cost = cost of transferring small tables to join node *
    //              degree of parallelism
    final Double leftRAverageSize = RelMetadataQuery.getAverageRowSize(join.getLeft());
    final Double rightRAverageSize = RelMetadataQuery.getAverageRowSize(join.getRight());
    if (leftRAverageSize == null || rightRAverageSize == null) {
      return null;
    }
    ImmutableList<Pair<Double,Double>> relationInfos = new ImmutableList.Builder<Pair<Double,Double>>().
            add(new Pair<Double,Double>(leftRCount,leftRAverageSize)).
            add(new Pair<Double,Double>(rightRCount,rightRAverageSize)).
            build();
    final int parallelism = RelMetadataQuery.splitCount(join) == null
            ? 1 : RelMetadataQuery.splitCount(join);
    final double ioCost = HiveCostUtil.computeBucketMapJoinIOCost(relationInfos, streaming, parallelism);
    // 4. Result
    return HiveCost.FACTORY.makeCost(rCount, cpuCost, ioCost);
  }

  private static RelOptCost computeCostSMBJoin(HiveJoin join) {
    // 1. Sum of input cardinalities
    final Double leftRCount = RelMetadataQuery.getRowCount(join.getLeft());
    final Double rightRCount = RelMetadataQuery.getRowCount(join.getRight());
    if (leftRCount == null || rightRCount == null) {
      return null;
    }
    final double rCount = leftRCount + rightRCount;
    // 2. CPU cost = HashTable  construction  cost  +
    //               join cost
    ImmutableList<Double> cardinalities = new ImmutableList.Builder<Double>().
            add(leftRCount).
            add(rightRCount).
            build();
    ImmutableBitSet.Builder streamingBuilder = new ImmutableBitSet.Builder();
    switch (join.getMapJoinStreamingSide()) {
      case LEFT_RELATION:
        streamingBuilder.set(0);
        break;
      case RIGHT_RELATION:
        streamingBuilder.set(1);
        break;
      default:
        return null;
    }
    ImmutableBitSet streaming = streamingBuilder.build();
    final double cpuCost = HiveCostUtil.computeSMBMapJoinCPUCost(cardinalities);
    // 3. IO cost = cost of transferring small tables to join node *
    //              degree of parallelism
    final Double leftRAverageSize = RelMetadataQuery.getAverageRowSize(join.getLeft());
    final Double rightRAverageSize = RelMetadataQuery.getAverageRowSize(join.getRight());
    if (leftRAverageSize == null || rightRAverageSize == null) {
      return null;
    }
    ImmutableList<Pair<Double,Double>> relationInfos = new ImmutableList.Builder<Pair<Double,Double>>().
            add(new Pair<Double,Double>(leftRCount,leftRAverageSize)).
            add(new Pair<Double,Double>(rightRCount,rightRAverageSize)).
            build();
    final int parallelism = RelMetadataQuery.splitCount(join) == null
            ? 1 : RelMetadataQuery.splitCount(join);
    final double ioCost = HiveCostUtil.computeSMBMapJoinIOCost(relationInfos, streaming, parallelism);
    // 4. Result
    return HiveCost.FACTORY.makeCost(rCount, cpuCost, ioCost);
  }

}
