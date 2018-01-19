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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelDistribution.Type;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin.MapJoinStreamingRelation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

/**
 * Cost model for Tez execution engine.
 */
public class HiveOnTezCostModel extends HiveCostModel {

  private static HiveOnTezCostModel INSTANCE;

  private static HiveAlgorithmsUtil algoUtils;

  private static transient final Logger LOG = LoggerFactory.getLogger(HiveOnTezCostModel.class);

  synchronized public static HiveOnTezCostModel getCostModel(HiveConf conf) {
    if (INSTANCE == null) {
      INSTANCE = new HiveOnTezCostModel(conf);
    }

    return INSTANCE;
  }

  private HiveOnTezCostModel(HiveConf conf) {
    super(Sets.newHashSet(
            TezCommonJoinAlgorithm.INSTANCE,
            TezMapJoinAlgorithm.INSTANCE,
            TezBucketJoinAlgorithm.INSTANCE,
            TezSMBJoinAlgorithm.INSTANCE));

    algoUtils = new HiveAlgorithmsUtil(conf);
  }

  @Override
  public RelOptCost getDefaultCost() {
    return HiveCost.FACTORY.makeZeroCost();
  }

  @Override
  public RelOptCost getScanCost(HiveTableScan ts, RelMetadataQuery mq) {
    return algoUtils.computeScanCost(mq.getRowCount(ts), mq.getAverageRowSize(ts));
  }

  @Override
  public RelOptCost getAggregateCost(HiveAggregate aggregate) {
    if (aggregate.isBucketedInput()) {
      return HiveCost.FACTORY.makeZeroCost();
    } else {
      final RelMetadataQuery mq = aggregate.getCluster().getMetadataQuery();
      // 1. Sum of input cardinalities
      final Double rCount = mq.getRowCount(aggregate.getInput());
      if (rCount == null) {
        return null;
      }
      // 2. CPU cost = sorting cost
      final double cpuCost = algoUtils.computeSortCPUCost(rCount);
      // 3. IO cost = cost of writing intermediary results to local FS +
      //              cost of reading from local FS for transferring to GBy +
      //              cost of transferring map outputs to GBy operator
      final Double rAverageSize = mq.getAverageRowSize(aggregate.getInput());
      if (rAverageSize == null) {
        return null;
      }
      final double ioCost = algoUtils.computeSortIOCost(new Pair<Double,Double>(rCount,rAverageSize));
      // 4. Result
      return HiveCost.FACTORY.makeCost(rCount, cpuCost, ioCost);
    }
  }

  /**
   * COMMON_JOIN is Sort Merge Join. Each parallel computation handles multiple
   * splits.
   */
  public static class TezCommonJoinAlgorithm implements JoinAlgorithm {

    public static final JoinAlgorithm INSTANCE = new TezCommonJoinAlgorithm();
    private static final String ALGORITHM_NAME = "CommonJoin";


    @Override
    public String toString() {
      return ALGORITHM_NAME;
    }

    @Override
    public boolean isExecutable(HiveJoin join) {
      return true;
    }

    @Override
    public RelOptCost getCost(HiveJoin join) {
      final RelMetadataQuery mq = join.getCluster().getMetadataQuery();
      // 1. Sum of input cardinalities
      final Double leftRCount = mq.getRowCount(join.getLeft());
      final Double rightRCount = mq.getRowCount(join.getRight());
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
      double cpuCost;
      try {
        cpuCost = algoUtils.computeSortMergeCPUCost(cardinalities, join.getSortedInputs());
      } catch (CalciteSemanticException e) {
        LOG.trace("Failed to compute sort merge cpu cost ", e);
        return null;
      }
      // 3. IO cost = cost of writing intermediary results to local FS +
      //              cost of reading from local FS for transferring to join +
      //              cost of transferring map outputs to Join operator
      final Double leftRAverageSize = mq.getAverageRowSize(join.getLeft());
      final Double rightRAverageSize = mq.getAverageRowSize(join.getRight());
      if (leftRAverageSize == null || rightRAverageSize == null) {
        return null;
      }
      ImmutableList<Pair<Double,Double>> relationInfos = new ImmutableList.Builder<Pair<Double,Double>>().
              add(new Pair<Double,Double>(leftRCount,leftRAverageSize)).
              add(new Pair<Double,Double>(rightRCount,rightRAverageSize)).
              build();
      final double ioCost = algoUtils.computeSortMergeIOCost(relationInfos);
      // 4. Result
      return HiveCost.FACTORY.makeCost(rCount, cpuCost, ioCost);
    }

    @Override
    public ImmutableList<RelCollation> getCollation(HiveJoin join) {
      return HiveAlgorithmsUtil.getJoinCollation(join.getJoinPredicateInfo(),
              MapJoinStreamingRelation.NONE);
    }

    @Override
    public RelDistribution getDistribution(HiveJoin join) {
      return HiveAlgorithmsUtil.getJoinRedistribution(join.getJoinPredicateInfo());
    }

    @Override
    public Double getMemory(HiveJoin join) {
      return HiveAlgorithmsUtil.getJoinMemory(join, MapJoinStreamingRelation.NONE);
    }

    @Override
    public Double getCumulativeMemoryWithinPhaseSplit(HiveJoin join) {
      JoinAlgorithm oldAlgo = join.getJoinAlgorithm();
      join.setJoinAlgorithm(TezCommonJoinAlgorithm.INSTANCE);

      final RelMetadataQuery mq = join.getCluster().getMetadataQuery();
      final Double memoryWithinPhase = mq.cumulativeMemoryWithinPhase(join);
      final Integer splitCount = mq.splitCount(join);
      join.setJoinAlgorithm(oldAlgo);

      if (memoryWithinPhase == null || splitCount == null) {
        return null;
      }

      return memoryWithinPhase / splitCount;
    }

    @Override
    public Boolean isPhaseTransition(HiveJoin join) {
      return true;
    }

    @Override
    public Integer getSplitCount(HiveJoin join) {
      return HiveAlgorithmsUtil.getSplitCountWithRepartition(join);
    }
  }

  /**
   * MAP_JOIN a hash join that keeps the whole data set of non streaming tables
   * in memory.
   */
  public static class TezMapJoinAlgorithm implements JoinAlgorithm {

    public static final JoinAlgorithm INSTANCE = new TezMapJoinAlgorithm();
    private static final String ALGORITHM_NAME = "MapJoin";


    @Override
    public String toString() {
      return ALGORITHM_NAME;
    }

    @Override
    public boolean isExecutable(HiveJoin join) {
      final Double maxMemory = join.getCluster().getPlanner().getContext().
              unwrap(HiveAlgorithmsConf.class).getMaxMemory();
      // Check streaming side
      RelNode smallInput = join.getStreamingInput();
      if (smallInput == null) {
        return false;
      }
      return HiveAlgorithmsUtil.isFittingIntoMemory(maxMemory, smallInput, 1);
    }

    @Override
    public RelOptCost getCost(HiveJoin join) {
      final RelMetadataQuery mq = join.getCluster().getMetadataQuery();
      // 1. Sum of input cardinalities
      final Double leftRCount = mq.getRowCount(join.getLeft());
      final Double rightRCount = mq.getRowCount(join.getRight());
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
      ImmutableBitSet.Builder streamingBuilder = ImmutableBitSet.builder();
      switch (join.getStreamingSide()) {
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
      final double cpuCost = HiveAlgorithmsUtil.computeMapJoinCPUCost(cardinalities, streaming);
      // 3. IO cost = cost of transferring small tables to join node *
      //              degree of parallelism
      final Double leftRAverageSize = mq.getAverageRowSize(join.getLeft());
      final Double rightRAverageSize = mq.getAverageRowSize(join.getRight());
      if (leftRAverageSize == null || rightRAverageSize == null) {
        return null;
      }
      ImmutableList<Pair<Double,Double>> relationInfos = new ImmutableList.Builder<Pair<Double,Double>>().
              add(new Pair<Double,Double>(leftRCount,leftRAverageSize)).
              add(new Pair<Double,Double>(rightRCount,rightRAverageSize)).
              build();
      JoinAlgorithm oldAlgo = join.getJoinAlgorithm();
      join.setJoinAlgorithm(TezMapJoinAlgorithm.INSTANCE);
      final int parallelism = mq.splitCount(join) == null
              ? 1 : mq.splitCount(join);
      join.setJoinAlgorithm(oldAlgo);
      final double ioCost = algoUtils.computeMapJoinIOCost(relationInfos, streaming, parallelism);
      // 4. Result
      return HiveCost.FACTORY.makeCost(rCount, cpuCost, ioCost);
    }

    @Override
    public ImmutableList<RelCollation> getCollation(HiveJoin join) {
      final MapJoinStreamingRelation streamingSide = join.getStreamingSide();
      if (streamingSide != MapJoinStreamingRelation.LEFT_RELATION
              && streamingSide != MapJoinStreamingRelation.RIGHT_RELATION) {
        // Error; default value
        LOG.warn("Streaming side for map join not chosen");
        return ImmutableList.of();
      }
      return HiveAlgorithmsUtil.getJoinCollation(join.getJoinPredicateInfo(),
              join.getStreamingSide());
    }

    @Override
    public RelDistribution getDistribution(HiveJoin join) {
      final MapJoinStreamingRelation streamingSide = join.getStreamingSide();
      if (streamingSide != MapJoinStreamingRelation.LEFT_RELATION
              && streamingSide != MapJoinStreamingRelation.RIGHT_RELATION) {
        // Error; default value
        LOG.warn("Streaming side for map join not chosen");
        return RelDistributions.SINGLETON;
      }
      return HiveAlgorithmsUtil.getJoinDistribution(join.getJoinPredicateInfo(),
              join.getStreamingSide());
    }

    @Override
    public Double getMemory(HiveJoin join) {
      return HiveAlgorithmsUtil.getJoinMemory(join);
    }

    @Override
    public Double getCumulativeMemoryWithinPhaseSplit(HiveJoin join) {
      // Check streaming side
      RelNode inMemoryInput;
      if (join.getStreamingSide() == MapJoinStreamingRelation.LEFT_RELATION) {
        inMemoryInput = join.getRight();
      } else if (join.getStreamingSide() == MapJoinStreamingRelation.RIGHT_RELATION) {
        inMemoryInput = join.getLeft();
      } else {
        return null;
      }
      // If simple map join, the whole relation goes in memory
      final RelMetadataQuery mq = join.getCluster().getMetadataQuery();
      return mq.cumulativeMemoryWithinPhase(inMemoryInput);
    }

    @Override
    public Boolean isPhaseTransition(HiveJoin join) {
      return false;
    }

    @Override
    public Integer getSplitCount(HiveJoin join) {
      return HiveAlgorithmsUtil.getSplitCountWithoutRepartition(join);
    }
  }

  /**
   * BUCKET_JOIN is a hash joins where one bucket of the non streaming tables
   * is kept in memory at the time.
   */
  public static class TezBucketJoinAlgorithm implements JoinAlgorithm {

    public static final JoinAlgorithm INSTANCE = new TezBucketJoinAlgorithm();
    private static final String ALGORITHM_NAME = "BucketJoin";


    @Override
    public String toString() {
      return ALGORITHM_NAME;
    }

    @Override
    public boolean isExecutable(HiveJoin join) {
      final RelMetadataQuery mq = join.getCluster().getMetadataQuery();
      final Double maxMemory = join.getCluster().getPlanner().getContext().
              unwrap(HiveAlgorithmsConf.class).getMaxMemory();
      // Check streaming side
      RelNode smallInput = join.getStreamingInput();
      if (smallInput == null) {
        return false;
      }
      // Get key columns
      JoinPredicateInfo joinPredInfo = join.getJoinPredicateInfo();
      List<ImmutableIntList> joinKeysInChildren = new ArrayList<ImmutableIntList>();
      joinKeysInChildren.add(
              ImmutableIntList.copyOf(
                      joinPredInfo.getProjsFromLeftPartOfJoinKeysInChildSchema()));
      joinKeysInChildren.add(
              ImmutableIntList.copyOf(
                      joinPredInfo.getProjsFromRightPartOfJoinKeysInChildSchema()));

      // Requirements: for Bucket, bucketed by their keys on both sides and fitting in memory
      // Obtain number of buckets
      //TODO: Incase of non bucketed splits would be computed based on data size/max part size
      // What we need is a way to get buckets not splits
      JoinAlgorithm oldAlgo = join.getJoinAlgorithm();
      join.setJoinAlgorithm(TezBucketJoinAlgorithm.INSTANCE);
      Integer buckets = mq.splitCount(smallInput);
      join.setJoinAlgorithm(oldAlgo);

      if (buckets == null) {
        return false;
      }
      if (!HiveAlgorithmsUtil.isFittingIntoMemory(maxMemory, smallInput, buckets)) {
        return false;
      }
      for (int i=0; i<join.getInputs().size(); i++) {
        RelNode input = join.getInputs().get(i);
        // Is bucketJoin possible? We need correct bucketing
        RelDistribution distribution = mq.distribution(input);
        if (distribution.getType() != Type.HASH_DISTRIBUTED) {
          return false;
        }
        if (!distribution.getKeys().containsAll(joinKeysInChildren.get(i))) {
          return false;
        }
      }
      return true;
    }

    @Override
    public RelOptCost getCost(HiveJoin join) {
      final RelMetadataQuery mq = join.getCluster().getMetadataQuery();
      // 1. Sum of input cardinalities
      final Double leftRCount = mq.getRowCount(join.getLeft());
      final Double rightRCount = mq.getRowCount(join.getRight());
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
      ImmutableBitSet.Builder streamingBuilder = ImmutableBitSet.builder();
      switch (join.getStreamingSide()) {
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
      final double cpuCost = algoUtils.computeBucketMapJoinCPUCost(cardinalities, streaming);
      // 3. IO cost = cost of transferring small tables to join node *
      //              degree of parallelism
      final Double leftRAverageSize = mq.getAverageRowSize(join.getLeft());
      final Double rightRAverageSize = mq.getAverageRowSize(join.getRight());
      if (leftRAverageSize == null || rightRAverageSize == null) {
        return null;
      }
      ImmutableList<Pair<Double,Double>> relationInfos = new ImmutableList.Builder<Pair<Double,Double>>().
              add(new Pair<Double,Double>(leftRCount,leftRAverageSize)).
              add(new Pair<Double,Double>(rightRCount,rightRAverageSize)).
              build();
      //TODO: No Of buckets is not same as no of splits
      JoinAlgorithm oldAlgo = join.getJoinAlgorithm();
      join.setJoinAlgorithm(TezBucketJoinAlgorithm.INSTANCE);
      final int parallelism = mq.splitCount(join) == null
              ? 1 : mq.splitCount(join);
      join.setJoinAlgorithm(oldAlgo);

      final double ioCost = algoUtils.computeBucketMapJoinIOCost(relationInfos, streaming, parallelism);
      // 4. Result
      return HiveCost.FACTORY.makeCost(rCount, cpuCost, ioCost);
    }

    @Override
    public ImmutableList<RelCollation> getCollation(HiveJoin join) {
      final MapJoinStreamingRelation streamingSide = join.getStreamingSide();
      if (streamingSide != MapJoinStreamingRelation.LEFT_RELATION
              && streamingSide != MapJoinStreamingRelation.RIGHT_RELATION) {
        // Error; default value
        LOG.warn("Streaming side for map join not chosen");
        return ImmutableList.of();
      }
      return HiveAlgorithmsUtil.getJoinCollation(join.getJoinPredicateInfo(),
              join.getStreamingSide());
    }

    @Override
    public RelDistribution getDistribution(HiveJoin join) {
      return HiveAlgorithmsUtil.getJoinRedistribution(join.getJoinPredicateInfo());
    }

    @Override
    public Double getMemory(HiveJoin join) {
      return HiveAlgorithmsUtil.getJoinMemory(join);
    }

    @Override
    public Double getCumulativeMemoryWithinPhaseSplit(HiveJoin join) {
      // Check streaming side
      RelNode inMemoryInput;
      if (join.getStreamingSide() == MapJoinStreamingRelation.LEFT_RELATION) {
        inMemoryInput = join.getRight();
      } else if (join.getStreamingSide() == MapJoinStreamingRelation.RIGHT_RELATION) {
        inMemoryInput = join.getLeft();
      } else {
        return null;
      }
      // If bucket map join, only a split goes in memory
      final RelMetadataQuery mq = join.getCluster().getMetadataQuery();
      final Double memoryInput = mq.cumulativeMemoryWithinPhase(inMemoryInput);
      final Integer splitCount = mq.splitCount(inMemoryInput);
      if (memoryInput == null || splitCount == null) {
        return null;
      }
      return memoryInput / splitCount;
    }

    @Override
    public Boolean isPhaseTransition(HiveJoin join) {
      return false;
    }

    @Override
    public Integer getSplitCount(HiveJoin join) {
      return HiveAlgorithmsUtil.getSplitCountWithoutRepartition(join);
    }
  }

  /**
   * SMB_JOIN is a Sort Merge Join. Each parallel computation handles one bucket.
   */
  public static class TezSMBJoinAlgorithm implements JoinAlgorithm {

    public static final JoinAlgorithm INSTANCE = new TezSMBJoinAlgorithm();
    private static final String ALGORITHM_NAME = "SMBJoin";


    @Override
    public String toString() {
      return ALGORITHM_NAME;
    }

    @Override
    public boolean isExecutable(HiveJoin join) {
      // Requirements: for SMB, sorted by their keys on both sides and bucketed.
      // Get key columns
      JoinPredicateInfo joinPredInfo = join.getJoinPredicateInfo();
      List<ImmutableIntList> joinKeysInChildren = new ArrayList<ImmutableIntList>();
      joinKeysInChildren.add(
              ImmutableIntList.copyOf(
                      joinPredInfo.getProjsFromLeftPartOfJoinKeysInChildSchema()));
      joinKeysInChildren.add(
              ImmutableIntList.copyOf(
                      joinPredInfo.getProjsFromRightPartOfJoinKeysInChildSchema()));

      final RelMetadataQuery mq = join.getCluster().getMetadataQuery();
      for (int i=0; i<join.getInputs().size(); i++) {
        RelNode input = join.getInputs().get(i);
        // Is smbJoin possible? We need correct order
        boolean orderFound;
        try {
          orderFound = join.getSortedInputs().get(i);
        } catch (CalciteSemanticException e) {
          LOG.trace("Not possible to do SMB Join ",e);
          return false;
        }
        if (!orderFound) {
          return false;
        }
        // Is smbJoin possible? We need correct bucketing
        RelDistribution distribution = mq.distribution(input);
        if (distribution.getType() != Type.HASH_DISTRIBUTED) {
          return false;
        }
        if (!distribution.getKeys().containsAll(joinKeysInChildren.get(i))) {
          return false;
        }
      }
      return true;
    }

    @Override
    public RelOptCost getCost(HiveJoin join) {
      final RelMetadataQuery mq = join.getCluster().getMetadataQuery();
      // 1. Sum of input cardinalities
      final Double leftRCount = mq.getRowCount(join.getLeft());
      final Double rightRCount = mq.getRowCount(join.getRight());
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
      ImmutableBitSet.Builder streamingBuilder = ImmutableBitSet.builder();
      switch (join.getStreamingSide()) {
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
      final double cpuCost = HiveAlgorithmsUtil.computeSMBMapJoinCPUCost(cardinalities);
      // 3. IO cost = cost of transferring small tables to join node *
      //              degree of parallelism
      final Double leftRAverageSize = mq.getAverageRowSize(join.getLeft());
      final Double rightRAverageSize = mq.getAverageRowSize(join.getRight());
      if (leftRAverageSize == null || rightRAverageSize == null) {
        return null;
      }
      ImmutableList<Pair<Double,Double>> relationInfos = new ImmutableList.Builder<Pair<Double,Double>>().
              add(new Pair<Double,Double>(leftRCount,leftRAverageSize)).
              add(new Pair<Double,Double>(rightRCount,rightRAverageSize)).
              build();

      // TODO: Split count is not the same as no of buckets
      JoinAlgorithm oldAlgo = join.getJoinAlgorithm();
      join.setJoinAlgorithm(TezSMBJoinAlgorithm.INSTANCE);
      final int parallelism = mq.splitCount(join) == null ? 1 : mq
          .splitCount(join);
      join.setJoinAlgorithm(oldAlgo);

      final double ioCost = algoUtils.computeSMBMapJoinIOCost(relationInfos, streaming, parallelism);
      // 4. Result
      return HiveCost.FACTORY.makeCost(rCount, cpuCost, ioCost);
    }

    @Override
    public ImmutableList<RelCollation> getCollation(HiveJoin join) {
      return HiveAlgorithmsUtil.getJoinCollation(join.getJoinPredicateInfo(),
              MapJoinStreamingRelation.NONE);
    }

    @Override
    public RelDistribution getDistribution(HiveJoin join) {
      return HiveAlgorithmsUtil.getJoinRedistribution(join.getJoinPredicateInfo());
    }

    @Override
    public Double getMemory(HiveJoin join) {
      return 0.0;
    }

    @Override
    public Double getCumulativeMemoryWithinPhaseSplit(HiveJoin join) {
      final RelMetadataQuery mq = join.getCluster().getMetadataQuery();
      // TODO: Split count is not same as no of buckets
      JoinAlgorithm oldAlgo = join.getJoinAlgorithm();
      join.setJoinAlgorithm(TezSMBJoinAlgorithm.INSTANCE);

      final Double memoryWithinPhase = mq.cumulativeMemoryWithinPhase(join);
      final Integer splitCount = mq.splitCount(join);
      join.setJoinAlgorithm(oldAlgo);

      if (memoryWithinPhase == null || splitCount == null) {
        return null;
      }
      return memoryWithinPhase / splitCount;
    }

    @Override
    public Boolean isPhaseTransition(HiveJoin join) {
      return false;
    }

    @Override
    public Integer getSplitCount(HiveJoin join) {
      return HiveAlgorithmsUtil.getSplitCountWithoutRepartition(join);
    }
  }

}
