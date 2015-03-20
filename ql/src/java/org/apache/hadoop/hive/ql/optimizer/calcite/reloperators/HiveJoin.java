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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistribution.Type;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories.JoinFactory;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCost;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCostUtil;

import com.google.common.collect.ImmutableList;

//TODO: Should we convert MultiJoin to be a child of HiveJoin
public class HiveJoin extends Join implements HiveRelNode {
  private static final Log LOG = LogFactory.getLog(HiveJoin.class);

  // NOTE: COMMON_JOIN & SMB_JOIN are Sort Merge Join (in case of COMMON_JOIN
  // each parallel computation handles multiple splits where as in case of SMB
  // each parallel computation handles one bucket). MAP_JOIN and BUCKET_JOIN is
  // hash joins where MAP_JOIN keeps the whole data set of non streaming tables
  // in memory where as BUCKET_JOIN keeps only the b
  public enum JoinAlgorithm {
    NONE, COMMON_JOIN, MAP_JOIN, BUCKET_JOIN, SMB_JOIN
  }

  public enum MapJoinStreamingRelation {
    NONE, LEFT_RELATION, RIGHT_RELATION
  }

  public static final JoinFactory HIVE_JOIN_FACTORY = new HiveJoinFactoryImpl();

  private final Double maxMemory;
  private final boolean leftSemiJoin;
  private JoinAlgorithm joinAlgorithm;
  private MapJoinStreamingRelation mapJoinStreamingSide;
  private RelOptCost joinCost;
  // Whether inputs are already sorted
  private ImmutableBitSet sortedInputs;

  public static HiveJoin getJoin(RelOptCluster cluster, RelNode left, RelNode right,
      RexNode condition, JoinRelType joinType, boolean leftSemiJoin) {
    try {
      Set<String> variablesStopped = Collections.emptySet();
      return new HiveJoin(cluster, null, left, right, condition, joinType, variablesStopped,
          JoinAlgorithm.NONE, MapJoinStreamingRelation.NONE, ImmutableBitSet.of(), leftSemiJoin);
    } catch (InvalidRelException e) {
      throw new RuntimeException(e);
    }
  }

  protected HiveJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right,
      RexNode condition, JoinRelType joinType, Set<String> variablesStopped,
      JoinAlgorithm joinAlgo, MapJoinStreamingRelation streamingSideForMapJoin,
      ImmutableBitSet sortedInputs, boolean leftSemiJoin) throws InvalidRelException {
    super(cluster, TraitsUtil.getDefaultTraitSet(cluster), left, right, condition, joinType,
        variablesStopped);
    this.joinAlgorithm = joinAlgo;
    this.mapJoinStreamingSide = streamingSideForMapJoin;
    this.sortedInputs = sortedInputs;
    this.leftSemiJoin = leftSemiJoin;
    this.maxMemory = (double) HiveConf.getLongVar(
            cluster.getPlanner().getContext().unwrap(HiveConf.class),
            HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD);
  }

  @Override
  public void implement(Implementor implementor) {
  }

  @Override
  public final HiveJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
      RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    try {
      Set<String> variablesStopped = Collections.emptySet();
      return new HiveJoin(getCluster(), traitSet, left, right, conditionExpr, joinType,
          variablesStopped, joinAlgorithm, mapJoinStreamingSide, sortedInputs, leftSemiJoin);
    } catch (InvalidRelException e) {
      // Semantic error not possible. Must be a bug. Convert to
      // internal error.
      throw new AssertionError(e);
    }
  }

  public JoinAlgorithm getJoinAlgorithm() {
    return joinAlgorithm;
  }

  public MapJoinStreamingRelation getMapJoinStreamingSide() {
    return mapJoinStreamingSide;
  }

  public boolean isLeftSemiJoin() {
    return leftSemiJoin;
  }

  /**
   * Model cost of join as size of Inputs.
   */
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    this.joinCost = chooseJoinAlgorithmAndGetCost();
    return this.joinCost;
  }

  private RelOptCost chooseJoinAlgorithmAndGetCost() {
    // 1. Choose streaming side
    chooseStreamingSide();
    // 2. Store order inputs
    checkInputsCorrectOrder();
    // 3. Get possible algorithms
    Set<JoinAlgorithm> possibleAlgorithms = obtainJoinAlgorithms();
    // 4. For each possible algorithm, calculate cost, and select best
    RelOptCost selfCost = null;
    for (JoinAlgorithm possibleAlgorithm : possibleAlgorithms) {
      switch (possibleAlgorithm) {
        case COMMON_JOIN:
          RelOptCost commonJoinCost = computeSelfCostCommonJoin();
          if (LOG.isDebugEnabled()) {
            LOG.debug("COMMONJOIN possible");
            LOG.debug("COMMONJOIN cost: " + commonJoinCost);
          }
          if (selfCost == null || commonJoinCost.isLt(selfCost) ) {
            this.joinAlgorithm = JoinAlgorithm.COMMON_JOIN;
            selfCost = commonJoinCost;
          }
          break;
        case MAP_JOIN:
          RelOptCost mapJoinCost = computeSelfCostMapJoin();
          if (LOG.isDebugEnabled()) {
            LOG.debug("MAPJOIN possible");
            LOG.debug("MAPJOIN cost: " + mapJoinCost);
          }
          if (selfCost == null || mapJoinCost.isLt(selfCost) ) {
            this.joinAlgorithm = JoinAlgorithm.MAP_JOIN;
            selfCost = mapJoinCost;
          }
          break;
        case BUCKET_JOIN:
          RelOptCost bucketJoinCost = computeSelfCostBucketJoin();
          if (LOG.isDebugEnabled()) {
            LOG.debug("BUCKETJOIN possible");
            LOG.debug("BUCKETJOIN cost: " + bucketJoinCost);
          }
          if (selfCost == null || bucketJoinCost.isLt(selfCost) ) {
            this.joinAlgorithm = JoinAlgorithm.BUCKET_JOIN;
            selfCost = bucketJoinCost;
          }
          break;
        case SMB_JOIN:
          RelOptCost smbJoinCost = computeSelfCostSMBJoin();
          if (LOG.isDebugEnabled()) {
            LOG.debug("SMBJOIN possible");
            LOG.debug("SMBJOIN cost: " + smbJoinCost);
          }
          if (selfCost == null || smbJoinCost.isLt(selfCost) ) {
            this.joinAlgorithm = JoinAlgorithm.SMB_JOIN;
            selfCost = smbJoinCost;
          }
          break;
        default:
          //TODO: Exception
      }
    }
    return selfCost;
  }

  private void chooseStreamingSide() {
    Double leftInputSize = RelMetadataQuery.memory(this.getLeft());
    Double rightInputSize = RelMetadataQuery.memory(this.getRight());
    if (leftInputSize == null && rightInputSize == null) {
      this.mapJoinStreamingSide = MapJoinStreamingRelation.NONE;
    } else if (leftInputSize != null &&
            (rightInputSize == null ||
            (leftInputSize < rightInputSize))) {
      this.mapJoinStreamingSide = MapJoinStreamingRelation.RIGHT_RELATION;
    } else if (rightInputSize != null &&
            (leftInputSize == null ||
            (rightInputSize <= leftInputSize))) {
      this.mapJoinStreamingSide = MapJoinStreamingRelation.LEFT_RELATION;
    }
  }

  private void checkInputsCorrectOrder() {
    JoinPredicateInfo joinPredInfo = HiveCalciteUtil.JoinPredicateInfo.
            constructJoinPredicateInfo(this);
    List<ImmutableIntList> joinKeysInChildren = new ArrayList<ImmutableIntList>();
    joinKeysInChildren.add(
            ImmutableIntList.copyOf(
                    joinPredInfo.getProjsFromLeftPartOfJoinKeysInChildSchema()));
    joinKeysInChildren.add(
            ImmutableIntList.copyOf(
                    joinPredInfo.getProjsFromRightPartOfJoinKeysInChildSchema()));

    for (int i=0; i<this.getInputs().size(); i++) {
      boolean correctOrderFound = RelCollations.contains(
              RelMetadataQuery.collations(getInputs().get(i)),
              joinKeysInChildren.get(i));
      if (correctOrderFound) {
        sortedInputs.set(i);
      }
    }
  }

  private Set<JoinAlgorithm> obtainJoinAlgorithms() {
    Set<JoinAlgorithm> possibleAlgorithms = new HashSet<JoinAlgorithm>();

    // Check streaming side
    RelNode smallInput;
    if (this.mapJoinStreamingSide == MapJoinStreamingRelation.LEFT_RELATION) {
      smallInput = this.getRight();
    } else if (this.mapJoinStreamingSide == MapJoinStreamingRelation.RIGHT_RELATION) {
      smallInput = this.getLeft();
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
              constructJoinPredicateInfo(this);
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
        bucketFitsMemory = isFittingIntoMemory(this.maxMemory, smallInput, buckets);
      }
      inputFitsMemory = bucketFitsMemory ?
              isFittingIntoMemory(this.maxMemory, smallInput, 1) : false;
      boolean orderedBucketed = true;
      boolean bucketed = true;
      for (int i=0; i<this.getInputs().size(); i++) {
        RelNode input = getInputs().get(i);
        // Is smbJoin possible? We need correct order
        if (orderedBucketed) {
          boolean orderFound = sortedInputs.get(i);
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
    return possibleAlgorithms;
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

  private RelOptCost computeSelfCostCommonJoin() {
    // 1. Sum of input cardinalities
    final Double leftRCount = RelMetadataQuery.getRowCount(getLeft());
    final Double rightRCount = RelMetadataQuery.getRowCount(getRight());
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
    final double cpuCost = HiveCostUtil.computeSortMergeCPUCost(cardinalities, sortedInputs);
    // 3. IO cost = cost of writing intermediary results to local FS +
    //              cost of reading from local FS for transferring to join +
    //              cost of transferring map outputs to Join operator
    final Double leftRAverageSize = RelMetadataQuery.getAverageRowSize(getLeft());
    final Double rightRAverageSize = RelMetadataQuery.getAverageRowSize(getRight());
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

  private RelOptCost computeSelfCostMapJoin() {
    // 1. Sum of input cardinalities
    final Double leftRCount = RelMetadataQuery.getRowCount(getLeft());
    final Double rightRCount = RelMetadataQuery.getRowCount(getRight());
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
    switch (mapJoinStreamingSide) {
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
    final Double leftRAverageSize = RelMetadataQuery.getAverageRowSize(getLeft());
    final Double rightRAverageSize = RelMetadataQuery.getAverageRowSize(getRight());
    if (leftRAverageSize == null || rightRAverageSize == null) {
      return null;
    }
    ImmutableList<Pair<Double,Double>> relationInfos = new ImmutableList.Builder<Pair<Double,Double>>().
            add(new Pair<Double,Double>(leftRCount,leftRAverageSize)).
            add(new Pair<Double,Double>(rightRCount,rightRAverageSize)).
            build();
    final int parallelism = RelMetadataQuery.splitCount(this) == null
            ? 1 : RelMetadataQuery.splitCount(this);
    final double ioCost = HiveCostUtil.computeMapJoinIOCost(relationInfos, streaming, parallelism);
    // 4. Result
    return HiveCost.FACTORY.makeCost(rCount, cpuCost, ioCost);
  }

  private RelOptCost computeSelfCostBucketJoin() {
    // 1. Sum of input cardinalities
    final Double leftRCount = RelMetadataQuery.getRowCount(getLeft());
    final Double rightRCount = RelMetadataQuery.getRowCount(getRight());
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
    switch (mapJoinStreamingSide) {
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
    final Double leftRAverageSize = RelMetadataQuery.getAverageRowSize(getLeft());
    final Double rightRAverageSize = RelMetadataQuery.getAverageRowSize(getRight());
    if (leftRAverageSize == null || rightRAverageSize == null) {
      return null;
    }
    ImmutableList<Pair<Double,Double>> relationInfos = new ImmutableList.Builder<Pair<Double,Double>>().
            add(new Pair<Double,Double>(leftRCount,leftRAverageSize)).
            add(new Pair<Double,Double>(rightRCount,rightRAverageSize)).
            build();
    final int parallelism = RelMetadataQuery.splitCount(this) == null
            ? 1 : RelMetadataQuery.splitCount(this);
    final double ioCost = HiveCostUtil.computeBucketMapJoinIOCost(relationInfos, streaming, parallelism);
    // 4. Result
    return HiveCost.FACTORY.makeCost(rCount, cpuCost, ioCost);
  }

  private RelOptCost computeSelfCostSMBJoin() {
    // 1. Sum of input cardinalities
    final Double leftRCount = RelMetadataQuery.getRowCount(getLeft());
    final Double rightRCount = RelMetadataQuery.getRowCount(getRight());
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
    switch (mapJoinStreamingSide) {
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
    final Double leftRAverageSize = RelMetadataQuery.getAverageRowSize(getLeft());
    final Double rightRAverageSize = RelMetadataQuery.getAverageRowSize(getRight());
    if (leftRAverageSize == null || rightRAverageSize == null) {
      return null;
    }
    ImmutableList<Pair<Double,Double>> relationInfos = new ImmutableList.Builder<Pair<Double,Double>>().
            add(new Pair<Double,Double>(leftRCount,leftRAverageSize)).
            add(new Pair<Double,Double>(rightRCount,rightRAverageSize)).
            build();
    final int parallelism = RelMetadataQuery.splitCount(this) == null
            ? 1 : RelMetadataQuery.splitCount(this);
    final double ioCost = HiveCostUtil.computeSMBMapJoinIOCost(relationInfos, streaming, parallelism);
    // 4. Result
    return HiveCost.FACTORY.makeCost(rCount, cpuCost, ioCost);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("joinAlgorithm", joinAlgorithm.name().toLowerCase())
        .item("cost", joinCost);
  }

  /**
   * @return returns rowtype representing only the left join input
   */
  @Override
  public RelDataType deriveRowType() {
    if (leftSemiJoin) {
      return deriveJoinRowType(left.getRowType(), null, JoinRelType.INNER,
          getCluster().getTypeFactory(), null,
          Collections.<RelDataTypeField> emptyList());
    }
    return super.deriveRowType();
  }

  private static class HiveJoinFactoryImpl implements JoinFactory {
    /**
     * Creates a join.
     *
     * @param left
     *          Left input
     * @param right
     *          Right input
     * @param condition
     *          Join condition
     * @param joinType
     *          Join type
     * @param variablesStopped
     *          Set of names of variables which are set by the LHS and used by
     *          the RHS and are not available to nodes above this JoinRel in the
     *          tree
     * @param semiJoinDone
     *          Whether this join has been translated to a semi-join
     */
    @Override
    public RelNode createJoin(RelNode left, RelNode right, RexNode condition, JoinRelType joinType,
        Set<String> variablesStopped, boolean semiJoinDone) {
      return getJoin(left.getCluster(), left, right, condition, joinType, false);
    }
  }
}
