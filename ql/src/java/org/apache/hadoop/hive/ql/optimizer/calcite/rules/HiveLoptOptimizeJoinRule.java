/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.hadoop.hive.ql.optimizer.calcite.Bug;
import org.checkerframework.checker.nullness.qual.KeyFor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Objects.requireNonNull;

public class HiveLoptOptimizeJoinRule extends RelRule<HiveLoptOptimizeJoinRule.Config>
        implements TransformationRule {

  private static final Logger LOG = LoggerFactory.getLogger(HiveLoptOptimizeJoinRule.class);

  /** Creates an HiveLoptOptimizeJoinRule. */
  protected HiveLoptOptimizeJoinRule(HiveLoptOptimizeJoinRule.Config config) {
    super(config);

    if (Bug.CALCITE_6737_FIXED) {
      throw new IllegalStateException("This class is redundant when the fix for CALCITE-6737 is merged into Calcite. "
              + "Remove it and use LoptOptimizeJoinRule instead of HiveLoptOptimizeJoinRule");
    }
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final MultiJoin multiJoinRel = call.rel(0);
    final HiveLoptMultiJoin multiJoin = new HiveLoptMultiJoin(multiJoinRel);
    final RelMetadataQuery mq = call.getMetadataQuery();

    findRemovableOuterJoins(mq, multiJoin);

    final RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
    final HiveLoptSemiJoinOptimizer semiJoinOpt =
            new HiveLoptSemiJoinOptimizer(call.getMetadataQuery(), multiJoin, rexBuilder);

    // determine all possible semijoins
    semiJoinOpt.makePossibleSemiJoins(multiJoin);

    // select the optimal join filters for semijoin filtering by
    // iteratively calling chooseBestSemiJoin; chooseBestSemiJoin will
    // apply semijoins in sort order, based on the cost of scanning each
    // factor; as it selects semijoins to apply and iterates through the
    // loop, the cost of scanning a factor will decrease in accordance
    // with the semijoins selected
    int iterations = 0;
    do {
      if (!semiJoinOpt.chooseBestSemiJoin(multiJoin)) {
        break;
      }
      if (iterations++ > 10) {
        break;
      }
    } while (true);

    multiJoin.setFactorWeights();

    findRemovableSelfJoins(mq, multiJoin);

    findBestOrderings(mq, call.builder(), multiJoin, semiJoinOpt, call, config.broadcastThreshold());
  }

  /**
   * Locates all null generating factors whose outer join can be removed. The
   * outer join can be removed if the join keys corresponding to the null
   * generating factor are unique and no columns are projected from it.
   *
   * @param multiJoin join factors being optimized
   */
  private static void findRemovableOuterJoins(RelMetadataQuery mq, HiveLoptMultiJoin multiJoin) {
    final List<Integer> removalCandidates = new ArrayList<>();
    for (int factIdx = 0;
         factIdx < multiJoin.getNumJoinFactors();
         factIdx++) {
      if (multiJoin.isNullGenerating(factIdx)) {
        removalCandidates.add(factIdx);
      }
    }

    while (!removalCandidates.isEmpty()) {
      final Set<Integer> retryCandidates = new HashSet<>();

      outerForLoop:
      for (int factIdx : removalCandidates) {
        // reject the factor if it is referenced in the projection list
        ImmutableBitSet projFields = multiJoin.getProjFields(factIdx);
        if ((projFields == null) || (projFields.cardinality() > 0)) {
          continue;
        }

        // setup a bitmap containing the equi-join keys corresponding to
        // the null generating factor; both operands in the filter must
        // be RexInputRefs and only one side corresponds to the null
        // generating factor
        RexNode outerJoinCond = multiJoin.getOuterJoinCond(factIdx);
        final List<RexNode> ojFilters = new ArrayList<>();
        RelOptUtil.decomposeConjunction(outerJoinCond, ojFilters);
        int numFields = multiJoin.getNumFieldsInJoinFactor(factIdx);
        final ImmutableBitSet.Builder joinKeyBuilder =
                ImmutableBitSet.builder();
        final ImmutableBitSet.Builder otherJoinKeyBuilder =
                ImmutableBitSet.builder();
        int firstFieldNum = multiJoin.getJoinStart(factIdx);
        int lastFieldNum = firstFieldNum + numFields;
        for (RexNode filter : ojFilters) {
          if (!(filter instanceof RexCall)) {
            continue;
          }
          RexCall filterCall = (RexCall) filter;
          if ((filterCall.getOperator() != SqlStdOperatorTable.EQUALS)
                  || !(filterCall.getOperands().get(0)
                  instanceof RexInputRef)
                  || !(filterCall.getOperands().get(1)
                  instanceof RexInputRef)) {
            continue;
          }
          int leftRef =
                  ((RexInputRef) filterCall.getOperands().get(0)).getIndex();
          int rightRef =
                  ((RexInputRef) filterCall.getOperands().get(1)).getIndex();
          setJoinKey(
                  joinKeyBuilder,
                  otherJoinKeyBuilder,
                  leftRef,
                  rightRef,
                  firstFieldNum,
                  lastFieldNum,
                  true);
        }

        if (joinKeyBuilder.cardinality() == 0) {
          continue;
        }

        // make sure the only join fields referenced are the ones in
        // the current outer join
        final ImmutableBitSet joinKeys = joinKeyBuilder.build();
        int [] joinFieldRefCounts =
                multiJoin.getJoinFieldRefCounts(factIdx);
        for (int i = 0; i < joinFieldRefCounts.length; i++) {
          if ((joinFieldRefCounts[i] > 1)
                  || (!joinKeys.get(i) && (joinFieldRefCounts[i] == 1))) {
            continue outerForLoop;
          }
        }

        // See if the join keys are unique.  Because the keys are
        // part of an equality join condition, nulls are filtered out
        // by the join.  So, it's ok if there are nulls in the join
        // keys.
        if (RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(mq,
                multiJoin.getJoinFactor(factIdx), joinKeys)) {
          multiJoin.addRemovableOuterJoinFactor(factIdx);

          // Since we are no longer joining this factor,
          // decrement the reference counters corresponding to
          // the join keys from the other factors that join with
          // this one.  Later, in the outermost loop, we'll have
          // the opportunity to retry removing those factors.
          final ImmutableBitSet otherJoinKeys = otherJoinKeyBuilder.build();
          for (int otherKey : otherJoinKeys) {
            int otherFactor = multiJoin.findRef(otherKey);
            if (multiJoin.isNullGenerating(otherFactor)) {
              retryCandidates.add(otherFactor);
            }
            int [] otherJoinFieldRefCounts =
                    multiJoin.getJoinFieldRefCounts(otherFactor);
            int offset = multiJoin.getJoinStart(otherFactor);
            --otherJoinFieldRefCounts[otherKey - offset];
          }
        }
      }
      removalCandidates.clear();
      removalCandidates.addAll(retryCandidates);
    }
  }

  /**
   * Sets a join key if only one of the specified input references corresponds
   * to a specified factor as determined by its field numbers. Also keeps
   * track of the keys from the other factor.
   *
   * @param joinKeys join keys to be set if a key is found
   * @param otherJoinKeys join keys for the other join factor
   * @param ref1 first input reference
   * @param ref2 second input reference
   * @param firstFieldNum first field number of the factor
   * @param lastFieldNum last field number + 1 of the factor
   * @param swap if true, check for the desired input reference in the second
   * input reference parameter if the first input reference isn't the correct
   * one
   */
  private static void setJoinKey(
          ImmutableBitSet.Builder joinKeys,
          ImmutableBitSet.Builder otherJoinKeys,
          int ref1,
          int ref2,
          int firstFieldNum,
          int lastFieldNum,
          boolean swap) {
    if ((ref1 >= firstFieldNum) && (ref1 < lastFieldNum)) {
      if (!((ref2 >= firstFieldNum) && (ref2 < lastFieldNum))) {
        joinKeys.set(ref1 - firstFieldNum);
        otherJoinKeys.set(ref2);
      }
      return;
    }
    if (swap) {
      setJoinKey(
              joinKeys,
              otherJoinKeys,
              ref2,
              ref1,
              firstFieldNum,
              lastFieldNum,
              false);
    }
  }

  /**
   * Locates pairs of joins that are self-joins where the join can be removed
   * because the join condition between the two factors is an equality join on
   * unique keys.
   *
   * @param multiJoin join factors being optimized
   */
  private static void findRemovableSelfJoins(RelMetadataQuery mq, HiveLoptMultiJoin multiJoin) {
    // Candidates for self-joins must be simple factors
    Map<Integer, RelOptTable> simpleFactors = getSimpleFactors(mq, multiJoin);

    // See if a simple factor is repeated and therefore potentially is
    // part of a self-join.  Restrict each factor to at most one
    // self-join.
    final List<RelOptTable> repeatedTables = new ArrayList<>();
    final Map<Integer, Integer> selfJoinPairs = new HashMap<>();
    @KeyFor("simpleFactors") Integer [] factors =
            new TreeSet<>(simpleFactors.keySet()).toArray(new Integer[0]);
    for (int i = 0; i < factors.length; i++) {
      if (repeatedTables.contains(simpleFactors.get(factors[i]))) {
        continue;
      }
      for (int j = i + 1; j < factors.length; j++) {
        @KeyFor("simpleFactors") int leftFactor = factors[i];
        @KeyFor("simpleFactors") int rightFactor = factors[j];
        if (simpleFactors.get(leftFactor).getQualifiedName().equals(
                simpleFactors.get(rightFactor).getQualifiedName())) {
          selfJoinPairs.put(leftFactor, rightFactor);
          repeatedTables.add(simpleFactors.get(leftFactor));
          break;
        }
      }
    }

    // From the candidate self-join pairs, determine if there is
    // the appropriate join condition between the two factors that will
    // allow the join to be removed.
    for (Integer factor1 : selfJoinPairs.keySet()) {
      final int factor2 = selfJoinPairs.get(factor1);
      final List<RexNode> selfJoinFilters = new ArrayList<>();
      for (RexNode filter : multiJoin.getJoinFilters()) {
        ImmutableBitSet joinFactors =
                multiJoin.getFactorsRefByJoinFilter(filter);
        if ((joinFactors.cardinality() == 2)
                && joinFactors.get(factor1)
                && joinFactors.get(factor2)) {
          selfJoinFilters.add(filter);
        }
      }
      if ((selfJoinFilters.size() > 0)
              && isSelfJoinFilterUnique(
              mq,
              multiJoin,
              factor1,
              factor2,
              selfJoinFilters)) {
        multiJoin.addRemovableSelfJoinPair(factor1, factor2);
      }
    }
  }

  /**
   * Retrieves join factors that correspond to simple table references. A
   * simple table reference is a single table reference with no grouping or
   * aggregation.
   *
   * @param multiJoin join factors being optimized
   *
   * @return map consisting of the simple factors and the tables they
   * correspond
   */
  private static Map<Integer, RelOptTable> getSimpleFactors(RelMetadataQuery mq,
                                                            HiveLoptMultiJoin multiJoin) {
    final Map<Integer, RelOptTable> returnList = new HashMap<>();

    // Loop through all join factors and locate the ones where each
    // column referenced from the factor is not derived and originates
    // from the same underlying table.  Also, discard factors that
    // are null-generating or will be removed because of semijoins.
    if (multiJoin.getMultiJoinRel().isFullOuterJoin()) {
      return returnList;
    }
    for (int factIdx = 0; factIdx < multiJoin.getNumJoinFactors(); factIdx++) {
      if (multiJoin.isNullGenerating(factIdx)
              || (multiJoin.getJoinRemovalFactor(factIdx) != null)) {
        continue;
      }
      final RelNode rel = multiJoin.getJoinFactor(factIdx);
      final RelOptTable table = mq.getTableOrigin(rel);
      if (table != null) {
        returnList.put(factIdx, table);
      }
    }

    return returnList;
  }

  /**
   * Determines if the equality join filters between two factors that map to
   * the same table consist of unique, identical keys.
   *
   * @param multiJoin join factors being optimized
   * @param leftFactor left factor in the join
   * @param rightFactor right factor in the join
   * @param joinFilterList list of join filters between the two factors
   *
   * @return true if the criteria are met
   */
  private static boolean isSelfJoinFilterUnique(
          RelMetadataQuery mq,
          HiveLoptMultiJoin multiJoin,
          int leftFactor,
          int rightFactor,
          List<RexNode> joinFilterList) {
    RexBuilder rexBuilder =
            multiJoin.getMultiJoinRel().getCluster().getRexBuilder();
    RelNode leftRel = multiJoin.getJoinFactor(leftFactor);
    RelNode rightRel = multiJoin.getJoinFactor(rightFactor);
    RexNode joinFilters =
            RexUtil.composeConjunction(rexBuilder, joinFilterList);

    // Adjust the offsets in the filter by shifting the left factor
    // to the left and shifting the right factor to the left and then back
    // to the right by the number of fields in the left
    int [] adjustments = new int[multiJoin.getNumTotalFields()];
    int leftAdjust = multiJoin.getJoinStart(leftFactor);
    int nLeftFields = leftRel.getRowType().getFieldCount();
    for (int i = 0; i < nLeftFields; i++) {
      adjustments[leftAdjust + i] = -leftAdjust;
    }
    int rightAdjust = multiJoin.getJoinStart(rightFactor);
    for (int i = 0; i < rightRel.getRowType().getFieldCount(); i++) {
      adjustments[rightAdjust + i] = -rightAdjust + nLeftFields;
    }
    joinFilters =
            joinFilters.accept(
                    new RelOptUtil.RexInputConverter(
                            rexBuilder,
                            multiJoin.getMultiJoinFields(),
                            leftRel.getRowType().getFieldList(),
                            rightRel.getRowType().getFieldList(),
                            adjustments));

    return areSelfJoinKeysUnique(mq, leftRel, rightRel, joinFilters);
  }

  /** {@code broadcastThreshold} value that keeps the legacy max-NDV ordering generation unchanged. */
  private static final long NO_BROADCAST_PREFERENCE = -1L;

  /**
   * Generates N optimal join orderings. Each ordering contains each factor as
   * the first factor in the ordering.
   *
   * @param multiJoin join factors being optimized
   * @param semiJoinOpt optimal semijoins for each factor
   * @param call RelOptRuleCall associated with this rule
   * @param broadcastThreshold map-join build-side size budget for the data-movement-aware
   *        ordering; negative = legacy behavior (see {@link Config#withBroadcastThreshold(long)})
   */
  private static void findBestOrderings(
          RelMetadataQuery mq,
          RelBuilder relBuilder,
          HiveLoptMultiJoin multiJoin,
          HiveLoptSemiJoinOptimizer semiJoinOpt,
          RelOptRuleCall call,
          long broadcastThreshold) {
    final List<RelNode> plans = new ArrayList<>();

    final List<String> fieldNames =
            multiJoin.getMultiJoinRel().getRowType().getFieldNames();

    final boolean shuffleCostSelect = broadcastThreshold >= 0;

    // generate the N legacy join orderings (one greedy ordering per seed factor). These are also the
    // default emitted set when shuffle-cost selection is off, and the fail-safe set when it is on.
    for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
      // first factor cannot be null generating
      if (multiJoin.isNullGenerating(i)) {
        continue;
      }
      addOrderingPlan(mq, relBuilder, multiJoin, semiJoinOpt, call, fieldNames, plans, i, NO_BROADCAST_PREFERENCE);
    }

    if (shuffleCostSelect && !plans.isEmpty()) {
      // Emit only the ordering with the lowest estimated SHUFFLE cost: the HEP planner's cumulative
      // row-count cost is data-movement-blind (cpu=io=0) and cannot make this choice. The pool is the
      // N legacy orderings PLUS a broadcast-preferring ordering per seed - the legacy max-NDV
      // tie-break never yields a fact-reducing ordering, so the variant supplies it, while keeping the
      // legacy orderings in the pool guarantees the selection can never do worse than the legacy
      // orderings BY THE SHUFFLE METRIC (the row-count metric no longer decides here).
      // FAIL-SAFE: if nothing is costable (missing stats) or anything throws, fall through to emit all N.
      RelNode bestPlan = null;
      try {
        final List<RelNode> candidates = new ArrayList<>(plans);
        for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
          if (!multiJoin.isNullGenerating(i)) {
            addOrderingPlan(mq, relBuilder, multiJoin, semiJoinOpt, call, fieldNames, candidates, i, broadcastThreshold);
          }
        }
        double bestCost = Double.POSITIVE_INFINITY;
        for (RelNode plan : candidates) {
          final double cost = estimateShuffleCost(plan, mq, broadcastThreshold);
          // strict '<' is a deterministic tie-break: the first candidate at the minimum cost wins
          // (candidates are stably ordered - the N legacy orderings by seed, then the variants).
          if (!Double.isNaN(cost) && cost < bestCost) {
            bestCost = cost;
            bestPlan = plan;
          }
        }
      } catch (Exception e) {
        // Unexpected: missing statistics surface as NaN and are skipped above, so an exception here
        // is a genuine estimation defect. Warn (not debug) - the plan silently degrades to the
        // legacy orderings and that should be visible in production logs.
        LOG.warn("Shuffle-cost join-order selection failed; falling back to the legacy orderings", e);
        bestPlan = null;
      }
      if (bestPlan != null) {
        call.transformTo(bestPlan);
        return;
      }
    }

    // transform the selected plans; note that we wait till then the end to
    // transform everything so any intermediate RelNodes we create are not
    // converted to RelSubsets The HEP planner will choose the join subtree
    // with the best cumulative cost. Volcano planner keeps the alternative
    // join subtrees and cost the final plan to pick the best one.
    for (RelNode plan : plans) {
      call.transformTo(plan);
    }
  }

  /**
   * Builds one candidate join ordering for the given seed factor (optionally preferring
   * broadcastable joins first) and, if successful, adds the corresponding top-projected plan to
   * {@code plans}.
   */
  private static void addOrderingPlan(
          RelMetadataQuery mq,
          RelBuilder relBuilder,
          HiveLoptMultiJoin multiJoin,
          HiveLoptSemiJoinOptimizer semiJoinOpt,
          RelOptRuleCall call,
          List<String> fieldNames,
          List<RelNode> plans,
          int firstFactor,
          long broadcastThreshold) {
    HiveLoptJoinTree joinTree =
            createOrdering(mq, relBuilder, multiJoin, semiJoinOpt, firstFactor, broadcastThreshold);
    if (joinTree == null) {
      return;
    }
    plans.add(createTopProject(call.builder(), multiJoin, joinTree, fieldNames));
  }

  /**
   * Whether a join build side of the given estimated bytes can be broadcast (map-join), i.e. its
   * size is known and fits {@code broadcastThreshold} (hive.auto.convert.join.noconditionaltask.size).
   * This threshold predicate is shared by the order GENERATION ({@link #getBestNextFactor}, which
   * feeds it the candidate factor's bytes) and the order SELECTION ({@link #estimateShuffleCost},
   * which feeds it the build side permitted by the join type) - the sides they test differ by
   * design (generation asks "can the incoming factor be a build side", selection scores the built
   * tree). It is a deliberate, conservative APPROXIMATION of the physical map-join decision
   * made later by ConvertJoinMapJoin (which also accounts for the summed small-side budget,
   * build-side hash-table sizing and bucket joins); here it is used only to RANK logical orderings,
   * and the physical planner still makes the final conversion call.
   */
  @VisibleForTesting
  static boolean isBroadcastable(double buildBytes, long broadcastThreshold) {
    return !Double.isNaN(buildBytes) && buildBytes <= broadcastThreshold;
  }

  /**
   * Estimates the data-movement (shuffle) cost of a candidate plan: the sum, over every Join in
   * the tree, of the bytes that must cross the network. A join whose build side (as permitted by
   * the join type) is broadcastable replicates only that build side; otherwise it is a shuffle
   * (reduce-side) join and both inputs are charged (cost = left + right). Charging the
   * build side rather than 0 for a broadcast keeps broadcast much cheaper than a shuffle without
   * over-rewarding plans that stack many small broadcasts. This makes "shuffle the large fact raw
   * early" expensive relative to "reduce the fact with broadcast joins first, then shuffle the
   * small result", independent of the (data-movement-blind) cumulative row-count cost.
   */
  @VisibleForTesting
  static double estimateShuffleCost(RelNode rel, RelMetadataQuery mq, long broadcastThreshold) {
    double cost = 0.0;
    for (RelNode input : rel.getInputs()) {
      final double childCost = estimateShuffleCost(input, mq, broadcastThreshold);
      if (Double.isNaN(childCost)) {
        return Double.NaN; // uncostable subtree -> propagate; caller skips this candidate
      }
      cost += childCost;
    }
    if (rel instanceof Join join) {
      final double leftBytes = estimatedBytes(mq, join.getLeft());
      final double rightBytes = estimatedBytes(mq, join.getRight());
      if (Double.isNaN(leftBytes) || Double.isNaN(rightBytes)) {
        return Double.NaN; // unknown stats -> uncostable, fail safe (do not guess)
      }
      // Only an INNER join is modelled as a broadcast map-join, building its smaller side.
      // Outer joins are conservatively charged as full shuffles: Hive CAN map-join a LEFT/RIGHT
      // outer join (building the non-preserved side), but modelling that cheapness lets the
      // reorder move outer joins on the strength of estimates alone - runtime-validated as
      // harmful (an estimate-better outer-join placement in TPC-DS query72 ran 14x slower).
      final double buildBytes = join.getJoinType() == JoinRelType.INNER
              ? Math.min(leftBytes, rightBytes)
              : Double.NaN; // never broadcastable (isBroadcastable is NaN-pessimistic)
      // Model as a broadcast (map-)join only what can HASH-join: eligibility additionally
      // requires a usable key (see hasCrossSideEquality). A keyless join (cross product / pure
      // theta) is charged conservatively as a full shuffle of both inputs - its |L|x|R|-shaped
      // output must not look cheaper than hash-joinable alternatives.
      final boolean broadcast = isBroadcastable(buildBytes, broadcastThreshold)
              && hasCrossSideEquality(join);
      // broadcast map-join replicates only the build side; a shuffle (reduce-side) join sends
      // both inputs across the network.
      cost += broadcast ? buildBytes : (leftBytes + rightBytes);
    }
    return cost;
  }

  /**
   * Whether the join condition contains at least one equality conjunct with one operand computed
   * purely from the left input's fields and the other purely from the right input's fields - i.e.
   * the join is hash-joinable on some (possibly computed) key, so its output cardinality is
   * bounded by the key match rate rather than |L|x|R|. Unlike {@code Join#analyzeCondition()},
   * which only recognizes bare column equalities, this also accepts expression keys such as
   * {@code l.a + l.b = r.c}. {@code HiveCalciteUtil.JoinPredicateInfo} is deliberately NOT
   * reused here: it classifies IS NOT DISTINCT FROM as non-equi (so null-safe equi joins would
   * be mistaken for cross products), throws checked exceptions, and allocates full key
   * structures - too heavy and wrong-shaped for a boolean probe on the selection hot path.
   */
  @VisibleForTesting
  static boolean hasCrossSideEquality(Join join) {
    final int nLeftFields = join.getLeft().getRowType().getFieldCount();
    final int nTotalFields = nLeftFields + join.getRight().getRowType().getFieldCount();
    final ImmutableBitSet leftFields = ImmutableBitSet.range(0, nLeftFields);
    final ImmutableBitSet rightFields = ImmutableBitSet.range(nLeftFields, nTotalFields);
    for (RexNode conjunct : RelOptUtil.conjunctions(join.getCondition())) {
      if (!(conjunct instanceof RexCall equality)
              || (equality.getKind() != SqlKind.EQUALS && equality.getKind() != SqlKind.IS_NOT_DISTINCT_FROM)) {
        continue;
      }
      final ImmutableBitSet op0 = RelOptUtil.InputFinder.bits(equality.getOperands().get(0));
      final ImmutableBitSet op1 = RelOptUtil.InputFinder.bits(equality.getOperands().get(1));
      if (op0.isEmpty() || op1.isEmpty()) {
        continue; // an equality against a constant is a filter, not a join key
      }
      if ((leftFields.contains(op0) && rightFields.contains(op1))
              || (leftFields.contains(op1) && rightFields.contains(op0))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Estimated bytes of a relation (rowCount * average row size), or NaN when the metadata is
   * unavailable - callers treat NaN as "uncostable" and fail safe rather than guess.
   */
  @VisibleForTesting
  static double estimatedBytes(RelMetadataQuery mq, RelNode rel) {
    final Double rows = mq.getRowCount(rel);
    final Double rowSize = mq.getAverageRowSize(rel);
    if (rows == null || rowSize == null) {
      return Double.NaN;
    }
    return rows * rowSize;
  }

  /**
   * Creates the topmost projection that will sit on top of the selected join
   * ordering. The projection needs to match the original join ordering. Also,
   * places any post-join filters on top of the project.
   *
   * @param multiJoin join factors being optimized
   * @param joinTree selected join ordering
   * @param fieldNames field names corresponding to the projection expressions
   *
   * @return created projection
   */
  private static RelNode createTopProject(
          RelBuilder relBuilder,
          HiveLoptMultiJoin multiJoin,
          HiveLoptJoinTree joinTree,
          List<String> fieldNames) {
    List<RexNode> newProjExprs = new ArrayList<>();
    RexBuilder rexBuilder =
            multiJoin.getMultiJoinRel().getCluster().getRexBuilder();

    // create a projection on top of the joins, matching the original
    // join order
    final List<Integer> newJoinOrder = joinTree.getTreeOrder();
    int nJoinFactors = multiJoin.getNumJoinFactors();
    List<RelDataTypeField> fields = multiJoin.getMultiJoinFields();

    // create a mapping from each factor to its field offset in the join
    // ordering
    final Map<Integer, Integer> factorToOffsetMap = new HashMap<>();
    for (int pos = 0, fieldStart = 0; pos < nJoinFactors; pos++) {
      factorToOffsetMap.put(newJoinOrder.get(pos), fieldStart);
      fieldStart +=
              multiJoin.getNumFieldsInJoinFactor(newJoinOrder.get(pos));
    }

    for (int currFactor = 0; currFactor < nJoinFactors; currFactor++) {
      // if the factor is the right factor in a removable self-join,
      // then where possible, remap references to the right factor to
      // the corresponding reference in the left factor
      Integer leftFactor = null;
      if (multiJoin.isRightFactorInRemovableSelfJoin(currFactor)) {
        leftFactor = multiJoin.getOtherSelfJoinFactor(currFactor);
      }
      for (int fieldPos = 0;
           fieldPos < multiJoin.getNumFieldsInJoinFactor(currFactor);
           fieldPos++) {
        int newOffset = requireNonNull(factorToOffsetMap.get(currFactor),
                () -> "factorToOffsetMap.get(currFactor)") + fieldPos;
        if (leftFactor != null) {
          Integer leftOffset =
                  multiJoin.getRightColumnMapping(currFactor, fieldPos);
          if (leftOffset != null) {
            newOffset =
                    requireNonNull(factorToOffsetMap.get(leftFactor),
                            "factorToOffsetMap.get(leftFactor)") + leftOffset;
          }
        }
        newProjExprs.add(
                rexBuilder.makeInputRef(
                        fields.get(newProjExprs.size()).getType(),
                        newOffset));
      }
    }

    relBuilder.push(joinTree.getJoinTree());
    relBuilder.project(newProjExprs, fieldNames);

    // Place the post-join filter (if it exists) on top of the final
    // projection.
    RexNode postJoinFilter =
            multiJoin.getMultiJoinRel().getPostJoinFilter();
    if (postJoinFilter != null) {
      relBuilder.filter(postJoinFilter);
    }
    return relBuilder.build();
  }

  /**
   * Computes the cardinality of the join columns from a particular factor,
   * when that factor is joined with another join tree.
   *
   * @param multiJoin join factors being optimized
   * @param semiJoinOpt optimal semijoins chosen for each factor
   * @param joinTree the join tree that the factor is being joined with
   * @param filters possible join filters to select from
   * @param factor the factor being added
   *
   * @return computed cardinality
   */
  private static @Nullable Double computeJoinCardinality(
          RelMetadataQuery mq,
          HiveLoptMultiJoin multiJoin,
          HiveLoptSemiJoinOptimizer semiJoinOpt,
          HiveLoptJoinTree joinTree,
          List<RexNode> filters,
          int factor) {
    final ImmutableBitSet childFactors =
            ImmutableBitSet.builder()
                    .addAll(joinTree.getTreeOrder())
                    .set(factor)
                    .build();

    int factorStart = multiJoin.getJoinStart(factor);
    int nFields = multiJoin.getNumFieldsInJoinFactor(factor);
    final ImmutableBitSet.Builder joinKeys = ImmutableBitSet.builder();

    // first loop through the inner join filters, picking out the ones
    // that reference only the factors in either the join tree or the
    // factor that will be added
    setFactorJoinKeys(
            multiJoin,
            filters,
            childFactors,
            factorStart,
            nFields,
            joinKeys);

    // then loop through the outer join filters where the factor being
    // added is the null generating factor in the outer join
    setFactorJoinKeys(
            multiJoin,
            RelOptUtil.conjunctions(multiJoin.getOuterJoinCond(factor)),
            childFactors,
            factorStart,
            nFields,
            joinKeys);

    // if the join tree doesn't contain all the necessary factors in
    // any of the join filters, then joinKeys will be empty, so return
    // null in that case
    if (joinKeys.isEmpty()) {
      return null;
    } else {
      return mq.getDistinctRowCount(semiJoinOpt.getChosenSemiJoin(factor),
              joinKeys.build(), null);
    }
  }

  /**
   * Locates from a list of filters those that correspond to a particular join
   * tree. Then, for each of those filters, extracts the fields corresponding
   * to a particular factor, setting them in a bitmap.
   *
   * @param multiJoin join factors being optimized
   * @param filters list of join filters
   * @param joinFactors bitmap containing the factors in a particular join
   * tree
   * @param factorStart the initial offset of the factor whose join keys will
   * be extracted
   * @param nFields the number of fields in the factor whose join keys will be
   * extracted
   * @param joinKeys the bitmap that will be set with the join keys
   */
  private static void setFactorJoinKeys(
          HiveLoptMultiJoin multiJoin,
          List<RexNode> filters,
          ImmutableBitSet joinFactors,
          int factorStart,
          int nFields,
          ImmutableBitSet.Builder joinKeys) {
    for (RexNode joinFilter : filters) {
      ImmutableBitSet filterFactors =
              multiJoin.getFactorsRefByJoinFilter(joinFilter);

      // if all factors in the join filter are in the bitmap containing
      // the factors in a join tree, then from that filter, add the
      // fields corresponding to the specified factor to the join key
      // bitmap; in doing so, adjust the join keys so they start at
      // offset 0
      if (joinFactors.contains(filterFactors)) {
        ImmutableBitSet joinFields =
                multiJoin.getFieldsRefByJoinFilter(joinFilter);
        for (int field = joinFields.nextSetBit(factorStart);
             (field >= 0)
                     && (field < (factorStart + nFields));
             field = joinFields.nextSetBit(field + 1)) {
          joinKeys.set(field - factorStart);
        }
      }
    }
  }

  /**
   * Generates a join tree with a specific factor as the first factor in the
   * join tree.
   *
   * @param multiJoin join factors being optimized
   * @param semiJoinOpt optimal semijoins for each factor
   * @param firstFactor first factor in the tree
   *
   * @return constructed join tree or null if it is not possible for
   * firstFactor to appear as the first factor in the join
   */
  private static @Nullable HiveLoptJoinTree createOrdering(
          RelMetadataQuery mq,
          RelBuilder relBuilder,
          HiveLoptMultiJoin multiJoin,
          HiveLoptSemiJoinOptimizer semiJoinOpt,
          int firstFactor,
          long broadcastThreshold) {
    HiveLoptJoinTree joinTree = null;
    final int nJoinFactors = multiJoin.getNumJoinFactors();
    final BitSet factorsToAdd = BitSets.range(0, nJoinFactors);
    final BitSet factorsAdded = new BitSet(nJoinFactors);
    final List<RexNode> filtersToAdd =
            new ArrayList<>(multiJoin.getJoinFilters());

    int prevFactor = -1;
    while (factorsToAdd.cardinality() > 0) {
      int nextFactor;
      boolean selfJoin = false;
      if (factorsAdded.cardinality() == 0) {
        nextFactor = firstFactor;
      } else {
        // If the factor just added is part of a removable self-join
        // and the other half of the self-join hasn't been added yet,
        // then add it next.  Otherwise, look for the optimal factor
        // to add next.
        Integer selfJoinFactor =
                multiJoin.getOtherSelfJoinFactor(prevFactor);
        if ((selfJoinFactor != null)
                && !factorsAdded.get(selfJoinFactor)) {
          nextFactor = selfJoinFactor;
          selfJoin = true;
        } else {
          nextFactor =
                  getBestNextFactor(
                          mq,
                          multiJoin,
                          factorsToAdd,
                          factorsAdded,
                          semiJoinOpt,
                          joinTree,
                          filtersToAdd,
                          broadcastThreshold);
        }
      }

      // add the factor; pass in a bitmap representing the factors
      // this factor joins with that have already been added to
      // the tree
      BitSet factorsNeeded =
              multiJoin.getFactorsRefByFactor(nextFactor).toBitSet();
      if (multiJoin.isNullGenerating(nextFactor)) {
        factorsNeeded.or(multiJoin.getOuterJoinFactors(nextFactor).toBitSet());
      }
      factorsNeeded.and(factorsAdded);
      joinTree =
              addFactorToTree(
                      mq,
                      relBuilder,
                      multiJoin,
                      semiJoinOpt,
                      joinTree,
                      nextFactor,
                      factorsNeeded,
                      filtersToAdd,
                      selfJoin);
      if (joinTree == null) {
        return null;
      }
      factorsToAdd.clear(nextFactor);
      factorsAdded.set(nextFactor);
      prevFactor = nextFactor;
    }

    assert filtersToAdd.size() == 0;
    return joinTree;
  }

  /**
   * Determines the best factor to be added next into a join tree.
   *
   * @param multiJoin join factors being optimized
   * @param factorsToAdd factors to choose from to add next
   * @param factorsAdded factors that have already been added to the join tree
   * @param semiJoinOpt optimal semijoins for each factor
   * @param joinTree join tree constructed thus far
   * @param filtersToAdd remaining filters that need to be added
   *
   * @return index of the best factor to add next
   */
  private static int getBestNextFactor(
          RelMetadataQuery mq,
          HiveLoptMultiJoin multiJoin,
          BitSet factorsToAdd,
          BitSet factorsAdded,
          HiveLoptSemiJoinOptimizer semiJoinOpt,
          @Nullable HiveLoptJoinTree joinTree,
          List<RexNode> filtersToAdd,
          long broadcastThreshold) {
    // A non-negative threshold turns on the data-movement-aware tie-break: on a weight tie, prefer
    // a factor whose build side fits the broadcast (map-join) threshold over a non-broadcastable
    // one, so the selective fact-reducing broadcast joins are sequenced before a non-broadcastable
    // shuffle join. This produces a fact-reducing ordering the legacy max-NDV tie-break never would;
    // it only widens the candidate pool - findBestOrderings still selects the final ordering by
    // estimated shuffle cost. A negative threshold leaves the legacy max-NDV tie-break unchanged.
    final boolean preferBroadcast = broadcastThreshold >= 0;
    // iterate through the remaining factors and determine the
    // best one to add next
    int nextFactor = -1;
    int bestWeight = 0;
    Double bestCardinality = null;
    // Initialized to the non-broadcastable sentinel so it can never spuriously beat a real candidate
    // (the first joinable factor wins via dimWeight > bestWeight and resets it regardless).
    boolean bestBroadcastable = false;
    int [][] factorWeights = multiJoin.getFactorWeights();
    for (int factor : BitSets.toIter(factorsToAdd)) {
      // if the factor corresponds to a dimension table whose
      // join we can remove, make sure the corresponding fact
      // table is in the current join tree
      Integer factIdx = multiJoin.getJoinRemovalFactor(factor);
      if (factIdx != null) {
        if (!factorsAdded.get(factIdx)) {
          continue;
        }
      }

      // can't add a null-generating factor if its dependent,
      // non-null generating factors haven't been added yet
      if (multiJoin.isNullGenerating(factor)
              && !BitSets.contains(factorsAdded,
              multiJoin.getOuterJoinFactors(factor))) {
        continue;
      }

      // determine the best weight between the current factor
      // under consideration and the factors that have already
      // been added to the tree
      int dimWeight = 0;
      for (int prevFactor : BitSets.toIter(factorsAdded)) {
        int[] factorWeight = requireNonNull(factorWeights, "factorWeights")[prevFactor];
        if (factorWeight[factor] > dimWeight) {
          dimWeight = factorWeight[factor];
        }
      }

      // only compute the join cardinality if we know that
      // this factor joins with some part of the current join
      // tree and is potentially better than other factors
      // already considered
      Double cardinality = null;
      if ((dimWeight > 0)
              && ((dimWeight > bestWeight) || (dimWeight == bestWeight))) {
        cardinality =
                computeJoinCardinality(
                        mq,
                        multiJoin,
                        semiJoinOpt,
                        requireNonNull(joinTree, "joinTree"),
                        filtersToAdd,
                        factor);
      }

      // whether joining this factor would be a broadcast (map-join): its build side fits the
      // noconditionaltask threshold. Only computed for candidates that can still win
      // (dimWeight >= bestWeight, mirroring the cardinality guard above) - lower-weight
      // candidates lose on weight before the broadcast tie-break is consulted.
      boolean broadcastable = true;
      if (preferBroadcast && (dimWeight > 0) && (dimWeight >= bestWeight)) {
        // unknown size -> treat as non-broadcastable (pessimistic), consistent with the
        // estimatedBytes/estimateShuffleCost selection below.
        final double factorBytes = estimatedBytes(mq, semiJoinOpt.getChosenSemiJoin(factor));
        broadcastable = isBroadcastable(factorBytes, broadcastThreshold);
      }

      // higher weight always wins. On a weight tie: when preferBroadcast is on, a broadcastable
      // candidate beats a non-broadcastable one (so the non-broadcastable shuffle join floats to
      // the end of the build, after the fact has been reduced); among equally-broadcastable
      // candidates fall back to the legacy max-NDV tiebreak.
      boolean newBest;
      if (dimWeight > bestWeight) {
        newBest = true;
      } else if (dimWeight < bestWeight) {
        newBest = false;
      } else if (preferBroadcast && (bestBroadcastable != broadcastable)) {
        newBest = broadcastable;
      } else {
        newBest = (bestCardinality == null)
                || ((cardinality != null) && (cardinality > bestCardinality));
      }

      if (newBest) {
        nextFactor = factor;
        bestWeight = dimWeight;
        bestCardinality = cardinality;
        bestBroadcastable = broadcastable;
      }
    }

    return nextFactor;
  }

  /**
   * Returns whether a RelNode corresponds to a Join that wasn't one of the
   * original MultiJoin input factors.
   */
  private static boolean isJoinTree(RelNode rel) {
    // full outer joins were already optimized in a prior instantiation
    // of this rule; therefore we should never see a join input that's
    // a full outer join
    if (rel instanceof Join) {
      assert ((Join) rel).getJoinType() != JoinRelType.FULL;
      return true;
    } else {
      return false;
    }
  }

  /**
   * Adds a new factor into the current join tree. The factor is either pushed
   * down into one of the subtrees of the join recursively, or it is added to
   * the top of the current tree, whichever yields a better ordering.
   *
   * @param multiJoin join factors being optimized
   * @param semiJoinOpt optimal semijoins for each factor
   * @param joinTree current join tree
   * @param factorToAdd new factor to be added
   * @param factorsNeeded factors that must precede the factor to be added
   * @param filtersToAdd filters remaining to be added; filters added to the
   * new join tree are removed from the list
   * @param selfJoin true if the join being created is a self-join that's
   * removable
   *
   * @return optimal join tree with the new factor added if it is possible to
   * add the factor; otherwise, null is returned
   */
  private static @Nullable HiveLoptJoinTree addFactorToTree(
          RelMetadataQuery mq,
          RelBuilder relBuilder,
          HiveLoptMultiJoin multiJoin,
          HiveLoptSemiJoinOptimizer semiJoinOpt,
          @Nullable HiveLoptJoinTree joinTree,
          int factorToAdd,
          BitSet factorsNeeded,
          List<RexNode> filtersToAdd,
          boolean selfJoin) {

    // if the factor corresponds to the null generating factor in an outer
    // join that can be removed, then create a replacement join
    if (multiJoin.isRemovableOuterJoinFactor(factorToAdd)) {
      return createReplacementJoin(
              relBuilder,
              multiJoin,
              semiJoinOpt,
              requireNonNull(joinTree, "joinTree"),
              -1,
              factorToAdd,
              ImmutableIntList.of(),
              null,
              filtersToAdd);
    }

    // if the factor corresponds to a dimension table whose join we
    // can remove, create a replacement join if the corresponding fact
    // table is in the current join tree
    if (multiJoin.getJoinRemovalFactor(factorToAdd) != null) {
      return createReplacementSemiJoin(
              relBuilder,
              multiJoin,
              semiJoinOpt,
              joinTree,
              factorToAdd,
              filtersToAdd);
    }

    // if this is the first factor in the tree, create a join tree with
    // the single factor
    if (joinTree == null) {
      return new HiveLoptJoinTree(
              semiJoinOpt.getChosenSemiJoin(factorToAdd),
              factorToAdd);
    }

    // Create a temporary copy of the filter list as we may need the
    // original list to pass into addToTop().  However, if no tree was
    // created by addToTop() because the factor being added is part of
    // a self-join, then pass the original filter list so the added
    // filters will still be removed from the list.
    final List<RexNode> tmpFilters = new ArrayList<>(filtersToAdd);
    HiveLoptJoinTree topTree =
            addToTop(
                    mq,
                    relBuilder,
                    multiJoin,
                    semiJoinOpt,
                    joinTree,
                    factorToAdd,
                    filtersToAdd,
                    selfJoin);
    HiveLoptJoinTree pushDownTree =
            pushDownFactor(
                    mq,
                    relBuilder,
                    multiJoin,
                    semiJoinOpt,
                    joinTree,
                    factorToAdd,
                    factorsNeeded,
                    (topTree == null) ? filtersToAdd : tmpFilters,
                    selfJoin);

    // pick the lower cost option, and replace the join ordering with
    // the ordering associated with the best option
    HiveLoptJoinTree bestTree;
    RelOptCost costPushDown = null;
    RelOptCost costTop = null;
    if (pushDownTree != null) {
      costPushDown = mq.getCumulativeCost(pushDownTree.getJoinTree());
    }
    if (topTree != null) {
      costTop = mq.getCumulativeCost(topTree.getJoinTree());
    }

    if (pushDownTree == null) {
      bestTree = topTree;
    } else if (topTree == null) {
      bestTree = pushDownTree;
    } else {
      requireNonNull(costPushDown, "costPushDown");
      requireNonNull(costTop, "costTop");
      if (costPushDown.isEqWithEpsilon(costTop)) {
        // if both plans cost the same (with an allowable round-off
        // margin of error), favor the one that passes
        // around the wider rows further up in the tree
        if (rowWidthCost(pushDownTree.getJoinTree())
                < rowWidthCost(topTree.getJoinTree())) {
          bestTree = pushDownTree;
        } else {
          bestTree = topTree;
        }
      } else if (costPushDown.isLt(costTop)) {
        bestTree = pushDownTree;
      } else {
        bestTree = topTree;
      }
    }

    return bestTree;
  }

  /**
   * Computes a cost for a join tree based on the row widths of the inputs
   * into the join. Joins where the inputs have the fewest number of columns
   * lower in the tree are better than equivalent joins where the inputs with
   * the larger number of columns are lower in the tree.
   *
   * @param tree a tree of RelNodes
   *
   * @return the cost associated with the width of the tree
   */
  private static int rowWidthCost(RelNode tree) {
    // The width cost is the width of the tree itself plus the widths
    // of its children.  Hence, skinnier rows are better when they're
    // lower in the tree since the width of a RelNode contributes to
    // the cost of each LogicalJoin that appears above that RelNode.
    int width = tree.getRowType().getFieldCount();
    if (isJoinTree(tree)) {
      Join joinRel = (Join) tree;
      width +=
              rowWidthCost(joinRel.getLeft())
                      + rowWidthCost(joinRel.getRight());
    }
    return width;
  }

  /**
   * Creates a join tree where the new factor is pushed down one of the
   * operands of the current join tree.
   *
   * @param multiJoin join factors being optimized
   * @param semiJoinOpt optimal semijoins for each factor
   * @param joinTree current join tree
   * @param factorToAdd new factor to be added
   * @param factorsNeeded factors that must precede the factor to be added
   * @param filtersToAdd filters remaining to be added; filters that are added
   * to the join tree are removed from the list
   * @param selfJoin true if the factor being added is part of a removable
   * self-join
   *
   * @return optimal join tree with the new factor pushed down the current
   * join tree if it is possible to do the pushdown; otherwise, null is
   * returned
   */
  private static @Nullable HiveLoptJoinTree pushDownFactor(
          RelMetadataQuery mq,
          RelBuilder relBuilder,
          HiveLoptMultiJoin multiJoin,
          HiveLoptSemiJoinOptimizer semiJoinOpt,
          HiveLoptJoinTree joinTree,
          int factorToAdd,
          BitSet factorsNeeded,
          List<RexNode> filtersToAdd,
          boolean selfJoin) {
    // pushdown option only works if we already have a join tree
    if (!isJoinTree(joinTree.getJoinTree())) {
      return null;
    }
    int childNo = -1;
    HiveLoptJoinTree left = joinTree.getLeft();
    HiveLoptJoinTree right = joinTree.getRight();
    Join joinRel = (Join) joinTree.getJoinTree();
    JoinRelType joinType = joinRel.getJoinType();

    // can't push factors pass self-joins because in order to later remove
    // them, we need to keep the factors together
    if (joinTree.isRemovableSelfJoin()) {
      return null;
    }

    // If there are no constraints as to which side the factor must
    // be pushed, arbitrarily push to the left.  In the case of a
    // self-join, always push to the input that contains the other
    // half of the self-join.
    if (selfJoin) {
      BitSet selfJoinFactor = new BitSet(multiJoin.getNumJoinFactors());
      Integer factor = requireNonNull(multiJoin.getOtherSelfJoinFactor(factorToAdd),
              () -> "multiJoin.getOtherSelfJoinFactor(" + factorToAdd + ") is null");
      selfJoinFactor.set(factor);
      if (multiJoin.hasAllFactors(left, selfJoinFactor)) {
        childNo = 0;
      } else {
        assert multiJoin.hasAllFactors(right, selfJoinFactor);
        childNo = 1;
      }
    } else if (
            (factorsNeeded.cardinality() == 0)
                    && !joinType.generatesNullsOnLeft()) {
      childNo = 0;
    } else {
      // push to the left if the LHS contains all factors that the
      // current factor needs and that side is not null-generating;
      // same check for RHS
      if (multiJoin.hasAllFactors(left, factorsNeeded)
              && !joinType.generatesNullsOnLeft()) {
        childNo = 0;
      } else if (
              multiJoin.hasAllFactors(right, factorsNeeded)
                      && !joinType.generatesNullsOnRight()) {
        childNo = 1;
      }
      // if it couldn't be pushed down to either side, then it can
      // only be put on top
    }
    if (childNo == -1) {
      return null;
    }

    // remember the original join order before the pushdown so we can
    // appropriately adjust any filters already attached to the join
    // node
    final List<Integer> origJoinOrder = joinTree.getTreeOrder();

    // recursively pushdown the factor
    HiveLoptJoinTree subTree = (childNo == 0) ? left : right;
    subTree =
            addFactorToTree(
                    mq,
                    relBuilder,
                    multiJoin,
                    semiJoinOpt,
                    subTree,
                    factorToAdd,
                    factorsNeeded,
                    filtersToAdd,
                    selfJoin);

    if (childNo == 0) {
      left = subTree;
    } else {
      right = subTree;
    }

    // adjust the join condition from the original join tree to reflect
    // pushdown of the new factor as well as any swapping that may have
    // been done during the pushdown
    RexNode newCondition =
            ((Join) joinTree.getJoinTree()).getCondition();
    newCondition =
            adjustFilter(
                    multiJoin,
                    requireNonNull(left, "left"),
                    requireNonNull(right, "right"),
                    newCondition,
                    factorToAdd,
                    origJoinOrder,
                    joinTree.getJoinTree().getRowType().getFieldList());

    // determine if additional filters apply as a result of adding the
    // new factor, provided this isn't a left or right outer join; for
    // those cases, the additional filters will be added on top of the
    // join in createJoinSubtree
    if ((joinType != JoinRelType.LEFT) && (joinType != JoinRelType.RIGHT)) {
      RexNode condition =
              addFilters(
                      multiJoin,
                      left,
                      -1,
                      right,
                      filtersToAdd,
                      true);
      RexBuilder rexBuilder =
              multiJoin.getMultiJoinRel().getCluster().getRexBuilder();
      newCondition =
              RelOptUtil.andJoinFilters(
                      rexBuilder,
                      newCondition,
                      condition);
    }

    // create the new join tree with the factor pushed down
    return createJoinSubtree(
            mq,
            relBuilder,
            multiJoin,
            left,
            right,
            newCondition,
            joinType,
            filtersToAdd,
            false,
            false);
  }

  /**
   * Creates a join tree with the new factor added to the top of the tree.
   *
   * @param multiJoin join factors being optimized
   * @param semiJoinOpt optimal semijoins for each factor
   * @param joinTree current join tree
   * @param factorToAdd new factor to be added
   * @param filtersToAdd filters remaining to be added; modifies the list to
   * remove filters that can be added to the join tree
   * @param selfJoin true if the join being created is a self-join that's
   * removable
   *
   * @return new join tree
   */
  private static @Nullable HiveLoptJoinTree addToTop(
          RelMetadataQuery mq,
          RelBuilder relBuilder,
          HiveLoptMultiJoin multiJoin,
          HiveLoptSemiJoinOptimizer semiJoinOpt,
          HiveLoptJoinTree joinTree,
          int factorToAdd,
          List<RexNode> filtersToAdd,
          boolean selfJoin) {
    // self-joins can never be created at the top of an existing
    // join tree because it needs to be paired directly with the
    // other self-join factor
    if (selfJoin && isJoinTree(joinTree.getJoinTree())) {
      return null;
    }

    // if the factor being added is null-generating, create the join
    // as a left outer join since it's being added to the RHS side of
    // the join; createJoinSubTree may swap the inputs and therefore
    // convert the left outer join to a right outer join; if the original
    // MultiJoin was a full outer join, these should be the only
    // factors in the join, so create the join as a full outer join
    JoinRelType joinType;
    if (multiJoin.getMultiJoinRel().isFullOuterJoin()) {
      assert multiJoin.getNumJoinFactors() == 2;
      joinType = JoinRelType.FULL;
    } else if (multiJoin.isNullGenerating(factorToAdd)) {
      joinType = JoinRelType.LEFT;
    } else {
      joinType = JoinRelType.INNER;
    }

    HiveLoptJoinTree rightTree =
            new HiveLoptJoinTree(
                    semiJoinOpt.getChosenSemiJoin(factorToAdd),
                    factorToAdd);

    // in the case of a left or right outer join, use the specific
    // outer join condition
    RexNode condition;
    if ((joinType == JoinRelType.LEFT) || (joinType == JoinRelType.RIGHT)) {
      condition = requireNonNull(multiJoin.getOuterJoinCond(factorToAdd),
              "multiJoin.getOuterJoinCond(factorToAdd)");
    } else {
      condition =
              addFilters(
                      multiJoin,
                      joinTree,
                      -1,
                      rightTree,
                      filtersToAdd,
                      false);
    }

    return createJoinSubtree(
            mq,
            relBuilder,
            multiJoin,
            joinTree,
            rightTree,
            condition,
            joinType,
            filtersToAdd,
            true,
            selfJoin);
  }

  /**
   * Determines which join filters can be added to the current join tree. Note
   * that the join filter still reflects the original join ordering. It will
   * only be adjusted to reflect the new join ordering if the "adjust"
   * parameter is set to true.
   *
   * @param multiJoin join factors being optimized
   * @param leftTree left subtree of the join tree
   * @param leftIdx if &ge; 0, only consider filters that reference leftIdx in
   * leftTree; otherwise, consider all filters that reference any factor in
   * leftTree
   * @param rightTree right subtree of the join tree
   * @param filtersToAdd remaining join filters that need to be added; those
   * that are added are removed from the list
   * @param adjust if true, adjust filter to reflect new join ordering
   *
   * @return AND'd expression of the join filters that can be added to the
   * current join tree
   */
  private static RexNode addFilters(
          HiveLoptMultiJoin multiJoin,
          HiveLoptJoinTree leftTree,
          int leftIdx,
          HiveLoptJoinTree rightTree,
          List<RexNode> filtersToAdd,
          boolean adjust) {
    // loop through the remaining filters to be added and pick out the
    // ones that reference only the factors in the new join tree
    final RexBuilder rexBuilder =
            multiJoin.getMultiJoinRel().getCluster().getRexBuilder();
    final ImmutableBitSet.Builder childFactorBuilder =
            ImmutableBitSet.builder();
    childFactorBuilder.addAll(rightTree.getTreeOrder());
    if (leftIdx >= 0) {
      childFactorBuilder.set(leftIdx);
    } else {
      childFactorBuilder.addAll(leftTree.getTreeOrder());
    }
    for (int child : rightTree.getTreeOrder()) {
      childFactorBuilder.set(child);
    }

    final ImmutableBitSet childFactor = childFactorBuilder.build();
    RexNode condition = null;
    final ListIterator<RexNode> filterIter = filtersToAdd.listIterator();
    while (filterIter.hasNext()) {
      RexNode joinFilter = filterIter.next();
      ImmutableBitSet filterBitmap =
              multiJoin.getFactorsRefByJoinFilter(joinFilter);

      // if all factors in the join filter are in the join tree,
      // AND the filter to the current join condition
      if (childFactor.contains(filterBitmap)) {
        if (condition == null) {
          condition = joinFilter;
        } else {
          condition =
                  rexBuilder.makeCall(
                          SqlStdOperatorTable.AND,
                          condition,
                          joinFilter);
        }
        filterIter.remove();
      }
    }

    if (adjust && (condition != null)) {
      int [] adjustments = new int[multiJoin.getNumTotalFields()];
      if (needsAdjustment(
              multiJoin,
              adjustments,
              leftTree,
              rightTree,
              false)) {
        condition =
                condition.accept(
                        new RelOptUtil.RexInputConverter(
                                rexBuilder,
                                multiJoin.getMultiJoinFields(),
                                leftTree.getJoinTree().getRowType().getFieldList(),
                                rightTree.getJoinTree().getRowType().getFieldList(),
                                adjustments));
      }
    }

    if (condition == null) {
      condition = rexBuilder.makeLiteral(true);
    }

    return condition;
  }

  /**
   * Adjusts a filter to reflect a newly added factor in the middle of an
   * existing join tree.
   *
   * @param multiJoin join factors being optimized
   * @param left left subtree of the join
   * @param right right subtree of the join
   * @param condition current join condition
   * @param factorAdded index corresponding to the newly added factor
   * @param origJoinOrder original join order, before factor was pushed into
   * the tree
   * @param origFields fields from the original join before the factor was
   * added
   *
   * @return modified join condition reflecting addition of the new factor
   */
  private static RexNode adjustFilter(
          HiveLoptMultiJoin multiJoin,
          HiveLoptJoinTree left,
          HiveLoptJoinTree right,
          RexNode condition,
          int factorAdded,
          List<Integer> origJoinOrder,
          List<RelDataTypeField> origFields) {
    final List<Integer> newJoinOrder = new ArrayList<>();
    left.getTreeOrder(newJoinOrder);
    right.getTreeOrder(newJoinOrder);

    int totalFields =
            left.getJoinTree().getRowType().getFieldCount()
                    + right.getJoinTree().getRowType().getFieldCount()
                    - multiJoin.getNumFieldsInJoinFactor(factorAdded);
    int [] adjustments = new int[totalFields];

    // go through each factor and adjust relative to the original
    // join order
    boolean needAdjust = false;
    int nFieldsNew = 0;
    for (int newPos = 0; newPos < newJoinOrder.size(); newPos++) {
      int nFieldsOld = 0;

      // no need to make any adjustments on the newly added factor
      int factor = newJoinOrder.get(newPos);
      if (factor != factorAdded) {
        // locate the position of the factor in the original join
        // ordering
        for (int pos : origJoinOrder) {
          if (factor == pos) {
            break;
          }
          nFieldsOld += multiJoin.getNumFieldsInJoinFactor(pos);
        }

        // fill in the adjustment array for this factor
        if (remapJoinReferences(
                multiJoin,
                factor,
                newJoinOrder,
                newPos,
                adjustments,
                nFieldsOld,
                nFieldsNew,
                false)) {
          needAdjust = true;
        }
      }
      nFieldsNew += multiJoin.getNumFieldsInJoinFactor(factor);
    }

    if (needAdjust) {
      RexBuilder rexBuilder =
              multiJoin.getMultiJoinRel().getCluster().getRexBuilder();
      condition =
              condition.accept(
                      new RelOptUtil.RexInputConverter(
                              rexBuilder,
                              origFields,
                              left.getJoinTree().getRowType().getFieldList(),
                              right.getJoinTree().getRowType().getFieldList(),
                              adjustments));
    }

    return condition;
  }

  /**
   * Sets an adjustment array based on where column references for a
   * particular factor end up as a result of a new join ordering.
   *
   * <p>If the factor is not the right factor in a removable self-join, then
   * it needs to be adjusted as follows:
   *
   * <ul>
   * <li>First subtract, based on where the factor was in the original join
   * ordering.
   * <li>Then add on the number of fields in the factors that now precede this
   * factor in the new join ordering.
   * </ul>
   *
   * <p>If the factor is the right factor in a removable self-join and its
   * column reference can be mapped to the left factor in the self-join, then:
   *
   * <ul>
   * <li>First subtract, based on where the column reference is in the new
   * join ordering.
   * <li>Then, add on the number of fields up to the start of the left factor
   * in the self-join in the new join ordering.
   * <li>Then, finally add on the offset of the corresponding column from the
   * left factor.
   * </ul>
   *
   * <p>Note that this only applies if both factors in the self-join are in the
   * join ordering. If they are, then the left factor always precedes the
   * right factor in the join ordering.
   *
   * @param multiJoin join factors being optimized
   * @param factor the factor whose references are being adjusted
   * @param newJoinOrder the new join ordering containing the factor
   * @param newPos the position of the factor in the new join ordering
   * @param adjustments the adjustments array that will be set
   * @param offset the starting offset within the original join ordering for
   * the columns of the factor being adjusted
   * @param newOffset the new starting offset in the new join ordering for the
   * columns of the factor being adjusted
   * @param alwaysUseDefault always use the default adjustment value
   * regardless of whether the factor is the right factor in a removable
   * self-join
   *
   * @return true if at least one column from the factor requires adjustment
   */
  private static boolean remapJoinReferences(
          HiveLoptMultiJoin multiJoin,
          int factor,
          List<Integer> newJoinOrder,
          int newPos,
          int[] adjustments,
          int offset,
          int newOffset,
          boolean alwaysUseDefault) {
    boolean needAdjust = false;
    int defaultAdjustment = -offset + newOffset;
    if (!alwaysUseDefault
            && multiJoin.isRightFactorInRemovableSelfJoin(factor)
            && (newPos != 0)
            && newJoinOrder.get(newPos - 1).equals(
            multiJoin.getOtherSelfJoinFactor(factor))) {
      int nLeftFields =
              multiJoin.getNumFieldsInJoinFactor(
                      newJoinOrder.get(
                              newPos - 1));
      for (int i = 0;
           i < multiJoin.getNumFieldsInJoinFactor(factor);
           i++) {
        Integer leftOffset = multiJoin.getRightColumnMapping(factor, i);

        // if the left factor doesn't reference the column, then
        // use the default adjustment value
        if (leftOffset == null) {
          adjustments[i + offset] = defaultAdjustment;
        } else {
          adjustments[i + offset] =
                  -(offset + i) + (newOffset - nLeftFields)
                          + leftOffset;
        }
        if (adjustments[i + offset] != 0) {
          needAdjust = true;
        }
      }
    } else {
      if (defaultAdjustment != 0) {
        needAdjust = true;
        for (int i = 0;
             i < multiJoin.getNumFieldsInJoinFactor(
                     newJoinOrder.get(newPos));
             i++) {
          adjustments[i + offset] = defaultAdjustment;
        }
      }
    }

    return needAdjust;
  }

  /**
   * In the event that a dimension table does not need to be joined because of
   * a semijoin, this method creates a join tree that consists of a projection
   * on top of an existing join tree. The existing join tree must contain the
   * fact table in the semijoin that allows the dimension table to be removed.
   *
   * <p>The projection created on top of the join tree mimics a join of the
   * fact and dimension tables. In order for the dimension table to have been
   * removed, the only fields referenced from the dimension table are its
   * dimension keys. Therefore, we can replace these dimension fields with the
   * fields corresponding to the semijoin keys from the fact table in the
   * projection.
   *
   * @param multiJoin join factors being optimized
   * @param semiJoinOpt optimal semijoins for each factor
   * @param factTree existing join tree containing the fact table
   * @param dimIdx dimension table factor id
   * @param filtersToAdd filters remaining to be added; filters added to the
   * new join tree are removed from the list
   *
   * @return created join tree or null if the corresponding fact table has not
   * been joined in yet
   */
  private static @Nullable HiveLoptJoinTree createReplacementSemiJoin(
          RelBuilder relBuilder,
          HiveLoptMultiJoin multiJoin,
          HiveLoptSemiJoinOptimizer semiJoinOpt,
          @Nullable HiveLoptJoinTree factTree,
          int dimIdx,
          List<RexNode> filtersToAdd) {
    // if the current join tree doesn't contain the fact table, then
    // don't bother trying to create the replacement join just yet
    if (factTree == null) {
      return null;
    }

    int factIdx = requireNonNull(multiJoin.getJoinRemovalFactor(dimIdx),
            () -> "multiJoin.getJoinRemovalFactor(dimIdx) for " + dimIdx + ", " + multiJoin);
    final List<Integer> joinOrder = factTree.getTreeOrder();
    assert joinOrder.contains(factIdx);

    // figure out the position of the fact table in the current jointree
    int adjustment = 0;
    for (Integer factor : joinOrder) {
      if (factor == factIdx) {
        break;
      }
      adjustment += multiJoin.getNumFieldsInJoinFactor(factor);
    }

    // map the dimension keys to the corresponding keys from the fact
    // table, based on the fact table's position in the current jointree
    List<RelDataTypeField> dimFields =
            multiJoin.getJoinFactor(dimIdx).getRowType().getFieldList();
    int nDimFields = dimFields.size();
    Integer [] replacementKeys = new Integer[nDimFields];
    LogicalJoin semiJoin = multiJoin.getJoinRemovalSemiJoin(dimIdx);
    ImmutableIntList dimKeys = semiJoin.analyzeCondition().leftKeys;
    ImmutableIntList factKeys = semiJoin.analyzeCondition().rightKeys;
    for (int i = 0; i < dimKeys.size(); i++) {
      replacementKeys[dimKeys.get(i)] = factKeys.get(i) + adjustment;
    }

    return createReplacementJoin(
            relBuilder,
            multiJoin,
            semiJoinOpt,
            factTree,
            factIdx,
            dimIdx,
            dimKeys,
            replacementKeys,
            filtersToAdd);
  }

  /**
   * Creates a replacement join, projecting either dummy columns or
   * replacement keys from the factor that doesn't actually need to be joined.
   *
   * @param multiJoin join factors being optimized
   * @param semiJoinOpt optimal semijoins for each factor
   * @param currJoinTree current join tree being added to
   * @param leftIdx if &ge; 0, when creating the replacement join, only consider
   * filters that reference leftIdx in currJoinTree; otherwise, consider all
   * filters that reference any factor in currJoinTree
   * @param factorToAdd new factor whose join can be removed
   * @param newKeys join keys that need to be replaced
   * @param replacementKeys the keys that replace the join keys; null if we're
   * removing the null generating factor in an outer join
   * @param filtersToAdd filters remaining to be added; filters added to the
   * new join tree are removed from the list
   *
   * @return created join tree with an appropriate projection for the factor
   * that can be removed
   */
  private static HiveLoptJoinTree createReplacementJoin(
          RelBuilder relBuilder,
          HiveLoptMultiJoin multiJoin,
          HiveLoptSemiJoinOptimizer semiJoinOpt,
          HiveLoptJoinTree currJoinTree,
          int leftIdx,
          int factorToAdd,
          ImmutableIntList newKeys,
          Integer @Nullable [] replacementKeys,
          List<RexNode> filtersToAdd) {
    // create a projection, projecting the fields from the join tree
    // containing the current joinRel and the new factor; for fields
    // corresponding to join keys, replace them with the corresponding key
    // from the replacementKeys passed in; for other fields, just create a
    // null expression as a placeholder for the column; this is done so we
    // don't have to adjust the offsets of other expressions that reference
    // the new factor; the placeholder expression values should never be
    // referenced, so that's why it's ok to create these possibly invalid
    // expressions
    RelNode currJoinRel = currJoinTree.getJoinTree();
    List<RelDataTypeField> currFields = currJoinRel.getRowType().getFieldList();
    final int nCurrFields = currFields.size();
    List<RelDataTypeField> newFields =
            multiJoin.getJoinFactor(factorToAdd).getRowType().getFieldList();
    final int nNewFields = newFields.size();
    List<Pair<RexNode, String>> projects = new ArrayList<>();
    RexBuilder rexBuilder = currJoinRel.getCluster().getRexBuilder();
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

    for (int i = 0; i < nCurrFields; i++) {
      projects.add(
              Pair.of(rexBuilder.makeInputRef(currFields.get(i).getType(), i),
                      currFields.get(i).getName()));
    }
    for (int i = 0; i < nNewFields; i++) {
      RexNode projExpr;
      RelDataType newType = newFields.get(i).getType();
      if (!newKeys.contains(i)) {
        if (replacementKeys == null) {
          // null generating factor in an outer join; so make the
          // type nullable
          newType =
                  typeFactory.createTypeWithNullability(newType, true);
        }
        projExpr = rexBuilder.makeNullLiteral(newType);
      } else {
        // TODO: is the above if (replacementKeys==null) check placed properly?
        requireNonNull(replacementKeys, "replacementKeys");
        RelDataTypeField mappedField = currFields.get(replacementKeys[i]);
        RexNode mappedInput =
                rexBuilder.makeInputRef(
                        mappedField.getType(),
                        replacementKeys[i]);

        // if the types aren't the same, create a cast
        if (mappedField.getType() == newType) {
          projExpr = mappedInput;
        } else {
          projExpr =
                  rexBuilder.makeCast(
                          newFields.get(i).getType(),
                          mappedInput);
        }
      }
      projects.add(Pair.of(projExpr, newFields.get(i).getName()));
    }

    relBuilder.push(currJoinRel);
    relBuilder.project(Pair.left(projects), Pair.right(projects));

    // remove the join conditions corresponding to the join we're removing;
    // we don't actually need to use them, but we need to remove them
    // from the list since they're no longer needed
    HiveLoptJoinTree newTree =
            new HiveLoptJoinTree(
                    semiJoinOpt.getChosenSemiJoin(factorToAdd),
                    factorToAdd);
    addFilters(
            multiJoin,
            currJoinTree,
            leftIdx,
            newTree,
            filtersToAdd,
            false);

    // Filters referencing factors other than leftIdx and factorToAdd
    // still do need to be applied.  So, add them into a separate
    // LogicalFilter placed on top off the projection created above.
    if (leftIdx >= 0) {
      addAdditionalFilters(
              relBuilder,
              multiJoin,
              currJoinTree,
              newTree,
              filtersToAdd);
    }

    // finally, create a join tree consisting of the current join's join
    // tree with the newly created projection; note that in the factor
    // tree, we act as if we're joining in the new factor, even
    // though we really aren't; this is needed so we can map the columns
    // from the new factor as we go up in the join tree
    return new HiveLoptJoinTree(
            relBuilder.build(),
            currJoinTree.getFactorTree(),
            newTree.getFactorTree());
  }

  /**
   * Creates a LogicalJoin given left and right operands and a join condition.
   * Swaps the operands if beneficial.
   *
   * @param multiJoin join factors being optimized
   * @param left left operand
   * @param right right operand
   * @param condition join condition
   * @param joinType the join type
   * @param fullAdjust true if the join condition reflects the original join
   * ordering and therefore has not gone through any type of adjustment yet;
   * otherwise, the condition has already been partially adjusted and only
   * needs to be further adjusted if swapping is done
   * @param filtersToAdd additional filters that may be added on top of the
   * resulting LogicalJoin, if the join is a left or right outer join
   * @param selfJoin true if the join being created is a self-join that's
   * removable
   *
   * @return created LogicalJoin
   */
  private static HiveLoptJoinTree createJoinSubtree(
          RelMetadataQuery mq,
          RelBuilder relBuilder,
          HiveLoptMultiJoin multiJoin,
          HiveLoptJoinTree left,
          HiveLoptJoinTree right,
          RexNode condition,
          JoinRelType joinType,
          List<RexNode> filtersToAdd,
          boolean fullAdjust,
          boolean selfJoin) {
    RexBuilder rexBuilder =
            multiJoin.getMultiJoinRel().getCluster().getRexBuilder();

    // swap the inputs if beneficial
    if (swapInputs(mq, multiJoin, left, right, selfJoin)) {
      HiveLoptJoinTree tmp = right;
      right = left;
      left = tmp;
      if (!fullAdjust) {
        condition =
                swapFilter(
                        rexBuilder,
                        multiJoin,
                        right,
                        left,
                        condition);
      }
      if ((joinType != JoinRelType.INNER)
              && (joinType != JoinRelType.FULL)) {
        joinType =
                (joinType == JoinRelType.LEFT) ? JoinRelType.RIGHT
                        : JoinRelType.LEFT;
      }
    }

    if (fullAdjust) {
      int [] adjustments = new int[multiJoin.getNumTotalFields()];
      if (needsAdjustment(
              multiJoin,
              adjustments,
              left,
              right,
              selfJoin)) {
        condition =
                condition.accept(
                        new RelOptUtil.RexInputConverter(
                                rexBuilder,
                                multiJoin.getMultiJoinFields(),
                                left.getJoinTree().getRowType().getFieldList(),
                                right.getJoinTree().getRowType().getFieldList(),
                                adjustments));
      }
    }

    relBuilder.push(left.getJoinTree())
            .push(right.getJoinTree())
            .join(joinType, condition);

    // if this is a left or right outer join, and additional filters can
    // be applied to the resulting join, then they need to be applied
    // as a filter on top of the outer join result
    if ((joinType == JoinRelType.LEFT) || (joinType == JoinRelType.RIGHT)) {
      assert !selfJoin;
      addAdditionalFilters(
              relBuilder,
              multiJoin,
              left,
              right,
              filtersToAdd);
    }

    return new HiveLoptJoinTree(
            relBuilder.build(),
            left.getFactorTree(),
            right.getFactorTree(),
            selfJoin);
  }

  /**
   * Determines whether any additional filters are applicable to a join tree.
   * If there are any, creates a filter node on top of the join tree with the
   * additional filters.
   *
   * @param relBuilder Builder holding current join tree
   * @param multiJoin join factors being optimized
   * @param left left side of join tree
   * @param right right side of join tree
   * @param filtersToAdd remaining filters
   */
  private static void addAdditionalFilters(
          RelBuilder relBuilder,
          HiveLoptMultiJoin multiJoin,
          HiveLoptJoinTree left,
          HiveLoptJoinTree right,
          List<RexNode> filtersToAdd) {
    RexNode filterCond =
            addFilters(multiJoin, left, -1, right, filtersToAdd, false);
    if (!filterCond.isAlwaysTrue()) {
      // adjust the filter to reflect the outer join output
      int [] adjustments = new int[multiJoin.getNumTotalFields()];
      if (needsAdjustment(multiJoin, adjustments, left, right, false)) {
        RexBuilder rexBuilder =
                multiJoin.getMultiJoinRel().getCluster().getRexBuilder();
        filterCond =
                filterCond.accept(
                        new RelOptUtil.RexInputConverter(
                                rexBuilder,
                                multiJoin.getMultiJoinFields(),
                                relBuilder.peek().getRowType().getFieldList(),
                                adjustments));
        relBuilder.filter(filterCond);
      }
    }
  }

  /**
   * Swaps the operands to a join, so the smaller input is on the right. Or,
   * if this is a removable self-join, swap so the factor that should be
   * preserved when the self-join is removed is put on the left.
   *
   * @param multiJoin join factors being optimized
   * @param left left side of join tree
   * @param right right hand side of join tree
   * @param selfJoin true if the join is a removable self-join
   *
   * @return true if swapping should be done
   */
  private static boolean swapInputs(
          RelMetadataQuery mq,
          HiveLoptMultiJoin multiJoin,
          HiveLoptJoinTree left,
          HiveLoptJoinTree right,
          boolean selfJoin) {
    boolean swap = false;

    if (selfJoin) {
      return !multiJoin.isLeftFactorInRemovableSelfJoin(
              ((HiveLoptJoinTree.Leaf) left.getFactorTree()).getId());
    }

    final Double leftRowCount = mq.getRowCount(left.getJoinTree());
    final Double rightRowCount = mq.getRowCount(right.getJoinTree());

    // The left side is smaller than the right if it has fewer rows,
    // or if it has the same number of rows as the right (excluding
    // roundoff), but fewer columns.
    if ((leftRowCount != null)
            && (rightRowCount != null)
            && ((leftRowCount < rightRowCount)
            || ((Math.abs(leftRowCount - rightRowCount) < RelOptUtil.EPSILON)
            && (rowWidthCost(left.getJoinTree()) < rowWidthCost(right.getJoinTree()))))) {
      swap = true;
    }
    return swap;
  }

  /**
   * Adjusts a filter to reflect swapping of join inputs.
   *
   * @param rexBuilder rexBuilder
   * @param multiJoin join factors being optimized
   * @param origLeft original LHS of the join tree (before swap)
   * @param origRight original RHS of the join tree (before swap)
   * @param condition original join condition
   *
   * @return join condition reflect swap of join inputs
   */
  private static RexNode swapFilter(
          RexBuilder rexBuilder,
          HiveLoptMultiJoin multiJoin,
          HiveLoptJoinTree origLeft,
          HiveLoptJoinTree origRight,
          RexNode condition) {
    int nFieldsOnLeft =
            origLeft.getJoinTree().getRowType().getFieldCount();
    int nFieldsOnRight =
            origRight.getJoinTree().getRowType().getFieldCount();
    int [] adjustments = new int[nFieldsOnLeft + nFieldsOnRight];

    for (int i = 0; i < nFieldsOnLeft; i++) {
      adjustments[i] = nFieldsOnRight;
    }
    for (int i = nFieldsOnLeft; i < (nFieldsOnLeft + nFieldsOnRight); i++) {
      adjustments[i] = -nFieldsOnLeft;
    }

    condition =
            condition.accept(
                    new RelOptUtil.RexInputConverter(
                            rexBuilder,
                            multiJoin.getJoinFields(origLeft, origRight),
                            multiJoin.getJoinFields(origRight, origLeft),
                            adjustments));

    return condition;
  }

  /**
   * Sets an array indicating how much each factor in a join tree needs to be
   * adjusted to reflect the tree's join ordering.
   *
   * @param multiJoin join factors being optimized
   * @param adjustments array to be filled out
   * @param joinTree join tree
   * @param otherTree null unless joinTree only represents the left side of
   * the join tree
   * @param selfJoin true if no adjustments need to be made for self-joins
   *
   * @return true if some adjustment is required; false otherwise
   */
  private static boolean needsAdjustment(
          HiveLoptMultiJoin multiJoin,
          int[] adjustments,
          HiveLoptJoinTree joinTree,
          HiveLoptJoinTree otherTree,
          boolean selfJoin) {
    boolean needAdjustment = false;

    final List<Integer> joinOrder = new ArrayList<>();
    joinTree.getTreeOrder(joinOrder);
    if (otherTree != null) {
      otherTree.getTreeOrder(joinOrder);
    }

    int nFields = 0;
    for (int newPos = 0; newPos < joinOrder.size(); newPos++) {
      int origPos = joinOrder.get(newPos);
      int joinStart = multiJoin.getJoinStart(origPos);

      // Determine the adjustments needed for join references.  Note
      // that if the adjustment is being done for a self-join filter,
      // we always use the default adjustment value rather than
      // remapping the right factor to reference the left factor.
      // Otherwise, we have no way of later identifying that the join is
      // self-join.
      if (remapJoinReferences(
              multiJoin,
              origPos,
              joinOrder,
              newPos,
              adjustments,
              joinStart,
              nFields,
              selfJoin)) {
        needAdjustment = true;
      }
      nFields += multiJoin.getNumFieldsInJoinFactor(origPos);
    }

    return needAdjustment;
  }

  /**
   * Determines whether a join is a removable self-join. It is if it's an
   * inner join between identical, simple factors and the equality portion of
   * the join condition consists of the same set of unique keys.
   *
   * @param joinRel the join
   *
   * @return true if the join is removable
   */
  public static boolean isRemovableSelfJoin(Join joinRel) {
    final RelNode left = joinRel.getLeft();
    final RelNode right = joinRel.getRight();

    if (joinRel.getJoinType().isOuterJoin()) {
      return false;
    }

    // Make sure the join is between the same simple factor
    final RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();
    final RelOptTable leftTable = mq.getTableOrigin(left);
    if (leftTable == null) {
      return false;
    }
    final RelOptTable rightTable = mq.getTableOrigin(right);
    if (rightTable == null) {
      return false;
    }
    if (!leftTable.getQualifiedName().equals(rightTable.getQualifiedName())) {
      return false;
    }

    // Determine if the join keys are identical and unique
    return areSelfJoinKeysUnique(mq, left, right, joinRel.getCondition());
  }

  /**
   * Determines if the equality portion of a self-join condition is between
   * identical keys that are unique.
   *
   * @param mq Metadata query
   * @param leftRel left side of the join
   * @param rightRel right side of the join
   * @param joinFilters the join condition
   *
   * @return true if the equality join keys are the same and unique
   */
  private static boolean areSelfJoinKeysUnique(RelMetadataQuery mq,
                                               RelNode leftRel, RelNode rightRel, RexNode joinFilters) {
    final JoinInfo joinInfo = JoinInfo.of(leftRel, rightRel, joinFilters);

    // Make sure each key on the left maps to the same simple column as the
    // corresponding key on the right
    for (IntPair pair : joinInfo.pairs()) {
      final RelColumnOrigin leftOrigin =
              mq.getColumnOrigin(leftRel, pair.source);
      if (leftOrigin == null || leftOrigin.isDerived()) {
        return false;
      }
      final RelColumnOrigin rightOrigin =
              mq.getColumnOrigin(rightRel, pair.target);
      if (rightOrigin == null || rightOrigin.isDerived()) {
        return false;
      }
      if (leftOrigin.getOriginColumnOrdinal()
              != rightOrigin.getOriginColumnOrdinal()) {
        return false;
      }
    }

    // Now that we've verified that the keys are the same, see if they
    // are unique.  When removing self-joins, if needed, we'll later add an
    // IS NOT NULL filter on the join keys that are nullable.  Therefore,
    // it's ok if there are nulls in the unique key.
    return RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(mq, leftRel,
            joinInfo.leftSet());
  }

  public static final RelOptRule INSTANCE = create(-1L);

  /**
   * Creates the rule. A non-negative {@code broadcastThreshold} (the map-join build-side size
   * budget, hive.auto.convert.join.noconditionaltask.size) enables the data-movement-aware
   * ordering (hive.cbo.join.reorder.shuffle.cost); a negative value keeps the legacy behavior.
   * The caller must only pass a non-negative threshold when map-join conversion is enabled -
   * the cost model ranks orderings against broadcast joins the physical planner must be able
   * to materialize.
   */
  public static RelOptRule create(long broadcastThreshold) {
    return new Config()
        .withBroadcastThreshold(broadcastThreshold)
        .withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs())
        .withDescription("HiveLoptOptimizeJoinRule")
        .toRule();
  }

  public static class Config extends HiveRuleConfig {
    private long broadcastThreshold = -1L;

    public Config withBroadcastThreshold(long broadcastThreshold) {
      this.broadcastThreshold = broadcastThreshold;
      return this;
    }

    public long broadcastThreshold() {
      return broadcastThreshold;
    }

    @Override
    public HiveLoptOptimizeJoinRule toRule() {
      return new HiveLoptOptimizeJoinRule(this);
    }
  }
}
