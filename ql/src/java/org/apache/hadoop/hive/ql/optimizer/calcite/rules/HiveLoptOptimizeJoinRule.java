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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.hadoop.hive.ql.optimizer.calcite.Bug;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.checkerframework.checker.nullness.qual.KeyFor;
import org.checkerframework.checker.nullness.qual.Nullable;

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

    findBestOrderings(mq, call.builder(), multiJoin, semiJoinOpt, call);
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

  /**
   * Generates N optimal join orderings. Each ordering contains each factor as
   * the first factor in the ordering.
   *
   * @param multiJoin join factors being optimized
   * @param semiJoinOpt optimal semijoins for each factor
   * @param call RelOptRuleCall associated with this rule
   */
  private static void findBestOrderings(
          RelMetadataQuery mq,
          RelBuilder relBuilder,
          HiveLoptMultiJoin multiJoin,
          HiveLoptSemiJoinOptimizer semiJoinOpt,
          RelOptRuleCall call) {
    final List<RelNode> plans = new ArrayList<>();

    final List<String> fieldNames =
            multiJoin.getMultiJoinRel().getRowType().getFieldNames();

    // generate the N join orderings
    for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
      // first factor cannot be null generating
      if (multiJoin.isNullGenerating(i)) {
        continue;
      }
      HiveLoptJoinTree joinTree =
              createOrdering(
                      mq,
                      relBuilder,
                      multiJoin,
                      semiJoinOpt,
                      i);
      if (joinTree == null) {
        continue;
      }

      RelNode newProject =
              createTopProject(call.builder(), multiJoin, joinTree, fieldNames);
      plans.add(newProject);
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
          int firstFactor) {
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
                          filtersToAdd);
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
          List<RexNode> filtersToAdd) {
    // iterate through the remaining factors and determine the
    // best one to add next
    int nextFactor = -1;
    int bestWeight = 0;
    Double bestCardinality = null;
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

      // if two factors have the same weight, pick the one
      // with the higher cardinality join key, relative to
      // the join being considered
      if ((dimWeight > bestWeight)
              || ((dimWeight == bestWeight)
              && ((bestCardinality == null)
              || ((cardinality != null)
              && (cardinality > bestCardinality))))) {
        nextFactor = factor;
        bestWeight = dimWeight;
        bestCardinality = cardinality;
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

  public static final RelOptRule INSTANCE =
      new Config().withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs())
          .withDescription("HiveLoptOptimizeJoinRule")
          .toRule();

  public static class Config extends HiveRuleConfig {
    @Override
    public HiveLoptOptimizeJoinRule toRule() {
      return new HiveLoptOptimizeJoinRule(this);
    }
  }
}
