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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import org.apache.hadoop.hive.ql.optimizer.calcite.Bug;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Implements the logic for determining the optimal
 * semi-joins to be used in processing joins in a query.
 */
public class HiveLoptSemiJoinOptimizer {
  //~ Static fields/initializers ---------------------------------------------

  // minimum score required for a join filter to be considered
  private static final int THRESHOLD_SCORE = 10;

  //~ Instance fields --------------------------------------------------------

  private final RexBuilder rexBuilder;

  private final RelMetadataQuery mq;

  /**
   * Semijoins corresponding to each join factor, if they are going to be
   * filtered by semijoins. Otherwise, the entry is the original join factor.
   */
  private RelNode [] chosenSemiJoins;

  /**
   * Associates potential semijoins with each fact table factor. The first
   * parameter in the map corresponds to the fact table. The second
   * corresponds to the dimension table and a SemiJoin that captures all
   * the necessary semijoin data between that fact and dimension table
   */
  private final Map<Integer, Map<Integer, LogicalJoin>> possibleSemiJoins = new HashMap<>();

  private final Ordering<Integer> factorCostOrdering =
      Ordering.from(new FactorCostComparator());

  //~ Constructors -----------------------------------------------------------

  public HiveLoptSemiJoinOptimizer(
      RelMetadataQuery mq,
      HiveLoptMultiJoin multiJoin,
      RexBuilder rexBuilder) {
    if (Bug.CALCITE_6737_FIXED) {
      throw new IllegalStateException("This class is redundant when the fix for CALCITE-6737 is merged into Calcite. "
              + "Remove it and use LoptSemiJoinOptimizer instead of HiveLoptSemiJoinOptimizer");
    }

    // there are no semijoins yet, so initialize to the original
    // factors
    this.mq = mq;
    int nJoinFactors = multiJoin.getNumJoinFactors();
    chosenSemiJoins = new RelNode[nJoinFactors];
    for (int i = 0; i < nJoinFactors; i++) {
      chosenSemiJoins[i] = multiJoin.getJoinFactor(i);
    }

    this.rexBuilder = rexBuilder;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Determines all possible semijoins that can be used by dimension tables to
   * filter fact tables. Constructs SemiJoinRels corresponding to potential
   * dimension table filters and stores them in the member field
   * "possibleSemiJoins"
   *
   * @param multiJoin join factors being optimized
   */
  public void makePossibleSemiJoins(HiveLoptMultiJoin multiJoin) {
    possibleSemiJoins.clear();

    // semijoins can't be used with any type of outer join, including full
    if (multiJoin.getMultiJoinRel().isFullOuterJoin()) {
      return;
    }

    int nJoinFactors = multiJoin.getNumJoinFactors();
    for (int factIdx = 0; factIdx < nJoinFactors; factIdx++) {
      final Map<Integer, List<RexNode>> dimFilters = new HashMap<>();
      final Map<Integer, LogicalJoin> semiJoinMap = new HashMap<>();

      // loop over all filters and find equality filters that reference
      // this factor and one other factor
      for (RexNode joinFilter : multiJoin.getJoinFilters()) {
        int dimIdx = isSuitableFilter(multiJoin, joinFilter, factIdx);
        if (dimIdx == -1) {
          continue;
        }

        // if either the fact or dimension table is null generating,
        // we cannot use semijoins
        if (multiJoin.isNullGenerating(factIdx)
            || multiJoin.isNullGenerating(dimIdx)) {
          continue;
        }

        // if we've already matched against this dimension factor,
        // then add the filter to the list associated with
        // that dimension factor; otherwise, create a new entry
        List<RexNode> currDimFilters = dimFilters.get(dimIdx);
        if (currDimFilters == null) {
          currDimFilters = new ArrayList<>();
        }
        currDimFilters.add(joinFilter);
        dimFilters.put(dimIdx, currDimFilters);
      }

      // if there are potential dimension filters, determine if there
      // are appropriate indexes
      Set<Integer> dimIdxes = dimFilters.keySet();
      for (Integer dimIdx : dimIdxes) {
        List<RexNode> joinFilters = dimFilters.get(dimIdx);
        if (joinFilters != null) {
          LogicalJoin semiJoin =
              findSemiJoinIndexByCost(
                  multiJoin,
                  joinFilters,
                  factIdx,
                  dimIdx);

          // if an index is available, keep track of it as a
          // possible semijoin
          if (semiJoin != null) {
            semiJoinMap.put(dimIdx, semiJoin);
            possibleSemiJoins.put(factIdx, semiJoinMap);
          }
        }
      }
    }
  }

  /**
   * Determines if a join filter can be used with a semijoin against a
   * specified fact table. A suitable filter is of the form "factable.col1 =
   * dimTable.col2".
   *
   * @param multiJoin join factors being optimized
   * @param joinFilter filter to be analyzed
   * @param factIdx index corresponding to the fact table
   *
   * @return index of corresponding dimension table if the filter is
   * appropriate; otherwise -1 is returned
   */
  private static int isSuitableFilter(
      HiveLoptMultiJoin multiJoin,
      RexNode joinFilter,
      int factIdx) {
    // ignore non-equality filters where the operands are not
    // RexInputRefs
    switch (joinFilter.getKind()) {
    case EQUALS:
      break;
    default:
      return -1;
    }
    List<RexNode> operands = ((RexCall) joinFilter).getOperands();
    if (!(operands.get(0) instanceof RexInputRef)
        || !(operands.get(1) instanceof RexInputRef)) {
      return -1;
    }

    // filter is suitable if each side of the filter only contains a
    // single factor reference and one side references the fact table and
    // the other references the dimension table; since we know this is
    // a join filter and we've already verified that the operands are
    // RexInputRefs, verify that the factors belong to the fact and
    // dimension table
    ImmutableBitSet joinRefs = multiJoin.getFactorsRefByJoinFilter(joinFilter);
    assert joinRefs.cardinality() == 2;
    int factor1 = joinRefs.nextSetBit(0);
    int factor2 = joinRefs.nextSetBit(factor1 + 1);
    if (factor1 == factIdx) {
      return factor2;
    }
    if (factor2 == factIdx) {
      return factor1;
    }
    return -1;
  }

  /**
   * Given a list of possible filters on a fact table, determine if there is
   * an index that can be used, provided all the fact table keys originate
   * from the same underlying table.
   *
   * @param multiJoin join factors being optimized
   * @param joinFilters filters to be used on the fact table
   * @param factIdx index in join factors corresponding to the fact table
   * @param dimIdx index in join factors corresponding to the dimension table
   *
   * @return SemiJoin containing information regarding the semijoin that
   * can be used to filter the fact table
   */
  private @Nullable LogicalJoin findSemiJoinIndexByCost(
      HiveLoptMultiJoin multiJoin,
      List<RexNode> joinFilters,
      int factIdx,
      int dimIdx) {
    // create a SemiJoin with the semi-join condition and keys
    RexNode semiJoinCondition =
        RexUtil.composeConjunction(rexBuilder, joinFilters);

    int leftAdjustment = 0;
    for (int i = 0; i < factIdx; i++) {
      leftAdjustment -= multiJoin.getNumFieldsInJoinFactor(i);
    }

    semiJoinCondition =
        adjustSemiJoinCondition(
            multiJoin,
            leftAdjustment,
            semiJoinCondition,
            factIdx,
            dimIdx);

    RelNode factRel = multiJoin.getJoinFactor(factIdx);
    RelNode dimRel = multiJoin.getJoinFactor(dimIdx);
    final JoinInfo joinInfo = JoinInfo.of(factRel, dimRel, semiJoinCondition);
    assert joinInfo.leftKeys.size() > 0;

    // mutable copies
    final List<Integer> leftKeys = Lists.newArrayList(joinInfo.leftKeys);
    final List<Integer> rightKeys = Lists.newArrayList(joinInfo.rightKeys);

    // make sure all the fact table keys originate from the same table
    // and are simple column references
    final List<Integer> actualLeftKeys = new ArrayList<>();
    LcsTable factTable =
        validateKeys(
            factRel,
            leftKeys,
            rightKeys,
            actualLeftKeys);
    if (factTable == null) {
      return null;
    }

    // find the best index
    final List<Integer> bestKeyOrder = new ArrayList<>();
    LcsTableScan tmpFactRel =
        (LcsTableScan) factTable.toRel(
            ViewExpanders.simpleContext(factRel.getCluster()));

    LcsIndexOptimizer indexOptimizer = new LcsIndexOptimizer(tmpFactRel);
    FemLocalIndex bestIndex =
        indexOptimizer.findSemiJoinIndexByCost(
            dimRel,
            actualLeftKeys,
            rightKeys,
            bestKeyOrder);

    if (bestIndex == null) {
      return null;
    }

    // if necessary, truncate the keys to reflect the ones that match
    // the index and remove the corresponding, unnecessary filters from
    // the condition; note that we don't save the actual keys here because
    // later when the semijoin is pushed past other RelNodes, the keys will
    // be converted
    final List<Integer> truncatedLeftKeys;
    final List<Integer> truncatedRightKeys;
    if (actualLeftKeys.size() == bestKeyOrder.size()) {
      truncatedLeftKeys = leftKeys;
      truncatedRightKeys = rightKeys;
    } else {
      truncatedLeftKeys = new ArrayList<>();
      truncatedRightKeys = new ArrayList<>();
      for (int key : bestKeyOrder) {
        truncatedLeftKeys.add(leftKeys.get(key));
        truncatedRightKeys.add(rightKeys.get(key));
      }
      semiJoinCondition =
          removeExtraFilters(
              truncatedLeftKeys,
              multiJoin.getNumFieldsInJoinFactor(factIdx),
              semiJoinCondition);
    }
    return LogicalJoin.create(factRel, dimRel, ImmutableList.of(),
        requireNonNull(semiJoinCondition, "semiJoinCondition"),
        ImmutableSet.of(), JoinRelType.SEMI);
  }

  /**
   * Modifies the semijoin condition to reflect the fact that the RHS is now
   * the second factor into a join and the LHS is the first.
   *
   * @param multiJoin join factors being optimized
   * @param leftAdjustment amount the left RexInputRefs need to be adjusted by
   * @param semiJoinCondition condition to be adjusted
   * @param leftIdx index of the join factor corresponding to the LHS of the
   * semijoin,
   * @param rightIdx index of the join factor corresponding to the RHS of the
   * semijoin
   *
   * @return modified semijoin condition
   */
  private RexNode adjustSemiJoinCondition(
      HiveLoptMultiJoin multiJoin,
      int leftAdjustment,
      RexNode semiJoinCondition,
      int leftIdx,
      int rightIdx) {
    // adjust the semijoin condition to reflect the fact that the
    // RHS is now the second factor into the semijoin and the LHS
    // is the first
    int rightAdjustment = 0;
    for (int i = 0; i < rightIdx; i++) {
      rightAdjustment -= multiJoin.getNumFieldsInJoinFactor(i);
    }
    int rightStart = -rightAdjustment;
    int numFieldsLeftIdx = multiJoin.getNumFieldsInJoinFactor(leftIdx);
    int numFieldsRightIdx = multiJoin.getNumFieldsInJoinFactor(rightIdx);
    rightAdjustment += numFieldsLeftIdx;

    // only adjust the filter if adjustments are required
    if ((leftAdjustment != 0) || (rightAdjustment != 0)) {
      int [] adjustments = new int[multiJoin.getNumTotalFields()];
      if (leftAdjustment != 0) {
        for (int i = -leftAdjustment;
            i < (-leftAdjustment + numFieldsLeftIdx);
            i++) {
          adjustments[i] = leftAdjustment;
        }
      }
      if (rightAdjustment != 0) {
        for (int i = rightStart;
            i < (rightStart + numFieldsRightIdx);
            i++) {
          adjustments[i] = rightAdjustment;
        }
      }
      return semiJoinCondition.accept(
          new RelOptUtil.RexInputConverter(
              rexBuilder,
              multiJoin.getMultiJoinFields(),
              adjustments));
    }

    return semiJoinCondition;
  }

  /**
   * Validates the candidate semijoin keys corresponding to the fact table.
   * Ensure the keys all originate from the same underlying table, and they
   * all correspond to simple column references. If unsuitable keys are found,
   * they're removed from the key list and a new list corresponding to the
   * remaining valid keys is returned.
   *
   * @param factRel fact table RelNode
   * @param leftKeys fact table semijoin keys
   * @param rightKeys dimension table semijoin keys
   * @param actualLeftKeys the remaining valid fact table semijoin keys
   *
   * @return the underlying fact table if the semijoin keys are valid;
   * otherwise null
   */
  private @Nullable LcsTable validateKeys(
      RelNode factRel,
      List<Integer> leftKeys,
      List<Integer> rightKeys,
      List<Integer> actualLeftKeys) {
    int keyIdx = 0;
    RelOptTable theTable = null;
    ListIterator<Integer> keyIter = leftKeys.listIterator();
    while (keyIter.hasNext()) {
      boolean removeKey = false;
      final RelColumnOrigin colOrigin =
          mq.getColumnOrigin(factRel, keyIter.next());

      // can't use the rid column as a semijoin key
      if ((colOrigin == null || !colOrigin.isDerived())
          || LucidDbSpecialOperators.isLcsRidColumnId(
            colOrigin.getOriginColumnOrdinal())) {
        removeKey = true;
      } else {
        RelOptTable table = colOrigin.getOriginTable();
        if (theTable == null) {
          if (!(table instanceof LcsTable)) {
            // not a column store table
            removeKey = true;
          } else {
            theTable = table;
          }
        } else {
          // the tables must match because the column has
          // a simple origin
          assert table == theTable;
        }
      }
      if (colOrigin != null && !removeKey) {
        actualLeftKeys.add(colOrigin.getOriginColumnOrdinal());
        keyIdx++;
      } else {
        keyIter.remove();
        rightKeys.remove(keyIdx);
      }
    }

    // if all keys have been removed, then we don't have any valid semijoin
    // keys
    if (actualLeftKeys.isEmpty()) {
      return null;
    } else {
      return (LcsTable) theTable;
    }
  }

  /**
   * Removes from an expression any sub-expressions that reference key values
   * that aren't contained in a key list passed in. The keys represent join
   * keys on one side of a join. The subexpressions are all assumed to be of
   * the form "tab1.col1 = tab2.col2".
   *
   * @param keys join keys from one side of the join
   * @param nFields number of fields in the side of the join for which the
   * keys correspond
   * @param condition original expression
   *
   * @return modified expression with filters that don't reference specified
   * keys removed
   */
  private @Nullable RexNode removeExtraFilters(
      List<Integer> keys,
      int nFields,
      RexNode condition) {
    // recursively walk the expression; if all sub-expressions are
    // removed from one side of the expression, just return what remains
    // from the other side
    assert condition instanceof RexCall;
    RexCall call = (RexCall) condition;
    if (condition.isA(SqlKind.AND)) {
      List<RexNode> operands = call.getOperands();
      RexNode left =
          removeExtraFilters(
              keys,
              nFields,
              operands.get(0));
      RexNode right =
          removeExtraFilters(
              keys,
              nFields,
              operands.get(1));
      if (left == null) {
        return right;
      }
      if (right == null) {
        return left;
      }
      return rexBuilder.makeCall(
          SqlStdOperatorTable.AND,
          left,
          right);
    }

    // determine which side of the equality filter references the join
    // operand we're interested in; then, check if it is contained in
    // our key list
    assert call.getOperator() == SqlStdOperatorTable.EQUALS;
    List<RexNode> operands = call.getOperands();
    assert operands.get(0) instanceof RexInputRef;
    assert operands.get(1) instanceof RexInputRef;
    int idx = ((RexInputRef) operands.get(0)).getIndex();
    if (idx < nFields) {
      if (!keys.contains(idx)) {
        return null;
      }
    } else {
      idx = ((RexInputRef) operands.get(1)).getIndex();
      if (!keys.contains(idx)) {
        return null;
      }
    }
    return condition;
  }

  /**
   * Finds the optimal semijoin for filtering the least costly fact table from
   * among the remaining possible semijoins to choose from. The chosen
   * semijoin is stored in the chosenSemiJoins array
   *
   * @param multiJoin join factors being optimized
   *
   * @return true if a suitable semijoin is found; false otherwise
   */
  public boolean chooseBestSemiJoin(HiveLoptMultiJoin multiJoin) {
    // sort the join factors based on the cost of each factor filtered by
    // semijoins, if semijoins have been chosen
    int nJoinFactors = multiJoin.getNumJoinFactors();
    List<Integer> sortedFactors =
        factorCostOrdering.immutableSortedCopy(Util.range(nJoinFactors));

    // loop through the factors in sort order, treating the factor as
    // a fact table; analyze the possible semijoins associated with
    // that fact table
    for (int i = 0; i < nJoinFactors; i++) {
      Integer factIdx = sortedFactors.get(i);
      RelNode factRel = chosenSemiJoins[factIdx];
      Map<Integer, LogicalJoin> possibleDimensions =
          possibleSemiJoins.get(factIdx);
      if (possibleDimensions == null) {
        continue;
      }
      double bestScore = 0.0;
      int bestDimIdx = -1;

      // loop through each dimension table associated with the current
      // fact table and analyze the ones that have semijoins with this
      // fact table
      Set<Integer> dimIdxes = possibleDimensions.keySet();
      for (Integer dimIdx : dimIdxes) {
        LogicalJoin semiJoin = possibleDimensions.get(dimIdx);
        if (semiJoin == null) {
          continue;
        }

        // keep track of the dimension table that has the best score
        // for filtering this fact table
        double score =
            computeScore(
                factRel,
                chosenSemiJoins[dimIdx],
                semiJoin);
        if ((score > THRESHOLD_SCORE) && (score > bestScore)) {
          bestDimIdx = dimIdx;
          bestScore = score;
        }
      }

      // if a suitable dimension table has been found, associate it
      // with the fact table in the chosenSemiJoins array; also remove
      // the entry from possibleSemiJoins so we won't chose it again;
      // note that we create the SemiJoin using the chosen semijoins
      // already created for each factor so any chaining of filters will
      // be accounted for
      if (bestDimIdx != -1) {
        int bestDimIdxFinal = bestDimIdx;
        LogicalJoin semiJoin = requireNonNull(possibleDimensions.get(bestDimIdxFinal),
            () -> "possibleDimensions.get(" + bestDimIdxFinal + ") is null");
        LogicalJoin chosenSemiJoin =
            LogicalJoin.create(factRel,
                chosenSemiJoins[bestDimIdx],
                ImmutableList.of(),
                semiJoin.getCondition(),
                ImmutableSet.of(),
                JoinRelType.SEMI);
        chosenSemiJoins[factIdx] = chosenSemiJoin;

        // determine if the dimension table doesn't need to be joined
        // as a result of this semijoin
        removeJoin(multiJoin, chosenSemiJoin, factIdx, bestDimIdx);

        removePossibleSemiJoin(
            possibleDimensions,
            factIdx,
            bestDimIdx);

        // need to also remove the semijoin from the possible
        // semijoins associated with this dimension table, as the
        // semijoin can only be used to filter one table, not both
        removePossibleSemiJoin(
            possibleSemiJoins.get(bestDimIdx),
            bestDimIdx,
            factIdx);
        return true;
      }

      // continue searching on the next fact table if we couldn't find
      // a semijoin for the current fact table
    }

    return false;
  }

  /**
   * Computes a score relevant to applying a set of semijoins on a fact table.
   * The higher the score, the better.
   *
   * @param factRel fact table being filtered
   * @param dimRel dimension table that participates in semijoin
   * @param semiJoin semijoin between fact and dimension tables
   *
   * @return computed score of applying the dimension table filters on the
   * fact table
   */
  private double computeScore(
      RelNode factRel,
      RelNode dimRel,
      LogicalJoin semiJoin) {
    // Estimate savings as a result of applying semijoin filter on fact
    // table.  As a heuristic, the selectivity of the semijoin needs to
    // be less than half.  There may be instances where an even smaller
    // selectivity value is required because of the overhead of
    // index lookups on a very large fact table.  Half was chosen as
    // a middle ground based on testing that was done with a large
    // data set.
    final ImmutableBitSet dimCols = ImmutableBitSet.of(semiJoin.analyzeCondition().rightKeys);
    final double selectivity =
        RelMdUtil.computeSemiJoinSelectivity(mq, factRel, dimRel, semiJoin);
    if (selectivity > .5) {
      return 0;
    }

    final RelOptCost factCost = mq.getCumulativeCost(factRel);

    // if not enough information, return a low score
    if (factCost == null) {
      return 0;
    }
    double savings =
        (1.0 - Math.sqrt(selectivity))
            * Math.max(1.0, factCost.getRows());

    // Additional savings if the dimension columns are unique.  We can
    // ignore nulls since they will be filtered out by the semijoin.
    boolean uniq =
        RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(mq,
            dimRel, dimCols);
    if (uniq) {
      savings *= 2.0;
    }

    // compute the cost of doing an extra scan on the dimension table,
    // including the distinct sort on top of the scan; if the dimension
    // columns are already unique, no need to add on the dup removal cost
    final Double dimSortCost = mq.getRowCount(dimRel);
    final Double dupRemCost = uniq ? 0 : dimSortCost;
    final RelOptCost dimCost = mq.getCumulativeCost(dimRel);
    if ((dimSortCost == null)
        || (dupRemCost == null)
        || (dimCost == null)) {
      return 0;
    }

    double dimRows = dimCost.getRows();
    if (dimRows < 1.0) {
      dimRows = 1.0;
    }
    return savings / dimRows;
  }

  /**
   * Determines whether a join of the dimension table in a semijoin can be
   * removed. It can be if the dimension keys are unique and the only fields
   * referenced from the dimension table are its semijoin keys. The semijoin
   * keys can be mapped to the corresponding keys from the fact table (because
   * of the equality condition associated with the semijoin keys). Therefore,
   * that's why the dimension table can be removed even though those fields
   * are referenced elsewhere in the query tree.
   *
   * @param multiJoin join factors being optimized
   * @param semiJoin semijoin under consideration
   * @param factIdx id of the fact table in the semijoin
   * @param dimIdx id of the dimension table in the semijoin
   */
  private void removeJoin(
      HiveLoptMultiJoin multiJoin,
      LogicalJoin semiJoin,
      int factIdx,
      int dimIdx) {
    // if the dimension can be removed because of another semijoin, then
    // no need to proceed any further
    if (multiJoin.getJoinRemovalFactor(dimIdx) != null) {
      return;
    }

    // Check if the semijoin keys corresponding to the dimension table
    // are unique.  The semijoin will filter out the nulls.
    final ImmutableBitSet dimKeys = ImmutableBitSet.of(semiJoin.analyzeCondition().rightKeys);
    final RelNode dimRel = multiJoin.getJoinFactor(dimIdx);
    if (!RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(mq, dimRel,
        dimKeys)) {
      return;
    }

    // check that the only fields referenced from the dimension table
    // in either its projection or join conditions are the dimension
    // keys
    ImmutableBitSet dimProjRefs = multiJoin.getProjFields(dimIdx);
    if (dimProjRefs == null) {
      int nDimFields = multiJoin.getNumFieldsInJoinFactor(dimIdx);
      dimProjRefs = ImmutableBitSet.range(0, nDimFields);
    }
    if (!dimKeys.contains(dimProjRefs)) {
      return;
    }
    int [] dimJoinRefCounts = multiJoin.getJoinFieldRefCounts(dimIdx);
    for (int i = 0; i < dimJoinRefCounts.length; i++) {
      if (dimJoinRefCounts[i] > 0) {
        if (!dimKeys.get(i)) {
          return;
        }
      }
    }

    // criteria met; keep track of the fact table and the semijoin that
    // allow the join of this dimension table to be removed
    multiJoin.setJoinRemovalFactor(dimIdx, factIdx);
    multiJoin.setJoinRemovalSemiJoin(dimIdx, semiJoin);

    // if the dimension table doesn't reference anything in its projection
    // and the only fields referenced in its joins are the dimension keys
    // of this semijoin, then we can decrement the join reference counts
    // corresponding to the fact table's semijoin keys, since the
    // dimension table doesn't need to use those keys
    if (dimProjRefs.cardinality() != 0) {
      return;
    }
    for (int i = 0; i < dimJoinRefCounts.length; i++) {
      if (dimJoinRefCounts[i] > 1) {
        return;
      } else if (dimJoinRefCounts[i] == 1) {
        if (!dimKeys.get(i)) {
          return;
        }
      }
    }
    int [] factJoinRefCounts = multiJoin.getJoinFieldRefCounts(factIdx);
    for (Integer key : semiJoin.analyzeCondition().leftKeys) {
      factJoinRefCounts[key]--;
    }
  }

  /**
   * Removes a dimension table from a fact table's list of possible semi-joins.
   *
   * @param possibleDimensions possible dimension tables associated with the
   * fact table
   * @param factIdx index corresponding to fact table
   * @param dimIdx index corresponding to dimension table
   */
  private void removePossibleSemiJoin(
      @Nullable Map<Integer, LogicalJoin> possibleDimensions,
      Integer factIdx,
      Integer dimIdx) {
    // dimension table may not have a corresponding semijoin if it
    // wasn't indexable
    if (possibleDimensions == null) {
      return;
    }
    possibleDimensions.remove(dimIdx);
    if (possibleDimensions.isEmpty()) {
      possibleSemiJoins.remove(factIdx);
    } else {
      possibleSemiJoins.put(factIdx, possibleDimensions);
    }
  }

  /**
   * Returns the optimal semijoin for the specified factor; may be the factor
   * itself if semijoins are not chosen for the factor.
   *
   * @param factIdx Index corresponding to the desired factor
   */
  public RelNode getChosenSemiJoin(int factIdx) {
    return chosenSemiJoins[factIdx];
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Compares factors. */
  private class FactorCostComparator
      implements Comparator<Integer> {
    @Override public int compare(Integer rel1Idx, Integer rel2Idx) {
      RelOptCost c1 =
          mq.getCumulativeCost(chosenSemiJoins[rel1Idx]);
      RelOptCost c2 =
          mq.getCumulativeCost(chosenSemiJoins[rel2Idx]);

      // nulls are arbitrarily sorted
      if ((c1 == null) || (c2 == null)) {
        return -1;
      }
      return c1.isLt(c2) ? -1 : (c1.equals(c2) ? 0 : 1);
    }
  }

  /** Dummy class to allow code to compile. */
  private abstract static class LcsTable implements RelOptTable {
  }

  /** Dummy class to allow code to compile. */
  private static class LcsTableScan {
  }

  /** Dummy class to allow code to compile. */
  private static class LcsIndexOptimizer {
    LcsIndexOptimizer(LcsTableScan rel) {}

    public @Nullable FemLocalIndex findSemiJoinIndexByCost(RelNode dimRel,
        List<Integer> actualLeftKeys, List<Integer> rightKeys,
        List<Integer> bestKeyOrder) {
      return null;
    }
  }

  /** Dummy class to allow code to compile. */
  private static class FemLocalIndex { }

  /** Dummy class to allow code to compile. */
  private static class LucidDbSpecialOperators {
    public static boolean isLcsRidColumnId(int originColumnOrdinal) {
      return false;
    }
  }
}
