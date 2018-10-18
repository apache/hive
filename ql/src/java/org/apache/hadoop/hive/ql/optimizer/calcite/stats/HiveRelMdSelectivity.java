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
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinLeafPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveConfPlannerContext;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import com.google.common.collect.ImmutableMap;

public class HiveRelMdSelectivity extends RelMdSelectivity {

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.SELECTIVITY.method, new HiveRelMdSelectivity());

  //~ Constructors -----------------------------------------------------------

  private HiveRelMdSelectivity() {}

  //~ Methods ----------------------------------------------------------------

  public Double getSelectivity(HiveTableScan t, RelMetadataQuery mq, RexNode predicate) {
    if (predicate != null) {
      FilterSelectivityEstimator filterSelEstmator = new FilterSelectivityEstimator(t, mq);
      return filterSelEstmator.estimateSelectivity(predicate);
    }

    return 1.0;
  }

  public Double getSelectivity(Join j, RelMetadataQuery mq, RexNode predicate) {
    if (j.getJoinType().equals(JoinRelType.INNER)) {
      return computeInnerJoinSelectivity(j, mq, predicate);
    } else if (j.getJoinType().equals(JoinRelType.LEFT) ||
            j.getJoinType().equals(JoinRelType.RIGHT)) {
      double left = mq.getRowCount(j.getLeft());
      double right = mq.getRowCount(j.getRight());
      double product = left * right;
      double innerJoinSelectivity = computeInnerJoinSelectivity(j, mq, predicate);
      if (j.getJoinType().equals(JoinRelType.LEFT)) {
        return Math.max(innerJoinSelectivity, left/product);
      }
      return Math.max(innerJoinSelectivity, right/product);
    }
    return 1.0;
  }

  private Double computeInnerJoinSelectivity(Join j, RelMetadataQuery mq, RexNode predicate) {
    Pair<Boolean, RexNode> predInfo =
        getCombinedPredicateForJoin(j, predicate);
    if (!predInfo.getKey()) {
      return
          new FilterSelectivityEstimator(j, mq).
          estimateSelectivity(predInfo.getValue());
    }

    RexNode combinedPredicate = predInfo.getValue();
    JoinPredicateInfo jpi;
    try {
      jpi = JoinPredicateInfo.constructJoinPredicateInfo(j,
          combinedPredicate);
    } catch (CalciteSemanticException e) {
      throw new RuntimeException(e);
    }
    ImmutableMap.Builder<Integer, Double> colStatMapBuilder = ImmutableMap
        .builder();
    ImmutableMap<Integer, Double> colStatMap;
    int rightOffSet = j.getLeft().getRowType().getFieldCount();

    // 1. Update Col Stats Map with col stats for columns from left side of
    // Join which are part of join keys
    for (Integer ljk : jpi.getProjsFromLeftPartOfJoinKeysInChildSchema()) {
      colStatMapBuilder.put(ljk,
          HiveRelMdDistinctRowCount.getDistinctRowCount(j.getLeft(), mq, ljk));
    }

    // 2. Update Col Stats Map with col stats for columns from right side of
    // Join which are part of join keys
    for (Integer rjk : jpi.getProjsFromRightPartOfJoinKeysInChildSchema()) {
      colStatMapBuilder.put(rjk + rightOffSet,
          HiveRelMdDistinctRowCount.getDistinctRowCount(j.getRight(), mq, rjk));
    }
    colStatMap = colStatMapBuilder.build();

    // 3. Walk through the Join Condition Building NDV for selectivity
    // NDV of the join can not exceed the cardinality of cross join.
    List<JoinLeafPredicateInfo> peLst = jpi.getEquiJoinPredicateElements();
    int noOfPE = peLst.size();
    double ndvEstimate = 1;
    if (noOfPE > 0) {
      boolean isCorrelatedColumns = j.getCluster().getPlanner().getContext().
          unwrap(HiveConfPlannerContext.class).getIsCorrelatedColumns();
      if (noOfPE > 1 && isCorrelatedColumns ){
        ndvEstimate = maxNdvForCorrelatedColumns(peLst, colStatMap);
      }
      else {
        ndvEstimate = exponentialBackoff(peLst, colStatMap);
      }

      if (j instanceof SemiJoin) {
        ndvEstimate = Math.min(mq.getRowCount(j.getLeft()),
            ndvEstimate);
      }else if (j instanceof HiveJoin){
        ndvEstimate = Math.min(mq.getRowCount(j.getLeft())
            * mq.getRowCount(j.getRight()), ndvEstimate);
      } else {
        throw new RuntimeException("Unexpected Join type: " + j.getClass().getName());
      }
    }

    // 4. Join Selectivity = 1/NDV
    return (1 / ndvEstimate);
  }

  // 3.2 if conjunctive predicate elements are more than one, then walk
  // through them one by one. Compute cross product of NDV. Cross product is
  // computed by multiplying the largest NDV of all of the conjunctive
  // predicate
  // elements with degraded NDV of rest of the conjunctive predicate
  // elements. NDV is
  // degraded using log function.Finally the ndvCrossProduct is fenced at
  // the join
  // cross product to ensure that NDV can not exceed worst case join
  // cardinality.<br>
  // NDV of a conjunctive predicate element is the max NDV of all arguments
  // to lhs, rhs expressions.
  // NDV(JoinCondition) = min (left cardinality * right cardinality,
  // ndvCrossProduct(JoinCondition))
  // ndvCrossProduct(JoinCondition) = ndv(pex)*log(ndv(pe1))*log(ndv(pe2))
  // where pex is the predicate element of join condition with max ndv.
  // ndv(pe) = max(NDV(left.Expr), NDV(right.Expr))
  // NDV(expr) = max(NDV( expr args))
  protected double logSmoothing(List<JoinLeafPredicateInfo> peLst, ImmutableMap<Integer, Double> colStatMap) {
    int noOfPE = peLst.size();
    double ndvCrossProduct = getMaxNDVForJoinSelectivity(peLst.get(0), colStatMap);
    if (noOfPE > 1) {
      double maxNDVSoFar = ndvCrossProduct;
      double ndvToBeSmoothed;
      double tmpNDV;

      for (int i = 1; i < noOfPE; i++) {
        tmpNDV = getMaxNDVForJoinSelectivity(peLst.get(i), colStatMap);
        if (tmpNDV > maxNDVSoFar) {
          ndvToBeSmoothed = maxNDVSoFar;
          maxNDVSoFar = tmpNDV;
          ndvCrossProduct = (ndvCrossProduct / ndvToBeSmoothed) * tmpNDV;
        } else {
          ndvToBeSmoothed = tmpNDV;
        }
        // TODO: revisit the fence
        if (ndvToBeSmoothed > 3)
          ndvCrossProduct *= Math.log(ndvToBeSmoothed);
        else
          ndvCrossProduct *= ndvToBeSmoothed;
      }
    }
    return ndvCrossProduct;
  }

  // max ndv across all column references from both sides of table
  protected double maxNdvForCorrelatedColumns(List<JoinLeafPredicateInfo> peLst,
  ImmutableMap<Integer, Double> colStatMap) {
    int noOfPE = peLst.size();
    List<Double> ndvs = new ArrayList<Double>(noOfPE);
    for (int i = 0; i < noOfPE; i++) {
      ndvs.add(getMaxNDVForJoinSelectivity(peLst.get(i), colStatMap));
    }
    return Collections.max(ndvs);
  }

  /*
   * a) Order predciates based on ndv in reverse order. b) ndvCrossProduct =
   * ndv(pe0) * ndv(pe1) ^(1/2) * ndv(pe2) ^(1/4) * ndv(pe3) ^(1/8) ...
   */
  protected double exponentialBackoff(List<JoinLeafPredicateInfo> peLst,
      ImmutableMap<Integer, Double> colStatMap) {
    int noOfPE = peLst.size();
    List<Double> ndvs = new ArrayList<Double>(noOfPE);
    for (int i = 0; i < noOfPE; i++) {
      ndvs.add(getMaxNDVForJoinSelectivity(peLst.get(i), colStatMap));
    }
    Collections.sort(ndvs);
    Collections.reverse(ndvs);
    double ndvCrossProduct = 1.0;
    for (int i = 0; i < ndvs.size(); i++) {
      double n = Math.pow(ndvs.get(i), Math.pow(1 / 2.0, i));
      ndvCrossProduct *= n;
    }
    return ndvCrossProduct;
  }

  /**
   *
   * @param j
   * @param additionalPredicate
   * @return if predicate is the join condition return (true, joinCond)
   * else return (false, minusPred)
   */
  private Pair<Boolean,RexNode> getCombinedPredicateForJoin(Join j, RexNode additionalPredicate) {
    RexNode minusPred = RelMdUtil.minusPreds(j.getCluster().getRexBuilder(), additionalPredicate,
        j.getCondition());

    if (minusPred != null) {
      List<RexNode> minusList = new ArrayList<RexNode>();
      minusList.add(j.getCondition());
      minusList.add(minusPred);

      return new Pair<Boolean,RexNode>(false, minusPred);
    }

    return new Pair<Boolean,RexNode>(true,j.getCondition());
  }

  /**
   * Compute Max NDV to determine Join Selectivity.
   *
   * @param jlpi
   * @param colStatMap
   *          Immutable Map of Projection Index (in Join Schema) to Column Stat
   * @param rightProjOffSet
   * @return
   */
  private static Double getMaxNDVForJoinSelectivity(JoinLeafPredicateInfo jlpi,
      ImmutableMap<Integer, Double> colStatMap) {
    Double maxNDVSoFar = 1.0;

    maxNDVSoFar = getMaxNDVFromProjections(colStatMap,
        jlpi.getProjsFromLeftPartOfJoinKeysInJoinSchema(), maxNDVSoFar);
    maxNDVSoFar = getMaxNDVFromProjections(colStatMap,
        jlpi.getProjsFromRightPartOfJoinKeysInJoinSchema(), maxNDVSoFar);

    return maxNDVSoFar;
  }

  private static Double getMaxNDVFromProjections(Map<Integer, Double> colStatMap,
      Set<Integer> projectionSet, Double defaultMaxNDV) {
    Double colNDV = null;
    Double maxNDVSoFar = defaultMaxNDV;

    for (Integer projIndx : projectionSet) {
      colNDV = colStatMap.get(projIndx);
      if (colNDV > maxNDVSoFar)
        maxNDVSoFar = colNDV;
    }

    return maxNDVSoFar;
  }

}
