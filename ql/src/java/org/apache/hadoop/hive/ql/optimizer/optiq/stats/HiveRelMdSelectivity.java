package org.apache.hadoop.hive.ql.optimizer.optiq.stats;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.hydromatic.optiq.BuiltinMethod;

import org.apache.hadoop.hive.ql.optimizer.optiq.JoinUtil.JoinLeafPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.optiq.JoinUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveTableScanRel;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.metadata.ReflectiveRelMetadataProvider;
import org.eigenbase.rel.metadata.RelMdSelectivity;
import org.eigenbase.rel.metadata.RelMdUtil;
import org.eigenbase.rel.metadata.RelMetadataProvider;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;

import com.google.common.collect.ImmutableMap;

public class HiveRelMdSelectivity extends RelMdSelectivity {
  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(
                                                     BuiltinMethod.SELECTIVITY.method,
                                                     new HiveRelMdSelectivity());

  protected HiveRelMdSelectivity() {
    super();
  }

  public Double getSelectivity(HiveTableScanRel t, RexNode predicate) {
    if (predicate != null) {
      FilterSelectivityEstimator filterSelEstmator = new FilterSelectivityEstimator(t);
      return filterSelEstmator.estimateSelectivity(predicate);
    }

    return 1.0;
  }

  public Double getSelectivity(HiveJoinRel j, RexNode predicate) {
    if (j.getJoinType().equals(JoinRelType.INNER)) {
      return computeInnerJoinSelectivity(j, predicate);
    }
    return 1.0;
  }

  private Double computeInnerJoinSelectivity(HiveJoinRel j, RexNode predicate) {
    double ndvCrossProduct = 1;
    RexNode combinedPredicate = getCombinedPredicateForJoin(j, predicate);
    JoinPredicateInfo jpi = JoinPredicateInfo.constructJoinPredicateInfo(j,
        combinedPredicate);
    ImmutableMap.Builder<Integer, Double> colStatMapBuilder = ImmutableMap
        .builder();
    ImmutableMap<Integer, Double> colStatMap;
    int rightOffSet = j.getLeft().getRowType().getFieldCount();

    // 1. Update Col Stats Map with col stats for columns from left side of
    // Join which are part of join keys
    for (Integer ljk : jpi.getProjsFromLeftPartOfJoinKeysInChildSchema()) {
      colStatMapBuilder.put(ljk,
          HiveRelMdDistinctRowCount.getDistinctRowCount(j.getLeft(), ljk));
    }

    // 2. Update Col Stats Map with col stats for columns from right side of
    // Join which are part of join keys
    for (Integer rjk : jpi.getProjsFromRightPartOfJoinKeysInChildSchema()) {
      colStatMapBuilder.put(rjk + rightOffSet,
          HiveRelMdDistinctRowCount.getDistinctRowCount(j.getRight(), rjk));
    }
    colStatMap = colStatMapBuilder.build();

    // 3. Walk through the Join Condition Building NDV for selectivity
    // NDV of the join can not exceed the cardinality of cross join.
    List<JoinLeafPredicateInfo> peLst = jpi.getEquiJoinPredicateElements();
    int noOfPE = peLst.size();
    if (noOfPE > 0) {
      // 3.1 Use first conjunctive predicate element's NDV as the seed
      ndvCrossProduct = getMaxNDVForJoinSelectivity(peLst.get(0), colStatMap);

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

        if (j.isLeftSemiJoin())
          ndvCrossProduct = Math.min(RelMetadataQuery.getRowCount(j.getLeft()),
              ndvCrossProduct);
        else
          ndvCrossProduct = Math.min(RelMetadataQuery.getRowCount(j.getLeft())
              * RelMetadataQuery.getRowCount(j.getRight()), ndvCrossProduct);
      }
    }

    // 4. Join Selectivity = 1/NDV
    return (1 / ndvCrossProduct);
  }

  private RexNode getCombinedPredicateForJoin(HiveJoinRel j, RexNode additionalPredicate) {
    RexNode minusPred = RelMdUtil.minusPreds(j.getCluster().getRexBuilder(), additionalPredicate,
        j.getCondition());

    if (minusPred != null) {
      List<RexNode> minusList = new ArrayList<RexNode>();
      minusList.add(j.getCondition());
      minusList.add(minusPred);

      return RexUtil.composeConjunction(j.getCluster().getRexBuilder(), minusList, true);
    }

    return j.getCondition();
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
