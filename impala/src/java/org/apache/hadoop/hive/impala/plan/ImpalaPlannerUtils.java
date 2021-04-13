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

package org.apache.hadoop.hive.impala.plan;

import com.google.common.base.Preconditions;
import org.apache.impala.analysis.AnalyticExpr;
import org.apache.impala.analysis.AnalyticWindow;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.SortInfo;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.AggregationNode;
import org.apache.impala.planner.AnalyticEvalNode;
import org.apache.impala.planner.JoinNode;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.planner.SelectNode;
import org.apache.impala.planner.SortNode;
import org.apache.impala.planner.SubplanNode;
import org.apache.impala.planner.UnionNode;

import java.util.ArrayList;
import java.util.List;

/**
 * A set of static utility methods that are borrowed from Impala such that
 * any customizations could be made. Where appropriate, a comment such as
 * 'Changes compared to Impala's implementation' has been added.
 */
public class ImpalaPlannerUtils {
  /**
   * Creates and initializes either a SortNode or a TopNNode depending on various
   * heuristics and configuration parameters.
   */
  public static SortNode createSortNode(PlannerContext planCtx, Analyzer analyzer,
      PlanNode root, SortInfo sortInfo,
      long limit, long offset, boolean hasLimit, boolean disableTopN)
      throws ImpalaException {
    SortNode sortNode;
    long topNBytesLimit = planCtx.getQueryOptions().topn_bytes_limit;

    if (hasLimit && offset == 0) {
      checkAndApplyLimitPushdown(root, sortInfo, limit, planCtx, analyzer);
    }

    if (hasLimit && !disableTopN) {
      if (topNBytesLimit <= 0) {
        sortNode =
            SortNode.createTopNSortNode(planCtx.getQueryOptions(), planCtx.getNextNodeId(), root, sortInfo, offset, limit, false);
      } else {
        long topNCardinality = PlanNode.capCardinalityAtLimit(root.getCardinality(), limit);
        long estimatedTopNMaterializedSize =
            sortInfo.estimateTopNMaterializedSize(topNCardinality, offset);

        if (estimatedTopNMaterializedSize < topNBytesLimit) {
          sortNode =
              SortNode.createTopNSortNode(planCtx.getQueryOptions(), planCtx.getNextNodeId(), root, sortInfo, offset, limit, false);
        } else {
          sortNode =
              SortNode.createTotalSortNode(planCtx.getNextNodeId(), root, sortInfo, offset);
        }
      }
    } else {
      sortNode =
          SortNode.createTotalSortNode(planCtx.getNextNodeId(), root, sortInfo, offset);
    }
    Preconditions.checkState(sortNode.hasValidStats());
    sortNode.setLimit(limit);
    sortNode.init(analyzer);

    return sortNode;
  }

  /**
   * For certain qualifying conditions, we can push a limit from the top level
   * sort down to the sort associated with an AnalyticEval node.
   */
  public static void checkAndApplyLimitPushdown(PlanNode root, SortInfo sortInfo, long limit,
      PlannerContext planCtx, Analyzer analyzer) {
    boolean pushdownLimit = false;
    AnalyticEvalNode analyticNode = null;
    List<PlanNode> intermediateNodes = new ArrayList<>();
    List<Expr>  partitioningExprs = new ArrayList<>();
    SortNode analyticNodeSort = null;
    PlanNode descendant = findDescendantAnalyticNode(root, intermediateNodes);
    if (descendant != null && intermediateNodes.size() <= 3) {
      Preconditions.checkArgument(descendant instanceof AnalyticEvalNode);
      analyticNode = (AnalyticEvalNode) descendant;
      if (!(analyticNode.getChild(0) instanceof SortNode)) {
        // if the over() clause is empty, there won't be a child SortNode
        // so limit pushdown is not applicable
        return;
      }
      analyticNodeSort = (SortNode) analyticNode.getChild(0);
      int numNodes = intermediateNodes.size();
      // NOTE: Changes compared to Impala's implementation
      // Since FENG plans generate single input Union nodes (to handle projections),
      // we take that into consideration when traversing the intermediate nodes.
      if (numNodes == 0) {
        pushdownLimit = isLimitPushdownSafe(analyticNode, sortInfo, null, limit, analyticNodeSort, partitioningExprs,
            planCtx.getRootAnalyzer());
      } else if (numNodes <= 3) {
        // The only allowed intermediate nodes are (note: here union means
        // single input union, not the set operator):
        // 1. union - select - union
        // 2. select - union
        // 3. select
        for (int i = 0; i < intermediateNodes.size(); i++) {
          PlanNode node = intermediateNodes.get(i);
          if (node instanceof SelectNode && i <= 1) {
            pushdownLimit =
                isLimitPushdownSafe(analyticNode, sortInfo, (SelectNode) node, limit, analyticNodeSort, partitioningExprs,
                    planCtx.getRootAnalyzer());
            break;
          } else if (!(node instanceof UnionNode)) {
            break;
          }
        }
      }
    }

    if (pushdownLimit) {
      Preconditions.checkArgument(analyticNode != null);
      Preconditions.checkArgument(analyticNode.getChild(0) instanceof SortNode);
      analyticNodeSort.tryConvertToTopN(limit, analyzer, false);
      // after the limit is pushed down, update stats for the analytic eval node
      // and intermediate nodes
      analyticNode.computeStats(analyzer);
      for (PlanNode n : intermediateNodes) {
        n.computeStats(analyzer);
      }
    }
  }

  /**
   * Starting from the supplied root PlanNode, traverse the descendants
   * to find the first AnalyticEvalNode.  If a blocking node such as
   * Join, Aggregate, Sort is encountered, return null. The
   * 'intermediateNodes' is populated with the nodes encountered during
   * traversal.
   */
  private static PlanNode findDescendantAnalyticNode(PlanNode root,
      List<PlanNode> intermediateNodes) {
    if (root == null || root instanceof AnalyticEvalNode) {
      return root;
    }
    // If we encounter a blocking operator (sort, aggregate, join), or a Subplan,
    // there's no need to go further. Also, we bail early if we encounter multi-input
    // operator such as union-all.  In the future, we could potentially extend the
    // limit pushdown to both sides of a union-all
    if (root instanceof SortNode || root instanceof AggregationNode ||
        root instanceof JoinNode || root instanceof SubplanNode ||
        root.getChildren().size() > 1) {
      return null;
    }
    intermediateNodes.add(root);
    return findDescendantAnalyticNode(root.getChild(0), intermediateNodes);
  }

  /**
   * Check if it is safe to push down limit to the Sort node of this AnalyticEval.
   * Qualifying checks:
   *  - The analytic node is evaluating a single analytic function which must be
   *    a ranking function.
   *  - The partition-by exprs must be a prefix of the sort exprs in sortInfo
   *  - If there is a predicate on the analytic function (provided through the
   *    selectNode), the predicate's eligibility is checked (see further below)
   * @param sortInfo The sort info from the outer sort node
   * @param selectNode The selection node with predicates on analytic function.
   *    This can be null if no such predicate is present.
   * @param limit Limit value from the outer sort node
   * @param analyticNodeSort The analytic sort associated with this analytic node
   * @param sortExprsForPartitioning A placeholder list supplied by caller that is
   *     populated with the sort exprs of the analytic sort that will be later used
   *     for hash partitioning of the distributed TopN.
   * @param analyzer analyzer instance
   * @return True if limit pushdown into analytic sort is safe, False if not
   */
  private static boolean isLimitPushdownSafe(AnalyticEvalNode analyticNode, SortInfo sortInfo,
      SelectNode selectNode, long limit, SortNode analyticNodeSort,
      List<Expr> sortExprsForPartitioning, Analyzer analyzer) {
    List<Expr> analyticFnCalls = analyticNode.getAnalyticFnCalls();
    if (analyticFnCalls.size() != 1) return false;
    Expr expr = analyticFnCalls.get(0);
    if (!(expr instanceof FunctionCallExpr) ||
        (!AnalyticExpr.isRankingFn(((FunctionCallExpr) expr).getFn()))) {
      return false;
    }
    List<Expr> analyticSortSortExprs = analyticNodeSort.getSortInfo().getSortExprs();

    // NOTE: Changes compared to Impala's implementation
    // We are using the workaround of SQL string when comparing the top level sort
    // exprs with the analytic functions partition exprs. In the future it would be
    // desirable to re-use the original Impala logic.
    List<Expr> sortExprs = sortInfo != null ? sortInfo.getOrigSortExprs() :
        new ArrayList<>();
    List<Expr> pbExprs = analyticNode.getPartitionExprs();
    if (sortExprs.size() == 0) {
      // if there is no sort expr in the parent sort but only limit, we can push
      // the limit to the sort below if there is no selection node or if
      // the predicate in the selection node is eligible
      if (selectNode == null) {
        return true;
      }
      Pair<Boolean, Double> status =
          isPredEligibleForLimitPushdown(selectNode.getConjuncts(), limit, analyticNode);
      if (status.first) {
        sortExprsForPartitioning.addAll(analyticSortSortExprs);
        selectNode.setSelectivity(status.second);
        return true;
      }
      return false;
    }

    Preconditions.checkArgument(analyticSortSortExprs.size() >= pbExprs.size());
    // Check if pby exprs are a prefix of the top level sort exprs
    // TODO: also check if subsequent expressions match. Need to check ASC and NULLS FIRST
    // compatibility more explicitly in the case.
    if (sortExprs.size() == 0) {
      sortExprsForPartitioning.addAll(pbExprs);
    } else {
      if (!analyticSortExprsArePrefix(
          sortInfo, sortExprs, analyticNodeSort.getSortInfo(), pbExprs)) {
        return false;
      }

      // get the corresponding sort exprs from the analytic sort
      // since that's what will eventually be used for hash partitioning
      sortExprsForPartitioning.addAll(analyticSortSortExprs.subList(0, pbExprs.size()));
    }

    AnalyticWindow analyticWindow = analyticNode.getAnalyticWindow();
    // check that the window frame is UNBOUNDED PRECEDING to CURRENT ROW
    if (!(analyticWindow.getLeftBoundary().getType() ==
        AnalyticWindow.BoundaryType.UNBOUNDED_PRECEDING
        && analyticWindow.getRightBoundary().getType() ==
        AnalyticWindow.BoundaryType.CURRENT_ROW)) {
      return false;
    }

    if (selectNode == null) {
      // Limit pushdown is valid if the pre-analytic sort puts rows into the same order
      // as sortExprs. We check the prefix match, since extra analytic sort exprs do not
      // affect the compatibility of the ordering with sortExprs.
      return analyticSortExprsArePrefix(sortInfo, sortExprs,
          analyticNodeSort.getSortInfo(), analyticSortSortExprs);
    } else {
      Pair<Boolean, Double> status =
          isPredEligibleForLimitPushdown(selectNode.getConjuncts(), limit, analyticNode);
      if (status.first) {
        selectNode.setSelectivity(status.second);
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if 'analyticSortExprs' is a prefix of the 'sortExprs' from an outer sort.
   * @param sortInfo sort info from the outer sort
   * @param sortExprs sort exprs from the outer sort. Must be a prefix of the full
   *                sort exprs from sortInfo.
   * @param analyticSortInfo sort info from the analytic sort
   * @param analyticSortExprs sort expressions from the analytic sort. Must be a prefix
   *                of the full sort exprs from analyticSortInfo.
   */
  private static boolean analyticSortExprsArePrefix(SortInfo sortInfo, List<Expr> sortExprs,
      SortInfo analyticSortInfo, List<Expr> analyticSortExprs) {
    if (analyticSortExprs.size() > sortExprs.size()) return false;
    for (int i = 0; i < analyticSortExprs.size(); i++) {
      Expr e1 = analyticSortExprs.get(i);
      Expr e2 = sortExprs.get(i);
      if (!(e1 instanceof SlotRef && e2 instanceof SlotRef)) return false;

      // NOTE: Changes compared to Impala's implementation
      // The equals comparison of the slot refs doesn't currently work for the FENG
      // plans mainly because of the intermediate Union nodes that are created which
      // introduce a new tuple descriptor. Hence, as a workaround we are using the
      // SQL string comparison below. In the future, it would be desirable to make the
      // equals comparison work.
      // We want to treat table.c1 same as c1 so use the last item in the qualified
      // name of the column
      String[] e1Sql = ((SlotRef) e1).toSql().split("\\.");
      String[] e2Sql = ((SlotRef) e2).toSql().split("\\.");
      if (!e1Sql[e1Sql.length-1].equals(e2Sql[e2Sql.length-1])) {
        // partition exprs are not a prefix of the top level sort exprs
        return false;
      }

      // check the ASC/DESC and NULLS FIRST/LAST compatibility.
      if (!sortInfo.getIsAscOrder().get(i).equals(
          analyticSortInfo.getIsAscOrder().get(i))) {
        return false;
      }
      if (!sortInfo.getNullsFirst().get(i).equals(
          analyticSortInfo.getNullsFirst().get(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check eligibility of a predicate (provided as list of conjuncts) for limit
   * pushdown optimization.
   * TODO: IMPALA-10296: this check is not strict enough to allow limit pushdown to
   * be safe in all circumstances.
   * @param conjuncts list of conjuncts from the predicate
   * @param limit limit from outer sort
   * @return a Pair whose first value is True if the conjuncts+limit allows pushdown,
   *   False otherwise. Second value is the predicate's estimated selectivity
   */
  private static Pair<Boolean, Double> isPredEligibleForLimitPushdown(List<Expr> conjuncts,
      long limit, AnalyticEvalNode analyticNode) {
    Pair<Boolean, Double> falseStatus = new Pair<>(false, -1.0);
    // Currently, single conjuncts are supported.  In the future, multiple conjuncts
    // involving a range e.g 'col >= 10 AND col <= 20' could potentially be supported
    if (conjuncts.size() > 1) return falseStatus;
    Expr conj = conjuncts.get(0);
    if (!(Expr.IS_BINARY_PREDICATE.apply(conj))) return falseStatus;
    BinaryPredicate pred = (BinaryPredicate) conj;
    Expr lhs = pred.getChild(0);
    Expr rhs = pred.getChild(1);
    // Lhs of the binary predicate must be a ranking function.
    // Also, it must be bound to the output tuple of this analytic eval node
    if (!(lhs instanceof SlotRef)) {
      return falseStatus;
    }

    // NOTE: Changes compared to Impala's implementation
    // We check the sql string for rank function. Ideally, we should check the tuple
    // binding by leveraging Impala's logic.
    if (!((SlotRef) lhs).toSql().equals("rank()")) {
      return falseStatus;
    }
    // Restrict the pushdown for =, <, <= predicates because these ensure the
    // qualifying rows are fully 'contained' within the LIMIT value. Other
    // types of predicates would select rows that fall outside the LIMIT range.
    if (!(pred.getOp() == BinaryPredicate.Operator.EQ ||
        pred.getOp() == BinaryPredicate.Operator.LT ||
        pred.getOp() == BinaryPredicate.Operator.LE)) {
      return falseStatus;
    }
    // Rhs of the predicate must be a numeric literal and its value
    // must be less than or equal to the limit.
    if (!(rhs instanceof NumericLiteral) ||
        ((NumericLiteral)rhs).getLongValue() > limit) {
      return falseStatus;
    }
    double selectivity = Expr.DEFAULT_SELECTIVITY;
    // Since the predicate is qualified for limit pushdown, estimate its selectivity.
    // For EQ conditions, leave it as the default.  For LT and LE, assume all of the
    // 'limit' rows will be returned.
    if (pred.getOp() == BinaryPredicate.Operator.LT ||
        pred.getOp() == BinaryPredicate.Operator.LE) {
      selectivity = 1.0;
    }
    return new Pair<Boolean, Double>(true, selectivity);
  }

}

