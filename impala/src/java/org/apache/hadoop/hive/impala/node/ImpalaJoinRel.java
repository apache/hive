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

package org.apache.hadoop.hive.impala.node;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.impala.plan.ImpalaPlannerContext;
import org.apache.hadoop.hive.impala.expr.ImpalaFunctionCallExpr;
import org.apache.hadoop.hive.impala.expr.ImpalaNullLiteral;
import org.apache.hadoop.hive.impala.expr.ImpalaTupleIsNullExpr;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaConjuncts;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaFunctionUtil;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaTypeConverter;
import org.apache.hadoop.hive.impala.funcmapper.ScalarFunctionDetails;
import org.apache.hadoop.hive.impala.rex.ImpalaRexVisitor.ImpalaInferMappingRexVisitor;
import org.apache.hadoop.hive.impala.rex.ReferrableNode;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.analysis.TupleIsNullPredicate;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.planner.JoinNode;
import org.apache.impala.planner.PlanNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ImpalaJoinRel extends ImpalaPlanRel {

  private JoinNode joinNode = null;
  private final Join join;
  private final HiveFilter filter;

  public ImpalaJoinRel(Join join) {
    this(join, null);
  }

  public ImpalaJoinRel(Join join, HiveFilter filter) {
    super(join.getCluster(), join.getTraitSet(), join.getInputs(), join.getRowType());
    this.join = join;
    this.filter = filter;
  }

  public PlanNode getPlanNode(ImpalaPlannerContext ctx) throws ImpalaException, HiveException, MetaException {
    if (joinNode != null) {
      return joinNode;
    }

    ImpalaPlanRel leftInputRel = getImpalaRelInput(0);
    ImpalaPlanRel rightInputRel = getImpalaRelInput(1);

    List<ReferrableNode> inputRels = new ArrayList<>();
    inputRels.add(leftInputRel);
    inputRels.add(rightInputRel);

    PlanNode leftInputNode = leftInputRel.getPlanNode(ctx);
    PlanNode rightInputNode = rightInputRel.getPlanNode(ctx);

    List<BinaryPredicate> equiJoinConjuncts = new ArrayList<>();
    List<Expr> nonEquiJoinConjuncts = new ArrayList<>();

    ImpalaInferMappingRexVisitor rexVisitor = new ImpalaInferMappingRexVisitor(
        ctx.getRootAnalyzer(), inputRels, getCluster().getRexBuilder());

    JoinOperator joinOp = getImpalaJoinOp(join);

    // CDPD-8688: Impala allows forcing hints for the distribution mode
    // - e.g force broadcast or hash partition join.  However, we are not
    // currently supporting hints from the new planner.
    JoinNode.DistributionMode distMode = JoinNode.DistributionMode.NONE;

    this.outputExprs = checkAndAddNullWrapping(ctx, leftInputRel, leftInputNode,
        rightInputRel, rightInputNode, joinOp);

    int numEquiJoins = 0;

    // check for equijoin and non-equijoins
    if (!join.getCondition().isAlwaysTrue()) {
      List<RexNode> conjuncts = RelOptUtil.conjunctions(join.getCondition());
      // convert the conjuncts to Impala Expr
      for (RexNode conj : conjuncts) {
        conj = getCanonical(conj); // get a canonicalized representation
        Expr impalaConjunct = conj.accept(rexVisitor);
        if (conj.isA(SqlKind.EQUALS)) {
          Preconditions.checkState(impalaConjunct instanceof BinaryPredicate);
          equiJoinConjuncts.add((BinaryPredicate) impalaConjunct);
          numEquiJoins++;
        } else {
          nonEquiJoinConjuncts.add(impalaConjunct);
        }
      }
    }

    this.nodeInfo = new ImpalaNodeInfo();

    if (numEquiJoins == 0) {
      // since there are no equijoins, we should generate a NestedLoopJoin plan
      joinNode = new ImpalaNestedLoopJoinNode(leftInputNode, rightInputNode,
          false /* not a straight join */, distMode, joinOp,
          nonEquiJoinConjuncts, nodeInfo);
    } else {
      // all other cases generate a hash join plan
      joinNode = new ImpalaHashJoinNode(leftInputNode, rightInputNode,
          false /* not a straight join */, distMode, joinOp, equiJoinConjuncts,
          nonEquiJoinConjuncts, nodeInfo);

      // register the equi and non-equi join conjuncts with the analyzer such that
      // value transfer graph creation can consume it
      List<Expr> equiJoinExprs = new ArrayList<Expr>(equiJoinConjuncts);
      ctx.getRootAnalyzer().registerConjuncts(equiJoinExprs);
      ctx.getRootAnalyzer().registerConjuncts(nonEquiJoinConjuncts);
    }

    joinNode.setId(ctx.getNextNodeId());

    ImpalaConjuncts conjuncts = ImpalaConjuncts.create(filter, ctx.getRootAnalyzer(), this);
    List<Expr> assignedConjuncts = conjuncts.getImpalaNonPartitionConjuncts();
    nodeInfo.setAssignedConjuncts(assignedConjuncts);
    joinNode.init(ctx.getRootAnalyzer());

    return joinNode;
  }

  /**
   * Checks and adds null wrapping for expressions fed into the null producing side of an
   * outer join (hence, both sides for a Full Outer Join).
   * This method populates 2 maps:
   *  1. The output exprs map of the Impala RelNode by combining the left and right input's
   *     output expr map
   *     Note: for some operators the output exprs are created using the TupleDescriptor.
   *     However, for Joins Impala does not require an associated TupleDescriptor.  Hence,
   *     we project whatever exprs (slots) are coming from the child inputs.
   *  2. The expr substitution output map of the Impala PlanNode inputs of this join if this
   *     is an outer join. The reason is that suppose the input tuple is null (e.g if it was
   *     previously produced by the null side of an outer join) and the expr is
   *     COALESCE(int_col, 10), we should not change the existing nullability of the tuple.
   *     Otherwise, if the int_col itself was null then apply the COALESCE.
   *
   *     Note that the COALESCE expression may be fed into the equijoin conjuncts of the outer
   *     join (if the join is on that expr) as well as projected by the outer join. Hence,
   *     by populating the substitution map of the inputs, we are ensuring both these
   *     situations are handled.
   *
   *     Returns an ImmutableMap where key is the ordinal position of the output expr and
   *     value is the output expr itself.
   */
  private ImmutableMap<Integer, Expr> checkAndAddNullWrapping(ImpalaPlannerContext ctx,
      ImpalaPlanRel leftInputRel, PlanNode leftInputNode, ImpalaPlanRel rightInputRel,
      PlanNode rightInputNode, JoinOperator joinOp) throws HiveException, ImpalaException {
    Map<Integer, Expr> exprMap = Maps.newHashMap();
    List<Expr> lhs1 = Lists.newArrayList();
    List<Expr> rhs1 = Lists.newArrayList();
    PlanNode candidateInputNode = null;
    for (Map.Entry<Integer, Expr> e : leftInputRel.getOutputExprsMap().entrySet()) {
      Expr expr = e.getValue();
      if ((joinOp == JoinOperator.RIGHT_OUTER_JOIN || joinOp == JoinOperator.FULL_OUTER_JOIN)
          && requiresNullWrapping(ctx, expr)) {
        Expr expr2 = createIfTupleIsNullPredicate(ctx.getRootAnalyzer(), expr,
            leftInputNode.getTupleIds());
        if (!Expr.IS_NON_NULL_LITERAL.apply(expr)) {
          lhs1.add(expr);
          rhs1.add(expr2);
          candidateInputNode = leftInputNode;
        }
        exprMap.put(e.getKey(), expr2);
      } else {
        exprMap.put(e.getKey(), expr);
      }
    }

    if (candidateInputNode != null && lhs1.size() > 0) {
      ExprSubstitutionMap newSmap = new ExprSubstitutionMap(lhs1, rhs1);
      ExprSubstitutionMap candidateSmap =
          ExprSubstitutionMap.compose(candidateInputNode.getOutputSmap(), newSmap,
              ctx.getRootAnalyzer());
      // See method comments about why we are populating the input smap
      candidateInputNode.setOutputSmap(candidateSmap);
    }

    candidateInputNode = null;
    List<Expr> lhs2 = Lists.newArrayList();
    List<Expr> rhs2 = Lists.newArrayList();

    // For (left) semi joins don't project the right input's output exprs
    if (!(join instanceof HiveSemiJoin)) {
      int sizeLeft = leftInputRel.numOutputExprs();
      for (Map.Entry<Integer, Expr> e : rightInputRel.getOutputExprsMap().entrySet()) {
        int newKey = e.getKey() + sizeLeft;
        Expr expr = e.getValue();
        if ((joinOp == JoinOperator.LEFT_OUTER_JOIN || joinOp == JoinOperator.FULL_OUTER_JOIN)
            && requiresNullWrapping(ctx, expr)) {
          Expr expr2 = createIfTupleIsNullPredicate(ctx.getRootAnalyzer(), expr,
              rightInputNode.getTupleIds());
          if (!Expr.IS_NON_NULL_LITERAL.apply(expr)) {
            if (!lhs1.contains(expr)) {
              lhs2.add(expr);
              rhs2.add(expr2);
            }
            candidateInputNode = rightInputNode;
          }
          exprMap.put(newKey, expr2);
        } else {
          exprMap.put(newKey, expr);
        }
      }
    }

    if (candidateInputNode != null && lhs2.size() > 0) {
      ExprSubstitutionMap newSmap = new ExprSubstitutionMap(lhs2, rhs2);
      ExprSubstitutionMap candidateSmap =
          ExprSubstitutionMap.compose(candidateInputNode.getOutputSmap(), newSmap,
              ctx.getRootAnalyzer());
      // See method comments about why we are populating the input smap
      candidateInputNode.setOutputSmap(candidateSmap);
    }

    return ImmutableMap.copyOf(exprMap);
  }

  /**
   * Returns a new conditional expr 'IF(TupleIsNull(tids), NULL, expr)' to
   * make an input expr nullable.  This is especially useful in cases where the Hive
   * planner generates a literal TRUE and later does a IS_NULL($x) or IS_NOT_NULL($x)
   * check on this column - this happens for NOT IN, NOT EXISTS queries where the planner
   * generates a Left Outer Join and checks the nullability of the column being output from
   * the right side of the LOJ. Since the literal TRUE is a non-null value coming into the join
   * but after the join becomes nullable, we add this function to ensure that happens. Without
   * adding this function the direct translation would be 'TRUE IS NULL' which is incorrect.
   */
  private static Expr createIfTupleIsNullPredicate(Analyzer analyzer, Expr expr,
      List<TupleId> tupleIds) throws HiveException {
    List<Expr> tmpArgs = new ArrayList<>();
    ImpalaTupleIsNullExpr tupleIsNullExpr = new ImpalaTupleIsNullExpr(
        tupleIds, analyzer);
    tmpArgs.add(tupleIsNullExpr);
    // null type needs to be cast to appropriate target type before thrift serialization
    ImpalaNullLiteral nullLiteral = new ImpalaNullLiteral(analyzer, expr.getType());
    tmpArgs.add(nullLiteral);
    tmpArgs.add(expr);
    List<Type> typeNames = ImmutableList.of(Type.BOOLEAN, expr.getType(), expr.getType());
    ScalarFunctionDetails conditionalFuncDetails =
        ScalarFunctionDetails.get("if", ImpalaTypeConverter.getRelDataTypesForArgs(typeNames),
            ImpalaTypeConverter.getRelDataType(expr.getType(), true));
    Preconditions.checkNotNull(conditionalFuncDetails,
        "Could not create IF function for arg types %s and return type %s",
        typeNames, expr.getType());
    Function conditionalFunc = ImpalaFunctionUtil.create(conditionalFuncDetails);
    return new ImpalaFunctionCallExpr(analyzer, conditionalFunc, tmpArgs, null, expr.getType());
  }

  /**
   * This function is a substitute for Impala's
   * {@link org.apache.impala.analysis.TupleIsNullPredicate.requiresNullWrapping()} and
   * is meant to check whether an expression needs to be wrapped with a TupleIsNullPredicate.
   * If we are executing this logic in the HIVE_IN_TEST mode, we fall back to a local
   * implementation of the function. Otherwise, we rely on the corresponding Impala method
   * which does an actual function evaluation to determine nullability.
   * The reason for the fallback in test mode is that we don't want to load libfesupport.so
   * which is indirectly invoked by Impala.
   *
   * Local test mode behavior:
   * Currently, this method returns True for the following types of expressions:
   * COALESCE, IFNULL (if they are the top level expression), any non-null constant literals,
   * a previously wrapped TupleIsNullPredicate and constant FunctionCallExprs (such as
   * CAST('2020-01-01' as TIMESTAMP) ).
   * Returns False for all other expressions.
   *
   * While this takes care of the common cases, it could miss doing null wrapping for
   * complex expressions. That's the main reason why for normal production mode, we
   * go through the function evaluation in Impala.
   */
  private static boolean requiresNullWrapping(ImpalaPlannerContext ctx, Expr expr) throws ImpalaException {
    if (!ctx.getQueryContext().getConf().getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)) {
      return TupleIsNullPredicate.requiresNullWrapping(expr, ctx.getRootAnalyzer());
    }
    // If the expr is already wrapped in an IF(TupleIsNull(), NULL, expr)
    // then it must definitely be wrapped again at this level.
    if (expr.contains(TupleIsNullPredicate.class)) return true;
    if (expr instanceof FunctionCallExpr) {
      if (expr.isConstant()) return true;
      String name = ((FunctionCallExpr) expr).getFnName().getFunction();
      return name.equalsIgnoreCase("coalesce") || name.equalsIgnoreCase("ifnull");
    }
    return Expr.IS_NON_NULL_LITERAL.apply(expr);

  }

  private static class MinIndexVisitor extends RexVisitorImpl<Void> {
    private int minIndex = Integer.MAX_VALUE;
    public MinIndexVisitor() {
      super(true);
    }
    @Override
    public Void visitInputRef(RexInputRef inputRef) {
      minIndex = Math.min(inputRef.getIndex(), minIndex);
      return null;
    }
    public int getMinIndex() {
      return minIndex;
    }
    public void reset() {
      minIndex = Integer.MAX_VALUE;
    }
  }

  /**
   * Canonicalize the equijoin condition such that it is
   * represented as =($M, $N) where M < N. The Impala backend
   * expects this. If the join condition has expressions
   * e.g CAST($5, INT) + $4 = CAST($3, INT) + $1
   * This method will traverse the left and right sides of the
   * condition and identify the minimum RexInputRef index
   * on each side. If the left's min index is greater than the
   * right side's min, it will swap the two sides.
   */
  private RexNode getCanonical(RexNode conjunct) {
    if (!conjunct.isA(SqlKind.EQUALS)) {
      return conjunct;
    }

    MinIndexVisitor visitor = new MinIndexVisitor();
    RexNode lhsRexCall = ((RexCall) conjunct).getOperands().get(0);
    RexNode rhsRexCall = ((RexCall) conjunct).getOperands().get(1);
    // visit the left and right child
    lhsRexCall.accept(visitor);
    int lhsMinIndex = visitor.getMinIndex();
    visitor.reset();
    rhsRexCall.accept(visitor);
    int rhsMinIndex = visitor.getMinIndex();
    if (lhsMinIndex >  rhsMinIndex) {
      // swap the left and right children
      RexNode newConjunct = join.getCluster().getRexBuilder().
          makeCall(SqlStdOperatorTable.EQUALS, rhsRexCall, lhsRexCall);
      return newConjunct;
    }
    return conjunct;
  }

  private JoinOperator getImpalaJoinOp(Join join) throws HiveException {
    switch (join.getJoinType()) {
    case INNER:
      return JoinOperator.INNER_JOIN;
    case FULL:
      return JoinOperator.FULL_OUTER_JOIN;
    case LEFT:
      return JoinOperator.LEFT_OUTER_JOIN;
    case RIGHT:
      return JoinOperator.RIGHT_OUTER_JOIN;
    case SEMI:
      // Mapping it to a Left Semi Join in Impala seems to make
      // sense since it is unclear when we would need a
      // Right Semi Join
      return JoinOperator.LEFT_SEMI_JOIN;
    }
    throw new HiveException("Unsupported join type: " + join.getJoinType());
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    RelWriter rw = super.explainTerms(pw);
    return rw.item("condition", join.getCondition())
        .item("joinType", join.getJoinType().lowerName)
        .itemIf(
            "systemFields",
            join.getSystemFieldList(),
            !join.getSystemFieldList().isEmpty());
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return filter != null ?
        mq.getNonCumulativeCost(filter) : mq.getNonCumulativeCost(join);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return filter != null ?
        mq.getRowCount(filter) : mq.getRowCount(join);
  }

}
