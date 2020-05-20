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

package org.apache.hadoop.hive.ql.plan.impala.node;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaPlannerContext;
import org.apache.hadoop.hive.ql.plan.impala.rex.ImpalaRexVisitor.ImpalaInferMappingRexVisitor;
import org.apache.hadoop.hive.ql.plan.impala.rex.ReferrableNode;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.JoinNode;
import org.apache.impala.planner.PlanNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        ctx.getRootAnalyzer(), inputRels);

    JoinOperator joinOp = getImpalaJoinOp(join);

    // CDPD-8688: Impala allows forcing hints for the distribution mode
    // - e.g force broadcast or hash partition join.  However, we are not
    // currently supporting hints from the new planner.
    JoinNode.DistributionMode distMode = JoinNode.DistributionMode.NONE;

    // create the output exprs map by combining the left and right input's
    // output expr map
    // NOTE: for some operators the output exprs are created using the
    // TupleDescriptor; however, for Joins Impala does not seem to require
    // an associated TupleDescriptor.  Hence, we project whatever exprs (slots)
    // are coming from the child inputs.
    Map<Integer, Expr> exprMap = Maps.newHashMap();
    for (Map.Entry<Integer, Expr> e : leftInputRel.getOutputExprsMap().entrySet()) {
      exprMap.put(e.getKey(), e.getValue());
    }

    // For (left) semi joins don't project the right input's output exprs
    if (!(join instanceof HiveSemiJoin)) {
      int sizeLeft = leftInputRel.numOutputExprs();
      for (Map.Entry<Integer, Expr> e : rightInputRel.getOutputExprsMap().entrySet()) {
        int newKey = e.getKey() + sizeLeft;
        exprMap.put(newKey, e.getValue());
      }
    }

    this.outputExprs = ImmutableMap.copyOf(exprMap);

    int numEquiJoins = 0;

    // check for equijoin and non-equijoins
    if (!join.getCondition().isAlwaysTrue()) {
      List<RexNode> conjuncts = RelOptUtil.conjunctions(join.getCondition());
      // convert the conjuncts to Impala Expr
      for (RexNode conj : conjuncts) {
        // canonicalize the equijoin condition such that it is
        // represented as =($M, $N) where M < N.  The Impala backend
        // expects this.
        if (conj.isA(SqlKind.EQUALS)) {
          RexNode n0 = ((RexCall)conj).getOperands().get(0);
          RexNode n1 = ((RexCall)conj).getOperands().get(1);
          // CDPD-8690: generalize the check below to allow RexCall instead of
          // just RexInputRef
          if (n0 instanceof RexInputRef && n1 instanceof RexInputRef &&
              ((RexInputRef) n0).getIndex() > ((RexInputRef) n1).getIndex()) {
            // swap the left and right
            RexNode newConj = join.getCluster().getRexBuilder().
                makeCall(SqlStdOperatorTable.EQUALS, n1, n0);
            conj = newConj;
          }
        }
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
    }

    joinNode.setId(ctx.getNextNodeId());

    List<TupleId> tupleIds = new ArrayList<>();

    if (joinOp == JoinOperator.LEFT_OUTER_JOIN) {
      tupleIds.addAll(rightInputNode.getTupleIds());
    } else if (joinOp == JoinOperator.RIGHT_OUTER_JOIN) {
      tupleIds.addAll(leftInputNode.getTupleIds());
    } else if (joinOp == JoinOperator.FULL_OUTER_JOIN) {
      tupleIds.addAll(leftInputNode.getTupleIds());
      tupleIds.addAll(rightInputNode.getTupleIds());
    }

    List<Expr> assignedConjuncts = getConjuncts(filter, ctx.getRootAnalyzer(), this, tupleIds);
    nodeInfo.setAssignedConjuncts(assignedConjuncts);
    joinNode.init(ctx.getRootAnalyzer());

    return joinNode;
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
