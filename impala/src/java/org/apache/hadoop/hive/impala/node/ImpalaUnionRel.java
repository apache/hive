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
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;

import org.apache.hadoop.hive.impala.plan.ImpalaPlannerContext;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaConjuncts;
import org.apache.hadoop.hive.impala.rex.ImpalaRexVisitor;
import org.apache.hadoop.hive.impala.rex.ImpalaRexVisitor.ImpalaInferMappingRexVisitor;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;
import org.apache.impala.planner.UnionNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Impala RelNode class for Union.
 * One note: Both "union distinct" and "union all" are handled by this class.
 * Calcite handles the "union distinct" by changing it to a "union all" plus
 * aggregation.
 * CDPD-9408: Need to see if we can take advantage of UnionNode.addConstExprList()
 * for performance gains.
 */
public class ImpalaUnionRel extends ImpalaPlanRel {

  // The associated Hive RelNode can be a HiveTableScan when
  // there is no "from" clause on the "select".
  private enum NodeType {
    SCAN("scan"),
    TABLEFUNCTION("tablefunction"),
    UNION("union");
    String type;
    private NodeType(String type) {
      this.type = type;
    }

    @Override
    public String toString() {
      return type;
    }
  }

  private PlanNode retNode = null;

  private final RelNode relNode;
  private final NodeType nodeType;
  private final HiveFilter filter;

  protected List<List<RexNode>> constExprLists_ = new ArrayList<>();

  RelDataType constRowType_ = null;

  public ImpalaUnionRel(HiveTableScan scan) {
    super(scan.getCluster(), scan.getTraitSet(), scan.getInputs(), scan.getRowType());
    this.relNode = scan;
    this.nodeType = NodeType.SCAN;
    this.filter = null;
  }

  public ImpalaUnionRel(HiveTableScan scan, HiveFilter filter) {
    super(scan.getCluster(), scan.getTraitSet(), scan.getInputs(), scan.getRowType());
    this.relNode = scan;
    this.nodeType = NodeType.SCAN;
    this.filter = filter;
  }

  public ImpalaUnionRel(HiveTableFunctionScan scan) {
    super(scan.getCluster(), scan.getTraitSet(), scan.getInputs(), scan.getRowType());
    this.relNode = scan;
    this.nodeType = NodeType.TABLEFUNCTION;
    this.filter = null;
  }

  public ImpalaUnionRel(HiveUnion union) {
    super(union.getCluster(), union.getTraitSet(), union.getInputs(), union.getRowType());
    this.relNode = union;
    this.nodeType = NodeType.UNION;
    this.filter = null;
  }

  public void addConstExprList(List<RexNode> exprs) {
    constExprLists_.add(exprs);
  }

  void setConstRowType(RelDataType constRowType) {
    constRowType_ = constRowType;
  }

  @Override
  public PlanNode getPlanNode(ImpalaPlannerContext ctx)
      throws ImpalaException, HiveException, MetaException {
    if (retNode != null) {
      return retNode;
    }
    PlanNodeId nodeId = ctx.getNextNodeId();

    RelDataType rowType = (nodeType == NodeType.SCAN)
        ? ((HiveTableScan) relNode).getPrunedRowType()
        : relNode.getRowType();

    int numInputs = getInputs().size();

    if (constExprLists_.size() != 0) {
      Preconditions.checkState(numInputs == 0);
      Preconditions.checkState(rowType.getFieldList().size() == 0);
      Preconditions.checkState(constRowType_ != null);
      Preconditions.checkState(constRowType_.getFieldList().size() > 0);
      rowType = constRowType_;
    }

    TupleDescriptorCreator tupleDescCreator =
        new TupleDescriptorCreator(nodeType.toString(), rowType);
    TupleDescriptor tupleDesc = tupleDescCreator.create(ctx.getRootAnalyzer());
    // The outputexprs are the SlotRef exprs passed to the parent node.
    this.outputExprs = createOutputExprs(tupleDesc.getSlots());

    List<Pair<PlanNode, List<Expr>>> planNodeAndExprsList = Lists.newArrayList();
    for(int i = 0; i < numInputs; ++i) {
      ImpalaPlanRel unionInputRel = getImpalaRelInput(i);
      PlanNode unionInputNode = unionInputRel.getPlanNode(ctx);
      planNodeAndExprsList.add(Pair.of(unionInputNode, unionInputRel.getOutputExprs()));
    }

    // register the value transfer from input of union to the output slots. This
    // gets used later during runtime filter generation (without this change, the
    // runtime filter would not get propagated past the union).
    for (Pair<PlanNode, List<Expr>> pair : planNodeAndExprsList) {
      List<Expr> exprs = pair.right;
      for (int i = 0; i < exprs.size(); i++) {
        Expr inputExpr = exprs.get(i);
        SlotRef inputSlotRef = inputExpr.unwrapSlotRef(true);
        SlotRef outputSlotRef = this.outputExprs.get(i).unwrapSlotRef(true);
        if (inputSlotRef == null || outputSlotRef == null) continue;
        ctx.getRootAnalyzer().registerValueTransfer(outputSlotRef.getSlotId(), inputSlotRef.getSlotId());
      }
    }

    // The nonConstOutputExprs will contain the list of outputExprs except in the case
    // where a TableScan is being used. The TableScan will only contain a list where
    // every expression is a constant expression, since the TableScan here will not
    // contain a "from" clause (e.g. select 3, 'hello').
    List<Expr> nonConstOutputExprs = (nodeType == NodeType.SCAN)
        ? Lists.newArrayList()
        : getOutputExprs();

    UnionNode unionNode =
          new ImpalaUnionNode(nodeId, tupleDesc.getId(), nonConstOutputExprs, planNodeAndExprsList);

    // If this union has a list of literals, use that to init plan node
    if (constExprLists_.size() > 0) {
      ImpalaRexVisitor visitor =
          new ImpalaInferMappingRexVisitor(ctx.getRootAnalyzer(),
              ImmutableList.of(this), getCluster().getRexBuilder());
      for (List<RexNode> exprList: constExprLists_) {
        List<Expr> impalaExprs = new ArrayList<>();
        for (RexNode rexNode : exprList) {
          impalaExprs.add(rexNode.accept(visitor));
        }
        unionNode.addConstExprList(impalaExprs);
      }
    } else if (nodeType == NodeType.SCAN) {
      unionNode.addConstExprList(getOutputExprs());
    }

    unionNode.init(ctx.getRootAnalyzer());

    retNode = unionNode;

    if (filter != null) {
      ImpalaConjuncts conjuncts = ImpalaConjuncts.create(filter, ctx.getRootAnalyzer(), this);
      List<Expr> assignedConjuncts = conjuncts.getImpalaNonPartitionConjuncts();
      ImpalaSelectNode selectNode =
          new ImpalaSelectNode(ctx.getNextNodeId(), unionNode, assignedConjuncts);
      selectNode.init(ctx.getRootAnalyzer());
      retNode = selectNode;
    }

    return retNode;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    RelWriter rw = null;
    switch(nodeType) {
    case SCAN:
    case TABLEFUNCTION:
      rw = super.explainTerms(pw);
      break;
    case UNION:
      rw = super.explainTerms(pw);
      for (int i = 0; i < getInputs().size(); i++) {
        RelNode e = getInput(i);
        rw.input("input#" + i, e);
      }
      rw = rw.item("all", ((HiveUnion)relNode).all);
      break;
    default:
      throw new RuntimeException("Unsupported option: " + nodeType);
    }
    if (filter != null) {
      rw = rw.item("condition", filter.getCondition());
    }
    return rw;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return filter != null ?
        mq.getNonCumulativeCost(filter) : mq.getNonCumulativeCost(relNode);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return filter != null ?
        mq.getRowCount(filter) : mq.getRowCount(relNode);
  }
}
