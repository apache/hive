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

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;

import org.apache.hadoop.hive.ql.plan.impala.ImpalaPlannerContext;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaTypeConverter;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;
import org.apache.impala.planner.UnionNode;

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

  private UnionNode unionNode = null;

  private final RelNode relNode;

  private final NodeType nodeType;

  public ImpalaUnionRel(HiveTableScan scan) {
    super(scan.getCluster(), scan.getTraitSet(), scan.getInputs(), scan.getRowType());
    this.relNode = scan;
    this.nodeType = NodeType.SCAN;
  }

  public ImpalaUnionRel(HiveUnion union) {
    super(union.getCluster(), union.getTraitSet(), union.getInputs(), union.getRowType());
    this.relNode = union;
    this.nodeType = NodeType.UNION;
  }

  @Override
  public PlanNode getPlanNode(ImpalaPlannerContext ctx)
      throws ImpalaException, HiveException, MetaException {
    if (unionNode != null) {
      return unionNode;
    }
    PlanNodeId nodeId = ctx.getNextNodeId();

    RelDataType rowType = (nodeType == NodeType.SCAN)
        ? ((HiveTableScan) relNode).getPrunedRowType()
        : relNode.getRowType();
    TupleDescriptor tupleDesc = createTupleDescriptor(ctx.getRootAnalyzer(), rowType);
    // The outputexprs are the SlotRef exprs passed to the parent node.
    this.outputExprs = createOutputExprs(tupleDesc.getSlots());

    int numInputs = getInputs().size();
    List<Pair<PlanNode, List<Expr>>> planNodeAndExprsList = Lists.newArrayList();
    for(int i = 0; i < numInputs; ++i) {
      ImpalaPlanRel unionInputRel = getImpalaRelInput(i);
      PlanNode unionInputNode = unionInputRel.getPlanNode(ctx);
      planNodeAndExprsList.add(Pair.of(unionInputNode, unionInputRel.getOutputExprs()));
    }

    // The nonConstOutputExprs will contain the list of outputExprs except in the case
    // where a TableScan is being used. The TableScan will only contain a list where
    // every expression is a constant expression, since the TableScan here will not
    // contain a "from" clause (e.g. select 3, 'hello').
    List<Expr> nonConstOutputExprs = (nodeType == NodeType.SCAN)
        ? Lists.newArrayList()
        : getOutputExprs();

    unionNode =
          new ImpalaUnionNode(nodeId, tupleDesc.getId(), nonConstOutputExprs, planNodeAndExprsList);

    if (nodeType == NodeType.SCAN) {
      unionNode.addConstExprList(getOutputExprs());
    }

    unionNode.init(ctx.getRootAnalyzer());
    return unionNode;
  }

  private TupleDescriptor createTupleDescriptor(Analyzer analyzer, RelDataType rowType) throws HiveException {
    TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor(nodeType.toString());
    tupleDesc.setIsMaterialized(true);

    for (RelDataTypeField relDataTypeField : rowType.getFieldList()) {
      SlotDescriptor slotDesc = analyzer.addSlotDescriptor(tupleDesc);
      slotDesc.setType(ImpalaTypeConverter.getImpalaType(relDataTypeField.getType()));
      slotDesc.setLabel(relDataTypeField.getName());
      slotDesc.setIsMaterialized(true);
    }
    return tupleDesc;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    RelWriter retWriter = null;
    switch(nodeType) {
      case SCAN: {
        retWriter = super.explainTerms(pw);
      }
      break;
      case UNION: {
        RelWriter rw = super.explainTerms(pw);
        for (int i = 0; i < getInputs().size(); i++) {
          RelNode e = getInput(i);
          rw.input("input#" + i, e);
        }
        retWriter = rw.item("all", ((HiveUnion)relNode).all);
      }
      break;
    }
    return retWriter;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return mq.getNonCumulativeCost(relNode);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return mq.getRowCount(relNode);
  }
}
