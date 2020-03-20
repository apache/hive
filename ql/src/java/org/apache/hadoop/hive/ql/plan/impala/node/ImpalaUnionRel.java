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
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
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

  private UnionNode unionNode = null;

  private final HiveUnion union;

  public ImpalaUnionRel(HiveUnion union) {
    super(union.getCluster(), union.getTraitSet(), union.getInputs(), union.getRowType());
    this.union = union;
  }

  @Override
  public PlanNode getPlanNode(ImpalaPlannerContext ctx)
      throws ImpalaException, HiveException, MetaException {
    if (unionNode != null) {
      return unionNode;
    }
    PlanNodeId nodeId = ctx.getNextNodeId();

    TupleDescriptor tupleDesc = createTupleDescriptor(ctx.getRootAnalyzer());
    // The outputexprs are the SlotRef exprs passed to the parent node.
    this.outputExprs = createOutputExprs(tupleDesc.getSlots());

    int numInputs = getInputs().size();
    List<Pair<PlanNode, List<Expr>>> planNodeAndExprsList = Lists.newArrayList();
    for(int i = 0; i < numInputs; ++i) {
      ImpalaPlanRel unionInputRel = getImpalaRelInput(i);
      PlanNode unionInputNode = unionInputRel.getPlanNode(ctx);
      planNodeAndExprsList.add(Pair.of(unionInputNode, unionInputRel.getOutputExprs()));
    }

    unionNode =
        new ImpalaUnionNode(nodeId, tupleDesc.getId(), getOutputExprs(), planNodeAndExprsList);
    unionNode.init(ctx.getRootAnalyzer());
    return unionNode;
  }

  private TupleDescriptor createTupleDescriptor(Analyzer analyzer) throws HiveException {
    TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor("union");
    tupleDesc.setIsMaterialized(true);

    for (RelDataTypeField relDataTypeField : union.getRowType().getFieldList()) {
      SlotDescriptor slotDesc = analyzer.addSlotDescriptor(tupleDesc);
      slotDesc.setType(ImpalaTypeConverter.getImpalaType(relDataTypeField.getType()));
      slotDesc.setIsMaterialized(true);
    }
    return tupleDesc;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    RelWriter rw = super.explainTerms(pw);
    for (Ord<RelNode> ord : Ord.zip(getInputs())) {
      rw.input("input#" + ord.i, ord.e);
    }
    return rw.item("all", union.all);
  }
}
