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
import com.google.common.collect.Lists;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

import org.apache.hadoop.hive.ql.plan.impala.ImpalaPlannerContext;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaTypeConverter;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;
import org.apache.impala.planner.UnionNode;

import java.util.List;

/**
 * Impala project relnode. This is the Impala Calcite intermediary representation
 * from the Calcite Project node to the Impala Union node. Impala does not have
 * a concept of a Project node, but the Union node where there is only one input
 * node essentially handles this concept.
 */

public class ImpalaProjectRel extends ImpalaProjectRelBase {

  private UnionNode unionNode = null;

  public ImpalaProjectRel(HiveProject project) {
    super(project);
  }

  @Override
  public PlanNode getPlanNode(ImpalaPlannerContext ctx) throws ImpalaException, HiveException, MetaException {
    if (unionNode != null) {
      return unionNode;
    }
    PlanNodeId nodeId = ctx.getNextNodeId();

    ImpalaPlanRel unionInputRel = getImpalaRelInput(0);

    PlanNode unionInputNode = unionInputRel.getPlanNode(ctx);
    Preconditions.checkArgument(getInputs().size() == 1);
    Preconditions.checkArgument(getInput(0) instanceof ImpalaPlanRel);
    TupleDescriptor tupleDesc = createTupleDescriptor(ctx.getRootAnalyzer());
    // The outputexprs are the SlotRef exprs passed to the parent node.
    this.outputExprs = createOutputExprs(tupleDesc.getSlots());
    // The project exprs are the Calcite RexNode exprs that are passed into the
    // Impala Union Node.
    List<Expr> projectExprs = createProjectExprs(ctx).values().asList();
    unionNode = new ImpalaUnionNode(nodeId, tupleDesc.getId(), unionInputNode,
        projectExprs);
    unionNode.init(ctx.getRootAnalyzer());
    return unionNode;
  }

  private TupleDescriptor createTupleDescriptor(Analyzer analyzer) throws HiveException {
    TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor("single input union");
    tupleDesc.setIsMaterialized(true);

    for (RelDataTypeField relDataTypeField : hiveProject.getRowType().getFieldList()) {
      SlotDescriptor slotDesc = analyzer.addSlotDescriptor(tupleDesc);
      slotDesc.setType(ImpalaTypeConverter.getImpalaType(relDataTypeField.getType()));
      slotDesc.setIsMaterialized(true);
    }
    tupleDesc.computeMemLayout();
    return tupleDesc;
  }
}
