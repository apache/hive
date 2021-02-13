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
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

import org.apache.hadoop.hive.impala.plan.ImpalaPlannerContext;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaConjuncts;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;

import java.util.List;

/**
 * Impala project relnode. This is the Impala Calcite intermediary representation
 * from the Calcite Project node to the Impala Union node. Impala does not have
 * a concept of a Project node, but the Union node where there is only one input
 * node essentially handles this concept.
 */
public class ImpalaProjectRel extends ImpalaProjectRelBase {

  private final HiveFilter filter;

  public ImpalaProjectRel(HiveProject project) {
    this(project, null);
  }

  public ImpalaProjectRel(HiveProject project, HiveFilter filter) {
    super(project);
    this.filter = filter;
  }

  @Override
  public PlanNode getPlanNode(ImpalaPlannerContext ctx) throws ImpalaException, HiveException, MetaException {
    if (planNode != null) {
      return planNode;
    }
    PlanNodeId nodeId = ctx.getNextNodeId();

    ImpalaPlanRel unionInputRel = getImpalaRelInput(0);
    planNode = unionInputRel.getPlanNode(ctx);

    Preconditions.checkArgument(getInputs().size() == 1);
    Preconditions.checkArgument(getInput(0) instanceof ImpalaPlanRel);
    List<Expr> projectExprs = createProjectExprs(ctx).values().asList();
    TupleDescriptorCreator tupleDescCreator =
        new TupleDescriptorCreator("single input union", projectExprs, project.getRowType());
    TupleDescriptor tupleDesc = tupleDescCreator.create(ctx.getRootAnalyzer());
    // The outputexprs are the SlotRef exprs passed to the parent node.
    this.outputExprs = createOutputExprs(tupleDesc.getSlots());
    // The project exprs are the Calcite RexNode exprs that are passed into the
    // Impala Union Node.
    Pair<PlanNode, List<Expr>> pair = Pair.of(planNode, projectExprs);
    planNode = new ImpalaUnionNode(nodeId, tupleDesc.getId(), getOutputExprs(),
        Lists.newArrayList(pair));
    planNode.init(ctx.getRootAnalyzer());

    if (filter != null) {
      ImpalaConjuncts conjuncts = ImpalaConjuncts.create(filter, ctx.getRootAnalyzer(), this);
      List<Expr> assignedConjuncts = conjuncts.getImpalaNonPartitionConjuncts();
      planNode = new ImpalaSelectNode(ctx.getNextNodeId(), planNode, assignedConjuncts);
      planNode.init(ctx.getRootAnalyzer());
    }

    return planNode;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return filter != null ?
        mq.getNonCumulativeCost(filter) : mq.getNonCumulativeCost(project);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return filter != null ?
        mq.getRowCount(filter) : mq.getRowCount(project);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    RelWriter rw = super.explainTerms(pw);

    if (filter != null) {
      rw = rw.item("condition", filter.getCondition());
    }
    return rw;
  }
}
