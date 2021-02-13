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
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;

import org.apache.hadoop.hive.impala.plan.ImpalaPlannerContext;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.EmptySetNode;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;

import java.math.BigDecimal;
import java.util.ArrayList;

/**
 * Impala RelNode class for EmptySet.
 * The EmptySet is created whenever we see a HiveSortLimit with a limit of 0.
 */
public class ImpalaEmptySetRel extends ImpalaPlanRel {

  private PlanNode retNode = null;

  private final HiveSortLimit sortLimit;

  public ImpalaEmptySetRel(HiveSortLimit sortLimit) {
    super(sortLimit.getCluster(), sortLimit.getTraitSet(), new ArrayList<>(), sortLimit.getRowType());
    Preconditions.checkNotNull(sortLimit.getFetchExpr());
    Preconditions.checkArgument(
        ((BigDecimal) RexLiteral.value(sortLimit.getFetchExpr())).longValue() == 0);
    this.sortLimit = sortLimit;
  }

  @Override
  public PlanNode getPlanNode(ImpalaPlannerContext ctx)
      throws ImpalaException, HiveException, MetaException {
    if (retNode != null) {
      return retNode;
    }
    PlanNodeId nodeId = ctx.getNextNodeId();

    RelDataType rowType = sortLimit.getRowType();

    TupleDescriptorCreator tupleDescCreator = new TupleDescriptorCreator("empty set", rowType);
    TupleDescriptor tupleDesc = tupleDescCreator.create(ctx.getRootAnalyzer());

    // The outputexprs are the SlotRef exprs passed to the parent node.
    this.outputExprs = createOutputExprs(tupleDesc.getSlots());

    EmptySetNode emptySetNode =
          new EmptySetNode(nodeId, ImmutableList.of(tupleDesc.getId()));

    emptySetNode.init(ctx.getRootAnalyzer());

    retNode = emptySetNode;

    return retNode;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return mq.getNonCumulativeCost(sortLimit);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return 0.0;
  }
}
