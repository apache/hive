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
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;

import org.apache.hadoop.hive.impala.plan.ImpalaPlannerContext;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.CardinalityCheckNode;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;

import java.util.List;

/**
 * Impala RelNode class for CardinalityCheck.
 * The CardinalityCheck RelNode is created whenever we see an sq_count_check function.
 */
public class ImpalaCardinalityCheckRel extends ImpalaPlanRel {

  private PlanNode retNode = null;

  private final HiveFilter filter;

  public ImpalaCardinalityCheckRel(HiveFilter filter, List<RelNode> inputs) {
    super(filter.getCluster(), filter.getTraitSet(), inputs, filter.getRowType());
    this.filter = filter;
  }

  @Override
  public PlanNode getPlanNode(ImpalaPlannerContext ctx)
      throws ImpalaException, HiveException, MetaException {
    if (retNode != null) {
      return retNode;
    }
    ImpalaPlanRel inputRel = getImpalaRelInput(0);
    PlanNode input = inputRel.getPlanNode(ctx);
    PlanNodeId nodeId = ctx.getNextNodeId();

    // The cardinality check node has a requirement that the underlying node
    // will pass in at most 2 rows. The 2nd row that comes in will force the
    // abort of the query.
    input.setLimit(2);

    TupleDescriptorCreator tupleDescCreator =
        new TupleDescriptorCreator("CARDINALITY CHECK", filter.getRowType());
    TupleDescriptor tupleDesc = tupleDescCreator.create(ctx.getRootAnalyzer());
    this.outputExprs = createOutputExprs(tupleDesc.getSlots());

    CardinalityCheckNode cardinalityCheckNode =
          new CardinalityCheckNode(nodeId, input, "CARDINALITY CHECK");

    cardinalityCheckNode.init(ctx.getRootAnalyzer());

    retNode = cardinalityCheckNode;

    return retNode;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return mq.getNonCumulativeCost(filter);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return 1.0;
  }
}
