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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;

import org.apache.hadoop.hive.ql.plan.impala.ImpalaPlannerContext;

import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.SortInfo;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.PlanNodeId;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ImpalaSortRel extends ImpalaPlanRel {

  private ImpalaSortNode sortNode = null;
  private final HiveSortLimit sortLimit;

  public ImpalaSortRel(HiveSortLimit sortLimit, List<RelNode> inputs) {
    super(sortLimit.getCluster(), sortLimit.getTraitSet(), inputs, sortLimit.getRowType());
    this.sortLimit = sortLimit;
  }

  public PlanNode getPlanNode(ImpalaPlannerContext ctx) throws ImpalaException, HiveException, MetaException {
    if (sortNode != null) {
      return sortNode;
    }

    PlanNodeId nodeId = ctx.getNextNodeId();
    ImpalaPlanRel sortInputRel = getImpalaRelInput(0);

    PlanNode sortInputNode = sortInputRel.getPlanNode(ctx);
    List<Boolean> isAscOrder = new ArrayList<>();
    List<Boolean> nullsFirstParams = new ArrayList<>();
    List<Expr> sortExprs = new ArrayList<>();
    Map<Integer, Expr> childOutputExprsMap = sortInputRel.getOutputExprsMap();

    for (RelFieldCollation fieldCollation : sortLimit.getCollation().getFieldCollations()) {
      int index = fieldCollation.getFieldIndex();
      isAscOrder.add(fieldCollation.getDirection() == RelFieldCollation.Direction.ASCENDING);
      nullsFirstParams.add(fieldCollation.nullDirection == RelFieldCollation.NullDirection.FIRST);
      sortExprs.add(childOutputExprsMap.get(index));
    }

    long limit = sortLimit.getFetchExpr() != null ?
        ((BigDecimal) RexLiteral.value(sortLimit.getFetchExpr())).longValue() : -1L;
    long offset = sortLimit.getOffsetExpr() != null ?
        ((BigDecimal) RexLiteral.value(sortLimit.getOffsetExpr())).longValue() : 0L;

    SortInfo sortInfo = new SortInfo(sortExprs, isAscOrder, nullsFirstParams);
    sortInfo.createSortTupleInfo(sortInputRel.getOutputExprs(), ctx.getRootAnalyzer());

    // createOutputExprs also marks the slot materialized, so call this before calling
    // materializeRequiredSlots because the latter checks for the isMaterialized flag
    this.outputExprs = createOutputExprs(sortInfo.getSortTupleDescriptor().getSlots());

    sortInfo.materializeRequiredSlots(ctx.getRootAnalyzer(), new ExprSubstitutionMap());
    TupleDescriptor tupleDesc = sortInfo.getSortTupleDescriptor();
    
    this.nodeInfo = new ImpalaNodeInfo();
    nodeInfo.setTupleDesc(tupleDesc);

    sortNode = new ImpalaSortNode(nodeId, sortInputNode, sortInfo, offset, nodeInfo);
    sortNode.setLimit(limit);
    sortNode.init(ctx.getRootAnalyzer());

    return sortNode;
  }

  @Override
  public ImpalaSortRel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ImpalaSortRel(sortLimit, inputs);
  }

}
