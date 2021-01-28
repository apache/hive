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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.SlotDescriptor;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.impala.plan.ImpalaPlannerContext;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaConjuncts;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.SortInfo;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.planner.SingleNodePlanner;
import org.apache.impala.planner.SortNode;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ImpalaSortRel extends ImpalaPlanRel {

  private PlanNode retNode = null;
  private final HiveSortLimit sortLimit;
  private final HiveFilter filter;

  public ImpalaSortRel(HiveSortLimit sortLimit, List<RelNode> inputs, HiveFilter filter) {
    super(sortLimit.getCluster(), sortLimit.getTraitSet(), inputs, sortLimit.getRowType());
    this.sortLimit = sortLimit;
    this.filter = filter;
  }

  public PlanNode getPlanNode(ImpalaPlannerContext ctx) throws ImpalaException, HiveException, MetaException {
    if (retNode != null) {
      return retNode;
    }

    ImpalaPlanRel sortInputRel = getImpalaRelInput(0);

    PlanNode sortInputNode = sortInputRel.getPlanNode(ctx);
    List<Boolean> isAscOrder = new ArrayList<>();
    List<Boolean> nullsFirstParams = new ArrayList<>();
    List<Expr> sortExprs = new ArrayList<>();
    Map<Integer, Expr> childOutputExprsMap = sortInputRel.getOutputExprsMap();

    for (RelFieldCollation fieldCollation : sortLimit.getCollation().getFieldCollations()) {
      int index = fieldCollation.getFieldIndex();
      boolean ascOrder = fieldCollation.direction == RelFieldCollation.Direction.ASCENDING;
      boolean nullsFirst = fieldCollation.nullDirection == RelFieldCollation.NullDirection.FIRST;
      isAscOrder.add(ascOrder);
      nullsFirstParams.add(nullsFirst);
      sortExprs.add(childOutputExprsMap.get(index));
    }

    long limit = sortLimit.getFetchExpr() != null ?
        ((BigDecimal) RexLiteral.value(sortLimit.getFetchExpr())).longValue() : -1L;
    long offset = sortLimit.getOffsetExpr() != null ?
        ((BigDecimal) RexLiteral.value(sortLimit.getOffsetExpr())).longValue() : 0L;

    // If there's limit without order-by, we don't need to generate
    // a sort or top-n node..just set the limit on the child
    if (sortExprs.size() == 0 && filter == null) {
      Preconditions.checkArgument(limit >= 0);
      Preconditions.checkArgument(offset == 0);
      sortInputNode.setLimit(limit);
      this.outputExprs = sortInputRel.getOutputExprsMap();
      retNode = sortInputNode;
      return retNode;
    }

    SortInfo sortInfo = new SortInfo(sortExprs, isAscOrder, nullsFirstParams);
    sortInfo.createSortTupleInfo(sortInputRel.getOutputExprs(), ctx.getRootAnalyzer());

    // createOutputExprs also marks the slot materialized, so call this before calling
    // materializeRequiredSlots because the latter checks for the isMaterialized flag
    this.outputExprs = createMappedOutputExprs(sortInfo, ctx.getRootAnalyzer());

    sortInfo.materializeRequiredSlots(ctx.getRootAnalyzer(), new ExprSubstitutionMap());

    SortNode sortNode = SingleNodePlanner.createSortNode(ctx, ctx.getRootAnalyzer(),
        sortInputNode, sortInfo, limit, offset, limit != -1,
        false /* don't disable outermost topN */);

    retNode = sortNode;

    if (filter != null) {
      ImpalaConjuncts conjuncts = ImpalaConjuncts.create(filter, ctx.getRootAnalyzer(), this);
      List<Expr> assignedConjuncts = conjuncts.getImpalaNonPartitionConjuncts();
      ImpalaSelectNode selectNode =
          new ImpalaSelectNode(ctx.getNextNodeId(), sortNode, assignedConjuncts);
      selectNode.init(ctx.getRootAnalyzer());
      retNode = selectNode;
    }

    return retNode;
  }

  @Override
  public ImpalaSortRel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ImpalaSortRel(sortLimit, inputs, filter);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    RelWriter rw = super.explainTerms(pw);
    Preconditions.checkState(
        sortLimit.getChildExps().size() == sortLimit.getCollation().getFieldCollations().size());
    if (rw.nest()) {
      rw.item("collation", sortLimit.getCollation());
    } else {
      for (int i = 0; i < sortLimit.getChildExps().size(); i++) {
        RexNode e = sortLimit.getChildExps().get(i);
        rw.item("sort" + i, e);
      }
      for (int i = 0; i < sortLimit.getCollation().getFieldCollations().size(); i++) {
        RelFieldCollation e = sortLimit.getCollation().getFieldCollations().get(i);
        rw.item("dir" + i, e.shortString());
      }
    }
    rw.itemIf("offset", sortLimit.getOffsetExpr(), sortLimit.getOffsetExpr() != null);
    rw.itemIf("fetch", sortLimit.getFetchExpr(), sortLimit.getFetchExpr() != null);
    if (filter != null) {
      rw = rw.item("condition", filter.getCondition());
    }
    return rw;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return filter != null ?
        mq.getNonCumulativeCost(filter) : mq.getNonCumulativeCost(sortLimit);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return filter != null ?
        mq.getRowCount(filter) : mq.getRowCount(sortLimit);
  }

  /**
   * Create the output expressions for the SortRel.
   * Impala can change the order in their SortInfo object. So the SlotDescriptors
   * do not necessarily line up with the indexes.   So we need to walk through the
   * expressions of the input node and match them up with the corresponding SlotRef.
   */
  public ImmutableMap<Integer, Expr> createMappedOutputExprs(SortInfo sortInfo,
      Analyzer analyzer) throws AnalysisException  {
    Map<Integer, Expr> childOutputExprsMap = getImpalaRelInput(0).getOutputExprsMap();
    Map<Integer, Expr> exprs = Maps.newLinkedHashMap();

    // project all columns coming from the child since Sort does not change
    // any projections but do the mapping based on its own substitution map
    for (Map.Entry<Integer, Expr> e : childOutputExprsMap.entrySet()) {
      exprs.put(e.getKey(), e.getValue().trySubstitute(sortInfo.getOutputSmap(),
          analyzer, true));
    }

    for (SlotDescriptor slotDesc : sortInfo.getSortTupleDescriptor().getSlots()) {
      slotDesc.setIsMaterialized(true);
    }
    return ImmutableMap.copyOf(exprs);
  }

}
