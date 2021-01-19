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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveGroupingID;
import org.apache.hadoop.hive.impala.plan.ImpalaBasicAnalyzer;
import org.apache.hadoop.hive.impala.plan.ImpalaPlannerContext;
import org.apache.hadoop.hive.impala.expr.ImpalaFunctionCallExpr;
import org.apache.hadoop.hive.impala.expr.ImpalaNullLiteral;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaConjuncts;
import org.apache.hadoop.hive.impala.funcmapper.ImpalaTypeConverter;
import org.apache.impala.analysis.AggregateInfo;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.FunctionParams;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.AggregationNode;
import org.apache.impala.planner.PlanNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ImpalaAggregateRel extends ImpalaPlanRel {

  public PlanNode aggNode;
  public final HiveAggregate aggregate;
  public final HiveFilter filter;
  private boolean generateGroupingId;
  private FunctionCallExpr countStarExpr;

  public ImpalaAggregateRel(HiveAggregate aggregate) {
    this(aggregate, null);
  }

  public ImpalaAggregateRel(HiveAggregate aggregate, HiveFilter filter) {
    super(aggregate.getCluster(), aggregate.getTraitSet(), aggregate.getInputs(),
        filter != null ? filter.getRowType() : aggregate.getRowType());
    this.aggregate = aggregate;
    this.filter = filter;
  }

  /**
   * Convert the Aggregation Rel Node into an Impala Plan Node.
   * Impala has its aggregate structure called MultiAggregateInfo. This structure
   * needs to be analyzed before converting it into the Impala aggregate node.
   * After analyzing, the final output expressions are retrieved through the
   * AggregateInfo structure.
   */
  @Override
  public PlanNode getPlanNode(ImpalaPlannerContext ctx) throws ImpalaException, HiveException, MetaException {
    Preconditions.checkState(getInputs().size() == 1);
    if (aggNode != null) {
      return aggNode;
    }
    ImpalaPlanRel relInput = getImpalaRelInput(0);
    PlanNode input = relInput.getPlanNode(ctx);

    ImpalaBasicAnalyzer analyzer = (ImpalaBasicAnalyzer) ctx.getRootAnalyzer();
    List<Expr> groupingExprs = getGroupingExprs();
    List<FunctionCallExpr> aggExprs = getAggregateExprs(ctx);

    // Impala's MultiAggregateInfo encapsulates functionality to represent
    // aggregation functions and grouping exprs belonging to multiple
    // aggregation classes - such as in the case of multiple DISTINCT
    // aggregates or multiple Grouping Sets.
    MultiAggregateInfo multiAggInfo;
    if (Aggregate.isSimple(aggregate)) {
      multiAggInfo =
          new MultiAggregateInfo(groupingExprs, aggExprs, null);
      // this is a 'simple' aggregate with no grouping sets..use the default analyze call
      multiAggInfo.analyze(analyzer);
    } else {
      List<List<Expr>> groupingSets = getGroupingSets(analyzer);
      multiAggInfo =
          new MultiAggregateInfo(groupingExprs, aggExprs, groupingSets);
      if (generateGroupingId) {
        // TODO
	// multiAggInfo.setGenerateGroupingId(true);
      }
      List<AggregateInfo> aggInfos = Lists.newArrayList();
      List<List<FunctionCallExpr>> aggClasses = Lists.newArrayList();
      for (List<Expr> gsGroupByExprs : groupingSets) {
        AggregateInfo aggInfo = AggregateInfo.create(gsGroupByExprs, aggExprs, analyzer);
        aggInfos.add(aggInfo);
        // Populate the agg class corresponding to this grouping set
        aggClasses.add(aggExprs);
      }
      multiAggInfo.analyzeCustomClasses(analyzer, aggClasses, aggInfos);
    }

    // Impala pushes expressions up the stack, but Calcite has already done this.
    // So all expressions generated by Calcite are materialized and this method
    // takes care of that.
    multiAggInfo.materializeRequiredSlots(analyzer, new ExprSubstitutionMap());

    AggregateInfo aggInfo = multiAggInfo.hasTransposePhase() ?
        multiAggInfo.getTransposeAggInfo() : multiAggInfo.getAggClasses().get(0);

    if (input instanceof ImpalaHdfsScanNode && countStarExpr != null) {
      List<FunctionCallExpr> newAggExprs =
          ((ImpalaHdfsScanNode) input).checkAndApplyCountStarOptimization(multiAggInfo,
            analyzer, countStarExpr);
      if (newAggExprs != null) {
        // Since the count(*) optimization was applied, use the new aggregate expr
        aggExprs = newAggExprs;
      }
    }

    aggNode = getTopLevelAggNode(input, multiAggInfo, ctx);

    this.outputExprs = createMappedOutputExprs(multiAggInfo, groupingExprs, aggExprs,
        aggInfo.getResultTupleDesc().getSlots());
    // This is the only way to shove in the "having" filter into the aggregate node.
    // In the init clause, the aggregate node calls into the analyzer to get all remaining
    // unassigned conjuncts.
    ImpalaConjuncts conjuncts = ImpalaConjuncts.create(filter, analyzer, this);
    analyzer.setUnassignedConjuncts(conjuncts.getImpalaNonPartitionConjuncts());
    aggNode.init(analyzer);
    analyzer.clearUnassignedConjuncts();

    return aggNode;
  }

  private List<Expr> getGroupingExprs() {
    List<Expr> exprs = Lists.newArrayList();

    Preconditions.checkState(getInputs().size() == 1);
    ImpalaPlanRel input = getImpalaRelInput(0);
    for (int group : this.aggregate.getGroupSet()) {
      exprs.add(input.getExpr(group));
    }
    return exprs;
  }

  private List<List<Expr>> getGroupingSets(Analyzer analyzer) throws HiveException {
    List<List<Expr>> allGroupSetExprs = Lists.newArrayList();

    Preconditions.checkState(getInputs().size() == 1);
    ImpalaPlanRel input = getImpalaRelInput(0);
    ImmutableList<ImmutableBitSet> groupSets = aggregate.getGroupSets();
    if (groupSets.size() == 0) {
      return allGroupSetExprs;
    }

    ImmutableBitSet groupSet = aggregate.getGroupSet();
    Map<Integer, Type> gbExprTypes = new HashMap<>();
    for (int groupByField : groupSet) {
      Expr gbExpr = input.getExpr(groupByField);
      gbExprTypes.put(groupByField, gbExpr.getType());
    }

    for (int i = 0; i < groupSets.size(); i++) {
      ImmutableBitSet presentGbFields = groupSets.get(i);
      Map<Integer, Expr> oneGroupExprs = new LinkedHashMap<>();
      for (int groupByField : groupSet) {
        if (presentGbFields.get(groupByField)) {
          oneGroupExprs.put(groupByField, input.getExpr(groupByField));
        } else {
          // fill missing slots with null with the appropriate data type
          Type nullType = gbExprTypes.get(groupByField);
          ImpalaNullLiteral nullLiteral = new ImpalaNullLiteral(analyzer, nullType);
          oneGroupExprs.put(groupByField, nullLiteral);
        }
      }
      allGroupSetExprs.add(new ArrayList<>(oneGroupExprs.values()));
    }
    return allGroupSetExprs;
  }

  /**
   * Get the top level agg node and create the needed agg nodes along the way.
   * Impala can break up the aggregation node up into multiple phases. This
   * code is similar to code that is found in SingleNodePlanner with the exception
   * that it creates ImpalaAggNode, a subclass of AggregationNode.
   */
  private PlanNode getTopLevelAggNode(PlanNode input, MultiAggregateInfo multiAggInfo,
      ImpalaPlannerContext ctx) throws ImpalaException, HiveException, MetaException {
    ImpalaBasicAnalyzer analyzer = (ImpalaBasicAnalyzer) ctx.getRootAnalyzer();

    this.nodeInfo = new ImpalaNodeInfo();

    AggregationNode firstPhaseAgg = new ImpalaAggNode(ctx.getNextNodeId(), input, multiAggInfo,
        MultiAggregateInfo.AggPhase.FIRST, nodeInfo, ctx);

    if (!multiAggInfo.hasSecondPhase() && !multiAggInfo.hasTransposePhase()) {
      // caller will call the "init" method
      return firstPhaseAgg;
    }

    firstPhaseAgg.init(analyzer);
    if (!multiAggInfo.getIsGroupingSet()) {
      firstPhaseAgg.unsetNeedsFinalize();
    }
    firstPhaseAgg.setIntermediateTuple();

    AggregationNode secondPhaseAgg = null;
    if (multiAggInfo.hasSecondPhase()) {
      // A second phase aggregation is needed when there is an aggregation on two different
      // groups but Calcite produces a single aggregation RelNode
      // (e.g. select count(distinct c1), min(c2) from tbl).
      secondPhaseAgg = new ImpalaAggNode(ctx.getNextNodeId(), firstPhaseAgg, multiAggInfo, MultiAggregateInfo.AggPhase.SECOND,
          nodeInfo, ctx);
      if (!multiAggInfo.hasTransposePhase()) {
        // caller will call the "init" method
        return secondPhaseAgg;
      }
      secondPhaseAgg.init(analyzer);
    }

    AggregationNode transposePhaseAgg = firstPhaseAgg;
    if (multiAggInfo.hasTransposePhase()) {
      AggregationNode inputAgg = secondPhaseAgg != null ? secondPhaseAgg : firstPhaseAgg;
      // A transpose aggregation is needed for grouping sets
      transposePhaseAgg =
          new ImpalaAggNode(ctx.getNextNodeId(), inputAgg, multiAggInfo, MultiAggregateInfo.AggPhase.TRANSPOSE, nodeInfo,
              ctx);
    }
    // caller will call the "init" method
    return transposePhaseAgg;
  }

  private List<FunctionCallExpr> getAggregateExprs(ImpalaPlannerContext ctx) throws HiveException,
      AnalysisException {
    List<FunctionCallExpr> exprs = Lists.newArrayList();
    ImpalaPlanRel input = getImpalaRelInput(0);
    for (AggregateCall aggCall : this.aggregate.getAggCallList()) {
      if (aggCall.getAggregation() == HiveGroupingID.INSTANCE) {
        generateGroupingId = true;
        continue;
      }
      List<Expr> operands = aggCall.getArgList()
          .stream()
          .map(input::getExpr)
          .collect(Collectors.toList());
      Function fn = getFunction(aggCall);

      Type impalaRetType = ImpalaTypeConverter.createImpalaType(aggCall.getType());
      FunctionParams params;
      boolean isCountStar = false;
      if (aggCall.getAggregation().getKind() == SqlKind.COUNT &&
          aggCall.getArgList().size() == 0) {
        params = FunctionParams.createStarParam();
        isCountStar = true;
      } else {
        params = new FunctionParams(aggCall.isDistinct(), operands);
      }
      FunctionCallExpr e =
          new ImpalaFunctionCallExpr(ctx.getRootAnalyzer(), fn, params, null, impalaRetType);
      if (isCountStar && countStarExpr == null) {
        countStarExpr = e;
      }
      exprs.add(e);
    }
    return exprs;
  }

  private Function getFunction(AggregateCall aggCall)
      throws HiveException {
    RelDataType retType = aggCall.getType();
    SqlAggFunction aggFunction = aggCall.getAggregation();
    List<RelDataType> operandTypes = Lists.newArrayList();
    ImpalaPlanRel input = getImpalaRelInput(0);
    for (int i : aggCall.getArgList()) {
      RelDataType relDataType = input.getRowType().getFieldList().get(i).getType();
      operandTypes.add(relDataType);
    }
    return ImpalaRelUtil.getAggregateFunction(aggFunction, retType, operandTypes);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    RelWriter rw = super.explainTerms(pw);
    rw = rw.item("group", aggregate.getGroupSet())
        .itemIf("groups", aggregate.getGroupSets(), aggregate.getGroupType() != Aggregate.Group.SIMPLE)
        .itemIf("indicator", !Aggregate.noIndicator(aggregate), !Aggregate.noIndicator(aggregate))
        .itemIf("aggs", aggregate.getAggCallList(), rw.nest());
    if (!rw.nest()) {
      for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
        AggregateCall e = aggregate.getAggCallList().get(i);
        rw.item(Util.first(e.name, "agg#" + i), e);
      }
    }
    if (filter != null) {
      rw = rw.item("condition", filter.getCondition());
    }
    return rw;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return filter != null ?
        mq.getNonCumulativeCost(filter) : mq.getNonCumulativeCost(aggregate);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return filter != null ?
        mq.getRowCount(filter) : mq.getRowCount(aggregate);
  }

  /**
   * Create the output expressions for the ImpalaAggregateRel.
   * Impala can change the order in their MultiAggInfo object. So the SlotDescriptors
   * do not necessarily line up with the indexes.   So we need to walk through the
   * grouping expressions and the aggExprs of the agg node and match them up with the
   * corresponding SlotRef. If there is a groupingId column, that slot will be at the end.
   */
  public ImmutableMap<Integer, Expr> createMappedOutputExprs(MultiAggregateInfo multiAggInfo,
      List<Expr> groupingExprs, List<FunctionCallExpr> aggExprs, List<SlotDescriptor> slotDescs) {
    Map<Integer, Expr> exprs = Maps.newLinkedHashMap();

    int numSlots = groupingExprs.size() + aggExprs.size() + (generateGroupingId ? 1 : 0);
    Preconditions.checkState(slotDescs.size() >= numSlots);

    int index = 0;

    for (Expr e : groupingExprs) {
      Expr slotRefExpr = multiAggInfo.getOutputSmap().get(e);
      Preconditions.checkNotNull(slotRefExpr);
      exprs.put(index++, slotRefExpr);
    }

    for (FunctionCallExpr e : aggExprs) {
      Expr slotRefExpr = multiAggInfo.getOutputSmap().get(e);
      Preconditions.checkNotNull(slotRefExpr);
      exprs.put(index++, slotRefExpr);
    }

    if (generateGroupingId) {
      exprs.put(index, new SlotRef(slotDescs.get(slotDescs.size() - 1)));
    }

    return ImmutableMap.copyOf(exprs);
  }
}
