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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaBasicAnalyzer;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaPlannerContext;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaFunctionCallExpr;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaNullLiteral;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaTypeConverter;
import org.apache.impala.analysis.AggregateInfo;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.FunctionParams;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.AggregationNode;
import org.apache.impala.planner.PlanNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ImpalaAggregateRel extends ImpalaPlanRel {
  public final HiveAggregate aggregate;

  public final HiveFilter filter;

  public PlanNode aggNode;

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
    MultiAggregateInfo multiAggInfo =
        new MultiAggregateInfo(groupingExprs, aggExprs);

    if (Aggregate.isSimple(aggregate)) {
      // this is a 'simple' aggregate with no grouping sets..use the default analyze call
      multiAggInfo.analyze(analyzer);
    } else {
      List<List<Expr>> groupingSets = getGroupingSets(analyzer);
      multiAggInfo.setIsGroupingSet(true);
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

    aggNode = getTopLevelAggNode(input, multiAggInfo, ctx);

    AggregateInfo aggInfo = multiAggInfo.hasTransposePhase() ?
        multiAggInfo.getTransposeAggInfo() : multiAggInfo.getAggClasses().get(0);

    this.outputExprs = createOutputExprs(aggInfo.getResultTupleDesc().getSlots());
    // This is the only way to shove in the "having" filter into the aggregate node.
    // In the init clause, the aggregate node calls into the analyzer to get all remaining
    // unassigned conjuncts.
    analyzer.setUnassignedConjuncts(getConjuncts(filter, analyzer, this));
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
      // identify which group-by expr is missing from this grouping set
      ImmutableBitSet missingGbFields = groupSet.except(presentGbFields);
      Map<Integer, Expr> oneGroupExprs = new HashMap<>();
      for (int presentField : presentGbFields) {
        oneGroupExprs.put(presentField, input.getExpr(presentField));
      }
      for (int missingField : missingGbFields) {
        // fill missing slots with null with the appropriate data type
        Type nullType = gbExprTypes.get(missingField);
        ImpalaNullLiteral nullLiteral = new ImpalaNullLiteral(analyzer, nullType);
        oneGroupExprs.put(missingField, nullLiteral);
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
      List<Integer> indexes = aggCall.getArgList();
      // index size should be 1, but could be 0 in the case of count(*)
      Preconditions.checkState(indexes.size() <= 1);
      List<Expr> operands = Lists.newArrayList();
      if (indexes.size() == 1) {
        operands.add(input.getExpr(indexes.get(0)));
      }
      Function fn = getFunction(aggCall);

      Type impalaRetType = ImpalaTypeConverter.getImpalaType(aggCall.getType());
      FunctionParams params = new FunctionParams(aggCall.isDistinct(), operands);
      FunctionCallExpr e =
          new ImpalaFunctionCallExpr(ctx.getRootAnalyzer(), fn, params, null, impalaRetType);
      exprs.add(e);
    }
    return exprs;
  }

  private Function getFunction(AggregateCall aggCall)
      throws HiveException {
    RelDataType retType = aggCall.getType();
    SqlAggFunction aggFunction = aggCall.getAggregation();
    List<SqlTypeName> operandTypes = Lists.newArrayList();
    ImpalaPlanRel input = getImpalaRelInput(0);
    for (int i : aggCall.getArgList()) {
      RelDataType relDataType = input.getRowType().getFieldList().get(i).getType();
      operandTypes.add(relDataType.getSqlTypeName());
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
      for (Ord<AggregateCall> ord : Ord.zip(aggregate.getAggCallList())) {
        rw.item(Util.first(ord.e.name, "agg#" + ord.i), ord.e);
      }
    }
    if (filter != null) {
      rw = rw.item("condition", filter.getCondition());
    }
    return rw;
  }
}
