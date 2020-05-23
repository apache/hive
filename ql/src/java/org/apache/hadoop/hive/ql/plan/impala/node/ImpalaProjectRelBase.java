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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.plan.impala.ImpalaPlannerContext;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaAnalyticExpr;
import org.apache.hadoop.hive.ql.plan.impala.expr.ImpalaFunctionCallExpr;
import org.apache.hadoop.hive.ql.plan.impala.funcmapper.ImpalaTypeConverter;
import org.apache.hadoop.hive.ql.plan.impala.rex.ImpalaRexVisitor;
import org.apache.hadoop.hive.ql.plan.impala.rex.ImpalaRexVisitor.ImpalaInferMappingRexVisitor;
import org.apache.hadoop.hive.ql.plan.impala.rex.ImpalaRexVisitor.ImpalaProvidedMappingRexVisitor;
import org.apache.impala.analysis.AnalyticExpr;
import org.apache.impala.analysis.AnalyticInfo;
import org.apache.impala.analysis.AnalyticWindow;
import org.apache.impala.analysis.AnalyticWindow.Boundary;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.analysis.OrderByElement;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.AnalyticPlanner;
import org.apache.impala.planner.PlanNode;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Impala Project relnode base. This serves as a shared base class for
 * Impala Project relnodes.
 */
abstract public class ImpalaProjectRelBase extends ImpalaPlanRel {

  protected static final Logger LOG = LoggerFactory.getLogger(ImpalaProjectRelBase.class);

  protected PlanNode planNode = null;
  protected final HiveProject project;

  public ImpalaProjectRelBase(HiveProject project) {
    super(project.getCluster(), project.getTraitSet(), project.getInputs(), project.getRowType());
    this.project = project;
  }

  /**
   * Translate the RexNode expressions in the Project to Impala Exprs.
   */
  protected ImmutableMap<Integer, Expr> createProjectExprs(ImpalaPlannerContext ctx)
      throws HiveException, ImpalaException, MetaException {
    ImpalaPlanRel inputRel = getImpalaRelInput(0);

    List<RexOver> overExprs = gatherRexOver(project.getChildExps());
    ImpalaRexVisitor visitor = overExprs.isEmpty()
        ? generateVisitor(ctx, inputRel)
        : generateAnalyticVisitor(ctx, inputRel, overExprs);

    Map<Integer, Expr> projectExprs = new LinkedHashMap<>();
    int index = 0;
    for (RexNode rexNode : project.getProjects()) {
      projectExprs.put(index++, rexNode.accept(visitor));
    }
    return ImmutableMap.copyOf(projectExprs);
  }

  private ImpalaInferMappingRexVisitor generateVisitor(ImpalaPlannerContext ctx,
      ImpalaPlanRel inputRel) {
    return new ImpalaInferMappingRexVisitor(ctx.getRootAnalyzer(),
        ImmutableList.of(inputRel));
  }

  private ImpalaProvidedMappingRexVisitor generateAnalyticVisitor(ImpalaPlannerContext ctx,
      ImpalaPlanRel inputRel, List<RexOver> overExprs)
      throws ImpalaException, HiveException, MetaException {
    PlanNode inputPlanNode = inputRel.getPlanNode(ctx);
    // We generate the analytic expressions
    List<AnalyticExpr> analyticExprs = new ArrayList<>();
    for (RexOver over : overExprs) {
      analyticExprs.add(getAnalyticExpr(over, ctx, inputRel));
    }
    // We create the analytic info from them
    AnalyticInfo analyticInfo = AnalyticInfo.create(
        analyticExprs, ctx.getRootAnalyzer());
    // Note: There should not be duplicates anymore after
    //       Calcite planner has run
    Preconditions.checkArgument(overExprs.size() == analyticExprs.size());
    AnalyticPlanner analyticPlanner =
        new AnalyticPlanner(analyticInfo, ctx.getRootAnalyzer(), ctx);
    // TODO: We should either pass the existing partitioning coming from
    //       Aggregate operator so we can avoid repartitioning unless
    //       required, or alternatively, take this into account in Calcite
    //       rules (CDPD-11379)
    planNode = analyticPlanner.createSingleNodePlan(
        inputPlanNode, Collections.emptyList(), new ArrayList<>());
    // Gather mappings from nodes created by analytic planner
    ExprSubstitutionMap logicalToPhysical = planNode.getOutputSmap();
    // We populate the outputs from the expressions
    Map<RexNode, Expr> mapping = new LinkedHashMap<>();
    for (int pos : HiveCalciteUtil.getInputRefs(project.getChildExps())) {
      mapping.put(RexInputRef.of(pos, inputRel.getRowType()),
          logicalToPhysical.get(inputRel.getExpr(pos)));
    }
    for (int i = 0; i < analyticExprs.size(); i++) {
      SlotDescriptor slotDesc =
          analyticInfo.getOutputTupleDesc().getSlots().get(i);
      SlotRef logicalOutputSlot = new SlotRef(slotDesc);
      mapping.put(overExprs.get(i),
          logicalToPhysical.get(logicalOutputSlot));
    }
    LOG.debug("Mapping from nodes created by analytic planner : {}", mapping);
    return new ImpalaProvidedMappingRexVisitor(ctx.getRootAnalyzer(),
        mapping);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    RelWriter rw = super.explainTerms(pw);
    if (rw.nest()) {
      rw.item("fields", project.getRowType().getFieldNames());
      rw.item("exprs", project.getChildExps());
    } else {
      int i = 0;
      for (RelDataTypeField e : project.getRowType().getFieldList()) {
        String fieldName = e.getName();
        if (fieldName == null) {
          fieldName = "field#" + i;
        }
        rw.item(fieldName, project.getChildExps().get(i));
        i++;
      }
    }

    // If we're generating a digest, include the rowtype. If two projects
    // differ in return type, we don't want to regard them as equivalent,
    // otherwise we will try to put rels of different types into the same
    // planner equivalence set.
    if ((rw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES)
        && false) {//Always false?
      rw.item("type", project.getRowType());
    }

    return rw;
  }

  private AnalyticExpr getAnalyticExpr(RexOver rexOver, ImpalaPlannerContext ctx, ImpalaPlanRel inputRel)
      throws HiveException {
    final RexWindow rexWindow = rexOver.getWindow();
    // First parameter is the function call
    Function fn = getFunction(rexOver);
    Type impalaRetType = ImpalaTypeConverter.createImpalaType(rexOver.getType());
    List<Expr> operands = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(rexOver.operands)) {
      operands = ImpalaRelUtil.getExprs(rexOver.operands,
          ctx.getRootAnalyzer(), inputRel);
    }
    FunctionCallExpr fnCall = new ImpalaFunctionCallExpr(
        ctx.getRootAnalyzer(), fn, operands, null, impalaRetType);
    // Second parameter are the partition expressions
    List<Expr> partitionExprs = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(rexWindow.partitionKeys)) {
      partitionExprs = ImpalaRelUtil.getExprs(rexWindow.partitionKeys,
          ctx.getRootAnalyzer(), inputRel);
    }
    // Third parameter are the sort expressions
    List<OrderByElement> orderByElements = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(rexWindow.orderKeys)) {
      for (RexFieldCollation ok : rexWindow.orderKeys) {
        OrderByElement orderByElement = new OrderByElement(
            ImpalaRelUtil.getExpr(
                ok.left, ctx.getRootAnalyzer(), inputRel),
            ok.getDirection() == RelFieldCollation.Direction.ASCENDING,
            ok.right.contains(SqlKind.NULLS_FIRST));
        orderByElements.add(orderByElement);
      }
    }
    // Fourth parameter is the window frame spec
    Boundary lBoundary = getWindowBoundary(rexWindow.getLowerBound(),
        ctx, inputRel);
    Boundary rBoundary = getWindowBoundary(rexWindow.getUpperBound(),
        ctx, inputRel);
    AnalyticWindow window = new AnalyticWindow(
        rexWindow.isRows() ? AnalyticWindow.Type.ROWS : AnalyticWindow.Type.RANGE,
        lBoundary, rBoundary);

    return new ImpalaAnalyticExpr(ctx.getRootAnalyzer(), fnCall, partitionExprs, orderByElements, window);
  }

  private Boundary getWindowBoundary(RexWindowBound wb, ImpalaPlannerContext ctx, ImpalaPlanRel inputRel) {
    // At this stage, FENG should have filled in the bound
    Preconditions.checkNotNull(wb);
    if (wb.isCurrentRow()) {
      return new Boundary(AnalyticWindow.BoundaryType.CURRENT_ROW, null);
    } else {
      if (wb.isPreceding()) {
        if (wb.isUnbounded()) {
          return new Boundary(AnalyticWindow.BoundaryType.UNBOUNDED_PRECEDING, null);
        }
        return new Boundary(AnalyticWindow.BoundaryType.PRECEDING,
            ImpalaRelUtil.getExpr(wb.getOffset(), ctx.getRootAnalyzer(), inputRel),
            new BigDecimal(RexLiteral.intValue(wb.getOffset())));
      } else {
        if (wb.isUnbounded()) {
          return new Boundary(AnalyticWindow.BoundaryType.UNBOUNDED_FOLLOWING, null);
        }
        return new Boundary(AnalyticWindow.BoundaryType.FOLLOWING,
            ImpalaRelUtil.getExpr(wb.getOffset(), ctx.getRootAnalyzer(), inputRel),
            new BigDecimal(RexLiteral.intValue(wb.getOffset())));
      }
    }
  }

  private List<RexOver> gatherRexOver(List<RexNode> exprs) {
    final List<RexOver> result = new ArrayList<>();
    RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true) {
      public Void visitOver(RexOver over) {
        result.add(over);
        return super.visitOver(over);
      }
    };
    for (RexNode expr : exprs) {
      expr.accept(visitor);
    }
    return result;
  }

  private Function getFunction(RexOver exp)
      throws HiveException {
    RelDataType retType = exp.getType();
    SqlAggFunction aggFunction = exp.getAggOperator();
    List<RelDataType> operandTypes = Lists.newArrayList();
    for (RexNode operand : exp.operands) {
      operandTypes.add(operand.getType());
    }
    return ImpalaRelUtil.getAggregateFunction(aggFunction, retType, operandTypes);
  }
}
