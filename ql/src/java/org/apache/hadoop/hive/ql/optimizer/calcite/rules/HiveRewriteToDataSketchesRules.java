/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.exec.DataSketchesFunctions;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter;
import org.apache.hive.plugin.api.HiveUDFPlugin.UDFDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * This rule could rewrite aggregate calls to be calculated using sketch based functions.
 *
 * <br/>
 * Currently it can rewrite:
 * <ul>
 *  <li>{@code count(distinct(x))} to distinct counting sketches
 *    <pre>
 *     SELECT COUNT(DISTINCT id) FROM sketch_input;
 *       ⇒ SELECT ROUND(ds_hll_estimate(ds_hll_sketch(id))) FROM sketch_input;
 *    </pre>
 *  </li>
 *  <li>{@code percentile_disc(0.2) within group (order by id)}
 *    <pre>
 *     SELECT PERCENTILE_DISC(0.2) WITHIN GROUP(ORDER BY ID) FROM sketch_input;
 *       ⇒ SELECT ds_kll_quantile(ds_kll_sketch(CAST(id AS FLOAT)), 0.2) FROM sketch_input;
 *    </pre>
 *  </li>
 *  <li>{@code cume_dist() over (order by id)}
 *    <pre>
 *     SELECT id, CUME_DIST() OVER (ORDER BY id) FROM sketch_input;
 *       ⇒ SELECT id, CUME_DIST() OVER (ORDER BY id),
 *           ds_kll_cdf(ds, CAST(id AS DOUBLE) - 0.5/ds_kll_cdf(ds) )[0]
 *         FROM sketch_input JOIN (
 *           SELECT ds_quantile_doubles_sketch(CAST(id AS DOUBLE)) AS ds FROM sketch_input
 *         ) q;
 *    </pre>
 *  </li>
 *  </ul>
 */
public final class HiveRewriteToDataSketchesRules {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveRewriteToDataSketchesRules.class);

  /**
   * Generic support for rewriting an Aggregate into a chain of Project->Aggregate->Project.
   * <p>
   *   The transformation here works on Aggregate nodes; the operations done are the following:
   * </p>
   * <ol>
   * <li>Identify candidate aggregate calls</li>
   * <li>A new Project is inserted below the Aggregate; to help with data pre-processing</li>
   * <li>A new Aggregate is created in which the aggregation is done by the sketch function</li>
   * <li>A new Project is inserted on top of the Aggregate; which unwraps the resulting estimation from the sketch representation</li>
   * </ol>
   */
  private static abstract class AggregateToProjectAggregateProject extends RelOptRule {

    private final ProjectFactory projectFactory;

    public AggregateToProjectAggregateProject(RelOptRuleOperand operand) {
      super(operand);
      projectFactory = HiveRelFactories.HIVE_PROJECT_FACTORY;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      VbuilderPAP vb = processCall(call);
      if (vb == null) {
        return;
      }

      Aggregate aggregate = vb.aggregate;
      if (aggregate.getAggCallList().equals(vb.newAggCalls)) {
        // rule didn't make any changes
        return;
      }

      List<AggregateCall> newAggCalls = vb.newAggCalls;
      List<String> fieldNames = new ArrayList<String>();
      for (int i = 0; i < vb.newProjectsBelow.size(); i++) {
        fieldNames.add("ff_" + i);
      }
      RelNode newProjectBelow = projectFactory.createProject(aggregate.getInput(), vb.newProjectsBelow, fieldNames);

      RelNode newAgg = aggregate.copy(aggregate.getTraitSet(), newProjectBelow, aggregate.getGroupSet(),
          aggregate.getGroupSets(), newAggCalls);

      RelNode newProject =
          projectFactory.createProject(newAgg, vb.newProjectsAbove, aggregate.getRowType().getFieldNames());

      call.transformTo(newProject);
      return;

    }

    protected abstract VbuilderPAP processCall(RelOptRuleCall call);

    private static abstract class VbuilderPAP {
      protected final RexBuilder rexBuilder;

      /** The original aggregate RelNode */
      protected final Aggregate aggregate;
      /** The list of the new aggregations */
      protected final List<AggregateCall> newAggCalls;
      /**
       * The new projections expressions inserted above the aggregate
       *
       *  These projections should do the neccessary conversions to behave like the original aggregate.
       *  Most important here is to CAST the final result to the same type as the original aggregate was producing.
       */
      protected final List<RexNode> newProjectsAbove;
      /** The new projections expressions inserted belove the aggregate
       *
       * These projections could be used to prepocess the incoming datastream.
       * For example a CAST might need to be injected.
       */
      protected final List<RexNode> newProjectsBelow;

      private final String sketchClass;

      protected VbuilderPAP(Aggregate aggregate, String sketchClass) {
        this.aggregate = aggregate;
        this.sketchClass = sketchClass;
        newAggCalls = new ArrayList<AggregateCall>();
        newProjectsAbove = new ArrayList<RexNode>();
        newProjectsBelow = new ArrayList<RexNode>();
        rexBuilder = aggregate.getCluster().getRexBuilder();
      }

      protected final void processAggregate() {
        // add identity projections
        addProjectedFields();

        for (AggregateCall aggCall : aggregate.getAggCallList()) {
          processAggCall(aggCall);
        }
      }

      private final void addProjectedFields() {
        for (int i = 0; i < aggregate.getGroupCount(); i++) {
          newProjectsAbove.add(rexBuilder.makeInputRef(aggregate, i));
        }
        int numInputFields = aggregate.getInput().getRowType().getFieldCount();
        for (int i = 0; i < numInputFields; i++) {
          newProjectsBelow.add(rexBuilder.makeInputRef(aggregate.getInput(), i));
        }
      }

      private final void processAggCall(AggregateCall aggCall) {
        if (isApplicable(aggCall)) {
          rewrite(aggCall);
        } else {
          appendAggCall(aggCall);
        }
      }

      private final void appendAggCall(AggregateCall aggCall) {
        RexNode projRex = rexBuilder.makeInputRef(aggCall.getType(), newProjectsAbove.size());

        newAggCalls.add(aggCall);
        newProjectsAbove.add(projRex);
      }

      protected final SqlOperator getSqlOperator(String fnName) {
        UDFDescriptor fn = DataSketchesFunctions.INSTANCE.getSketchFunction(sketchClass, fnName);
        if (!fn.getCalciteFunction().isPresent()) {
          throw new RuntimeException(fn.toString() + " doesn't have a Calcite function associated with it");
        }
        return fn.getCalciteFunction().get();
      }

      abstract boolean isApplicable(AggregateCall aggCall);

      abstract void rewrite(AggregateCall aggCall);

    }
  };

  public static class CountDistinctRewrite extends AggregateToProjectAggregateProject {

    private final String sketchType;

    public CountDistinctRewrite(String sketchType) {
      super(operand(HiveAggregate.class, any()));
      this.sketchType = sketchType;
    }

    @Override
    protected VBuilderPAP processCall(RelOptRuleCall call) {
      final Aggregate aggregate = call.rel(0);

      if (aggregate.getGroupSets().size() != 1) {
        // not yet supported
        return null;
      }

      return new VBuilderPAP(aggregate, sketchType);
    }

    private static class VBuilderPAP extends AggregateToProjectAggregateProject.VbuilderPAP {

      protected VBuilderPAP(Aggregate aggregate, String sketchClass) {
        super(aggregate, sketchClass);
        processAggregate();
      }

      @Override
      boolean isApplicable(AggregateCall aggCall) {
        return aggCall.isDistinct() && aggCall.getArgList().size() == 1
            && aggCall.getAggregation().getKind() == SqlKind.COUNT && !aggCall.hasFilter();
      }

      @Override
      void rewrite(AggregateCall aggCall) {
        RelDataType origType = aggregate.getRowType().getFieldList().get(newProjectsAbove.size()).getType();

        Integer argIndex = aggCall.getArgList().get(0);
        RexNode call = rexBuilder.makeInputRef(aggregate.getInput(), argIndex);
        newProjectsBelow.add(call);

        SqlAggFunction aggFunction = (SqlAggFunction) getSqlOperator(DataSketchesFunctions.DATA_TO_SKETCH);
        boolean distinct = false;
        boolean approximate = true;
        boolean ignoreNulls = true;
        List<Integer> argList = Lists.newArrayList(newProjectsBelow.size() - 1);
        int filterArg = aggCall.filterArg;
        RelCollation collation = aggCall.getCollation();
        RelDataType type = rexBuilder.deriveReturnType(aggFunction, Collections.emptyList());
        String name = aggFunction.getName();

        AggregateCall newAgg = AggregateCall.create(aggFunction, distinct, approximate, ignoreNulls, argList, filterArg,
            collation, type, name);

        SqlOperator projectOperator = getSqlOperator(DataSketchesFunctions.SKETCH_TO_ESTIMATE);
        RexNode projRex = rexBuilder.makeInputRef(newAgg.getType(), newProjectsAbove.size());
        projRex = rexBuilder.makeCall(projectOperator, ImmutableList.of(projRex));
        projRex = rexBuilder.makeCall(SqlStdOperatorTable.ROUND, ImmutableList.of(projRex));
        projRex = rexBuilder.makeCast(origType, projRex);

        newAggCalls.add(newAgg);
        newProjectsAbove.add(projRex);
      }
    }
  }

  public static class PercentileDiscRewrite extends AggregateToProjectAggregateProject {

    private final String sketchType;

    public PercentileDiscRewrite(String sketchType) {
      super(operand(HiveAggregate.class, operand(HiveProject.class, any())));
      this.sketchType = sketchType;
    }

    @Override
    protected VBuilderPAP processCall(RelOptRuleCall call) {
      final Aggregate aggregate = call.rel(0);
      final Project project = call.rel(1);

      if (aggregate.getGroupSets().size() != 1) {
        // not yet supported
        return null;
      }

      return new VBuilderPAP(aggregate, project, sketchType);
    }

    private static class VBuilderPAP extends AggregateToProjectAggregateProject.VbuilderPAP {

      private final Project aggInput;

      protected VBuilderPAP(Aggregate aggregate, Project project, String sketchClass) {
        super(aggregate, sketchClass);
        aggInput = project;
        processAggregate();
      }

      @Override
      boolean isApplicable(AggregateCall aggCall) {
        if ((aggInput instanceof Project)
            && !aggCall.isDistinct() && aggCall.getArgList().size() == 4
            && aggCall.getAggregation().getName().equalsIgnoreCase("percentile_disc")
            && !aggCall.hasFilter()) {
          List<Integer> argList = aggCall.getArgList();
          RexNode orderLiteral = aggInput.getChildExps().get(argList.get(2));
          if (orderLiteral.isA(SqlKind.LITERAL)) {
            RexLiteral lit = (RexLiteral) orderLiteral;
            return BigDecimal.valueOf(1).equals(lit.getValue());
          }
        }
        return false;
      }

      @Override
      void rewrite(AggregateCall aggCall) {
        RelDataType origType = aggregate.getRowType().getFieldList().get(newProjectsAbove.size()).getType();

        Integer argIndex = aggCall.getArgList().get(1);
        RexNode call = rexBuilder.makeInputRef(aggregate.getInput(), argIndex);

        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        RelDataType notNullFloatType = typeFactory.createSqlType(SqlTypeName.FLOAT);
        RelDataType floatType = typeFactory.createTypeWithNullability(notNullFloatType, true);

        call = rexBuilder.makeCast(floatType, call);
        newProjectsBelow.add(call);

        SqlAggFunction aggFunction = (SqlAggFunction) getSqlOperator(DataSketchesFunctions.DATA_TO_SKETCH);
        boolean distinct = false;
        boolean approximate = true;
        boolean ignoreNulls = true;
        List<Integer> argList = Lists.newArrayList(newProjectsBelow.size() - 1);
        int filterArg = aggCall.filterArg;
        RelCollation collation = aggCall.getCollation();
        RelDataType type = rexBuilder.deriveReturnType(aggFunction, Collections.emptyList());
        String name = aggFunction.getName();

        AggregateCall newAgg = AggregateCall.create(aggFunction, distinct, approximate, ignoreNulls, argList, filterArg,
            collation, type, name);

        Integer origFractionIdx = aggCall.getArgList().get(0);
        RexNode fraction = aggInput.getChildExps().get(origFractionIdx);
        fraction = rexBuilder.makeCast(floatType, fraction);

        SqlOperator projectOperator = getSqlOperator(DataSketchesFunctions.GET_QUANTILE);
        RexNode projRex = rexBuilder.makeInputRef(newAgg.getType(), newProjectsAbove.size());
        projRex = rexBuilder.makeCall(projectOperator, ImmutableList.of(projRex, fraction));
        projRex = rexBuilder.makeCast(origType, projRex);

        newAggCalls.add(newAgg);
        newProjectsAbove.add(projRex);
      }
    }
  }

  /**
   * Generic support for rewriting Windowing expression into a different form usually using joins.
   */
  private static abstract class WindowingToProjectAggregateJoinProject extends RelOptRule {

    protected final String sketchType;

    public WindowingToProjectAggregateJoinProject(String sketchType) {
      super(operand(HiveProject.class, any()));
      this.sketchType = sketchType;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {

      final Project project = call.rel(0);

      VbuilderPAP vb = buildProcessor(call);
      RelNode newProject = vb.processProject(project);

      if (newProject instanceof Project && ((Project) newProject).getChildExps().equals(project.getChildExps())) {
        return;
      } else {
        call.transformTo(newProject);
      }
    }

    protected abstract VbuilderPAP buildProcessor(RelOptRuleCall call);


    protected static abstract class VbuilderPAP {
      private final String sketchClass;
      protected final RelBuilder relBuilder;
      protected final RexBuilder rexBuilder;

      protected VbuilderPAP(String sketchClass, RelBuilder relBuilder) {
        this.sketchClass = sketchClass;
        this.relBuilder = relBuilder;
        rexBuilder = relBuilder.getRexBuilder();
      }

      protected RelNode processProject(Project project) {
        relBuilder.push(project.getInput());
        // FIXME later use shuttle
        List<RexNode> newProjects = new ArrayList<RexNode>();
        for (RexNode expr : project.getChildExps()) {
          newProjects.add(processCall(expr));
        }
        relBuilder.project(newProjects);
        return relBuilder.build();
      }

      private final RexNode processCall(RexNode expr) {
        if (expr instanceof RexOver) {
          RexOver over = (RexOver) expr;
          if (isApplicable(over)) {
            return rewrite(over);
          }
        }
        return expr;
      }

      protected final SqlOperator getSqlOperator(String fnName) {
        UDFDescriptor fn = DataSketchesFunctions.INSTANCE.getSketchFunction(sketchClass, fnName);
        if (!fn.getCalciteFunction().isPresent()) {
          throw new RuntimeException(fn.toString() + " doesn't have a Calcite function associated with it");
        }
        return fn.getCalciteFunction().get();
      }

      abstract RexNode rewrite(RexOver expr);

      abstract boolean isApplicable(RexOver expr);

    }

  }

  public static class CumeDistRewrite extends WindowingToProjectAggregateJoinProject {

    public CumeDistRewrite(String sketchType) {
      super(sketchType);
    }

    @Override
    protected VbuilderPAP buildProcessor(RelOptRuleCall call) {
      return new VB(sketchType, call.builder());
    }

    private static class VB extends VbuilderPAP {

      protected VB(String sketchClass, RelBuilder relBuilder) {
        super(sketchClass, relBuilder);
      }

      @Override
      boolean isApplicable(RexOver over) {
        SqlAggFunction aggOp = over.getAggOperator();
        RexWindow window = over.getWindow();
        if (aggOp.getName().equalsIgnoreCase("cume_dist") && window.orderKeys.size() == 1
            && window.getLowerBound().isUnbounded() && window.getUpperBound().isUnbounded()) {
          return true;
        }
        return false;
      }

      @Override
      RexNode rewrite(RexOver over) {

        over.getOperands();
        RexWindow w = over.getWindow();
        // FIXME: NULLs first/last collation stuff
        // we don't really support nulls in aggregate/etc...they are actually ignored
        // so some hack will be needed for NULLs anyway..
        RexNode orderKey = w.orderKeys.get(0).getKey();
        ImmutableList<RexNode> partitionKeys = w.partitionKeys;

        relBuilder.push(relBuilder.peek());
        RexNode castedKey = rexBuilder.makeCast(getFloatType(), orderKey);

        ImmutableList<RexNode> projExprs = ImmutableList.<RexNode>builder().addAll(partitionKeys).add(castedKey).build();
        relBuilder.project(projExprs);
        ImmutableBitSet groupSets = ImmutableBitSet.range(partitionKeys.size());

        SqlAggFunction aggFunction = (SqlAggFunction) getSqlOperator(DataSketchesFunctions.DATA_TO_SKETCH);
        boolean distinct = false;
        boolean approximate = true;
        boolean ignoreNulls = true;
        List<Integer> argList = Lists.newArrayList(partitionKeys.size());
        int filterArg = -1;
        RelCollation collation = RelCollations.EMPTY;
        RelDataType type = rexBuilder.deriveReturnType(aggFunction, Collections.emptyList());
        String name = aggFunction.getName();
        AggregateCall newAgg = AggregateCall.create(aggFunction, distinct, approximate, ignoreNulls, argList, filterArg,
                      collation, type, name);

        RelNode agg = HiveRelFactories.HIVE_AGGREGATE_FACTORY.createAggregate(
            relBuilder.build(),
            groupSets, ImmutableList.of(groupSets),
            Lists.newArrayList(newAgg));
        relBuilder.push(agg);

        List<RexNode> joinConditions;
        joinConditions = Ord.zip(partitionKeys).stream().map(o -> {
          RexNode f = relBuilder.field(2, 1, o.i);
          return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, o.e, f);
        }).collect(Collectors.toList());
        relBuilder.join(JoinRelType.INNER, joinConditions);

        int sketchFieldIndex = relBuilder.peek().getRowType().getFieldCount() - 1;
        // long story short: CAST(CDF(X+EPS)[0] AS FLOAT)
        RexInputRef sketchInputRef = relBuilder.field(sketchFieldIndex);
        SqlOperator projectOperator = getSqlOperator(DataSketchesFunctions.GET_CDF);
        RexNode projRex = relBuilder.literal(Float.MIN_NORMAL);
        projRex = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, castedKey, projRex);
        projRex = rexBuilder.makeCast(getFloatType(), castedKey);
        projRex = rexBuilder.makeCall(projectOperator, ImmutableList.of(sketchInputRef, projRex));
        projRex = getItemOperator(projRex, relBuilder.literal(0));
        projRex = rexBuilder.makeCast(over.getType(), projRex);

        return projRex;
      }

      private RexNode getItemOperator(RexNode arr, RexNode offset) {

        if(getClass().desiredAssertionStatus()) {
          try {
            SqlKind.class.getField("ITEM");
            throw new RuntimeException("bind SqlKind.ITEM instead of this workaround - C1.23 a02155a70a");
           } catch(NoSuchFieldException e) {
             // ignore
          }
        }

        try {
        SqlOperator indexFn = SqlFunctionConverter.getCalciteFn("index",
            ImmutableList.of(arr.getType(),offset.getType()),
            arr.getType().getComponentType(), true, false);
          RexNode call = rexBuilder.makeCall(indexFn, arr, offset);
          return call;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      private RelDataType getFloatType() {
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        RelDataType notNullFloatType = typeFactory.createSqlType(SqlTypeName.FLOAT);
        RelDataType floatType = typeFactory.createTypeWithNullability(notNullFloatType, true);
        return floatType;
      }
    }
  }

  public static void main(String[] args) {
    System.out.println(0.0f + Float.MIN_NORMAL);
  }

}
