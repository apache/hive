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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.hadoop.hive.ql.exec.DataSketchesFunctions;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hive.plugin.api.HiveUDFPlugin.UDFDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * This rule could rewrite aggregate calls to be calculated using sketch based functions.
 *
 * <br>
 * Currently it can rewrite:
 * <ul>
 *  <li>{@code count(distinct(x))} using {@code CountDistinctRewrite}
 *  </li>
 *  <li>{@code percentile_disc(0.2) within group (order by id)} using {@code PercentileDiscRewrite}
 *  </li>
 *  <li>{@code cume_dist() over (order by id)} using {@code CumeDistRewrite}
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

    public AggregateToProjectAggregateProject(RelOptRuleOperand operand, String description) {
      super(operand, description);
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
      RelNode newProjectBelow = projectFactory.createProject(aggregate.getInput(), Collections.emptyList(), vb.newProjectsBelow, fieldNames);

      RelNode newAgg = aggregate.copy(aggregate.getTraitSet(), newProjectBelow, aggregate.getGroupSet(),
          aggregate.getGroupSets(), newAggCalls);

      RelNode newProject =
          projectFactory.createProject(newAgg, Collections.emptyList(), vb.newProjectsAbove, aggregate.getRowType().getFieldNames());

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

  /**
   * Rewrites {@code count(distinct(x))} to distinct counting sketches.
   *
   * <pre>
   *   SELECT COUNT(DISTINCT id) FROM sketch_input;
   *   ⇒ SELECT ROUND(ds_hll_estimate(ds_hll_sketch(id))) FROM sketch_input;
   * </pre>
   */
  public static class CountDistinctRewrite extends AggregateToProjectAggregateProject {

    private final String sketchType;

    public CountDistinctRewrite(String sketchType) {
      super(operand(HiveAggregate.class, any()),
          "AggregateToProjectAggregateProject(CountDistinctRewrite)");
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
        boolean approximate = false;
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

  /**
   * Rewrites {@code percentile_disc(0.2) within group (order by id)}.
   *
   * <pre>
   *   SELECT PERCENTILE_DISC(0.2) WITHIN GROUP(ORDER BY ID) FROM sketch_input;
   *   ⇒ SELECT ds_kll_quantile(ds_kll_sketch(CAST(id AS FLOAT)), 0.2) FROM sketch_input;
   * </pre>
   */
  public static class PercentileDiscRewrite extends AggregateToProjectAggregateProject {

    private final String sketchType;

    public PercentileDiscRewrite(String sketchType) {
      super(operand(HiveAggregate.class, operand(HiveProject.class, any())),
          "AggregateToProjectAggregateProject(PercentileDiscRewrite)");
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
        if (aggInput != null
            && !aggCall.isDistinct() && aggCall.getArgList().size() == 1
            && aggCall.getAggregation().getName().equalsIgnoreCase("percentile_disc")
            && !aggCall.hasFilter()
            && aggCall.collation.getFieldCollations().size() == 1) {
          RelFieldCollation fieldCollation = aggCall.collation.getFieldCollations().get(0);
          return fieldCollation.getDirection() == RelFieldCollation.Direction.ASCENDING;
        }
        return false;
      }

      @Override
      void rewrite(AggregateCall aggCall) {
        RelDataType origType = aggregate.getRowType().getFieldList().get(newProjectsAbove.size()).getType();

        Integer collationKeyIndex = aggCall.collation.getFieldCollations().get(0).getFieldIndex();
        RexNode call = rexBuilder.makeInputRef(aggregate.getInput(), collationKeyIndex);

        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        RelDataType notNullFloatType = typeFactory.createSqlType(SqlTypeName.FLOAT);
        RelDataType floatType = typeFactory.createTypeWithNullability(notNullFloatType, true);

        call = rexBuilder.makeCast(floatType, call);
        newProjectsBelow.add(call);

        SqlAggFunction aggFunction = (SqlAggFunction) getSqlOperator(DataSketchesFunctions.DATA_TO_SKETCH);
        boolean distinct = false;
        boolean approximate = false;
        boolean ignoreNulls = true;
        List<Integer> argList = Lists.newArrayList(newProjectsBelow.size() - 1);
        int filterArg = aggCall.filterArg;
        RelDataType type = rexBuilder.deriveReturnType(aggFunction, Collections.emptyList());
        String name = aggFunction.getName();

        AggregateCall newAgg = AggregateCall.create(aggFunction, distinct, approximate, ignoreNulls, argList, filterArg,
            RelCollations.EMPTY, type, name);

        Integer origFractionIdx = aggCall.getArgList().get(0);
        RexNode fraction = aggInput.getProjects().get(origFractionIdx);
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

    public WindowingToProjectAggregateJoinProject(String sketchType, String description) {
      super(operand(HiveProject.class, any()), HiveRelFactories.HIVE_BUILDER, description);
      this.sketchType = sketchType;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);

      VbuilderPAP vb = buildProcessor(call);
      RelNode newProject = vb.processProject(project);

      if (newProject == project) {
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

      final class ProcessShuttle extends RexShuttle {
        public RexNode visitOver(RexOver over) {
          return processCall(over);
        }
      };

      protected final RelNode processProject(Project project) {
        RelNode origInput = project.getInput();
        relBuilder.push(origInput);
        RexShuttle shuttle = new ProcessShuttle();
        List<RexNode> newProjects = new ArrayList<RexNode>();
        for (RexNode expr : project.getProjects()) {
          newProjects.add(expr.accept(shuttle));
        }
        if (relBuilder.peek() == origInput) {
          relBuilder.clear();
          return project;
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

      protected final RelDataType getFloatType() {
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        RelDataType notNullFloatType = typeFactory.createSqlType(SqlTypeName.FLOAT);
        RelDataType floatType = typeFactory.createTypeWithNullability(notNullFloatType, true);
        return floatType;
      }

      /**
       * Do the rewrite for the given expression.
       *
       * When this method is invoked the {@link #relBuilder} will only contain the current input.
       * Expectation is to leave the new input there after the method finishes.
       */
      abstract RexNode rewrite(RexOver expr);

      abstract boolean isApplicable(RexOver expr);

    }
  }

  /**
   * Provides a generic way to rewrite function into using an estimation based on CDF.
   *
   *  There are a few methods which could be supported this way: NTILE, CUME_DIST, RANK
   *
   *  For example:
   *  <pre>
   *   SELECT id, CUME_DIST() OVER (ORDER BY id) FROM sketch_input;
   *     ⇒ SELECT id, ds_kll_cdf(ds, CAST(id AS FLOAT) )[0]
   *       FROM sketch_input JOIN (
   *         SELECT ds_kll_sketch(CAST(id AS FLOAT)) AS ds FROM sketch_input
   *       ) q;
   *  </pre>
   */
  public static abstract class AbstractRankBasedRewriteRule extends WindowingToProjectAggregateJoinProject {

    public AbstractRankBasedRewriteRule(String sketchType, String description) {
      super(sketchType, description);
    }

    protected static abstract class AbstractRankBasedRewriteBuilder extends VbuilderPAP {

      protected AbstractRankBasedRewriteBuilder(String sketchClass, RelBuilder relBuilder) {
        super(sketchClass, relBuilder);
      }

      @Override
      final boolean isApplicable(RexOver over) {
        RexWindow window = over.getWindow();
        if (window.orderKeys.size() == 1
            && window.getLowerBound().isUnbounded() && window.getUpperBound().isUnbounded()
            && isApplicable1(over)) {
          RelDataType type = window.orderKeys.get(0).getKey().getType();
          return type.getFamily() == SqlTypeFamily.NUMERIC;
        }
        return false;
      }

      @Override
      final RexNode rewrite(RexOver over) {
        RexWindow w = over.getWindow();
        RexFieldCollation orderKey = w.orderKeys.get(0);
        // we don't really support nulls in aggregate/etc...they are actually ignored
        // so some hack will be needed for NULLs anyway..
        ImmutableList<RexNode> partitionKeys = w.partitionKeys;

        relBuilder.push(relBuilder.peek());
        // the CDF function utilizes the '<' operator;
        // negating the input will mirror the values on the x axis
        // by using 1-CDF(-x) we could get a <= operator
        RexNode key = orderKey.getKey();
        key = rexBuilder.makeCast(getFloatType(), key);

        SqlAggFunction dataToSketchFunction = (SqlAggFunction) getSqlOperator(DataSketchesFunctions.DATA_TO_SKETCH);
        AggCall aggCall = relBuilder.aggregateCall(dataToSketchFunction, key).ignoreNulls(true);

        relBuilder.aggregate(relBuilder.groupKey(partitionKeys), aggCall);

        List<RexNode> joinConditions;
        joinConditions = Ord.zip(partitionKeys).stream().map(o -> {
          RexNode f = relBuilder.field(2, 1, o.i);
          return rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, o.e, f);
        }).collect(Collectors.toList());
        relBuilder.join(JoinRelType.INNER, joinConditions);

        int sketchFieldIndex = relBuilder.peek().getRowType().getFieldCount() - 1;
        RexInputRef sketchInputRef = relBuilder.field(sketchFieldIndex);
        SqlOperator projectOperator = getSqlOperator(DataSketchesFunctions.GET_RANK);

        // NULLs will be replaced by this value - to be before / after the other values
        // note: the sketch will ignore NULLs entirely but they will be placed at 0.0 or 1.0
        final RexNode nullReplacement =
            relBuilder.literal(orderKey.getNullDirection() == NullDirection.LAST ? Float.MAX_VALUE : -Float.MAX_VALUE);

        RexNode projRex = key;
        projRex = rexBuilder.makeCall(SqlStdOperatorTable.COALESCE, key, nullReplacement);
        projRex = rexBuilder.makeCast(getFloatType(), projRex);
        projRex = rexBuilder.makeCall(projectOperator, ImmutableList.of(sketchInputRef, projRex));
        projRex = evaluateRankValue(projRex, over, sketchInputRef);
        projRex = rexBuilder.makeCast(over.getType(), projRex);

        return projRex;
      }

      /**
       * The concreate rewrite should filter supported expressions.
       *
       * @param over the windowing expression in question
       * @return
       */
      protected abstract boolean isApplicable1(RexOver over);

      /**
       * The concreate rewrite should transform the rank value into the desired range/type/etc.
       *
       * @param rank the current rank value is in the range of [0,1]
       * @param over the windowing expression
       * @param sketchInputRef the sketch is accessible thru this fields if needed
       * @return
       */
      protected abstract RexNode evaluateRankValue(RexNode rank, RexOver over, RexInputRef sketchInputRef);

    }
  }

  /**
   * Rewrites {@code cume_dist() over (order by id)}.
   *
   *  <pre>
   *   SELECT id, CUME_DIST() OVER (ORDER BY id) FROM sketch_input;
   *     ⇒ SELECT id, ds_kll_cdf(ds, CAST(id AS FLOAT) )[0]
   *       FROM sketch_input JOIN (
   *         SELECT ds_kll_sketch(CAST(-id AS FLOAT)) AS ds FROM sketch_input
   *       ) q;
   *  </pre>
   */
  public static class CumeDistRewriteRule extends AbstractRankBasedRewriteRule {

    public CumeDistRewriteRule(String sketchType) {
      super(sketchType, "WindowingToProjectAggregateJoinProject(CumeDistRewriteRule)");
    }

    @Override
    protected VbuilderPAP buildProcessor(RelOptRuleCall call) {
      return new CumeDistRewriteBuilder(sketchType, call.builder());
    }

    private static class CumeDistRewriteBuilder extends AbstractRankBasedRewriteBuilder {

      protected CumeDistRewriteBuilder(String sketchClass, RelBuilder relBuilder) {
        super(sketchClass, relBuilder);
      }

      @Override
      protected boolean isApplicable1(RexOver over) {
        SqlAggFunction aggOp = over.getAggOperator();
        return aggOp.getName().equalsIgnoreCase("cume_dist");
      }

      @Override
      protected RexNode evaluateRankValue(RexNode projRex, RexOver over, RexInputRef sketchInputRef) {
        return projRex;
      }
    }
  }

  /**
   * Rewrites {@code ntile(n) over (order by id)}.
   *
   *  <pre>
   *   SELECT id, NTILE(4) OVER (ORDER BY id) FROM sketch_input;
   *     ⇒ SELECT id, CASE
   *                    WHEN CEIL(ds_kll_cdf(ds, CAST(id AS FLOAT) )[0]) &lt; 1
   *                      THEN 1
   *                    ELSE CEIL(ds_kll_cdf(ds, CAST(id AS FLOAT) )[0])
   *                  END
   *       FROM sketch_input JOIN (
   *         SELECT ds_kll_sketch(CAST(id AS FLOAT)) AS ds FROM sketch_input
   *       ) q;
   *  </pre>
   */
  public static class NTileRewrite extends AbstractRankBasedRewriteRule {

    public NTileRewrite(String sketchType) {
      super(sketchType, "WindowingToProjectAggregateJoinProject(NTileRewrite)");
    }

    @Override
    protected VbuilderPAP buildProcessor(RelOptRuleCall call) {
      return new NTileRewriteBuilder(sketchType, call.builder());
    }

    private static class NTileRewriteBuilder extends AbstractRankBasedRewriteBuilder {

      protected NTileRewriteBuilder(String sketchClass, RelBuilder relBuilder) {
        super(sketchClass, relBuilder);
      }

      @Override
      protected boolean isApplicable1(RexOver over) {
        SqlAggFunction aggOp = over.getAggOperator();
        return aggOp.getName().equalsIgnoreCase("ntile");
      }

      @Override
      protected RexNode evaluateRankValue(final RexNode projRex, RexOver over, RexInputRef sketchInputRef) {
        RexNode ntileOperand = over.getOperands().get(0);
        RexNode ret = projRex;
        RexNode literal1 = relBuilder.literal(1.0);

        ret = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, ret, ntileOperand);
        ret = rexBuilder.makeCall(SqlStdOperatorTable.CEIL, ret);
        ret = rexBuilder.makeCall(SqlStdOperatorTable.CASE, lt(ret, literal1), literal1, ret);
        return ret;
      }

      private RexNode lt(RexNode op1, RexNode op2) {
        return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, op1, op2);
      }
    }
  }

  /**
   * Rewrites {@code rank() over (order by id)}.
   *
   *  <pre>
   *   SELECT id, RANK() OVER (ORDER BY id) FROM sketch_input;
   *     ⇒ SELECT id, CASE
   *                    WHEN ds_kll_n(ds) &lt; (ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*ds_kll_n(ds))+1)
   *                    THEN ds_kll_n(ds)
   *                    ELSE (ceil(ds_kll_rank(ds, CAST(id AS FLOAT) )*ds_kll_n(ds))+1)
   *                  END
   *       FROM sketch_input JOIN (
   *         SELECT ds_kll_sketch(CAST(id AS FLOAT)) AS ds FROM sketch_input
   *       ) q;
   *  </pre>
   */
  public static class RankRewriteRule extends AbstractRankBasedRewriteRule {

    public RankRewriteRule(String sketchType) {
      super(sketchType, "WindowingToProjectAggregateJoinProject(RankRewriteRule)");
    }

    @Override
    protected VbuilderPAP buildProcessor(RelOptRuleCall call) {
      return new RankRewriteBuilder(sketchType, call.builder());
    }

    private static class RankRewriteBuilder extends AbstractRankBasedRewriteBuilder {

      protected RankRewriteBuilder(String sketchClass, RelBuilder relBuilder) {
        super(sketchClass, relBuilder);
      }

      @Override
      protected boolean isApplicable1(RexOver over) {
        SqlAggFunction aggOp = over.getAggOperator();
        return aggOp.getName().equalsIgnoreCase("rank");
      }

      @Override
      protected RexNode evaluateRankValue(final RexNode projRex, RexOver over, RexInputRef sketchInputRef) {
        RexNode ret = projRex;
        RexNode literal1 = relBuilder.literal(1.0);

        SqlOperator getNOperator = getSqlOperator(DataSketchesFunctions.GET_N);
        RexNode n = rexBuilder.makeCall(getNOperator, ImmutableList.of(sketchInputRef));

        ret = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, ret, n);
        ret = rexBuilder.makeCall(SqlStdOperatorTable.CEIL, ret);
        ret = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, ret, literal1);
        ret = rexBuilder.makeCall(SqlStdOperatorTable.CASE, lt(n, ret), n, ret);
        return ret;
      }

      private RexNode lt(RexNode op1, RexNode op2) {
        return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, op1, op2);
      }
    }
  }
}
