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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableBitSet.Builder;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.DataSketchesFunctions;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hive.plugin.api.HiveUDFPlugin.UDFDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * This rule could rewrite {@code count(distinct(x))} calls to be calculated using sketch based functions.
 *
 * The transformation here works on Aggregate nodes; the operations done are the following:
 *
 * 1. Identify candidate {@code count(distinct)} aggregate calls
 * 2. A new Aggregate is created in which the aggregation is done by the sketch function
 * 3. A new Project is inserted on top of the Aggregate; which unwraps the resulting
 *    count-distinct estimation from the sketch representation
 */
public final class HiveRewriteCountDistinctToDataSketches extends RelOptRule {

  protected static final Logger LOG = LoggerFactory.getLogger(HiveRewriteCountDistinctToDataSketches.class);
  private final String sketchClass;
  private final String sketchClass2 = "kll";
  private final ProjectFactory projectFactory;

  public HiveRewriteCountDistinctToDataSketches(String sketchClass) {
    super(operand(HiveAggregate.class, any()));
    this.sketchClass = sketchClass;
    projectFactory = HiveRelFactories.HIVE_PROJECT_FACTORY;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);

    if (aggregate.getGroupSets().size() != 1) {
      // not yet supported
      return;
    }

    List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();

    VBuilder vb = new VBuilder(aggregate);

    if (aggregate.getAggCallList().equals(vb.newAggCalls)) {
      // rule didn't made any changes
      return;
    }

    newAggCalls = vb.newAggCalls;
    List<String> filedNames=new ArrayList<String>();
    for (int i=0;i<vb.newProjectsBelow.size();i++ ) {
      filedNames.add("ff_"+i);
    }
    RelNode newProjectBelow=
        projectFactory.createProject(aggregate.getInput(), vb.newProjectsBelow, filedNames);

    RelNode newAgg = aggregate.copy(aggregate.getTraitSet(), newProjectBelow, aggregate.getGroupSet(),
        aggregate.getGroupSets(), newAggCalls);

    RelNode newProject = projectFactory.createProject(newAgg, vb.newProjects, aggregate.getRowType().getFieldNames());

    call.transformTo(newProject);
    return;
  }

  /**
   * Helper class to help in building a new Aggregate and Project.
   */
  // NOTE: methods in this class are not re-entrant; drop-to-frame to constructor during debugging
  class VBuilder {

    private Aggregate aggregate;
    private List<AggregateCall> newAggCalls;
    private List<RexNode> newProjects;
    private final RexBuilder rexBuilder;
    private List<RexNode> newProjectsBelow;

    public VBuilder(Aggregate aggregate) {
      this.aggregate = aggregate;
      newAggCalls = new ArrayList<AggregateCall>();
      newProjects = new ArrayList<RexNode>();
      newProjectsBelow = new ArrayList<RexNode>();
      rexBuilder = aggregate.getCluster().getRexBuilder();

      // add identity projections
      addProjectedFields();

      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        processAggCall(aggCall);
      }
    }

    private void addProjectedFields() {
      for (int i = 0; i < aggregate.getGroupCount(); i++) {
        newProjects.add(rexBuilder.makeInputRef(aggregate.getInput(), i));
      }
      Builder b = ImmutableBitSet.builder();
      b.addAll(aggregate.getGroupSet());
      for (AggregateCall aggCall: aggregate.getAggCallList()) {
        b.addAll(aggCall.getArgList());
      }
      ImmutableBitSet inputs = b.build();
      Integer maxIdx = Collections.max(inputs.asSet());
      for (int i = 0; i < maxIdx; i++) {
        newProjectsBelow.add(rexBuilder.makeInputRef(aggregate.getInput(), i));
      }
    }

    private void processAggCall(AggregateCall aggCall) {
      if (isSimpleCountDistinct(aggCall)) {
        rewriteCountDistinct(aggCall);
        return;
      }
      if (isPercentileCont(aggCall)) {
        rewritePercentileCont(aggCall);
        return;
      }
      appendAggCall(aggCall);
    }

    private void appendAggCall(AggregateCall aggCall) {
      RexNode projRex = rexBuilder.makeInputRef(aggCall.getType(), newProjects.size());

      newAggCalls.add(aggCall);
      newProjects.add(projRex);
    }

    private boolean isSimpleCountDistinct(AggregateCall aggCall) {
      return aggCall.isDistinct() && aggCall.getArgList().size() == 1
          && aggCall.getAggregation().getName().equalsIgnoreCase("count") && !aggCall.hasFilter();
    }

    private boolean isPercentileCont(AggregateCall aggCall) {
      // FIXME: also check that args are: ?,?,1,0 - other cases are not supported
      return !aggCall.isDistinct() && aggCall.getArgList().size() == 4
          && aggCall.getAggregation().getName().equalsIgnoreCase("percentile_cont") && !aggCall.hasFilter();
    }

    private void rewriteCountDistinct(AggregateCall aggCall) {

      RelDataType origType = aggregate.getRowType().getFieldList().get(newProjects.size()).getType();

      Integer argIndex = aggCall.getArgList().get(0);
      RexNode call = rexBuilder.makeInputRef(aggregate.getInput(), argIndex);
      newProjectsBelow.add(call);

      ArrayList<Integer> newArgList = Lists.newArrayList(newProjectsBelow.size() - 1);

      SqlAggFunction aggFunction = (SqlAggFunction) getSqlOperator(sketchClass, DataSketchesFunctions.DATA_TO_SKETCH);
      boolean distinct = false;
      boolean approximate = true;
      boolean ignoreNulls = aggCall.ignoreNulls();
      List<Integer> argList = newArgList;
      int filterArg = aggCall.filterArg;
      RelCollation collation = aggCall.getCollation();
      int groupCount = aggregate.getGroupCount();
      RelNode input = aggregate.getInput();
      RelDataType type = rexBuilder.deriveReturnType(aggFunction, Collections.emptyList());
      String name = aggFunction.getName();

      AggregateCall newAgg = AggregateCall.create(aggFunction, distinct, approximate, ignoreNulls, argList, filterArg,
          collation, groupCount, input, type, name);

      SqlOperator projectOperator = getSqlOperator(sketchClass, DataSketchesFunctions.SKETCH_TO_ESTIMATE);
      RexNode projRex = rexBuilder.makeInputRef(newAgg.getType(), newProjects.size());
      projRex = rexBuilder.makeCall(projectOperator, ImmutableList.of(projRex));
      projRex = rexBuilder.makeCast(origType, projRex);

      newAggCalls.add(newAgg);
      newProjects.add(projRex);
    }

    private void rewritePercentileCont(AggregateCall aggCall) {

      RelDataType origType = aggregate.getRowType().getFieldList().get(newProjects.size()).getType();

      Integer argIndex = aggCall.getArgList().get(1);
      RexNode call = rexBuilder.makeInputRef(aggregate.getInput(), argIndex);

      RelDataType doubleType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
      RelDataType floatType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.FLOAT);
      call = rexBuilder.makeCast(floatType, call);
      newProjectsBelow.add(call);

      ArrayList<Integer> newArgList = Lists.newArrayList(newProjectsBelow.size() - 1);

      SqlAggFunction aggFunction = (SqlAggFunction) getSqlOperator(sketchClass2, DataSketchesFunctions.DATA_TO_SKETCH);
      boolean distinct = false;
      boolean approximate = true;
      boolean ignoreNulls = aggCall.ignoreNulls();
      List<Integer> argList = newArgList;
      int filterArg = aggCall.filterArg;
      RelCollation collation = aggCall.getCollation();
      RelDataType type = rexBuilder.deriveReturnType(aggFunction, Collections.emptyList());
      String name = aggFunction.getName();

      AggregateCall newAgg = AggregateCall.create(aggFunction, distinct, approximate, ignoreNulls, argList, filterArg,
          collation, type, name);

      Integer origFractionIdx = aggCall.getArgList().get(0);
      RexNode fraction = getProject(aggregate.getInput()).getChildExps().get(origFractionIdx);
      fraction = rexBuilder.makeCast(floatType, fraction);

      SqlOperator projectOperator = getSqlOperator(sketchClass2, DataSketchesFunctions.GET_QUANTILE);
      RexNode projRex = rexBuilder.makeInputRef(newAgg.getType(), newProjects.size());
      projRex = rexBuilder.makeCall(projectOperator, ImmutableList.of(projRex, fraction));
      projRex = rexBuilder.makeCast(origType, projRex);

      newAggCalls.add(newAgg);
      newProjects.add(projRex);
    }

    private Project getProject(RelNode input) {
      if (input instanceof Project) {
        return (Project) input;
      }
      if (input instanceof HepRelVertex) {
        HepRelVertex hepRelVertex = (HepRelVertex) input;
        return (Project) hepRelVertex.getCurrentRel();
      }
      throw new RuntimeException("unexpected");
    }

    private SqlOperator getSqlOperator(String sketchClass, String fnName) {
      UDFDescriptor fn = DataSketchesFunctions.INSTANCE.getSketchFunction(sketchClass, fnName);
      if (!fn.getCalciteFunction().isPresent()) {
        throw new RuntimeException(fn.toString() + " doesn't have a Calcite function associated with it");
      }
      return fn.getCalciteFunction().get();
    }
  }

  abstract class RewriteBase {

    private SqlOperator getSqlOperator(String fnName) {
      UDFDescriptor fn = DataSketchesFunctions.INSTANCE.getSketchFunction(sketchClass, fnName);
      if (!fn.getCalciteFunction().isPresent()) {
        throw new RuntimeException(fn.toString() + " doesn't have a Calcite function associated with it");
      }
      return fn.getCalciteFunction().get();
    }

  }

  static class CountDistinctRewrite {

  }
}
