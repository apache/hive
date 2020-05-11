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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlOperator;
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
    RelNode newAgg = aggregate.copy(aggregate.getTraitSet(), aggregate.getInput(), aggregate.getGroupSet(),
        aggregate.getGroupSets(), newAggCalls);

    RelNode newProject = projectFactory.createProject(newAgg, vb.newProjects, aggregate.getRowType().getFieldNames());

    call.transformTo(newProject);
    return;
  }

  /**
   * Helper class to aid rewriting in transforming an Aggregate to Project -> Aggregate -> Project.
   */
  // NOTE: methods in this class are not re-entrant; drop-to-frame to constructor during debugging
  class VBuilder {

    private Aggregate aggregate;
    private List<AggregateCall> newAggCalls;
    private List<RexNode> newProjects;

    private final RexBuilder rexBuilder;

    public VBuilder(Aggregate aggregate) {
      this.aggregate = aggregate;
      newAggCalls = new ArrayList<AggregateCall>();
      newProjects = new ArrayList<RexNode>();
      rexBuilder = aggregate.getCluster().getRexBuilder();

      // add non-aggregated fields - as identity projections
      addGroupFields();

      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        processAggCall(aggCall);
      }
    }

    private void addGroupFields() {
      for (int i = 0; i < aggregate.getGroupCount(); i++) {
        newProjects.add(rexBuilder.makeInputRef(aggregate, i));
      }
    }

    private void processAggCall(AggregateCall aggCall) {
      if (isSimpleCountDistinct(aggCall)) {
        rewriteCountDistinct(aggCall);
        return;
      }
      appendAggCall(aggCall, null);
    }

    private void appendAggCall(AggregateCall aggCall, SqlOperator projectOperator) {
      RelDataType origType = aggregate.getRowType().getFieldList().get(newProjects.size()).getType();
      RexNode projRex = rexBuilder.makeInputRef(aggCall.getType(), newProjects.size());
      if (projectOperator != null) {
        projRex = rexBuilder.makeCall(projectOperator, ImmutableList.of(projRex));
        projRex = rexBuilder.makeCast(origType, projRex);
      }
      newAggCalls.add(aggCall);
      newProjects.add(projRex);
    }

    private boolean isSimpleCountDistinct(AggregateCall aggCall) {
      return aggCall.isDistinct() && aggCall.getArgList().size() == 1
          && aggCall.getAggregation().getName().equalsIgnoreCase("count") && !aggCall.hasFilter();
    }

    private void rewriteCountDistinct(AggregateCall aggCall) {
      SqlAggFunction aggFunction = (SqlAggFunction) getSqlOperator(DataSketchesFunctions.DATA_TO_SKETCH);
      boolean distinct = false;
      boolean approximate = true;
      boolean ignoreNulls = aggCall.ignoreNulls();
      List<Integer> argList = aggCall.getArgList();
      int filterArg = aggCall.filterArg;
      RelCollation collation = aggCall.getCollation();
      int groupCount = aggregate.getGroupCount();
      RelNode input = aggregate.getInput();
      RelDataType type = rexBuilder.deriveReturnType(aggFunction, Collections.emptyList());
      String name = aggFunction.getName();

      AggregateCall ret = AggregateCall.create(aggFunction, distinct, approximate, ignoreNulls, argList, filterArg,
          collation, groupCount, input, type, name);

      appendAggCall(ret, getSqlOperator(DataSketchesFunctions.SKETCH_TO_ESTIMATE));
    }

    private SqlOperator getSqlOperator(String fnName) {
      UDFDescriptor fn = DataSketchesFunctions.INSTANCE.getSketchFunction(sketchClass, fnName);
      if (!fn.getCalciteFunction().isPresent()) {
        throw new RuntimeException(fn.toString() + " doesn't have a Calcite function associated with it");
      }
      return fn.getCalciteFunction().get();
    }
  }
}
