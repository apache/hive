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
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.RelFactories.AggregateFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveMergeablAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSqlFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * Planner rule that expands distinct aggregates
 * (such as {@code COUNT(DISTINCT x)}) from a
 * {@link org.apache.calcite.rel.core.Aggregate}.
 *
 * <p>How this is done depends upon the arguments to the function. If all
 * functions have the same argument
 * (e.g. {@code COUNT(DISTINCT x), SUM(DISTINCT x)} both have the argument
 * {@code x}) then one extra {@link org.apache.calcite.rel.core.Aggregate} is
 * sufficient.
 *
 * <p>If there are multiple arguments
 * (e.g. {@code COUNT(DISTINCT x), COUNT(DISTINCT y)})
 * the rule creates separate {@code Aggregate}s and combines using a
 * {@link org.apache.calcite.rel.core.Join}.
 */

// Stripped down version of org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule
// This is adapted for Hive, but should eventually be deleted from Hive and make use of above.

public final class HiveRewriteCountDistinctToDataSketches extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** The default instance of the rule; operates only on logical expressions. */
  public static final HiveRewriteCountDistinctToDataSketches INSTANCE = new HiveRewriteCountDistinctToDataSketches();

  @Deprecated
  private static RelFactories.ProjectFactory projFactory;

  protected static final Logger LOG = LoggerFactory.getLogger(HiveRewriteCountDistinctToDataSketches.class);

  public HiveRewriteCountDistinctToDataSketches() {
    super(operand(HiveAggregate.class, any()));
    projFactory = HiveRelFactories.HIVE_PROJECT_FACTORY;
  }


  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);

    List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();

    AggregateFactory f = HiveRelFactories.HIVE_AGGREGATE_FACTORY;

    VBuilder vb = new VBuilder(aggregate);

    newAggCalls = aggregate.getAggCallList();
    // FIXME HiveAggregate?
    RelNode newCall = aggregate.copy(aggregate.getTraitSet(), aggregate.getInput(), aggregate.getGroupSet(),
        aggregate.getGroupSets(), newAggCalls);
    call.transformTo(newCall);
    return;
  }

  // NOTE: methods in this class are not re-entrant; drop-to-frame to constructor during debugging
  static class VBuilder {

    private Aggregate aggregate;
    private List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();
    private List<RexNode> newProjects = new ArrayList<RexNode>();
    private final RexBuilder rexBuilder;

    public VBuilder(Aggregate aggregate) {

      this.aggregate = aggregate;
      rexBuilder = aggregate.getCluster().getRexBuilder();
      for (AggregateCall aggCall : aggregate.getAggCallList()) {
        processAggCall(aggCall);
      }
    }

    private void processAggCall(AggregateCall aggCall) {
      if (isSimpleCountDistinct(aggCall)) {
        rewriteCountDistinct(aggCall);
        return;
      }
      addAggCall(aggCall, null);

    }

    private void addAggCall(AggregateCall aggCall, SqlOperator sqlOperator) {

//      Set<Integer> allFields = RelOptUtil.getAllFields(aggregate);
//
//      final Map<Integer, Integer> map = new HashMap<>();
//
//      for (Integer source: allFields) {
//        map.put(source,)
//      }
//
      newAggCalls.add(aggCall);
      newProjects.add(buildIdentityRexNode(sqlOperator));

    }

    private RexNode buildIdentityRexNode(SqlOperator sqlOperator) {
//      aggregate.
      RexNode fieldRef = null;
      if (sqlOperator != null) {
        return rexBuilder.makeCall(sqlOperator, ImmutableList.of(fieldRef));
//        return
      }
      return fieldRef;
    }

    private boolean isSimpleCountDistinct(AggregateCall aggCall) {
      return aggCall.isDistinct() && aggCall.getArgList().size() == 1 && aggCall.getName().equalsIgnoreCase("count")
          && !aggCall.hasFilter();
    }

  private void rewriteCountDistinct(AggregateCall aggCall) {

      SqlAggFunction aggFunction = getDS_FN(aggCall.getAggregation());
      boolean distinct = false;
      boolean approximate = true;
      boolean ignoreNulls = aggCall.ignoreNulls();
      List<Integer> argList = aggCall.getArgList();
      int filterArg = aggCall.filterArg;
      RelCollation collation = aggCall.getCollation();
      int groupCount = aggregate.getGroupCount();
      RelNode input = aggregate.getInput();
      RelDataType type = aggCall.getType();
      String name = aggFunction.getName();
      //      AggregateCall ret = null;
      AggregateCall ret = AggregateCall.create(aggFunction, distinct, approximate, ignoreNulls, argList, filterArg,
          collation, groupCount, input, type, name);
      //    aggCall
      //    aggCall.copy(aggCall.getArgList(), aggCall.filterArg, aggCall.getCollation());

      addAggCall(ret, createSqlOperator());

//    projExpressions.add();
  }

  private SqlOperator createSqlOperator() {
    SqlOperator ret;
    String name="ds_hll_estimate";
    SqlOperandTypeInference xx=InferTypes.ANY_NULLABLE;
    ret=new HiveSqlFunction(name, SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.DOUBLE),
        xx, OperandTypes.family(),
        SqlFunctionCategory.USER_DEFINED_FUNCTION, true, false);
    return ret;
  }

  // FIXME move this to common place
  private SqlAggFunction getDS_FN(SqlAggFunction oldAggFunction) {
HiveMergeablAggregate union = new HiveMergeablAggregate(
    "ds_hll_union",
    SqlKind.OTHER_FUNCTION,
    oldAggFunction.getReturnTypeInference(),
    oldAggFunction.getOperandTypeInference(),
    oldAggFunction.getOperandTypeChecker()
    );
    return new HiveMergeablAggregate(
        "ds_hll_sketch",
        SqlKind.OTHER_FUNCTION,
        oldAggFunction.getReturnTypeInference(),
        oldAggFunction.getOperandTypeInference(),
        oldAggFunction.getOperandTypeChecker(),union
        );
  }
}

}

// End AggregateExpandDistinctAggregatesRule.java