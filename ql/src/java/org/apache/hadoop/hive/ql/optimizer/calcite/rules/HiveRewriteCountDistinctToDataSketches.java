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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.RelFactories.AggregateFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.exec.DataSketchesFunctions;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveMergeablAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveGroupingID;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSqlFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.math.IntMath;

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

  RelOptCluster cluster = null;
  RexBuilder rexBuilder = null;

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);

    List<AggregateCall> newAggCalls = new ArrayList<AggregateCall>();

    AggregateFactory f = HiveRelFactories.HIVE_AGGREGATE_FACTORY;

    VBuilder vb = new VBuilder(aggregate);

    // FIXME HiveAggregate?
    RelNode newCall = aggregate.copy(aggregate.getTraitSet(), aggregate.getInput(), aggregate.getGroupSet(),
        aggregate.getGroupSets(), newAggCalls);
    call.transformTo(newCall);
    return;
  }

  // NOTE: methods in this class are not re-entrant; drop-to-frame to constructor during debugging
  static class VBuilder {

    private Aggregate aggregate;

    public VBuilder(Aggregate aggregate) {

      this.aggregate = aggregate;
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
      // TODO Auto-generated method stub

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
    String name="x";
    ret=new HiveSqlFunction(name, SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.ANY),
        OperandTypes.ANY, OperandTypes.family(),
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


  /**
   * Converts an aggregate relational expression that contains only
   * count(distinct) to grouping sets with count. For example select
   * count(distinct department_id), count(distinct gender), count(distinct
   * education_level) from employee; can be transformed to
   * select
   * count(case when i=1 and department_id is not null then 1 else null end) as c0,
   * count(case when i=2 and gender is not null then 1 else null end) as c1,
   * count(case when i=4 and education_level is not null then 1 else null end) as c2
   * from (select
   * grouping__id as i, department_id, gender, education_level from employee
   * group by department_id, gender, education_level grouping sets
   * (department_id, gender, education_level))subq;
   * @throws CalciteSemanticException
   */
  private RelNode convert(Aggregate aggregate, List<List<Integer>> argList, ImmutableBitSet newGroupSet)
      throws CalciteSemanticException {
    // we use this map to map the position of argList to the position of grouping set
    Map<Integer, Integer> map = new HashMap<>();
    List<List<Integer>> cleanArgList = new ArrayList<>();
    final Aggregate groupingSets = createGroupingSets(aggregate, argList, cleanArgList, map, newGroupSet);
    return createCount(groupingSets, argList, cleanArgList, map, aggregate.getGroupSet(), newGroupSet);
  }

  private int getGroupingIdValue(List<Integer> list, ImmutableBitSet originalGroupSet, ImmutableBitSet newGroupSet,
      int groupCount) {
    int ind = IntMath.pow(2, groupCount) - 1;
    for (int pos : originalGroupSet) {
      ind &= ~(1 << groupCount - newGroupSet.indexOf(pos) - 1);
    }
    for (int i : list) {
      ind &= ~(1 << groupCount - newGroupSet.indexOf(i) - 1);
    }
    return ind;
  }

  /**
   * @param aggr: the original aggregate
   * @param argList: the original argList in aggregate
   * @param cleanArgList: the new argList without duplicates
   * @param map: the mapping from the original argList to the new argList
   * @param newGroupSet: the sorted positions of groupset
   * @return
   * @throws CalciteSemanticException
   */
  private RelNode createCount(Aggregate aggr, List<List<Integer>> argList, List<List<Integer>> cleanArgList,
      Map<Integer, Integer> map, ImmutableBitSet originalGroupSet, ImmutableBitSet newGroupSet)
      throws CalciteSemanticException {
    final List<RexNode> originalInputRefs = aggr.getRowType().getFieldList().stream()
        .map(input -> new RexInputRef(input.getIndex(), input.getType())).collect(Collectors.toList());
    final List<RexNode> gbChildProjLst = Lists.newArrayList();
    // for singular arg, count should not include null
    // e.g., count(case when i=1 and department_id is not null then 1 else null end) as c0,
    // for non-singular args, count can include null, i.e. (,) is counted as 1
    for (List<Integer> list : cleanArgList) {
      RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
          originalInputRefs.get(originalInputRefs.size() - 1), rexBuilder.makeExactLiteral(
              new BigDecimal(getGroupingIdValue(list, originalGroupSet, newGroupSet, aggr.getGroupCount()))));
      if (list.size() == 1) {
        int pos = list.get(0);
        RexNode notNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, originalInputRefs.get(pos));
        condition = rexBuilder.makeCall(SqlStdOperatorTable.AND, condition, notNull);
      }
      RexNode caseExpr1 = rexBuilder.makeExactLiteral(BigDecimal.ONE);
      RexNode caseExpr2 = rexBuilder.makeNullLiteral(caseExpr1.getType());
      RexNode when = rexBuilder.makeCall(SqlStdOperatorTable.CASE, condition, caseExpr1, caseExpr2);
      gbChildProjLst.add(when);
    }

    for (int pos : originalGroupSet) {
      gbChildProjLst.add(originalInputRefs.get(newGroupSet.indexOf(pos)));
    }

    // create the project before GB
    RelNode gbInputRel = HiveProject.create(aggr, gbChildProjLst, null);

    // create the aggregate
    List<AggregateCall> aggregateCalls = Lists.newArrayList();
    RelDataType aggFnRetType = TypeConverter.convert(TypeInfoFactory.longTypeInfo, cluster.getTypeFactory());
    for (int i = 0; i < cleanArgList.size(); i++) {
      AggregateCall aggregateCall =
          HiveCalciteUtil.createSingleArgAggCall("count", cluster, TypeInfoFactory.longTypeInfo, i, aggFnRetType);
      aggregateCalls.add(aggregateCall);
    }
    ImmutableBitSet groupSet =
        ImmutableBitSet.range(cleanArgList.size(), cleanArgList.size() + originalGroupSet.cardinality());
    Aggregate aggregate = new HiveAggregate(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION), gbInputRel, groupSet,
        null, aggregateCalls);

    // create the project after GB. For those repeated values, e.g., select
    // count(distinct x, y), count(distinct y, x), we find the correct mapping.
    if (map.isEmpty()) {
      return aggregate;
    } else {
      final List<RexNode> originalAggrRefs = aggregate.getRowType().getFieldList().stream()
          .map(input -> new RexInputRef(input.getIndex(), input.getType())).collect(Collectors.toList());
      final List<RexNode> projLst = Lists.newArrayList();
      int index = 0;
      for (int i = 0; i < groupSet.cardinality(); i++) {
        projLst.add(originalAggrRefs.get(index++));
      }
      for (int i = 0; i < argList.size(); i++) {
        if (map.containsKey(i)) {
          projLst.add(originalAggrRefs.get(map.get(i)));
        } else {
          projLst.add(originalAggrRefs.get(index++));
        }
      }
      return HiveProject.create(aggregate, projLst, null);
    }
  }

  /**
   * @param aggregate: the original aggregate
   * @param argList: the original argList in aggregate
   * @param cleanArgList: the new argList without duplicates
   * @param map: the mapping from the original argList to the new argList
   * @param groupSet: new group set
   * @return
   */
  private Aggregate createGroupingSets(Aggregate aggregate, List<List<Integer>> argList,
      List<List<Integer>> cleanArgList, Map<Integer, Integer> map, ImmutableBitSet groupSet) {
    final List<ImmutableBitSet> origGroupSets = new ArrayList<>();

    for (int i = 0; i < argList.size(); i++) {
      List<Integer> list = argList.get(i);
      ImmutableBitSet bitSet = aggregate.getGroupSet().union(ImmutableBitSet.of(list));
      int prev = origGroupSets.indexOf(bitSet);
      if (prev == -1) {
        origGroupSets.add(bitSet);
        cleanArgList.add(list);
      } else {
        map.put(i, prev);
      }
    }
    // Calcite expects the grouping sets sorted and without duplicates
    origGroupSets.sort(ImmutableBitSet.COMPARATOR);

    List<AggregateCall> aggregateCalls = new ArrayList<AggregateCall>();
    // Create GroupingID column
    AggregateCall aggCall =
        AggregateCall.create(HiveGroupingID.INSTANCE, false, new ImmutableList.Builder<Integer>().build(), -1,
            this.cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT), HiveGroupingID.INSTANCE.getName());
    aggregateCalls.add(aggCall);
    return new HiveAggregate(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION), aggregate.getInput(), groupSet,
        origGroupSets, aggregateCalls);
  }

  /**
   * Returns the number of count DISTINCT
   *
   * @return the number of count DISTINCT
   */
  private int getNumCountDistinctCall(Aggregate hiveAggregate) {
    int cnt = 0;
    for (AggregateCall aggCall : hiveAggregate.getAggCallList()) {
      if (aggCall.isDistinct() && (aggCall.getAggregation().getName().equalsIgnoreCase("count"))) {
        cnt++;
      }
    }
    return cnt;
  }

  /**
   * Converts an aggregate relational expression that contains just one
   * distinct aggregate function (or perhaps several over the same arguments)
   * and no non-distinct aggregate functions.
   */
  private RelNode convertMonopole(Aggregate aggregate, List<Integer> argList) {
    // For example,
    //    SELECT deptno, COUNT(DISTINCT sal), SUM(DISTINCT sal)
    //    FROM emp
    //    GROUP BY deptno
    //
    // becomes
    //
    //    SELECT deptno, COUNT(distinct_sal), SUM(distinct_sal)
    //    FROM (
    //      SELECT DISTINCT deptno, sal AS distinct_sal
    //      FROM EMP GROUP BY deptno)
    //    GROUP BY deptno

    // Project the columns of the GROUP BY plus the arguments
    // to the agg function.
    Map<Integer, Integer> sourceOf = new HashMap<Integer, Integer>();
    final Aggregate distinct = createSelectDistinct(aggregate, argList, sourceOf);

    // Create an aggregate on top, with the new aggregate list.
    final List<AggregateCall> newAggCalls = Lists.newArrayList(aggregate.getAggCallList());
    rewriteAggCalls(newAggCalls, argList, sourceOf);
    final int cardinality = aggregate.getGroupSet().cardinality();
    return aggregate.copy(aggregate.getTraitSet(), distinct, aggregate.indicator, ImmutableBitSet.range(cardinality),
        null, newAggCalls);
  }

  private static void rewriteAggCalls(List<AggregateCall> newAggCalls, List<Integer> argList,
      Map<Integer, Integer> sourceOf) {
    // Rewrite the agg calls. Each distinct agg becomes a non-distinct call
    // to the corresponding field from the right; for example,
    // "COUNT(DISTINCT e.sal)" becomes   "COUNT(distinct_e.sal)".
    for (int i = 0; i < newAggCalls.size(); i++) {
      final AggregateCall aggCall = newAggCalls.get(i);

      // Ignore agg calls which are not distinct or have the wrong set
      // arguments. If we're rewriting aggs whose args are {sal}, we will
      // rewrite COUNT(DISTINCT sal) and SUM(DISTINCT sal) but ignore
      // COUNT(DISTINCT gender) or SUM(sal).
      if (!aggCall.isDistinct()) {
        continue;
      }
      if (!aggCall.getArgList().equals(argList)) {
        continue;
      }

      // Re-map arguments.
      final int argCount = aggCall.getArgList().size();
      final List<Integer> newArgs = new ArrayList<Integer>(argCount);
      for (int j = 0; j < argCount; j++) {
        final Integer arg = aggCall.getArgList().get(j);
        newArgs.add(sourceOf.get(arg));
      }
      final AggregateCall newAggCall =
          new AggregateCall(aggCall.getAggregation(), false, newArgs, aggCall.getType(), aggCall.getName());
      newAggCalls.set(i, newAggCall);
    }
  }

  /**
   * Given an {@link org.apache.calcite.rel.logical.LogicalAggregate}
   * and the ordinals of the arguments to a
   * particular call to an aggregate function, creates a 'select distinct'
   * relational expression which projects the group columns and those
   * arguments but nothing else.
   *
   * <p>For example, given
   *
   * <blockquote>
   * <pre>select f0, count(distinct f1), count(distinct f2)
   * from t group by f0</pre>
   * </blockquote>
   *
   * and the arglist
   *
   * <blockquote>{2}</blockquote>
   *
   * returns
   *
   * <blockquote>
   * <pre>select distinct f0, f2 from t</pre>
   * </blockquote>
   *
   * '
   *
   * <p>The <code>sourceOf</code> map is populated with the source of each
   * column; in this case sourceOf.get(0) = 0, and sourceOf.get(1) = 2.</p>
   *
   * @param aggregate Aggregate relational expression
   * @param argList   Ordinals of columns to make distinct
   * @param sourceOf  Out parameter, is populated with a map of where each
   *                  output field came from
   * @return Aggregate relational expression which projects the required
   * columns
   */
  private static Aggregate createSelectDistinct(Aggregate aggregate, List<Integer> argList,
      Map<Integer, Integer> sourceOf) {
    final List<Pair<RexNode, String>> projects = new ArrayList<Pair<RexNode, String>>();
    final RelNode child = aggregate.getInput();
    final List<RelDataTypeField> childFields = child.getRowType().getFieldList();
    for (int i : aggregate.getGroupSet()) {
      sourceOf.put(i, projects.size());
      projects.add(RexInputRef.of2(i, childFields));
    }
    for (Integer arg : argList) {
      if (sourceOf.get(arg) != null) {
        continue;
      }
      sourceOf.put(arg, projects.size());
      projects.add(RexInputRef.of2(arg, childFields));
    }
    final RelNode project = projFactory.createProject(child, Pair.left(projects), Pair.right(projects));

    // Get the distinct values of the GROUP BY fields and the arguments
    // to the agg functions.
    return aggregate.copy(aggregate.getTraitSet(), project, false, ImmutableBitSet.range(projects.size()), null,
        ImmutableList.<AggregateCall> of());
  }
}

// End AggregateExpandDistinctAggregatesRule.java