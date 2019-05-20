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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveGroupingID;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
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

public final class HiveExpandDistinctAggregatesRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** The default instance of the rule; operates only on logical expressions. */
  public static final HiveExpandDistinctAggregatesRule INSTANCE =
      new HiveExpandDistinctAggregatesRule(HiveAggregate.class,
          HiveRelFactories.HIVE_PROJECT_FACTORY);

  private static RelFactories.ProjectFactory projFactory;
  
  protected static final Logger LOG = LoggerFactory.getLogger(HiveExpandDistinctAggregatesRule.class);

  //~ Constructors -----------------------------------------------------------

  public HiveExpandDistinctAggregatesRule(
      Class<? extends Aggregate> clazz,RelFactories.ProjectFactory projectFactory) {
    super(operand(clazz, any()));
    projFactory = projectFactory;
  }

  RelOptCluster cluster = null;
  RexBuilder rexBuilder = null;

  //~ Methods ----------------------------------------------------------------

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    int numCountDistinct = getNumCountDistinctCall(aggregate);
    if (numCountDistinct == 0) {
      return;
    }

    // Find all of the agg expressions. We use a List (for all count(distinct))
    // as well as a Set (for all others) to ensure determinism.
    int nonDistinctCount = 0;
    List<List<Integer>> argListList = new ArrayList<List<Integer>>();
    Set<List<Integer>> argListSets = new LinkedHashSet<List<Integer>>();
    Set<Integer> positions = new HashSet<>();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (!aggCall.isDistinct()) {
        ++nonDistinctCount;
        continue;
      }
      ArrayList<Integer> argList = new ArrayList<Integer>();
      for (Integer arg : aggCall.getArgList()) {
        argList.add(arg);
        positions.add(arg);
      }
      // Aggr checks for sorted argList.
      argListList.add(argList);
      argListSets.add(argList);
    }
    Util.permAssert(argListSets.size() > 0, "containsDistinctCall lied");

    if (numCountDistinct > 1 && numCountDistinct == aggregate.getAggCallList().size()
        && aggregate.getGroupSet().isEmpty()) {
      LOG.debug("Trigger countDistinct rewrite. numCountDistinct is " + numCountDistinct);
      // now positions contains all the distinct positions, i.e., $5, $4, $6
      // we need to first sort them as group by set
      // and then get their position later, i.e., $4->1, $5->2, $6->3
      cluster = aggregate.getCluster();
      rexBuilder = cluster.getRexBuilder();
      RelNode converted = null;
      List<Integer> sourceOfForCountDistinct = new ArrayList<>();
      sourceOfForCountDistinct.addAll(positions);
      Collections.sort(sourceOfForCountDistinct);
      try {
        converted = convert(aggregate, argListList, sourceOfForCountDistinct);
      } catch (CalciteSemanticException e) {
        LOG.debug(e.toString());
        throw new RuntimeException(e);
      }
      call.transformTo(converted);
      return;
    }

    // If all of the agg expressions are distinct and have the same
    // arguments then we can use a more efficient form.
    final RelMetadataQuery mq = call.getMetadataQuery();
    if ((nonDistinctCount == 0) && (argListSets.size() == 1)) {
      for (Integer arg : argListSets.iterator().next()) {
        Set<RelColumnOrigin> colOrigs = mq.getColumnOrigins(aggregate.getInput(), arg);
        if (null != colOrigs) {
          for (RelColumnOrigin colOrig : colOrigs) {
            RelOptHiveTable hiveTbl = (RelOptHiveTable)colOrig.getOriginTable();
            if(hiveTbl.getPartColInfoMap().containsKey(colOrig.getOriginColumnOrdinal())) {
              // Encountered partitioning column, this will be better handled by MetadataOnly optimizer.
              return;
            }
          }
        }
      }
      RelNode converted =
          convertMonopole(
              aggregate,
              argListSets.iterator().next());
      call.transformTo(converted);
      return;
    }
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
  private RelNode convert(Aggregate aggregate, List<List<Integer>> argList, List<Integer> sourceOfForCountDistinct) throws CalciteSemanticException {
    // we use this map to map the position of argList to the position of grouping set
    Map<Integer, Integer> map = new HashMap<>();
    List<List<Integer>> cleanArgList = new ArrayList<>();
    final Aggregate groupingSets = createGroupingSets(aggregate, argList, cleanArgList, map, sourceOfForCountDistinct);
    return createCount(groupingSets, argList, cleanArgList, map, sourceOfForCountDistinct);
  }

  private int getGroupingIdValue(List<Integer> list, List<Integer> sourceOfForCountDistinct,
          int groupCount) {
    int ind = IntMath.pow(2, groupCount) - 1;
    for (int i : list) {
      ind &= ~(1 << groupCount - sourceOfForCountDistinct.indexOf(i) - 1);
    }
    return ind;
  }

  /**
   * @param aggr: the original aggregate
   * @param argList: the original argList in aggregate
   * @param cleanArgList: the new argList without duplicates
   * @param map: the mapping from the original argList to the new argList
   * @param sourceOfForCountDistinct: the sorted positions of groupset
   * @return
   * @throws CalciteSemanticException
   */
  private RelNode createCount(Aggregate aggr, List<List<Integer>> argList,
      List<List<Integer>> cleanArgList, Map<Integer, Integer> map,
      List<Integer> sourceOfForCountDistinct) throws CalciteSemanticException {
    List<RexNode> originalInputRefs = Lists.transform(aggr.getRowType().getFieldList(),
        new Function<RelDataTypeField, RexNode>() {
          @Override
          public RexNode apply(RelDataTypeField input) {
            return new RexInputRef(input.getIndex(), input.getType());
          }
        });
    final List<RexNode> gbChildProjLst = Lists.newArrayList();
    // for singular arg, count should not include null
    // e.g., count(case when i=1 and department_id is not null then 1 else null end) as c0, 
    // for non-singular args, count can include null, i.e. (,) is counted as 1
    for (List<Integer> list : cleanArgList) {
      RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, originalInputRefs
          .get(originalInputRefs.size() - 1), rexBuilder.makeExactLiteral(new BigDecimal(
          getGroupingIdValue(list, sourceOfForCountDistinct, aggr.getGroupCount()))));
      if (list.size() == 1) {
        int pos = list.get(0);
        RexNode notNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL,
            originalInputRefs.get(pos));
        condition = rexBuilder.makeCall(SqlStdOperatorTable.AND, condition, notNull);
      }
      RexNode when = rexBuilder.makeCall(SqlStdOperatorTable.CASE, condition,
          rexBuilder.makeExactLiteral(BigDecimal.ONE), rexBuilder.constantNull());
      gbChildProjLst.add(when);
    }

    // create the project before GB
    RelNode gbInputRel = HiveProject.create(aggr, gbChildProjLst, null);

    // create the aggregate
    List<AggregateCall> aggregateCalls = Lists.newArrayList();
    RelDataType aggFnRetType = TypeConverter.convert(TypeInfoFactory.longTypeInfo,
        cluster.getTypeFactory());
    for (int i = 0; i < cleanArgList.size(); i++) {
      AggregateCall aggregateCall = HiveCalciteUtil.createSingleArgAggCall("count", cluster,
          TypeInfoFactory.longTypeInfo, i, aggFnRetType);
      aggregateCalls.add(aggregateCall);
    }
    Aggregate aggregate = new HiveAggregate(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION), gbInputRel,
        ImmutableBitSet.of(), null, aggregateCalls);

    // create the project after GB. For those repeated values, e.g., select
    // count(distinct x, y), count(distinct y, x), we find the correct mapping.
    if (map.isEmpty()) {
      return aggregate;
    } else {
      List<RexNode> originalAggrRefs = Lists.transform(aggregate.getRowType().getFieldList(),
          new Function<RelDataTypeField, RexNode>() {
            @Override
            public RexNode apply(RelDataTypeField input) {
              return new RexInputRef(input.getIndex(), input.getType());
            }
          });
      final List<RexNode> projLst = Lists.newArrayList();
      int index = 0;
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
   * @param sourceOfForCountDistinct: the sorted positions of groupset
   * @return
   */
  private Aggregate createGroupingSets(Aggregate aggregate, List<List<Integer>> argList,
      List<List<Integer>> cleanArgList, Map<Integer, Integer> map,
      List<Integer> sourceOfForCountDistinct) {
    final ImmutableBitSet groupSet = ImmutableBitSet.of(sourceOfForCountDistinct);
    final List<ImmutableBitSet> origGroupSets = new ArrayList<>();

    for (int i = 0; i < argList.size(); i++) {
      List<Integer> list = argList.get(i);
      ImmutableBitSet bitSet = ImmutableBitSet.of(list);
      int prev = origGroupSets.indexOf(bitSet);
      if (prev == -1) {
        origGroupSets.add(bitSet);
        cleanArgList.add(list);
      } else {
        map.put(i, prev);
      }
    }
    // Calcite expects the grouping sets sorted and without duplicates
    Collections.sort(origGroupSets, ImmutableBitSet.COMPARATOR);

    List<AggregateCall> aggregateCalls = new ArrayList<AggregateCall>();
    // Create GroupingID column
    AggregateCall aggCall = AggregateCall.create(HiveGroupingID.INSTANCE, false,
        new ImmutableList.Builder<Integer>().build(), -1, this.cluster.getTypeFactory()
            .createSqlType(SqlTypeName.BIGINT), HiveGroupingID.INSTANCE.getName());
    aggregateCalls.add(aggCall);
    return new HiveAggregate(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
        aggregate.getInput(), groupSet, origGroupSets, aggregateCalls);
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
  private RelNode convertMonopole(
      Aggregate aggregate,
      List<Integer> argList) {
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
    final Aggregate distinct =
        createSelectDistinct(aggregate, argList, sourceOf);

    // Create an aggregate on top, with the new aggregate list.
    final List<AggregateCall> newAggCalls =
        Lists.newArrayList(aggregate.getAggCallList());
    rewriteAggCalls(newAggCalls, argList, sourceOf);
    final int cardinality = aggregate.getGroupSet().cardinality();
    return aggregate.copy(aggregate.getTraitSet(), distinct,
        aggregate.indicator, ImmutableBitSet.range(cardinality), null,
        newAggCalls);
  }

  private static void rewriteAggCalls(
      List<AggregateCall> newAggCalls,
      List<Integer> argList,
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
          new AggregateCall(
              aggCall.getAggregation(),
              false,
              newArgs,
              aggCall.getType(),
              aggCall.getName());
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
  private static Aggregate createSelectDistinct(
      Aggregate aggregate,
      List<Integer> argList,
      Map<Integer, Integer> sourceOf) {
    final List<Pair<RexNode, String>> projects =
        new ArrayList<Pair<RexNode, String>>();
    final RelNode child = aggregate.getInput();
    final List<RelDataTypeField> childFields =
        child.getRowType().getFieldList();
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
    final RelNode project =
        projFactory.createProject(child, Pair.left(projects), Pair.right(projects));

    // Get the distinct values of the GROUP BY fields and the arguments
    // to the agg functions.
    return aggregate.copy(aggregate.getTraitSet(), project, false,
        ImmutableBitSet.range(projects.size()),
        null, ImmutableList.<AggregateCall>of());
  }
}

// End AggregateExpandDistinctAggregatesRule.java