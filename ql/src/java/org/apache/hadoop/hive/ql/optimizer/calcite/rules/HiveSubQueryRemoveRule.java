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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.LogicVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSubqueryRuntimeException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveSubQRemoveRelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.SubqueryConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

/**
 * NOTE: this rule is replicated from Calcite's SubqueryRemoveRule
 * Transform that converts IN, EXISTS and scalar sub-queries into joins.
 * TODO:
 * Reason this is replicated instead of using Calcite's is
 * Calcite creates null literal with null type but hive needs it to be properly typed
 *
 * <p>Sub-queries are represented by {@link RexSubQuery} expressions.
 *
 * <p>A sub-query may or may not be correlated. If a sub-query is correlated,
 * the wrapped {@link RelNode} will contain a {@link org.apache.calcite.rex.RexCorrelVariable} before
 * the rewrite, and the product of the rewrite will be a {@link org.apache.calcite.rel.core.Correlate}.
 * The Correlate can be removed using {@link org.apache.calcite.sql2rel.RelDecorrelator}.
 */
public class HiveSubQueryRemoveRule extends RelOptRule {

  private HiveConf conf;

  public HiveSubQueryRemoveRule(HiveConf conf) {
    super(operand(RelNode.class, null, HiveSubQueryFinder.RELNODE_PREDICATE, any()),
        HiveRelFactories.HIVE_BUILDER, "SubQueryRemoveRule:Filter");
    this.conf = conf;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final RelNode relNode = call.rel(0);
    final HiveSubQRemoveRelBuilder builder =
        new HiveSubQRemoveRelBuilder(null, call.rel(0).getCluster(), null);

    // if subquery is in FILTER
    if (relNode instanceof Filter) {
      final Filter filter = call.rel(0);
      final RexSubQuery e = RexUtil.SubQueryFinder.find(filter.getCondition());
      assert e != null;

      final RelOptUtil.Logic logic =
          LogicVisitor.find(RelOptUtil.Logic.TRUE, ImmutableList.of(filter.getCondition()), e);
      builder.push(filter.getInput());
      final int fieldCount = builder.peek().getRowType().getFieldCount();

      assert (filter instanceof HiveFilter);
      SubqueryConf subqueryConfig = filter.getCluster().getPlanner().
          getContext().unwrap(SubqueryConf.class);
      boolean isCorrScalarQuery = subqueryConfig.getCorrScalarRexSQWithAgg().contains(e.rel);

      final RexNode target =
          apply(call.getMetadataQuery(), e, HiveFilter.getVariablesSet(e), logic, builder, 1,
              fieldCount, isCorrScalarQuery);
      final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
      builder.filter(shuttle.apply(filter.getCondition()));
      builder.project(fields(builder, filter.getRowType().getFieldCount()));
      RelNode newRel = builder.build();
      call.transformTo(newRel);
    } else if (relNode instanceof Project) {
      // if subquery is in PROJECT
      final Project project = call.rel(0);
      final RexSubQuery e = RexUtil.SubQueryFinder.find(project.getProjects());
      assert e != null;

      final RelOptUtil.Logic logic =
          LogicVisitor.find(RelOptUtil.Logic.TRUE_FALSE_UNKNOWN, project.getProjects(), e);
      builder.push(project.getInput());
      final int fieldCount = builder.peek().getRowType().getFieldCount();

      SubqueryConf subqueryConfig =
          project.getCluster().getPlanner().getContext().unwrap(SubqueryConf.class);
      boolean isCorrScalarQuery = subqueryConfig.getCorrScalarRexSQWithAgg().contains(e.rel);

      final RexNode target =
          apply(call.getMetadataQuery(), e, HiveFilter.getVariablesSet(e), logic, builder, 1,
              fieldCount, isCorrScalarQuery);
      final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
      builder.project(shuttle.apply(project.getProjects()), project.getRowType().getFieldNames());
      call.transformTo(builder.build());
    }
  }

  // given a subquery it checks to see what is the aggegate function
  /// if COUNT returns true since COUNT produces 0 on empty result set
  private boolean isAggZeroOnEmpty(RexSubQuery e) {
    //as this is corr scalar subquery with agg we expect one aggregate
    assert (e.getKind() == SqlKind.SCALAR_QUERY);
    assert (e.rel.getInputs().size() == 1);
    Aggregate relAgg = (Aggregate) e.rel.getInput(0);
    assert (relAgg.getAggCallList().size() == 1); //should only have one aggregate
    if (relAgg.getAggCallList().get(0).getAggregation().getKind() == SqlKind.COUNT) {
      return true;
    }
    return false;
  }

  private SqlTypeName getAggTypeForScalarSub(RexSubQuery e) {
    assert (e.getKind() == SqlKind.SCALAR_QUERY);
    assert (e.rel.getInputs().size() == 1);
    Aggregate relAgg = (Aggregate) e.rel.getInput(0);
    assert (relAgg.getAggCallList().size() == 1); //should only have one aggregate
    return relAgg.getAggCallList().get(0).getType().getSqlTypeName();
  }

  private RexNode rewriteScalar(RelMetadataQuery mq, RexSubQuery e, Set<CorrelationId> variablesSet,
      HiveSubQRemoveRelBuilder builder, int offset, int inputCount, boolean isCorrScalarAgg) {
    // if scalar query has aggregate and no windowing and no gby avoid adding sq_count_check
    // since it is guaranteed to produce at most one row
    Double maxRowCount = mq.getMaxRowCount(e.rel);
    boolean shouldIntroSQCountCheck = maxRowCount == null || maxRowCount > 1.0;
    if (shouldIntroSQCountCheck) {
      builder.push(e.rel);
      // returns single row/column
      builder.aggregate(builder.groupKey(), builder.count(false, "cnt"));

      SqlFunction countCheck =
          new SqlFunction("sq_count_check", SqlKind.OTHER_FUNCTION, ReturnTypes.BIGINT,
              InferTypes.RETURN_TYPE, OperandTypes.NUMERIC,
              SqlFunctionCategory.USER_DEFINED_FUNCTION);

      //we create FILTER (sq_count_check(count()) <= 1) instead of PROJECT because RelFieldTrimmer
      // ends up getting rid of Project since it is not used further up the tree
      builder.filter(builder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
          builder.call(countCheck, builder.field("cnt")), builder.literal(1)));
      if (!variablesSet.isEmpty()) {
        builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet);
      } else {
        builder.join(JoinRelType.INNER, builder.literal(true), variablesSet);
      }
      offset++;
    }
    if (isCorrScalarAgg) {
      // Transformation :
      // Outer Query Left Join (inner query) on correlated predicate
      //      and preserve rows only from left side.
      builder.push(e.rel);
      final List<RexNode> parentQueryFields = new ArrayList<>();
      parentQueryFields.addAll(builder.fields());

      // id is appended since there could be multiple scalar subqueries and FILTER
      // is created using field name
      String indicator = "trueLiteral";
      parentQueryFields.add(builder.alias(builder.literal(true), indicator));
      builder.project(parentQueryFields);
      builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet);

      final ImmutableList.Builder<RexNode> operands = ImmutableList.builder();
      RexNode literal;
      if (isAggZeroOnEmpty(e)) {
        // since count has a return type of BIG INT we need to make a literal of type big int
        // relbuilder's literal doesn't allow this
        literal = e.rel.getCluster().getRexBuilder().makeBigintLiteral(new BigDecimal(0));
      } else {
        literal = e.rel.getCluster().getRexBuilder().makeNullLiteral(getAggTypeForScalarSub(e));
      }
      operands.add((builder.isNull(builder.field(indicator))), literal);
      operands.add(field(builder, 1, builder.fields().size() - 2));
      return builder.call(SqlStdOperatorTable.CASE, operands.build());
    }

    //Transformation is to left join for correlated predicates and inner join otherwise,
    // but do a count on inner side before that to make sure it generates atmost 1 row.
    builder.push(e.rel);
    builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet);
    return field(builder, inputCount, offset);
  }

  private RexNode rewriteSomeAll(RexSubQuery e, Set<CorrelationId> variablesSet,
      HiveSubQRemoveRelBuilder builder) {
    final SqlQuantifyOperator op = (SqlQuantifyOperator) e.op;

    // SOME_EQ & SOME_NE should have been rewritten into IN/ NOT IN
    assert (op == SqlStdOperatorTable.SOME_GE || op == SqlStdOperatorTable.SOME_LE
        || op == SqlStdOperatorTable.SOME_LT || op == SqlStdOperatorTable.SOME_GT);

    if (variablesSet.isEmpty()) {
      // for non-correlated case queries such as
      // select e.deptno, e.deptno < some (select deptno from emp) as v
      // from emp as e
      //
      // becomes
      //
      // select e.deptno,
      //   case
      //   when q.c = 0 then false // sub-query is empty
      //   when (e.deptno < q.m) is true then true
      //   when q.c > q.d then unknown // sub-query has at least one null
      //   else e.deptno < q.m
      //   end as v
      // from emp as e
      // cross join (
      //   select max(deptno) as m, count(*) as c, count(deptno) as d
      //   from emp) as q
      builder.push(e.rel).aggregate(builder.groupKey(), op.comparisonKind == SqlKind.GREATER_THAN
              || op.comparisonKind == SqlKind.GREATER_THAN_OR_EQUAL ? builder
              .min("m", builder.field(0)) : builder.max("m", builder.field(0)),
          builder.count(false, "c"), builder.count(false, "d", builder.field(0))).as("q")
          .join(JoinRelType.INNER);
      return builder.call(SqlStdOperatorTable.CASE,
          builder.call(SqlStdOperatorTable.EQUALS, builder.field("q", "c"), builder.literal(0)),
          builder.literal(false), builder.call(SqlStdOperatorTable.IS_TRUE, builder
              .call(RelOptUtil.op(op.comparisonKind, null), e.operands.get(0),
                  builder.field("q", "m"))), builder.literal(true), builder
              .call(SqlStdOperatorTable.GREATER_THAN, builder.field("q", "c"),
                  builder.field("q", "d")),
          e.rel.getCluster().getRexBuilder().makeNullLiteral(SqlTypeName.BOOLEAN), builder
              .call(RelOptUtil.op(op.comparisonKind, null), e.operands.get(0),
                  builder.field("q", "m")));
    } else {
      // for correlated case queries such as
      // select e.deptno, e.deptno < some (select deptno from emp where emp.name = e.name) as v
      // from emp as e
      //
      // becomes
      //
      // select e.deptno,
      //   case
      //   when indicator is null then false // sub-query is empty for corresponding corr value
      //   when q.c = 0 then false // sub-query is empty
      //   when (e.deptno < q.m) is true then true
      //   when q.c > q.d then unknown // sub-query has at least one null
      //   else e.deptno < q.m
      //   end as v
      // from emp as e
      // left outer join (
      //   select max(deptno) as m, count(*) as c, count(deptno) as d, "trueLiteral" as indicator
      //   group by name from emp) as q on e.name = q.name
      subqueryRestriction(e.rel);
      builder.push(e.rel);
      builder.aggregate(builder.groupKey(), op.comparisonKind == SqlKind.GREATER_THAN
              || op.comparisonKind == SqlKind.GREATER_THAN_OR_EQUAL ? builder
              .min("m", builder.field(0)) : builder.max("m", builder.field(0)),
          builder.count(false, "c"), builder.count(false, "d", builder.field(0)));

      final List<RexNode> parentQueryFields = new ArrayList<>();
      parentQueryFields.addAll(builder.fields());
      String indicator = "trueLiteral" ;
      parentQueryFields.add(builder.alias(builder.literal(true), indicator));
      builder.project(parentQueryFields).as("q");
      builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet);
      return builder.call(SqlStdOperatorTable.CASE,
          builder.call(SqlStdOperatorTable.IS_NULL, builder.field(indicator)),
          builder.literal(false),
          builder.call(SqlStdOperatorTable.EQUALS, builder.field("q", "c"), builder.literal(0)),
          builder.literal(false), builder.call(SqlStdOperatorTable.IS_TRUE, builder
              .call(RelOptUtil.op(op.comparisonKind, null), e.operands.get(0),
                  builder.field("q", "m"))), builder.literal(true), builder
              .call(SqlStdOperatorTable.GREATER_THAN, builder.field("q", "c"),
                  builder.field("q", "d")),
          e.rel.getCluster().getRexBuilder().makeNullLiteral(SqlTypeName.BOOLEAN), builder
              .call(RelOptUtil.op(op.comparisonKind, null), e.operands.get(0),
                  builder.field("q", "m")));
    }

  }

  private RexNode rewriteInExists(RexSubQuery e, Set<CorrelationId> variablesSet,
      RelOptUtil.Logic logic, HiveSubQRemoveRelBuilder builder, int offset,
      boolean isCorrScalarAgg) {
    // Most general case, where the left and right keys might have nulls, and
    // caller requires 3-valued logic return.
    //
    // select e.deptno, e.deptno in (select deptno from emp)
    //
    // becomes
    //
    // select e.deptno,
    //   case
    //   when ct.c = 0 then false
    //   when dt.i is not null then true
    //   when e.deptno is null then null
    //   when ct.ck < ct.c then null
    //   else false
    //   end
    // from e
    // left join (
    //   (select count(*) as c, count(deptno) as ck from emp) as ct
    //   cross join (select distinct deptno, true as i from emp)) as dt
    //   on e.deptno = dt.deptno
    //
    // If keys are not null we can remove "ct" and simplify to
    //
    // select e.deptno,
    //   case
    //   when dt.i is not null then true
    //   else false
    //   end
    // from e
    // left join (select distinct deptno, true as i from emp) as dt
    //   on e.deptno = dt.deptno
    //
    // We could further simplify to
    //
    // select e.deptno,
    //   dt.i is not null
    // from e
    // left join (select distinct deptno, true as i from emp) as dt
    //   on e.deptno = dt.deptno
    //
    // but have not yet.
    //
    // If the logic is TRUE we can just kill the record if the condition
    // evaluates to FALSE or UNKNOWN. Thus the query simplifies to an inner
    // join:
    //
    // select e.deptno,
    //   true
    // from e
    // inner join (select distinct deptno from emp) as dt
    //   on e.deptno = dt.deptno
    //

    builder.push(e.rel);
    final List<RexNode> fields = new ArrayList<>();
    if (e.getKind() == SqlKind.IN) {
      fields.addAll(builder.fields());
      // Transformation: sq_count_check(count(*), true) FILTER is generated on top
      //  of subquery which is then joined (LEFT or INNER) with outer query
      //  This transformation is done to add run time check using sq_count_check to
      //  throw an error if subquery is producing zero row, since with aggregate this
      //  will produce wrong results (because we further rewrite such queries into JOIN)
      if (isCorrScalarAgg) {
        // returns single row/column
        builder.aggregate(builder.groupKey(), builder.count(false, "cnt_in"));

        if (!variablesSet.isEmpty()) {
          builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet);
        } else {
          builder.join(JoinRelType.INNER, builder.literal(true), variablesSet);
        }

        SqlFunction inCountCheck =
            new SqlFunction("sq_count_check", SqlKind.OTHER_FUNCTION, ReturnTypes.BIGINT,
                InferTypes.RETURN_TYPE, OperandTypes.NUMERIC,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);

        // we create FILTER (sq_count_check(count()) > 0) instead of PROJECT
        // because RelFieldTrimmer ends up getting rid of Project
        // since it is not used further up the tree
        builder.filter(builder.call(SqlStdOperatorTable.GREATER_THAN,
            //true here indicates that sq_count_check is for IN/NOT IN subqueries
            builder.call(inCountCheck, builder.field("cnt_in"), builder.literal(true)),
            builder.literal(0)));
        offset = offset + 1;
        builder.push(e.rel);
      }
    }

    // First, the cross join
    switch (logic) {
    case TRUE_FALSE_UNKNOWN:
    case UNKNOWN_AS_TRUE:
      // Since EXISTS/NOT EXISTS are not affected by presence of
      // null keys we do not need to generate count(*), count(c)
      if (e.getKind() == SqlKind.EXISTS) {
        logic = RelOptUtil.Logic.TRUE_FALSE;
        break;
      }
      builder.aggregate(builder.groupKey(), builder.count(false, "c"),
          builder.aggregateCall(SqlStdOperatorTable.COUNT, false, null, "ck", builder.fields()));
      builder.as("ct");
      if (!variablesSet.isEmpty()) {
        //builder.join(JoinRelType.INNER, builder.literal(true), variablesSet);
        builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet);
      } else {
        builder.join(JoinRelType.INNER, builder.literal(true), variablesSet);
      }

      offset += 2;
      builder.push(e.rel);
      break;
    }

    // Now the left join
    String trueLiteral = "literalTrue";
    switch (logic) {
    case TRUE:
      if (fields.isEmpty()) {
        builder.project(builder.alias(builder.literal(true), trueLiteral));
        if (!variablesSet.isEmpty() && (e.getKind() == SqlKind.EXISTS
            || e.getKind() == SqlKind.IN)) {
          // avoid adding group by for correlated IN/EXISTS queries
          // since this is rewritting into semijoin
          break;
        } else {
          builder.aggregate(builder.groupKey(0));
        }
      } else {
        if (!variablesSet.isEmpty() && (e.getKind() == SqlKind.EXISTS
            || e.getKind() == SqlKind.IN)) {
          // avoid adding group by for correlated IN/EXISTS queries
          // since this is rewritting into semijoin
          break;
        } else {
          builder.aggregate(builder.groupKey(fields));
        }
      }
      break;
    default:
      fields.add(builder.alias(builder.literal(true), trueLiteral));
      builder.project(fields);
      builder.distinct();
    }
    builder.as("dt");
    final List<RexNode> conditions = new ArrayList<>();
    for (Pair<RexNode, RexNode> pair : Pair.zip(e.getOperands(), builder.fields())) {
      conditions.add(builder.equals(pair.left, RexUtil.shift(pair.right, offset)));
    }
    switch (logic) {
    case TRUE:
      builder.join(JoinRelType.INNER, builder.and(conditions), variablesSet, true);
      return builder.literal(true);
    }
    builder.join(JoinRelType.LEFT, builder.and(conditions), variablesSet);

    final List<RexNode> keyIsNulls = new ArrayList<>();
    for (RexNode operand : e.getOperands()) {
      if (operand.getType().isNullable()) {
        keyIsNulls.add(builder.isNull(operand));
      }
    }
    final ImmutableList.Builder<RexNode> operands = ImmutableList.builder();
    switch (logic) {
    case TRUE_FALSE_UNKNOWN:
    case UNKNOWN_AS_TRUE:
      operands.add(builder.equals(builder.field("ct", "c"), builder.literal(0)),
          builder.literal(false));
      //now that we are using LEFT OUTER JOIN to join inner count, count(*)
      // with outer table, we wouldn't be able to tell if count is zero
      // for inner table since inner join with correlated values will get rid
      // of all values where join cond is not true (i.e where actual inner table
      // will produce zero result). To  handle this case we need to check both
      // count is zero or count is null
      operands.add((builder.isNull(builder.field("ct", "c"))), builder.literal(false));
      break;
    }
    operands
        .add(builder.isNotNull(builder.field("dt",trueLiteral)), builder.literal(true));
    if (!keyIsNulls.isEmpty()) {
      //Calcite creates null literal with Null type here but
      // because HIVE doesn't support null type it is appropriately typed boolean
      operands.add(builder.or(keyIsNulls),
          e.rel.getCluster().getRexBuilder().makeNullLiteral(SqlTypeName.BOOLEAN));
      // we are creating filter here so should not be returning NULL.
      // Not sure why Calcite return NULL
    }
    RexNode b = builder.literal(true);
    switch (logic) {
    case TRUE_FALSE_UNKNOWN:
      b = e.rel.getCluster().getRexBuilder().makeNullLiteral(SqlTypeName.BOOLEAN);
      // fall through
    case UNKNOWN_AS_TRUE:
      operands.add(builder
              .call(SqlStdOperatorTable.LESS_THAN, builder.field("ct", "ck"), builder.field("ct", "c")),
          b);
      break;
    }
    operands.add(builder.literal(false));
    return builder.call(SqlStdOperatorTable.CASE, operands.build());
  }

  protected RexNode apply(RelMetadataQuery mq, RexSubQuery e, Set<CorrelationId> variablesSet,
      RelOptUtil.Logic logic, HiveSubQRemoveRelBuilder builder, int inputCount, int offset,
      boolean isCorrScalarAgg) {
    switch (e.getKind()) {
    case SCALAR_QUERY:
      return rewriteScalar(mq, e, variablesSet, builder, offset, inputCount, isCorrScalarAgg);
    case SOME:
      return rewriteSomeAll(e, variablesSet, builder);
    case IN:
    case EXISTS:
      return rewriteInExists(e, variablesSet, logic, builder, offset, isCorrScalarAgg);
    default:
      throw new AssertionError(e.getKind());
    }
  }

  /**
   * Returns a reference to a particular field, by offset, across several
   * inputs on a {@link RelBuilder}'s stack.
   */
  private RexInputRef field(HiveSubQRemoveRelBuilder builder, int inputCount, int offset) {
    for (int inputOrdinal = 0; ;) {
      final RelNode r = builder.peek(inputCount, inputOrdinal);
      if (offset < r.getRowType().getFieldCount()) {
        return builder.field(inputCount, inputOrdinal, offset);
      }
      ++inputOrdinal;
      offset -= r.getRowType().getFieldCount();
    }
  }

  /**
   * Returns a list of expressions that project the first {@code fieldCount}
   * fields of the top input on a {@link RelBuilder}'s stack.
   */
  private static List<RexNode> fields(HiveSubQRemoveRelBuilder builder, int fieldCount) {
    final List<RexNode> projects = new ArrayList<>();
    for (int i = 0; i < fieldCount; i++) {
      projects.add(builder.field(i));
    }
    return projects;
  }

  /**
   * Shuttle that replaces occurrences of a given
   * {@link org.apache.calcite.rex.RexSubQuery} with a replacement
   * expression.
   */
  private static class ReplaceSubQueryShuttle extends RexShuttle {
    private final RexSubQuery subQuery;
    private final RexNode replacement;

    ReplaceSubQueryShuttle(RexSubQuery subQuery, RexNode replacement) {
      this.subQuery = subQuery;
      this.replacement = replacement;
    }

    @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
      return RexUtil.eq(subQuery, this.subQuery) ? replacement : subQuery;
    }
  }

  // TODO:
  // Following HiveSubQueryFinder has been copied from RexUtil::SubQueryFinder
  // since there is BUG in there (CALCITE-1726).
  // Once CALCITE-1726 is fixed we should get rid of the following code

  /**
   * Visitor that throws {@link org.apache.calcite.util.Util.FoundOne} if
   * applied to an expression that contains a {@link RexSubQuery}.
   */
  public static final class HiveSubQueryFinder extends RexVisitorImpl<Void> {
    public static final HiveSubQueryFinder INSTANCE = new HiveSubQueryFinder();

    /**
     * Returns whether a {@link Project} contains a sub-query.
     */
    public static final Predicate<RelNode> RELNODE_PREDICATE = new Predicate<RelNode>() {
      @Override public boolean apply(RelNode relNode) {
        if (relNode instanceof Project) {
          Project project = (Project) relNode;
          for (RexNode node : project.getProjects()) {
            try {
              node.accept(INSTANCE);
            } catch (Util.FoundOne e) {
              return true;
            }
          }
          return false;
        } else if (relNode instanceof Filter) {
          try {
            ((Filter) relNode).getCondition().accept(INSTANCE);
            return false;
          } catch (Util.FoundOne e) {
            return true;
          }
        }
        return false;
      }
    };

    private HiveSubQueryFinder() {
      super(true);
    }

    @Override public Void visitSubQuery(RexSubQuery subQuery) {
      throw new Util.FoundOne(subQuery);
    }

    public static RexSubQuery find(Iterable<RexNode> nodes) {
      for (RexNode node : nodes) {
        try {
          node.accept(INSTANCE);
        } catch (Util.FoundOne e) {
          return (RexSubQuery) e.getNode();
        }
      }
      return null;
    }

    public static RexSubQuery find(RexNode node) {
      try {
        node.accept(INSTANCE);
        return null;
      } catch (Util.FoundOne e) {
        return (RexSubQuery) e.getNode();
      }
    }
  }

  public static void subqueryRestriction(RelNode relNode) {
    if (relNode instanceof HiveAggregate) {
      HiveAggregate aggregate = (HiveAggregate) relNode;
      if (!aggregate.getAggCallList().isEmpty() && aggregate.getGroupSet().isEmpty()) {
        throw new CalciteSubqueryRuntimeException(
            "Subquery rewrite: Aggregate without group by is not allowed");
      }
    } else if (relNode instanceof HiveProject || relNode instanceof HiveFilter) {
      subqueryRestriction(relNode.getInput(0));
    }
  }
}

// End SubQueryRemoveRule.java
