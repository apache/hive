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

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.LogicVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
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
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSubqueryRuntimeException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelShuttleImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.correlation.HiveCorrelationInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;

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

  public static RelOptRule forProject(HiveConf conf) {
    return new HiveSubQueryRemoveRule(
        RelOptRule.operandJ(HiveProject.class, null, RexUtil.SubQueryFinder::containsSubQuery, any()),
        "SubQueryRemoveRule:Project", conf);
  }

  public static RelOptRule forFilter(HiveConf conf) {
    return new HiveSubQueryRemoveRule(
        RelOptRule.operandJ(HiveFilter.class, null, RexUtil.SubQueryFinder::containsSubQuery, any()),
        "SubQueryRemoveRule:Filter", conf);
  }

  private final HiveConf conf;

  private HiveSubQueryRemoveRule(RelOptRuleOperand operand, String description, HiveConf conf) {
    super(operand, HiveRelFactories.HIVE_BUILDER, description);
    this.conf = conf;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final RelNode relNode = call.rel(0);
    final RelBuilder builder = call.builder();

    // if subquery is in FILTER
    if (relNode instanceof HiveFilter) {
      final HiveFilter filter = call.rel(0);
      // Since there is a RexSubQuery, there should be a HiveCorrelationInfo
      // in the RelNode
      Preconditions.checkState(!filter.getCorrelationInfos().isEmpty());
      HiveCorrelationInfo correlationInfo = filter.getCorrelationInfos().get(0);
      final RelOptUtil.Logic logic = LogicVisitor.find(RelOptUtil.Logic.TRUE,
          ImmutableList.of(filter.getCondition()), correlationInfo.rexSubQuery);
      builder.push(filter.getInput());
      final int fieldCount = builder.peek().getRowType().getFieldCount();

      boolean isCorrScalarQuery = correlationInfo.isCorrScalarQuery();

      final RexNode target =
          apply(call.getMetadataQuery(), correlationInfo.rexSubQuery,
              correlationInfo.correlationIds, logic, builder, 1, fieldCount, isCorrScalarQuery);
      final RexShuttle shuttle = new ReplaceSubQueryShuttle(correlationInfo.rexSubQuery, target);
      builder.filter(shuttle.apply(filter.getCondition()));
      builder.project(fields(builder, filter.getRowType().getFieldCount()));
      RelNode newRel = builder.build();
      call.transformTo(newRel);
    } else if (relNode instanceof HiveProject) {
      final HiveProject project = call.rel(0);
      // Since there is a RexSubQuery, there should be a HiveCorrelationInfo
      // in the RelNode
      Preconditions.checkState(!project.getCorrelationInfos().isEmpty());
      HiveCorrelationInfo correlationInfo = project.getCorrelationInfos().get(0);

      final RelOptUtil.Logic logic = LogicVisitor.find(RelOptUtil.Logic.TRUE_FALSE_UNKNOWN,
          project.getProjects(), correlationInfo.rexSubQuery);
      builder.push(project.getInput());
      final int fieldCount = builder.peek().getRowType().getFieldCount();

      boolean isCorrScalarQuery = correlationInfo.isCorrScalarQuery();

      final RexNode target =
          apply(call.getMetadataQuery(), correlationInfo.rexSubQuery,
              correlationInfo.correlationIds, logic, builder, 1, fieldCount, isCorrScalarQuery);
      final RexShuttle shuttle = new ReplaceSubQueryShuttle(correlationInfo.rexSubQuery, target);
      builder.project(shuttle.apply(project.getProjects()), project.getRowType().getFieldNames());
      call.transformTo(builder.build());
    }
  }

  // given a subquery it checks to see what is the aggregate function
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
      RelBuilder builder, int offset, int inputCount, boolean isCorrScalarAgg) {
    // if scalar query has aggregate and no windowing and no gby avoid adding sq_count_check
    // since it is guaranteed to produce at most one row
    Double maxRowCount = mq.getMaxRowCount(e.rel);
    boolean shouldIntroSQCountCheck = maxRowCount == null || maxRowCount > 1.0;
    if (shouldIntroSQCountCheck) {
      builder.push(e.rel);
      // returns single row/column
      builder.aggregate(builder.groupKey(), builder.count(false, "cnt"));

      SqlFunction countCheck =
          new SqlFunction("sq_count_check", SqlKind.OTHER_FUNCTION, ReturnTypes.BOOLEAN,
              InferTypes.RETURN_TYPE, OperandTypes.NUMERIC,
              SqlFunctionCategory.USER_DEFINED_FUNCTION);

      //we create FILTER (sq_count_check(count())) instead of PROJECT because RelFieldTrimmer
      // ends up getting rid of Project since it is not used further up the tree
      //sq_count_check returns true when subquery returns single row, else it fails
      builder.filter(builder.call(countCheck, builder.field("cnt")));
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
      RelBuilder builder) {
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
      RelOptUtil.Logic logic, RelBuilder builder, int offset,
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

    final List<RexNode> fields = new ArrayList<>();
    if (e.getKind() == SqlKind.IN) {
      builder.push(e.rel);
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
            new SqlFunction("sq_count_check", SqlKind.OTHER_FUNCTION, ReturnTypes.BOOLEAN,
                InferTypes.RETURN_TYPE, OperandTypes.NUMERIC,
                SqlFunctionCategory.USER_DEFINED_FUNCTION);

        // we create FILTER (sq_count_check(count())) instead of PROJECT
        // because RelFieldTrimmer ends up getting rid of Project
        // since it is not used further up the tree.
        // sq_count_check returns true when subquery returns single row, else it fails
        builder.filter(
            //true here indicates that sq_count_check is for IN/NOT IN subqueries
            builder.call(inCountCheck, builder.field("cnt_in"), builder.literal(true))
            );
        offset = offset + 1;
        builder.push(e.rel);
      }
    } else if (e.getKind() == SqlKind.EXISTS && !variablesSet.isEmpty()) {
      // Query has 'exists' and correlation:
      // select * from web_sales ws1
      // where exists (select 1 from web_sales ws2 where ws1.ws_order_number = ws2.ws_order_number limit 1);
      //
      // HiveRelDecorrelator will replace LogicalCorrelate with a SemiJoin. Hence the right hand side won't be
      // evaluated for every row coming from left and SortLimit cuts the right result set incorrectly. (HIVE-24199)
      builder.push(e.rel.accept(new HiveSortLimitRemover()));
    } else {
      // Query may has exists but no correlation
      // select * from web_sales ws1
      // where exists (select 1 from web_sales ws2 where ws2.ws_order_number = 2 limit 1);
      builder.push(e.rel);
    }
    boolean isCandidateForAntiJoin = false;
    // First, the cross join
    switch (logic) {
    case TRUE_FALSE_UNKNOWN:
    case UNKNOWN_AS_TRUE:
      // Since EXISTS/NOT EXISTS are not affected by presence of
      // null keys we do not need to generate count(*), count(c)
      if (e.getKind() == SqlKind.EXISTS) {
        logic = RelOptUtil.Logic.TRUE_FALSE;
        if (conf.getBoolVar(HiveConf.ConfVars.HIVE_CONVERT_ANTI_JOIN)) {
          //TODO : As of now anti join is first converted to left outer join
          // and then converted to anti join.
          //logic = RelOptUtil.Logic.FALSE;

          isCandidateForAntiJoin = true;
        }
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
    case FALSE:
      if (fields.isEmpty()) {
        if (logic == RelOptUtil.Logic.TRUE) {
          builder.project(builder.alias(builder.literal(true), trueLiteral));
        } else {
          builder.project(builder.alias(builder.literal(false), "literalFalse"));
        }
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
      // If, not-exists is first converted to left outer join with null
      // filter and then to anti join, then the distinct clause is added
      // later during semi/anti join processing at genMapGroupByForSemijoin.
      if (!isCandidateForAntiJoin || variablesSet.isEmpty()) {
        builder.distinct();
      }
    }
    builder.as("dt");
    final List<RexNode> conditions = new ArrayList<>();
    for (Pair<RexNode, RexNode> pair : Pair.zip(e.getOperands(), builder.fields())) {
      conditions.add(builder.equals(pair.left, RexUtil.shift(pair.right, offset)));
    }
    switch (logic) {
    case TRUE:
      builder.join(JoinRelType.SEMI, builder.and(conditions), variablesSet);
      return builder.literal(true);
    case FALSE:
      builder.join(JoinRelType.ANTI, builder.and(conditions), variablesSet);
      return builder.literal(false);
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
      RelOptUtil.Logic logic, RelBuilder builder, int inputCount, int offset,
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
  private RexInputRef field(RelBuilder builder, int inputCount, int offset) {
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
  private static List<RexNode> fields(RelBuilder builder, int fieldCount) {
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
      return subQuery.equals(this.subQuery) ? replacement : subQuery;
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

  public static class HiveSortLimitRemover extends HiveRelShuttleImpl {
    @Override
    public RelNode visit(HiveSortLimit sort) {
      RexNode rexNode = sort.getOffsetExpr();
      if (rexNode != null && rexNode.getKind() == SqlKind.LITERAL) {
        RexLiteral offsetExpr = (RexLiteral)rexNode;
        if (!BigDecimal.ZERO.equals(offsetExpr.getValue())) {
          throw new RuntimeException(org.apache.hadoop.hive.ql.ErrorMsg.OFFSET_NOT_SUPPORTED_IN_SUBQUERY.getMsg());
        }
      }
      rexNode = sort.getFetchExpr();
      if (rexNode != null && rexNode.getKind() == SqlKind.LITERAL) {
        RexLiteral fetchExpr = (RexLiteral) rexNode;
        if (BigDecimal.ZERO.equals(fetchExpr.getValue())) {
          return super.visit(sort);
        }
      }
      return super.visit(sort.getInput());
    }
  }
}

// End SubQueryRemoveRule.java
