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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.CompositeList;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlCountAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlSumAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This rule is a copy of {@link org.apache.calcite.rel.rules.AggregateReduceFunctionsRule}
 * that regenerates Hive specific aggregate operators.
 *
 * TODO: When CALCITE-2216 is completed, we should be able to remove much of this code and
 * just override the relevant methods.
 *
 * Planner rule that reduces aggregate functions in
 * {@link org.apache.calcite.rel.core.Aggregate}s to simpler forms.
 *
 * <p>Rewrites:
 * <ul>
 *
 * <li>AVG(x) &rarr; SUM(x) / COUNT(x)
 *
 * <li>STDDEV_POP(x) &rarr; SQRT(
 *     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
 *    / COUNT(x))
 *
 * <li>STDDEV_SAMP(x) &rarr; SQRT(
 *     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
 *     / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END)
 *
 * <li>VAR_POP(x) &rarr; (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
 *     / COUNT(x)
 *
 * <li>VAR_SAMP(x) &rarr; (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
 *        / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END
 * </ul>
 */
public class HiveAggregateReduceFunctionsRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** The singleton. */
  public static final HiveAggregateReduceFunctionsRule INSTANCE =
      new HiveAggregateReduceFunctionsRule();

  //~ Constructors -----------------------------------------------------------

  /** Creates an HiveAggregateReduceFunctionsRule. */
  public HiveAggregateReduceFunctionsRule() {
    super(operand(HiveAggregate.class, any()),
        HiveRelFactories.HIVE_BUILDER, null);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call) {
    if (!super.matches(call)) {
      return false;
    }
    Aggregate oldAggRel = (Aggregate) call.rels[0];
    return containsAvgStddevVarCall(oldAggRel.getAggCallList());
  }

  @Override
  public void onMatch(RelOptRuleCall ruleCall) {
    Aggregate oldAggRel = (Aggregate) ruleCall.rels[0];
    reduceAggs(ruleCall, oldAggRel);
  }

  /**
   * Returns whether any of the aggregates are calls to AVG, STDDEV_*, VAR_*.
   *
   * @param aggCallList List of aggregate calls
   */
  private boolean containsAvgStddevVarCall(List<AggregateCall> aggCallList) {
    for (AggregateCall call : aggCallList) {
      if (isReducible(call.getAggregation().getKind())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns whether the aggregate call is a reducible function
   */
  private boolean isReducible(final SqlKind kind) {
    if (SqlKind.AVG_AGG_FUNCTIONS.contains(kind)) {
      return true;
    }
    if (kind == SqlKind.SUM0) {
      return true;
    }
    return false;
  }

  /**
   * Reduces all calls to SUM0, AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP in
   * the aggregates list to.
   *
   * <p>It handles newly generated common subexpressions since this was done
   * at the sql2rel stage.
   */
  private void reduceAggs(
      RelOptRuleCall ruleCall,
      Aggregate oldAggRel) {
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

    List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
    final int groupCount = oldAggRel.getGroupCount();

    final List<AggregateCall> newCalls = Lists.newArrayList();
    final Map<AggregateCall, RexNode> aggCallMapping = Maps.newHashMap();

    final List<RexNode> projList = Lists.newArrayList();

    // pass through group key
    for (int i = 0; i < groupCount; ++i) {
      projList.add(
          rexBuilder.makeInputRef(
              getFieldType(oldAggRel, i),
              i));
    }

    // List of input expressions. If a particular aggregate needs more, it
    // will add an expression to the end, and we will create an extra
    // project.
    final RelBuilder relBuilder = ruleCall.builder();
    relBuilder.push(oldAggRel.getInput());
    final List<RexNode> inputExprs = new ArrayList<>(relBuilder.fields());

    // create new agg function calls and rest of project list together
    for (AggregateCall oldCall : oldCalls) {
      projList.add(
          reduceAgg(
              oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs));
    }

    final int extraArgCount =
        inputExprs.size() - relBuilder.peek().getRowType().getFieldCount();
    if (extraArgCount > 0) {
      relBuilder.project(inputExprs,
          CompositeList.of(
              relBuilder.peek().getRowType().getFieldNames(),
              Collections.<String>nCopies(extraArgCount, null)));
    }
    newAggregateRel(relBuilder, oldAggRel, newCalls);
    relBuilder.project(projList, oldAggRel.getRowType().getFieldNames())
        .convert(oldAggRel.getRowType(), false);
    ruleCall.transformTo(relBuilder.build());
  }

  private RexNode reduceAgg(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    final SqlKind kind = oldCall.getAggregation().getKind();
    if (isReducible(kind)) {
      switch (kind) {
      case SUM0:
        // replace original SUM0(x) with COALESCE(SUM(x), 0)
        return reduceSum0(oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs);
      case AVG:
        // replace original AVG(x) with SUM(x) / COUNT(x)
        return reduceAvg(oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs);
      case STDDEV_POP:
        // replace original STDDEV_POP(x) with
        //   SQRT(
        //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
        //     / COUNT(x))
        return reduceStddev(oldAggRel, oldCall, true, true, newCalls,
            aggCallMapping, inputExprs);
      case STDDEV_SAMP:
        // replace original STDDEV_SAMP(x) with
        //   SQRT(
        //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
        //     / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END)
        return reduceStddev(oldAggRel, oldCall, false, true, newCalls,
            aggCallMapping, inputExprs);
      case VAR_POP:
        // replace original VAR_POP(x) with
        //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
        //     / COUNT(x)
        return reduceStddev(oldAggRel, oldCall, true, false, newCalls,
            aggCallMapping, inputExprs);
      case VAR_SAMP:
        // replace original VAR_SAMP(x) with
        //     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
        //     / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END
        return reduceStddev(oldAggRel, oldCall, false, false, newCalls,
            aggCallMapping, inputExprs);
      default:
        throw Util.unexpected(kind);
      }
    } else {
      // anything else:  preserve original call
      RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
      final int nGroups = oldAggRel.getGroupCount();
      List<RelDataType> oldArgTypes =
          SqlTypeUtil.projectTypes(
              oldAggRel.getInput().getRowType(), oldCall.getArgList());
      return rexBuilder.addAggCall(oldCall,
          nGroups,
          newCalls,
          aggCallMapping,
          oldArgTypes);
    }
  }

  private AggregateCall createAggregateCallWithBinding(
      RelDataTypeFactory typeFactory,
      SqlAggFunction aggFunction,
      RelDataType operandType,
      Aggregate oldAggRel,
      AggregateCall oldCall,
      int argOrdinal) {
    final Aggregate.AggCallBinding binding =
        new Aggregate.AggCallBinding(typeFactory, aggFunction,
            ImmutableList.of(operandType), oldAggRel.getGroupCount(),
            oldCall.filterArg >= 0);
    return AggregateCall.create(aggFunction,
        oldCall.isDistinct(),
        oldCall.isApproximate(),
        oldCall.ignoreNulls(),
        ImmutableIntList.of(argOrdinal),
        oldCall.filterArg,
        oldCall.getCollation(),
        aggFunction.inferReturnType(binding),
        null);
  }

  private RexNode reduceSum0(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    final int nGroups = oldAggRel.getGroupCount();
    final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    final RelDataTypeFactory typeFactory = oldAggRel.getCluster().getTypeFactory();
    final int iAvgInput = oldCall.getArgList().get(0);
    final RelDataType sum0InputType = typeFactory.createTypeWithNullability(
        getFieldType(oldAggRel.getInput(), iAvgInput), true);
    final RelDataType sumReturnType = getSumReturnType(
        rexBuilder.getTypeFactory(), sum0InputType);
    final AggregateCall sumCall =
        AggregateCall.create(
            new HiveSqlSumAggFunction(
                oldCall.isDistinct(),
                ReturnTypes.explicit(sumReturnType),
                oldCall.getAggregation().getOperandTypeInference(),
                oldCall.getAggregation().getOperandTypeChecker()), //SqlStdOperatorTable.SUM,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.getArgList(),
            oldCall.filterArg,
            oldAggRel.getGroupCount(),
            oldAggRel.getInput(),
            null,
            null);

    RexNode refSum =
        rexBuilder.addAggCall(sumCall,
            nGroups,
            oldAggRel.indicator,
            newCalls,
            aggCallMapping,
            ImmutableList.of(sum0InputType));
    refSum = rexBuilder.ensureType(oldCall.getType(), refSum, true);

    final RexNode coalesce = rexBuilder.makeCall(
        SqlStdOperatorTable.COALESCE, refSum, rexBuilder.makeZeroLiteral(refSum.getType()));
    return rexBuilder.makeCast(oldCall.getType(), coalesce);
  }

  private RexNode reduceAvg(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    final int nGroups = oldAggRel.getGroupCount();
    final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    final RelDataTypeFactory typeFactory = oldAggRel.getCluster().getTypeFactory();

    Preconditions.checkState(oldCall.getArgList().size() == 1);
    // We compute SUM with either BIGINT, DOUBLE, or DECIMAL
    final RexNode arg = getAvgInput(inputExprs.get(oldCall.getArgList().get(0)), rexBuilder, typeFactory);
    final List<Integer> argIndices = Collections.singletonList(lookupOrAdd(inputExprs, arg));
    final RelDataType sumReturnType = getSumReturnType(
        rexBuilder.getTypeFactory(), arg.getType());
    final AggregateCall sumCall =
        AggregateCall.create(
            new HiveSqlSumAggFunction(
                oldCall.isDistinct(),
                ReturnTypes.explicit(sumReturnType),
                oldCall.getAggregation().getOperandTypeInference(),
                oldCall.getAggregation().getOperandTypeChecker()), //SqlStdOperatorTable.SUM,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            argIndices,
            oldCall.filterArg,
            oldCall.getCollation(),
            sumReturnType,
            null);
    // The denominator of AVG is always BIGINT
    final RelDataType countRetType = typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.BIGINT), true);
    final AggregateCall countCall =
        AggregateCall.create(
            new HiveSqlCountAggFunction(
                oldCall.isDistinct(),
                ReturnTypes.explicit(countRetType),
                oldCall.getAggregation().getOperandTypeInference(),
                oldCall.getAggregation().getOperandTypeChecker()), //SqlStdOperatorTable.COUNT,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            // We need to use the cast argument here to apply the same null-check as SUM
            argIndices,
            oldCall.filterArg,
            oldCall.getCollation(),
            countRetType,
            null);

    // NOTE:  these references are with respect to the output
    // of newAggRel
    RexNode numeratorRef =
        rexBuilder.addAggCall(sumCall,
            nGroups,
            newCalls,
            aggCallMapping,
            ImmutableList.of(arg.getType()));
    final RexNode denominatorRef =
        rexBuilder.addAggCall(countCall,
            nGroups,
            newCalls,
            aggCallMapping,
            ImmutableList.of(arg.getType()));

    if (numeratorRef.getType().getSqlTypeName() != SqlTypeName.DECIMAL) {
      // If type is not decimal, we enforce the same type as the avg to comply with
      // Hive semantics
      numeratorRef = rexBuilder.ensureType(oldCall.getType(), numeratorRef, true);
    }
    final RexNode divideRef =
        rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, numeratorRef, denominatorRef);
    return rexBuilder.makeCast(oldCall.getType(), divideRef);
  }

  private RexNode reduceStddev(
      Aggregate oldAggRel,
      AggregateCall oldCall,
      boolean biased,
      boolean sqrt,
      List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    // stddev_pop(x) ==>
    //   power(
    //     (sum(x * x) - sum(x) * sum(x) / count(x))
    //     / count(x),
    //     .5)
    //
    // stddev_samp(x) ==>
    //   power(
    //     (sum(x * x) - sum(x) * sum(x) / count(x))
    //     / nullif(count(x) - 1, 0),
    //     .5)
    final int nGroups = oldAggRel.getGroupCount();
    final RelOptCluster cluster = oldAggRel.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    Preconditions.checkState(oldCall.getArgList().size() == 1);
    final RelDataType oldCallType =
        typeFactory.createTypeWithNullability(oldCall.getType(), true);

    final RexNode argRef = rexBuilder.ensureType(oldCallType, inputExprs.get(oldCall.getArgList().get(0)), false);
    final int argRefOrdinal = lookupOrAdd(inputExprs, argRef);
    final RelDataType sumReturnType = getSumReturnType(
        rexBuilder.getTypeFactory(), argRef.getType());

    final RexNode argSquared = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY,
        argRef, argRef);
    final int argSquaredOrdinal = lookupOrAdd(inputExprs, argSquared);
    final RelDataType sumSquaredReturnType = getSumReturnType(
        rexBuilder.getTypeFactory(), argSquared.getType());

    final AggregateCall sumArgSquaredAggCall =
        createAggregateCallWithBinding(typeFactory,
            new HiveSqlSumAggFunction(
                oldCall.isDistinct(),
                ReturnTypes.explicit(sumSquaredReturnType),
                InferTypes.explicit(Collections.singletonList(argSquared.getType())),
                oldCall.getAggregation().getOperandTypeChecker()), //SqlStdOperatorTable.SUM,
            argSquared.getType(), oldAggRel, oldCall, argSquaredOrdinal);

    final RexNode sumArgSquared =
        rexBuilder.addAggCall(sumArgSquaredAggCall,
            nGroups,
            newCalls,
            aggCallMapping,
            ImmutableList.of(sumArgSquaredAggCall.getType()));

    final AggregateCall sumArgAggCall =
        AggregateCall.create(
            new HiveSqlSumAggFunction(
                oldCall.isDistinct(),
                ReturnTypes.explicit(sumReturnType),
                InferTypes.explicit(Collections.singletonList(argRef.getType())),
                oldCall.getAggregation().getOperandTypeChecker()), //SqlStdOperatorTable.SUM,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            ImmutableIntList.of(argRefOrdinal),
            oldCall.filterArg,
            oldCall.getCollation(),
            sumReturnType,
            null);

    final RexNode sumArg =
        rexBuilder.addAggCall(sumArgAggCall,
            nGroups,
            newCalls,
            aggCallMapping,
            ImmutableList.of(sumArgAggCall.getType()));
    final RexNode sumArgCast = rexBuilder.ensureType(oldCallType, sumArg, true);
    final RexNode sumSquaredArg =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MULTIPLY, sumArgCast, sumArgCast);

    // COUNT always retains the number of elements as BIGINT
    final RelDataType countRetType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
    final AggregateCall countArgAggCall =
        AggregateCall.create(
            new HiveSqlCountAggFunction(
                oldCall.isDistinct(),
                ReturnTypes.explicit(countRetType),
                oldCall.getAggregation().getOperandTypeInference(),
                oldCall.getAggregation().getOperandTypeChecker()), //SqlStdOperatorTable.COUNT,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            // We need to use the cast argument here to null-check it with the same logic as SUM
            ImmutableIntList.of(argRefOrdinal),
            oldCall.filterArg,
            oldCall.getCollation(),
            countRetType,
            null);

    final RexNode countArg =
        rexBuilder.addAggCall(countArgAggCall,
            nGroups,
            newCalls,
            aggCallMapping,
            ImmutableList.of(argRef.getType()));

    final RexNode avgSumSquaredArg =
        rexBuilder.makeCall(
            SqlStdOperatorTable.DIVIDE, sumSquaredArg, countArg);

    final RexNode diff =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MINUS,
            sumArgSquared, avgSumSquaredArg);

    final RexNode denominator;
    if (biased) {
      denominator = countArg;
    } else {
      final RexLiteral one =
          rexBuilder.makeExactLiteral(BigDecimal.ONE);
      final RexNode nul = rexBuilder.makeNullLiteral(countArg.getType());
      final RexNode countMinusOne =
          rexBuilder.makeCall(
              SqlStdOperatorTable.MINUS, countArg, one);
      final RexNode countEqOne =
          rexBuilder.makeCall(
              SqlStdOperatorTable.EQUALS, countArg, one);
      denominator =
          rexBuilder.makeCall(
              SqlStdOperatorTable.CASE,
              countEqOne, nul, countMinusOne);
    }

    final RexNode div =
        rexBuilder.makeCall(
            SqlStdOperatorTable.DIVIDE, diff, denominator);

    RexNode result = div;
    if (sqrt) {
      final RexNode half =
          rexBuilder.makeExactLiteral(new BigDecimal("0.5"));
      result =
          rexBuilder.makeCall(
              SqlStdOperatorTable.POWER, div, half);
    }

    return rexBuilder.makeCast(
        oldCall.getType(), result);
  }

  /**
   * Finds the ordinal of an element in a list, or adds it.
   *
   * @param list    List
   * @param element Element to lookup or add
   * @return Ordinal of element in list
   */
  private static int lookupOrAdd(List<RexNode> list, RexNode element) {
    for (int ordinal = 0; ordinal < list.size(); ordinal++) {
      if (list.get(ordinal).toString().equals(element.toString())) {
        return ordinal;
      }
    }
    list.add(element);
    return list.size() - 1;
  }

  /**
   * Do a shallow clone of oldAggRel and update aggCalls. Could be refactored
   * into Aggregate and subclasses - but it's only needed for some
   * subclasses.
   *
   * @param relBuilder Builder of relational expressions; at the top of its
   *                   stack is its input
   * @param oldAggregate LogicalAggregate to clone.
   * @param newCalls  New list of AggregateCalls
   */
  protected void newAggregateRel(RelBuilder relBuilder,
      Aggregate oldAggregate, List<AggregateCall> newCalls) {
    relBuilder.aggregate(
        relBuilder.groupKey(oldAggregate.getGroupSet(),
            oldAggregate.getGroupSets()),
        newCalls);
  }

  private RelDataType getFieldType(RelNode relNode, int i) {
    final RelDataTypeField inputField =
        relNode.getRowType().getFieldList().get(i);
    return inputField.getType();
  }

  private RexNode getAvgInput(RexNode input, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
    if (input.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
      // GenericUDAFSum implicitly casts texts into DOUBLE. That conversion could generate NULL when the text is not a
      // valid numeric expression.
      // So, we have to explicitly cast those types here. Otherwise, The COUNT UDF can evaluate values with inconsistent
      // semantics with SUM.
      // For example, if a set of values are `"10"`, `"invalid"` and `"20"`, the average should be (10 + 20) / 2 = 15.
      // Without excluding invalid numeric texts, the result becomes (10 + 20) / 3 = 10.
      // Additionally, SUM and COUNT should refer to the same cast expression so that they can reuse the same column
      // as their input. Otherwise, we may need double columns of `x` for SUM and `CAST(x AS DOUBLE)` for COUNT.
      final RelDataType targetType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
      final RelDataType nullableTargetType = typeFactory.createTypeWithNullability(targetType, true);
      return rexBuilder.makeCast(nullableTargetType, input);
    }
    return input;
  }

  private RelDataType getSumReturnType(RelDataTypeFactory typeFactory,
      RelDataType inputType) {
    switch (inputType.getSqlTypeName()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return TypeConverter.convert(TypeInfoFactory.longTypeInfo, typeFactory);
      case TIMESTAMP:
      case FLOAT:
      case DOUBLE:
      case VARCHAR:
      case CHAR:
        return TypeConverter.convert(TypeInfoFactory.doubleTypeInfo, typeFactory);
      case DECIMAL:
        return typeFactory.getTypeSystem().deriveSumType(typeFactory, inputType);
      default:
        // Unsupported types will be validated when GenericUDAFSum is initialized. We keep the original expression here
        return inputType;
    }
  }
}
