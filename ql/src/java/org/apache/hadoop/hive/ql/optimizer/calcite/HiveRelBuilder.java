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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveMergeableAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlCountAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlMinMaxAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlSumAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.HiveSqlSumEmptyIsZeroAggFunction;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFloorDate;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;


/**
 * Builder for relational expressions in Hive.
 *
 * <p>{@code RelBuilder} does not make possible anything that you could not
 * also accomplish by calling the factory methods of the particular relational
 * expression. But it makes common tasks more straightforward and concise.
 *
 * <p>It is not thread-safe.
 */
public class HiveRelBuilder extends RelBuilder {

  private final FunctionHelper functionHelper;

  private HiveRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
    super(context, cluster, relOptSchema);
    this.functionHelper = cluster.getPlanner().getContext().unwrap(FunctionHelper.class);
  }

  /** Creates a {@link RelBuilderFactory}, a partially-created RelBuilder.
   * Just add a {@link RelOptCluster} and a {@link RelOptSchema} */
  public static RelBuilderFactory proto(final Context context) {
    return (cluster, schema) -> new HiveRelBuilder(context, cluster, schema);
  }

  @Override
  public RelBuilder filter(Iterable<? extends RexNode> predicates) {
    final RexNode x = RexUtil.composeConjunction(
        cluster.getRexBuilder(), predicates, false);
    if (!x.isAlwaysTrue()) {
      final RelNode input = build();
      final RelNode filter = HiveRelFactories.HIVE_FILTER_FACTORY.createFilter(input, x);
      return this.push(filter);
    }
    return this;
  }

  /**
   * Empty relationship can be expressed in many different ways, e.g.,
   * filter(cond=false), empty LogicalValues(), etc. Calcite default implementation
   * uses empty LogicalValues(); however, currently there is not an equivalent to
   * this expression in Hive. Thus, we use limit 0, since Hive already includes
   * optimizations that will do early pruning of the result tree when it is found,
   * e.g., GlobalLimitOptimizer.
   */
  @Override
  public RelBuilder empty() {
    final RelNode input = build();
    final RelNode sort = HiveRelFactories.HIVE_SORT_FACTORY.createSort(
            input, RelCollations.of(), null, literal(0));
    return this.push(sort);
  }

  public static SqlFunction getFloorSqlFunction(TimeUnitRange flag) {
    switch (flag) {
      case YEAR:
        return HiveFloorDate.YEAR;
      case QUARTER:
        return HiveFloorDate.QUARTER;
      case MONTH:
        return HiveFloorDate.MONTH;
      case DAY:
        return HiveFloorDate.DAY;
      case HOUR:
        return HiveFloorDate.HOUR;
      case MINUTE:
        return HiveFloorDate.MINUTE;
      case SECOND:
        return HiveFloorDate.SECOND;
    }
    return SqlStdOperatorTable.FLOOR;
  }

  public static SqlAggFunction getRollup(SqlAggFunction aggregation) {
    if (aggregation instanceof HiveMergeableAggregate) {
      HiveMergeableAggregate mAgg = (HiveMergeableAggregate) aggregation;
      return mAgg.getMergeAggFunction();
    }
    if (aggregation instanceof HiveSqlSumAggFunction
        || aggregation instanceof HiveSqlMinMaxAggFunction
        || aggregation instanceof HiveSqlSumEmptyIsZeroAggFunction) {
      return aggregation;
    }
    if (aggregation instanceof HiveSqlCountAggFunction) {
      HiveSqlCountAggFunction countAgg = (HiveSqlCountAggFunction) aggregation;
      return new HiveSqlSumEmptyIsZeroAggFunction(countAgg.isDistinct(), countAgg.getReturnTypeInference(),
          countAgg.getOperandTypeInference(), countAgg.getOperandTypeChecker());
    }
    return null;
  }

  @Override
  protected boolean shouldMergeProject() {
    /* CALCITE-2470 added ability to merge Project-s together.
     * The problem with it is that it may merge 2 windowing expressions.
     */
    return false;
  }

  @Override
  @Nonnull
  public RexNode call(SqlOperator operator, RexNode... operands) {
    return this.call(operator, ImmutableList.copyOf(operands));
  }

  @Override
  @Nonnull
  public RexNode call(SqlOperator operator, Iterable<? extends RexNode> operands) {
    return this.call(operator, ImmutableList.copyOf(operands));
  }

  @Nonnull
  private RexNode call(SqlOperator operator, List<RexNode> operandList) {
    switch (operator.getKind()) {
    // These operators should not go through the engine-specific helper
    // because they are not actual engine functions.
    case AS:
    case DESCENDING:
    case NULLS_FIRST:
    case NULLS_LAST:
    // These operators do not need to go through the engine-specific helper
    // because they do not need adjustments and their nullability would
    // need to be set correctly in all of them.
    case IS_NOT_NULL:
    case IS_NULL:
    case IS_NOT_TRUE:
    case IS_TRUE:
    case IS_NOT_FALSE:
    case IS_FALSE:
      return super.call(operator, operandList);
    default:
      try {
        return functionHelper.makeCall(this.cluster.getRexBuilder(), operator, operandList);
      } catch (SemanticException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
