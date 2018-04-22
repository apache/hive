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
package org.apache.hadoop.hive.ql.optimizer.calcite.functions;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlSplittableAggFunction.CountSplitter;
import org.apache.calcite.sql.SqlSplittableAggFunction.Registry;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;

import com.google.common.collect.ImmutableList;

public class HiveSqlCountAggFunction extends SqlAggFunction implements CanAggregateDistinct {

  final boolean                isDistinct;
  final SqlReturnTypeInference returnTypeInference;
  final SqlOperandTypeInference operandTypeInference;
  final SqlOperandTypeChecker operandTypeChecker;

  public HiveSqlCountAggFunction(boolean isDistinct, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker) {
    super(
        "count",
        SqlKind.COUNT,
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker,
        SqlFunctionCategory.NUMERIC);
    this.isDistinct = isDistinct;
    this.returnTypeInference = returnTypeInference;
    this.operandTypeChecker = operandTypeChecker;
    this.operandTypeInference = operandTypeInference;
  }

  @Override
  public boolean isDistinct() {
    return isDistinct;
  }

  @Override
  public SqlSyntax getSyntax() {
    return SqlSyntax.FUNCTION_STAR;
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    if (clazz == SqlSplittableAggFunction.class) {
      return clazz.cast(new HiveCountSplitter());
    }
    return super.unwrap(clazz);
  }

  // We need to override these methods due to difference in nullability between Hive and
  // Calcite for the return types of the aggregation  (in particular, for COUNT and SUM0).
  // TODO: We should close the semantics gaps between Hive and Calcite for nullability of
  // aggregation calls return types. This might be useful to trigger some additional
  // rewriting rules that would remove unnecessary predicates, etc.
  class HiveCountSplitter extends CountSplitter {

    @Override
    public AggregateCall other(RelDataTypeFactory typeFactory, AggregateCall e) {
      return AggregateCall.create(
          new HiveSqlCountAggFunction(isDistinct, returnTypeInference, operandTypeInference, operandTypeChecker),
          false, ImmutableIntList.of(), -1,
          typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true), "count");
    }

    @Override
    public AggregateCall topSplit(RexBuilder rexBuilder,
        Registry<RexNode> extra, int offset, RelDataType inputRowType,
        AggregateCall aggregateCall, int leftSubTotal, int rightSubTotal) {
      final List<RexNode> merges = new ArrayList<>();
      if (leftSubTotal >= 0) {
        merges.add(
            rexBuilder.makeInputRef(aggregateCall.type, leftSubTotal));
      }
      if (rightSubTotal >= 0) {
        merges.add(
            rexBuilder.makeInputRef(aggregateCall.type, rightSubTotal));
      }
      RexNode node;
      switch (merges.size()) {
      case 1:
        node = merges.get(0);
        break;
      case 2:
        node = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, merges);
        break;
      default:
        throw new AssertionError("unexpected count " + merges);
      }
      int ordinal = extra.register(node);
      return AggregateCall.create(
          new HiveSqlSumEmptyIsZeroAggFunction(isDistinct, returnTypeInference, operandTypeInference, operandTypeChecker),
          false, ImmutableList.of(ordinal), -1, aggregateCall.type, aggregateCall.name);
    }
  }
}
