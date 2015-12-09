/**
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

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.SqlSplittableAggFunction.CountSplitter;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;

public class HiveSqlCountAggFunction extends SqlAggFunction {

  final SqlReturnTypeInference returnTypeInference;
  final SqlOperandTypeInference operandTypeInference;
  final SqlOperandTypeChecker operandTypeChecker;

  public HiveSqlCountAggFunction(SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker) {
    super(
        "count",
        SqlKind.OTHER_FUNCTION,
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker,
        SqlFunctionCategory.NUMERIC);
    this.returnTypeInference = returnTypeInference;
    this.operandTypeChecker = operandTypeChecker;
    this.operandTypeInference = operandTypeInference;
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    if (clazz == SqlSplittableAggFunction.class) {
      return clazz.cast(new HiveCountSplitter());
    }
    return super.unwrap(clazz);
  }

  class HiveCountSplitter extends CountSplitter {

    @Override
    public AggregateCall other(RelDataTypeFactory typeFactory, AggregateCall e) {

      return AggregateCall.create(
          new HiveSqlCountAggFunction(returnTypeInference, operandTypeInference, operandTypeChecker),
          false, ImmutableIntList.of(), -1,
          typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true), "count");
    }
  }
}
