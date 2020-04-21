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

package org.apache.hadoop.hive.ql.plan.impala.funcmapper;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.hadoop.hive.ql.optimizer.calcite.ImpalaTypeSystemImpl;
import org.apache.impala.analysis.ArithmeticExpr;

import java.util.List;
import java.util.Map;

/**
 * Impala Calcite Arithmetic Operators
 */
public class ImpalaArithmeticOperators {

  /**
   * Set up the infer return type using Impala's code.
   */
  private static final SqlReturnTypeInference ARITHMETIC_INFERENCE =
      opBinding -> {
        List<RelDataType> relDataTypes =
            Lists.newArrayList(opBinding.getOperandType(0), opBinding.getOperandType(1));
        return deriveReturnType(opBinding.getTypeFactory(), opBinding.getOperator(), relDataTypes);
      };

  public static final SqlOperator PLUS =
      new SqlMonotonicBinaryOperator(
      "+",
      SqlKind.PLUS,
      40,
      true,
      ARITHMETIC_INFERENCE,
      InferTypes.FIRST_KNOWN,
      OperandTypes.PLUS_OPERATOR);

  public static final SqlOperator MINUS =
      new SqlMonotonicBinaryOperator(
      "-",
      SqlKind.MINUS,
      40,
      true,
      ARITHMETIC_INFERENCE,
      InferTypes.FIRST_KNOWN,
      OperandTypes.MINUS_OPERATOR);

  static final SqlOperator MULTIPLY =
      new SqlMonotonicBinaryOperator(
      "*",
      SqlKind.TIMES,
      60,
      true,
      ARITHMETIC_INFERENCE,
      InferTypes.FIRST_KNOWN,
      OperandTypes.MULTIPLY_OPERATOR);

  static final SqlOperator DIVIDE =
      new SqlBinaryOperator(
      "/",
      SqlKind.DIVIDE,
      60,
      true,
      ARITHMETIC_INFERENCE,
      InferTypes.FIRST_KNOWN,
      OperandTypes.DIVISION_OPERATOR);

  public static RelDataType deriveReturnType(RelDataTypeFactory factory, SqlOperator op,
      List<RelDataType> inputTypes) {
    ArithmeticExpr.Operator impalaOp = ImpalaTypeSystemImpl.getImpalaArithOp(op.getKind());
    if (impalaOp == ArithmeticExpr.Operator.ADD ||
        impalaOp == ArithmeticExpr.Operator.SUBTRACT) {
      return ImpalaTypeSystemImpl.derivePlusType(factory, inputTypes.get(0),
          inputTypes.get(1), impalaOp);
    }
    return ImpalaTypeSystemImpl.deriveArithmeticType(factory, inputTypes.get(0),
        inputTypes.get(1), impalaOp);
  }
}
