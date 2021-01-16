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

package org.apache.hadoop.hive.impala.funcmapper;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;

import java.util.List;

/**
 * Special override for functions that have its scale adjusted.
 */
public class AdjustScaleFunctionResolver extends ImpalaFunctionResolverImpl {

  public AdjustScaleFunctionResolver(FunctionHelper helper, SqlOperator op,
      List<RexNode> inputNodes) {
    super(helper, op, inputNodes);
  }

  /**
   * When the return type is a Decimal, the precision and scale need to be recalculated.
   * The truncated scale value will either be "0" or in the second operand. The precision
   * is adjusted to be the number of possible digits that can occur before the decimal point.
   * TODO: CDPD-12453: It would be preferable to infer the reference type than to call it
   * explicitly. This should be addressed for all function resolvers.
   */
  @Override
  public RelDataType getRetType(ImpalaFunctionSignature funcSig, List<RexNode> operands) {
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    Preconditions.checkState(operands.size() > 0);
    RexNode nodeToAdjust = operands.get(0);
    if (nodeToAdjust.getType().getSqlTypeName() != SqlTypeName.DECIMAL) {
      return super.getRetType(funcSig, operands);
    }

    int precision = nodeToAdjust.getType().getPrecision();
    int oldScale = nodeToAdjust.getType().getScale();
    int digitsBefore = precision - oldScale;
    int truncatedScale = operands.size() > 1 ? RexLiteral.intValue(operands.get(1)) : 0;
    int newScale = Math.min(oldScale, truncatedScale);
    newScale = Math.max(newScale, 0);
    if (newScale < oldScale) {
      // just in case there is rounding up, we need an extra digit.
      digitsBefore++;
    }

    return typeFactory.createSqlType(SqlTypeName.DECIMAL, digitsBefore + newScale, newScale);
  }

  // TODO: CDPD-12454: Use singleton model that Hive is using for functions that it was
  // introducing rather than names
  static boolean isAdjustScaleFunction(String function) {
    return function.equals("round") || function.equals("dround") ||
        function.equals("trunc") || function.equals("dtrunc") || function.equals("truncate");
  }
}
