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
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;
import org.apache.impala.catalog.Type;

import java.util.List;

/**
 * Time Interval Op function resolver.  In Calcite, operations that involve
 * addition or subtraction of a time interval uses the SqlKind.PLUS or SqlKind.MINUS
 * operation. This helper class maps these functions into months_add/sub or
 * milliseconds_add/sub.
 */
public class TimeIntervalOpFunctionResolver extends ImpalaFunctionResolverImpl {

  TimeIntervalOpFunctionResolver(FunctionHelper helper, String funcName,
      SqlKind kind, List<RexNode> inputNodes) {
    super(helper, createSpecialOperator(funcName, kind), funcName, reorderArgs(inputNodes));
    Preconditions.checkState(argTypes.size() == 2);
  }

  @Override
  public List<RelDataType> getCastOperandTypes(ImpalaFunctionSignature castCandidate) {
    Preconditions.checkState(castCandidate.getArgTypes().size() == argTypes.size());
    List<RelDataType> result = Lists.newArrayList();
    for (int i = 0; i < argTypes.size(); ++i) {
      // The impala function signature doesn't know about interval types.
      // At the very end, it will just be written to impala as a bigint.
      if (SqlTypeName.INTERVAL_TYPES.contains(argTypes.get(i).getSqlTypeName())) {
        result.add(argTypes.get(i));
      } else {
        result.add(castCandidate.getArgTypes().get(i));
      }
    }
    return result;
  }

  public static boolean isTimestampArithExpr(String fnName) {
    return fnName.contains("months") || fnName.contains("milliseconds");
  }

  public static String getTimeUnitIdent(String fnName) throws HiveException {
    if (fnName.contains("months")) {
      return "MONTH";
    }
    if (fnName.contains("milliseconds")) {
      return "MILLISECOND";
    }
    throw new HiveException("Could not get Time Unit Identifier for function " + fnName);
  }

  /**
   * Reorder args, if necessary. The Impala function can only handle args in the format of
   * TIMESTAMP + INTERVAL. So if it comes in the order of INTERVAL + TIMESTAMP, we reorder the
   * function up front.
   */
  private static List<RexNode> reorderArgs(List<RexNode> inputNodes) {
    Preconditions.checkState(inputNodes.size() == 2);
    return SqlTypeName.INTERVAL_TYPES.contains(inputNodes.get(0).getType().getSqlTypeName())
        ? Lists.newArrayList(inputNodes.get(1), inputNodes.get(0))
        : inputNodes;
  }

  /**
   * create a special operator which consists of the Impala function name
   * (either "+", "-", or "date_*"), and an SqlKind ("+" or "-").
   */
  private static SqlOperator createSpecialOperator(String funcName, SqlKind kind) {
    return new SqlSpecialOperator(funcName, kind);
  }

  public static String getFunctionName(String funcName, SqlKind kind, List<RelDataType> types) {
    // If it's the date_add or date_sub function and the second argument is an interval type,
    // then we have to change the function name because the RelDataType passed in will be in
    // the form of months or milliseconds.  If the second argument is an INT type column
    // (e.g. date_add(timestamp_col, int_col)), the date_add or date_sub function name can
    // be used as/is.
    if (funcName.startsWith("date_") &&
        SqlTypeName.INT_TYPES.contains(types.get(1).getSqlTypeName())) {
      return funcName;
    }
    String opType = kind == SqlKind.PLUS ? "add" : "sub";
    // We only need to support months* and milliseconds*. Days intervals and anything less all
    // get translated into milliseconds within calcite.  Months and years both get translated into
    // months.
    for (RelDataType type : types) {
      if (SqlTypeName.YEAR_INTERVAL_TYPES.contains(type.getSqlTypeName())) {
        // Use the months_*_interval function.  The months_add and months_add_interval
        // function differ slightly when leap years are involved.
        return "months_" + opType + "_interval";
      }
      if (SqlTypeName.DAY_INTERVAL_TYPES.contains(type.getSqlTypeName())) {
        return "milliseconds_" + opType;
      }
    }
    throw new RuntimeException("Unable to resolve time interval type for " + kind);
  }

  /**
   * Checks to see if the given RexNode inputs would cause a timestamp interval operation.
   * This should be called with the arguments to a + or - operation. Some examples
   * of operations needing a timesamp interval operation are: +(TIMESTAMP, INT),
   * +(INT, TIMESTAMP), and +(STRING, YEAR_TO_MONTH_INTERVAL).
   */
  public static boolean rexNodesHaveTimeIntervalOp(List<RexNode> inputs) {
    if (inputs.size() != 2) {
      return false;
    }
    for (RexNode input : inputs) {
      if (isTimeIntervalOp(input.getType().getSqlTypeName())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks to see if the given RelDataType inputs would cause a timestamp interval operation.
   * This should be called with the arguments to a + or - operation. Some examples
   * of operations needing a timesamp interval operation are: +(TIMESTAMP, INT),
   * +(INT, TIMESTAMP), and +(STRING, YEAR_TO_MONTH_INTERVAL).
   */
  public static boolean argTypesHaveTimeIntervalOp(List<RelDataType> argTypes) {
    if (argTypes.size() != 2) {
      return false;
    }
    for (RelDataType argType : argTypes) {
      if (isTimeIntervalOp(argType.getSqlTypeName())) {
        return true;
      }
    }
    return false;
  }

  public static boolean isTimeIntervalOp(SqlTypeName type) {
    return SqlTypeName.INTERVAL_TYPES.contains(type) || SqlTypeName.DATETIME_TYPES.contains(type);
  }
}
