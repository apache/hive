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

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.List;

/**
 * Time Interval Op function signature.  In Calcite, operations that involve
 * addition or subtraction of a time interval uses the SqlKind.PLUS or SqlKind.MINUS
 * operation. This helper class maps these functions into months_add/sub or
 * milliseconds_add/sub.
 */
public class TimeIntervalOpFunctionSignature extends DefaultFunctionSignature {

  public TimeIntervalOpFunctionSignature(SqlKind kind, List<SqlTypeName> argTypes,
      SqlTypeName retType) throws HiveException {
    super(getFunctionName(kind, argTypes), argTypes, retType, false);
    Preconditions.checkState(argTypes.size() == 2);
    Preconditions.checkArgument(argTypes.get(0) == SqlTypeName.TIMESTAMP ||
        argTypes.get(1) == SqlTypeName.TIMESTAMP);
  }

  /**
   * The primary argument for time intervals is the timestamp, since that is the
   * first argument used within the Impala signature in the resource file.
   */
  @Override
  protected SqlTypeName getPrimaryArg() {
    return SqlTypeName.TIMESTAMP;
  }

  /**
   * For Time interval operations, the timestamp and interval arguments can occur
   * in either order.
   */
  @Override
  protected boolean okToFlipArgs() {
    return true;
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

  public static String getFunctionName(SqlKind kind, List<SqlTypeName> argTypes) throws HiveException {
    // We only need to support months* and milliseconds*. Days intervals and anything less all
    // get translated into milliseconds within calcite.  Months and years both get translated into
    // months.
    for (SqlTypeName argType : argTypes) {
      if (SqlTypeName.YEAR_INTERVAL_TYPES.contains(argType)) {
        if (kind == SqlKind.PLUS) {
          return "months_add";
        }
        if (kind == SqlKind.MINUS) {
          return "months_sub";
        }
      }
      if (SqlTypeName.DAY_INTERVAL_TYPES.contains(argType)) {
        if (kind == SqlKind.PLUS) {
          return "milliseconds_add";
        }
        if (kind == SqlKind.MINUS) {
          return "milliseconds_sub";
        }
      }
    }
    throw new HiveException("Could not find a matching time interval function signature");
  }
}
