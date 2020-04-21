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
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;

import java.util.List;

/**
 * Time Interval Op function resolver.  In Calcite, operations that involve
 * addition or subtraction of a time interval uses the SqlKind.PLUS or SqlKind.MINUS
 * operation. This helper class maps these functions into months_add/sub or
 * milliseconds_add/sub.
 */
public class TimeIntervalOpFunctionResolver extends ImpalaFunctionResolverImpl {

  TimeIntervalOpFunctionResolver(FunctionHelper helper, SqlOperator op, List<RexNode> inputNodes,
      RelDataType returnType) {
    super(helper, op, reorderArgs(inputNodes), returnType);
    Preconditions.checkState(argTypes.size() == 2);
  }

  @Override
  public List<SqlTypeName> getCastOperandTypes(ImpalaFunctionSignature castCandidate) {
    Preconditions.checkState(castCandidate.getArgTypes().size() == argTypes.size());
    List<SqlTypeName> result = Lists.newArrayList();
    for (int i = 0; i < argTypes.size(); ++i) {
      // The impala function signature doesn't know about interval types.
      // At the very end, it will just be written to impala as a bigint.
      if (SqlTypeName.INTERVAL_TYPES.contains(argTypes.get(i))) {
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
}
