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

import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TPrimitiveType;

public class ImpalaFunctionMapper{

  private final String func;

  private final List<RelDataType> argTypes;

  private final RelDataType retType;

  private final ImpalaFunctionSignature funcSig;

  public ImpalaFunctionMapper(String func, List<RelDataType> argTypes, RelDataType retType) {
    assert func != null;
    this.func = func.toLowerCase();
    this.argTypes = ImmutableList.copyOf(argTypes);
    this.retType = retType;
    this.funcSig = getFuncSignature(func, argTypes, retType);
  }

  /*
   * For "this" signature, check if there exists a function signature which is compatible.
   * Returns a pair of the return type and the operand types.
   */
  public Pair<RelDataType, List<RelDataType>> mapOperands(RelDataTypeFactory dtFactory) {
    // if there is an exact match of name, args, ans return type existing in the builtins, we can
    // use the rexCall as/is.
    if (ScalarFunctionDetails.SCALAR_BUILTINS_INSTANCE.containsKey(this.funcSig)) {
      return new Pair<RelDataType, List<RelDataType>>(this.retType, this.argTypes);
    }

    // Skip over "cast".  We don't want to do a "cast" within a "cast".
    if (this.func.equals("cast")) {
      return new Pair<RelDataType, List<RelDataType>>(this.retType, this.argTypes);
    }

    // castCandidates contains a list of potential functions that can match "this" signature.
    // These candidates will have the same function name, but different operand/return types.
    List<ImpalaFunctionSignature> castCandidates =
        ImpalaFunctionSignature.CAST_CHECK_BUILTINS_INSTANCE.get(this.func);
    if (castCandidates == null) {
      throw new RuntimeException("Could not find function name " +
          func + " in Impala.");
    }

    // iterate through list of potential functions which we could cast.  These functions should be
    // in a pre-determined optimal order.
    // For instance:  we have sum(BIGINT), sum(DOUBLE), and sum(DECIMAL) in that order.  If the arg
    // is a TINYINT, we shoudl return a RexNode of "sum(CAST arg AS BIGINT)"
    for (ImpalaFunctionSignature castCandidate : castCandidates) {
      if (canCastToCandidate(castCandidate, this.argTypes)) {
        return new Pair<RelDataType, List<RelDataType>>(castCandidate.getRelRetType(dtFactory),
            getAllVarArgs(castCandidate.getRelArgTypes(dtFactory), this.argTypes.size()));
      }
    }
    throw new RuntimeException("Could not cast for function name " + func);
  }

  /**
   * Helper method when var args exist.  If there are no var args, no action is
   * needed and we immediately pass back the args structure.  If there are varargs,
   * we repeat the last args until we have the exact number of args that we need.
   */
  private List<RelDataType> getAllVarArgs(List<RelDataType> currArgTypes, int numArgs) {
    // if the function didn't have var args, we expect it to return right away.
    if (currArgTypes.size() == numArgs) {
      return currArgTypes;
    }
    List<RelDataType> result = Lists.newArrayList(currArgTypes);
    RelDataType lastArg = result.get(result.size() - 1);
    while (result.size() < numArgs) {
      result.add(lastArg);
    }
    return result;
  }

  /*
   * returns true if all of the given operands can be cast to the given function
   * prototype
   */
  private boolean canCastToCandidate(
      ImpalaFunctionSignature castCandidate, List<RelDataType> operandTypes) {
    SqlTypeName castTo = null;
    for (int i = 0; i < operandTypes.size(); ++i) {
      SqlTypeName castFrom = operandTypes.get(i).getSqlTypeName();
      // if we have var args, the last arg will be repeating, so we don't have to
      // get the last one.
      if (!castCandidate.hasVarArgs() || i < castCandidate.getArgTypes().size()) {
        castTo = castCandidate.getArgTypes().get(i);
      }
      // if they are equal, it doesn't need to be cast, so we move to the next
      // argument
      if (!castFrom.equals(castTo)) {
        // make sure the casting function exists for this argument.  If it doesn't
        // exist, we stop processing and return false.
        ImpalaFunctionSignature castSig =
            new ImpalaFunctionSignature("cast", Lists.newArrayList(castFrom), castTo);
        if (!ImpalaFunctionSignature.UPCAST_BUILTINS_INSTANCE.contains(castSig)) {
          return false;
        }
      }
    }
    return true;
  }

  private ImpalaFunctionSignature getFuncSignature(String func,
      List<RelDataType> argTypes, RelDataType retType) {
    List<SqlTypeName> sqlArgTypes = Lists.newArrayList();
    for (RelDataType argType : argTypes) {
      sqlArgTypes.add(argType.getSqlTypeName());
    }
    return new ImpalaFunctionSignature(func, sqlArgTypes, retType.getSqlTypeName());
  }
}
