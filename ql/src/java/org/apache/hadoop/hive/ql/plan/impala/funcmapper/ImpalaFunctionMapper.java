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

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

public class ImpalaFunctionMapper{

  private final String func;

  private final List<SqlTypeName> argTypes;

  private final SqlTypeName retType;

  private final ImpalaFunctionSignature funcSig;

  public ImpalaFunctionMapper(String func, List<SqlTypeName> argTypes, SqlTypeName retType) {
    assert func != null;
    this.func = func.toLowerCase();
    this.argTypes = ImmutableList.copyOf(argTypes);
    this.retType = retType;
    this.funcSig = new ImpalaFunctionSignature(func, argTypes, retType);
  }

  /*
   * For "this" signature, check if there exists a function signature which is compatible.
   * Returns a pair of the return type and the operand types.
   */
  public Pair<SqlTypeName, List<SqlTypeName>> mapOperands(
      Map<ImpalaFunctionSignature, ? extends FunctionDetails> functionDetailsMap) {
    // if there is an exact match of name, args, ans return type existing in the builtins, we can
    // use the rexCall as/is.
    if (functionDetailsMap.containsKey(this.funcSig)) {
      return new Pair<SqlTypeName, List<SqlTypeName>>(this.retType, this.argTypes);
    }

    // Skip over "cast".  We don't want to do a "cast" within a "cast".
    if (this.func.equals("cast")) {
      return new Pair<SqlTypeName, List<SqlTypeName>>(this.retType, this.argTypes);
    }

    // castCandidates contains a list of potential functions that can match "this" signature.
    // These candidates will have the same function name, but different operand/return types.
    List<ImpalaFunctionSignature> castCandidates =
        ImpalaFunctionSignature.CAST_CHECK_BUILTINS_INSTANCE.get(this.func);
    if (castCandidates == null) {
      throw new RuntimeException("Could not find function name " +
          func + " in resource file");
    }

    // iterate through list of potential functions which we could cast.  These functions should be
    // in a pre-determined optimal order.
    // For instance:  we have sum(BIGINT), sum(DOUBLE), and sum(DECIMAL) in that order.  If the arg
    // is a TINYINT, we should return a RexNode of "sum(CAST arg AS BIGINT)"
    for (ImpalaFunctionSignature castCandidate : castCandidates) {
      if (canCastToCandidate(castCandidate, this.argTypes)) {
        return new Pair<SqlTypeName, List<SqlTypeName>>(castCandidate.getRetType(),
            getAllVarArgs(castCandidate.getArgTypes(), castCandidate.getArgTypes().size()));
      }
    }
    throw new RuntimeException("Could not cast for function name " + func);
  }

  /**
   * Helper method when var args exist.  If there are no var args, no action is
   * needed and we immediately pass back the args structure.  If there are varargs,
   * we repeat the last args until we have the exact number of args that we need.
   */
  private List<SqlTypeName> getAllVarArgs(List<SqlTypeName> currArgTypes, int numArgs) {
    // if the function didn't have var args, we expect it to return right away.
    if (currArgTypes.size() == numArgs) {
      return currArgTypes;
    }
    List<SqlTypeName> result = Lists.newArrayList(currArgTypes);
    SqlTypeName lastArg = result.get(result.size() - 1);
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
      ImpalaFunctionSignature castCandidate, List<SqlTypeName> operandTypes) {
    SqlTypeName castTo = null;
    for (int i = 0; i < operandTypes.size(); ++i) {
      SqlTypeName castFrom = operandTypes.get(i);
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
}
