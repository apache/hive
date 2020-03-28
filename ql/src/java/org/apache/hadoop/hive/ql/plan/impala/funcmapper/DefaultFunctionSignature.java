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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * The default function signature serves as the signature for all Impala classes
 * directly and for most function signatures coming out of Calcite.
 */
public class DefaultFunctionSignature extends ImpalaFunctionSignature {

  public DefaultFunctionSignature(String func, List<SqlTypeName> argTypes, SqlTypeName retType,
      boolean hasVarArgs) {
    super(func, argTypes, retType, hasVarArgs);
  }

  public DefaultFunctionSignature(String name, List<SqlTypeName> argTypes, SqlTypeName retType) {
    this(name, argTypes, retType, false);
  }

  @Override
  public List<SqlTypeName> getSignatureArgTypes() {
    // In the default case, the argtypes are passed back as/is.
    return argTypes;
  }

  /**
   * The use signature types method is a check on whether a found signature
   * has matching arguments and all variable arguments match as well.  The
   * last argument in the variable argument signature should repeat itself
   * for the remaining unchecked arguments.
   */
  @Override
  public boolean useSignatureTypes(Map<ImpalaFunctionSignature,
      ? extends FunctionDetails> functionDetailsMap) {
    FunctionDetails candidate = functionDetailsMap.get(this);
    // not found in the map at all, so return false.
    if (candidate == null) {
      return false;
    }
    ImpalaFunctionSignature candIfs = candidate.getSignature();

    // Since the signatures match and there are no variable arguments, it
    // is a match.
    if (candIfs.argTypes.size() == this.argTypes.size()) {
      return true;
    }

    // At this point, we can only jmatch if the candidate has a variable number
    // of arguments.
    if (!candIfs.hasVarArgs()) {
      return false;
    }

    // Check the remaining arguments to see if they match the last candidate
    // argument.
    SqlTypeName lastParam = candIfs.argTypes.get(candIfs.argTypes.size() - 1);
    for (int i = candIfs.argTypes.size(); i < this.argTypes.size(); ++i) {
      if (!this.argTypes.get(i).equals(lastParam)) {
        return false;
      }
    }
    return true;
  }

  /*
   * Returns true if both the return type and all of the given operands can be cast to
   * the given function prototype.
   */
  @Override
  public boolean canCastToCandidate(ImpalaFunctionSignature castCandidate) {
    // Return types must match.
    if (!castCandidate.getRetType().equals(retType)) {
      return false;
    }

    if (!castCandidate.hasVarArgs() &&
        argTypes.size() != castCandidate.getArgTypes().size()) {
      return false;
    }

    SqlTypeName castTo = null;
    for (int i = 0; i < argTypes.size(); ++i) {
      SqlTypeName castFrom = argTypes.get(i);
      // if we have var args, the last arg will be repeating, so we don't have to
      // get the last one.
      if (i < castCandidate.getArgTypes().size()) {
        castTo = castCandidate.getArgTypes().get(i);
      }
      // if they are equal, it doesn't need to be cast, so we move to the next
      // argument
      if (!castFrom.equals(castTo)) {
        // make sure the casting function exists for this argument.  If it doesn't
        // exist, we stop processing and return false.
        ImpalaFunctionSignature castSig = new CastFunctionSignature(Lists.newArrayList(castFrom), castTo);
        if (!ImpalaFunctionSignature.UPCAST_BUILTINS_INSTANCE.contains(castSig)) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public List<SqlTypeName> getCastOperandTypes(
      ImpalaFunctionSignature castCandidate) {
    // We need the arguments from the cast candidate signature, but "this" may contain
    // more arguments than the cast in the case where the cast candidate has a variable
    // number of arguments.  So we call "getAllVarArgs" with "this" number of arguments
    // so that the returned number of arguments matches what is expected.
    return getAllVarArgs(castCandidate.getArgTypes(), argTypes.size());
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

    // Fill in the remaining arguments with the last argument so the number of arguments
    // match what is expected.
    List<SqlTypeName> result = Lists.newArrayList(currArgTypes);
    SqlTypeName lastArg = result.get(result.size() - 1);
    while (result.size() < numArgs) {
      result.add(lastArg);
    }
    return result;
  }

  @Override
  public String toString() {
    return retType + " " + func + "(" + StringUtils.join(argTypes, ", ") + ")";
  }
}
