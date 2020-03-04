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

import com.google.common.base.Enums;
import com.google.common.base.Preconditions;
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
 * Case function signature.  The case function signature comes from the Impala function
 * which is of the form:
 * CASE [expr] WHEN expr THEN expr [WHEN whenexpr THEN rettype ...] [ELSE elseexpr] END
 *
 * In Calcite, the "WHEN" expression is always of type <boolean> and the THEN expression
 * is always the same type as the return type.
 *
 * In the case where the optional first expression (i.e. CASE [expr]) is given, Calcite rewrites
 * the WHEN expression so that all WHEN expressions are of type <boolean>
 * (e.g.WHEN expr == whenexpr)
 *
 * The ELSE condition in Calcite will also always be the same type as the return type.
 *
 * Impala simplified this signature for the function to be case(<rettype>) since all other
 * arguments are optional or derived.
 *
 * The purpose of this class is to help map the Calcite signature at runtime to the Impala
 * signature.
 */
public class CaseFunctionSignature extends ImpalaFunctionSignature {

  public CaseFunctionSignature(List<SqlTypeName> argTypes, SqlTypeName retType)
      throws HiveException {
    super("case", argTypes, retType, false);
  }

  @Override
  public List<SqlTypeName> getSignatureArgTypes() {
    // The Impala signature only contains the return type as its sole argument.
    return Lists.newArrayList(retType);
  }

  @Override
  public boolean useSignatureTypes(Map<ImpalaFunctionSignature,
      ? extends FunctionDetails> functionDetailsMap) {
    FunctionDetails candidate = functionDetailsMap.get(this);
    if (candidate == null) {
      // No case function found for existing arguments.
      return false;
    }

    // Check if every odd parameter matches the candidate argument.
    ImpalaFunctionSignature candIfs = candidate.getSignature();
    for(int i = 1; i < this.argTypes.size(); i += 2) {
      if (!candIfs.retType.equals(this.argTypes.get(i))) {
        return false;
      }
    }

    // If there is a dangling "else" argument, check this matches
    // the candidate argument.
    if ((this.argTypes.size() % 2) == 1) {
      if (!candIfs.retType.equals(this.argTypes.get(argTypes.size() - 1))) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean canCastToCandidate(ImpalaFunctionSignature castCandidate) {
    SqlTypeName castTo = castCandidate.getArgTypes().get(0);
    // For loop constructs this signature's arguments in pairs.
    for (int i = 0; i < this.argTypes.size() / 2; ++i) {
      int currentArg = 2 * i;
      // First argument of pair should always be a boolean
      Preconditions.checkState(this.argTypes.get(currentArg).equals(SqlTypeName.BOOLEAN));
      currentArg += 1;
      // Check to see if the second argument can be cast.
      SqlTypeName castFrom = this.argTypes.get(currentArg);
      if (!castFrom.equals(castTo)) {
        ImpalaFunctionSignature castSig = new CastFunctionSignature(Lists.newArrayList(castFrom), castTo);
        if (!ImpalaFunctionSignature.UPCAST_BUILTINS_INSTANCE.contains(castSig)) {
          return false;
        }
      }
    }
    // If there is an "else" parameter, we see if that parameter can be cast.
    if (this.argTypes.size() % 2 == 1) {
      SqlTypeName castFrom = this.argTypes.get(this.argTypes.size() - 1);
      if (!castFrom.equals(castTo)) {
        ImpalaFunctionSignature castSig = new CastFunctionSignature(Lists.newArrayList(castFrom), castTo);
        if (!ImpalaFunctionSignature.UPCAST_BUILTINS_INSTANCE.contains(castSig)) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public Pair<SqlTypeName, List<SqlTypeName>> getCastOpAndRetTypes(
      ImpalaFunctionSignature castCandidate) {
    List<SqlTypeName> castArgTypes = Lists.newArrayList();

    // For loop constructs this signature's arguments in pairs.
    for (int i = 0; i < this.argTypes.size() / 2; ++i) {
      // first argument in pair is always a boolean
      castArgTypes.add(SqlTypeName.BOOLEAN);
      // second argument is the candidate type.
      castArgTypes.add(castCandidate.getArgTypes().get(0));
    }
    // Handle "else" argument if it exists.
    if (this.argTypes.size() % 2 == 1) {
      castArgTypes.add(castCandidate.getArgTypes().get(0));
    }
    return new Pair<SqlTypeName, List<SqlTypeName>>(castCandidate.getRetType(), castArgTypes);
  }
}
