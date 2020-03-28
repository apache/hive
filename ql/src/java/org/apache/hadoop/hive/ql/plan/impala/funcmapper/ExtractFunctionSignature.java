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
 * Extract function signature.  This is used for date specific Calcite
 * signatures that need mapping into Impala. The Calcite signatures have
 * a signature of "EXTRACT(FLAG(YEAR):SYMBOL, DATE)" while the Impala
 * signatures are like "YEAR(DATE)". The "SYMBOL" Calcite parameter needs
 * to be stripped when comparing or mapping to Impala signatures.
 */
public class ExtractFunctionSignature extends ImpalaFunctionSignature {

  private final ImpalaFunctionSignature defaultFuncSig;

  private enum ExtractFunctions {
    YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND
  }

  public ExtractFunctionSignature(String name, List<SqlTypeName> argTypes, SqlTypeName retType)
      throws HiveException {
    super(name, argTypes, retType, false);
    if (argTypes.size() != 2) {
      throw new HiveException("Extract function " + func + " should have exactly 2 arguments");
    }

    // the extract function first argument is ignored, so we can use the regular default
    // signature once is is removed.
    defaultFuncSig = new DefaultFunctionSignature(name, argTypes.subList(1,2), retType, false);
  }

  @Override
  public List<SqlTypeName> getSignatureArgTypes() {
    // The signature arg types eliminate the first "SYMBOL" parameter
    return argTypes.subList(1,2);
  }

  @Override
  public boolean useSignatureTypes(Map<ImpalaFunctionSignature,
      ? extends FunctionDetails> functionDetailsMap) {
    return defaultFuncSig.useSignatureTypes(functionDetailsMap);
  }

  /**
   * The primary argument for the extract statement is found in the default
   * signature, which matches the Impala signature in the resource file.
   */
  @Override
  protected SqlTypeName getPrimaryArg() {
    return defaultFuncSig.getPrimaryArg();
  }

  @Override
  public boolean canCastToCandidate(ImpalaFunctionSignature castCandidate) {
    return defaultFuncSig.canCastToCandidate(castCandidate);
  }

  @Override
  public List<SqlTypeName> getCastOperandTypes(
      ImpalaFunctionSignature castCandidate) {
    Preconditions.checkState(castCandidate.getArgTypes().size() == 1,
        "Num of arguments for " + this.func + " expected to be 1.");
    // first argument in candidate is "SYMBOL" and does not have to be cast, so we
    // grab it from this Calcite signature. The second argument needs to be
    // grabbed from the first argument of the cast candidate.
    return Lists.newArrayList(argTypes.get(0), castCandidate.getArgTypes().get(0));
  }

  static public boolean isExtract(String func) {
    return Enums.getIfPresent(ExtractFunctions.class, func.toUpperCase()).isPresent();
  }
}
