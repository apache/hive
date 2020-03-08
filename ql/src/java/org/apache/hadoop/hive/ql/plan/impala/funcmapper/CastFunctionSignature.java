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
 * A cast function signature created from a RexCall. This class will
 * be compared against a known ImpalaFunctionSignature to retrieve
 * the Impala FunctionDetails.
 */
public class CastFunctionSignature extends ImpalaFunctionSignature {

  public CastFunctionSignature(List<SqlTypeName> argTypes, SqlTypeName retType) {
    super("cast", argTypes, retType, false);
  }

  @Override
  public boolean useSignatureTypes(Map<ImpalaFunctionSignature,
      ? extends FunctionDetails> functionDetailsMap) {
    // always use the signature for cast.  We never want to cast a cast.
    return true;
  }

  @Override
  public List<SqlTypeName> getSignatureArgTypes() {
    return argTypes;
  }

  @Override
  public boolean canCastToCandidate(ImpalaFunctionSignature castCandidate) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public Pair<SqlTypeName, List<SqlTypeName>> getCastOpAndRetTypes(
      ImpalaFunctionSignature castCandidate) {
    throw new RuntimeException("Not implemented");
  }
}
