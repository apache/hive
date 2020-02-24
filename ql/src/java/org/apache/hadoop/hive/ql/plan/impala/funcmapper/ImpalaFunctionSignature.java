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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class ImpalaFunctionSignature {

  // A map of the function name to a list of possible signatures.
  // For instance, the "sum" function has an instance where BIGINT is an operand and has an instance
  // where DOUBLE is an operand.
  static public Map<String, List<ImpalaFunctionSignature>> CAST_CHECK_BUILTINS_INSTANCE = Maps.newHashMap();

  // A list of all the "cast" signatures which are upcasts.
  // For instance, "cast" where the operand is "TINYINT" and the return type is "SMALLINT" would be here,
  // while the "cast" where the operand is "SMALLINT" and the return type is "TINYINT" would not be here.
  static public Set<ImpalaFunctionSignature> UPCAST_BUILTINS_INSTANCE = Sets.newHashSet();

  // populate the static structures
  static {
    Reader reader =
        new InputStreamReader(ImpalaFunctionSignature.class.getResourceAsStream("/impala_scalars.json"));
    Gson gson = new Gson();
    Type scalarFuncDetailsType = new TypeToken<ArrayList<ScalarFunctionDetails>>(){}.getType();
    List<ScalarFunctionDetails> scalarDetails = gson.fromJson(reader, scalarFuncDetailsType);

    reader =
        new InputStreamReader(ImpalaFunctionSignature.class.getResourceAsStream("/impala_aggs.json"));
    Type aggFuncDetailsType = new TypeToken<ArrayList<AggFunctionDetails>>(){}.getType();
    List<AggFunctionDetails> aggDetails = gson.fromJson(reader, aggFuncDetailsType);

    try {
      for (ScalarFunctionDetails sfd : scalarDetails) {
        List<SqlTypeName> argTypes = Lists.newArrayList();
        if (sfd.argTypes != null) {
          argTypes = ImpalaTypeConverter.getSqlTypeNames(sfd.argTypes);
        }
        SqlTypeName retType = ImpalaTypeConverter.getSqlTypeName(sfd.retType);
        ImpalaFunctionSignature ifs = new ImpalaFunctionSignature(sfd.fnName, argTypes, retType,
            sfd.hasVarArgs);
        List<ImpalaFunctionSignature> castIfsList = CAST_CHECK_BUILTINS_INSTANCE.get(sfd.fnName);
        if (castIfsList == null) {
          // first time we've seen this function so put an entry into the map.
          castIfsList = Lists.newArrayList();
          CAST_CHECK_BUILTINS_INSTANCE.put(sfd.fnName, castIfsList);
        }
        castIfsList.add(ifs);

        if (sfd.castUp) {
          UPCAST_BUILTINS_INSTANCE.add(ifs);
        }
      }

      for (AggFunctionDetails afd : aggDetails) {
        List<SqlTypeName> argTypes = Lists.newArrayList();
        if (afd.argTypes != null) {
          argTypes = ImpalaTypeConverter.getSqlTypeNames(afd.argTypes);
        }
        SqlTypeName retType = ImpalaTypeConverter.getSqlTypeName(afd.retType);
        ImpalaFunctionSignature ifs = new ImpalaFunctionSignature(afd.fnName, argTypes, retType,
            false);
        List<ImpalaFunctionSignature> castIfsList = CAST_CHECK_BUILTINS_INSTANCE.get(afd.fnName);
        if (castIfsList == null) {
          // first time we've seen this function so put an entry into the map.
          castIfsList = Lists.newArrayList();
          CAST_CHECK_BUILTINS_INSTANCE.put(afd.fnName, castIfsList);
        }
        castIfsList.add(ifs);
      }
    } catch (HiveException e) {
      // if an exception is hit here, we have a problem in our resource file.
      throw new RuntimeException(e);
    }
  }

  private final String func;

  private final List<SqlTypeName> argTypes;

  private final SqlTypeName retType;

  private final boolean hasVarArgs;

  public ImpalaFunctionSignature(String func, List<SqlTypeName> argTypes, SqlTypeName retType,
      boolean hasVarArgs) {
    assert func != null;
    this.func = func;
    this.argTypes = ImmutableList.copyOf(argTypes);
    this.retType = retType;
    this.hasVarArgs = hasVarArgs;
  }

  public ImpalaFunctionSignature(String name, List<SqlTypeName> argTypes, SqlTypeName retType) {
    this(name, argTypes, retType, false);
  }

  public SqlTypeName getRetType() { return retType; }

  public List<SqlTypeName> getArgTypes() { return argTypes; }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if ((obj == null) || !(obj instanceof ImpalaFunctionSignature)) {
      return false;
    }

    ImpalaFunctionSignature other = (ImpalaFunctionSignature) obj;
    return this.func.equals(other.func) && this.argTypes.equals(other.argTypes)
        && this.retType.equals(other.retType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(func, argTypes, retType);
  }

  public boolean hasVarArgs() {
    return hasVarArgs;
  }

  @Override
  public String toString() {
    return retType + " " + func + "(" + StringUtils.join(argTypes, ", ") + ")";
  }
}
