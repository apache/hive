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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class ImpalaFunctionSignature {

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
        ImpalaFunctionSignature ifs = new DefaultFunctionSignature(sfd.fnName, argTypes, retType,
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
        ImpalaFunctionSignature ifs = new DefaultFunctionSignature(afd.fnName, argTypes, retType,
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

  protected final String func;

  protected final List<SqlTypeName> argTypes;

  protected final SqlTypeName retType;

  protected final boolean hasVarArgs;

  public ImpalaFunctionSignature(String func, List<SqlTypeName> argTypes, SqlTypeName retType,
      boolean hasVarArgs) {
    Preconditions.checkNotNull(func);
    this.func = func;
    this.argTypes = ImmutableList.copyOf(argTypes);
    this.retType = retType;
    this.hasVarArgs = hasVarArgs;
  }

  public ImpalaFunctionSignature(String name, List<SqlTypeName> argTypes, SqlTypeName retType) {
    this(name, argTypes, retType, false);
  }

  public String getFunc() {
     return func;
  }

  public SqlTypeName getRetType() {
     return retType;
  }

  public List<SqlTypeName> getArgTypes() {
    return argTypes;
  }

  public boolean hasVarArgs() {
    return hasVarArgs;
  }

  /**
   * Returns the expected arguments for the function signature.
   **/
  public abstract List<SqlTypeName> getSignatureArgTypes();

  /**
   * Method for whether we should this signature. We will allow it to use these
   * signature types if it is found in the map. But just checking for a matching
   * signature type in the map isn't enough, because there may be a variable number
   * of arguments in "this" signature. So we also have to check if the variable number
   * of arguments also matches the found signature.
   **/
  public abstract boolean useSignatureTypes(Map<ImpalaFunctionSignature,
      ? extends FunctionDetails> functionDetailsMap);

  /**
   * Returns true if all of the given operands can be cast to the given function
   * signature.
   **/
  public abstract boolean canCastToCandidate(ImpalaFunctionSignature castCandidate);

  /**
   * Gets the full signature of the expected return type and all the cast operand types.
   * The "left" part of the pair is the return type. The "right" part of the pair are the
   * types of all the operands.
   * It is possible that the cast candidate has a variable number of operands, and thus
   * can have less than the number of operands in "this" object. But in any case, the
   * number of operands returned should match the number of operands in "this" object.
   **/ 
  public abstract Pair<SqlTypeName, List<SqlTypeName>> getCastOpAndRetTypes(
      ImpalaFunctionSignature castCandidate);

  @Override
  public boolean equals(Object obj) {
    if ((obj == null) || !(obj instanceof ImpalaFunctionSignature)) {
      return false;
    }

    ImpalaFunctionSignature other = (ImpalaFunctionSignature) obj;
    if (!this.func.equals(other.func)) {
      return false;
    }

    if (!this.retType.equals(other.retType)) {
      return false;
    }

    List<SqlTypeName> thisArgTypes = this.getSignatureArgTypes();
    List<SqlTypeName> otherArgTypes = other.getSignatureArgTypes();

    int argsToCheck = thisArgTypes.size();

    // if the number of arguments for both objects are the same, we just need
    // to check them.  If they are not the same, we check if either one has
    // a variable amount of arguments.  If neither one does, we return false.
    // If one of the arguments has variable arguments, we only check the arguments
    // specified by the signature of the object with the variable arguments for
    // equality.
    if (thisArgTypes.size() != otherArgTypes.size()) {
      if (other.hasVarArgs()) {
        argsToCheck = otherArgTypes.size();
      } else if (!this.hasVarArgs()) {
        return false;
      }
    }

    for (int i = 0; i < argsToCheck; ++i) {
      if (!thisArgTypes.get(i).equals(otherArgTypes.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    // For the hash code, we need to ensure that all potential matching signatures go
    // to the same bucket. It is possible for signatures to be equal when they have
    // a different number of arguments. For this reason, we will return the same
    // hashcode based only on the first argument, if it exists.
    List<SqlTypeName> argTypes = this.getSignatureArgTypes();
    if (argTypes.size() == 0) {
      return Objects.hash(func, retType);
    }
    return Objects.hash(func, argTypes.get(0), retType);
  }

  @Override
  public String toString() {
    return retType + " " + func + "(" + StringUtils.join(argTypes, ", ") + ")";
  }
}
