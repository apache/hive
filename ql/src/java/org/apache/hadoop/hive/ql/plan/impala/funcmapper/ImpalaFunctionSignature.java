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
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ImpalaFunctionSignature implements Comparable<ImpalaFunctionSignature> {

  private enum SqlTypeOrdering {
    CHAR,
    VARCHAR,
    STRING,
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INTEGER,
    BIGINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    DATE,
    TIMESTAMP
  }

  // A map of the function name to a list of possible signatures.
  // For instance, the "sum" function has an instance where BIGINT is an operand and has an instance
  // where DOUBLE is an operand.
  static Map<String, List<ImpalaFunctionSignature>> CAST_CHECK_BUILTINS_INSTANCE = Maps.newHashMap();

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
        List<ImpalaFunctionSignature> castIfsList =
            CAST_CHECK_BUILTINS_INSTANCE.computeIfAbsent(sfd.fnName, k -> Lists.newArrayList());
        castIfsList.add(ifs);
      }

      for (AggFunctionDetails afd : aggDetails) {
        List<SqlTypeName> argTypes = Lists.newArrayList();
        if (afd.argTypes != null) {
          argTypes = ImpalaTypeConverter.getSqlTypeNames(afd.argTypes);
        }
        SqlTypeName retType = ImpalaTypeConverter.getSqlTypeName(afd.retType);
        ImpalaFunctionSignature ifs = new ImpalaFunctionSignature(afd.fnName, argTypes, retType,
            false);
        List<ImpalaFunctionSignature> castIfsList =
            CAST_CHECK_BUILTINS_INSTANCE.computeIfAbsent(afd.fnName, k -> Lists.newArrayList());
        castIfsList.add(ifs);
      }

      for (String fnName : CAST_CHECK_BUILTINS_INSTANCE.keySet()) {
        List<ImpalaFunctionSignature> ifsList = CAST_CHECK_BUILTINS_INSTANCE.get(fnName);
        Collections.sort(ifsList);
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

  private ImpalaFunctionSignature(String func, List<SqlTypeName> argTypes, SqlTypeName retType,
      boolean hasVarArgs) {
    Preconditions.checkNotNull(func);
    this.func = getFunctionName(func, argTypes);
    this.argTypes = translateArgTypes(argTypes);
    this.retType = retType;
    this.hasVarArgs = hasVarArgs;
  }

  private ImpalaFunctionSignature(String name, List<SqlTypeName> argTypes, SqlTypeName retType) {
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
   * Checks that the function to resolve matches the signature in the file exactly, with
   * no conversions. If the return type is present, that will be matched too. If it is not
   * present (set to null), the return type will be ignored when trying to match.
   */
  private ImpalaFunctionSignature getMatchingSignature(
      Map<ImpalaFunctionSignature, ? extends FunctionDetails> functionDetailsMap) {
    FunctionDetails candidate = functionDetailsMap.get(this);
    // not found in the map at all, so return false.
    if (candidate == null) {
      return null;
    }
    // While the signatures for 'this' and the one in the functionDetailsMap are equivalent,
    // they are not exactly the same. The difference is that 'this' may have variable arguments.
    // The signature within the candidate will have the signature without the variable arguments
    // which is the signature that the caller has requested.
    return candidate.getSignature();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ImpalaFunctionSignature)) {
      return false;
    }

    ImpalaFunctionSignature other = (ImpalaFunctionSignature) obj;
    if (!this.func.equals(other.func)) {
      return false;
    }

    // If one of the return types is null, then don't compare the return type
    // for equality
    if (this.retType != null && other.retType != null) {
      if (!this.retType.equals(other.retType)) {
        return false;
      }
    }

    int argsToCheck = this.argTypes.size();

    // if the number of arguments for both objects are the same, we just need
    // to check them.  If they are not the same, we check if either one has
    // a variable amount of arguments.  If neither one does, we return false.
    // If one of the arguments has variable arguments, we only check the arguments
    // specified by the signature of the object with the variable arguments for
    // equality.
    if (this.argTypes.size() != other.argTypes.size()) {
      if (other.hasVarArgs()) {
        argsToCheck = other.argTypes.size();
      } else if (!this.hasVarArgs()) {
        return false;
      }
    }

    boolean retVal = true;
    for (int i = 0; i < argsToCheck; ++i) {
      if (!this.argTypes.get(i).equals(other.argTypes.get(i))) {
        retVal = false;
        break;
      }
    }
    return retVal;
  }

  @Override
  public int hashCode() {
    // For the hash code, we need to ensure that all potential matching signatures go
    // to the same bucket. It is possible for signatures to be equal when they have
    // a different number of arguments. For this reason, we will return the same
    // hashcode based only on the first argument, if it exists.
    if (argTypes.size() == 0) {
      return Objects.hash(func);
    }
    return Objects.hash(func, argTypes.get(0));
  }

  @Override
  public String toString() {
    return retType + " " + func + "(" + StringUtils.join(argTypes, ", ") + ")";
  }


  /**
   * Translate SqlTypeNames to a common type supported by Impala.
   */
  private List<SqlTypeName> translateArgTypes(List<SqlTypeName> argTypes) {
    ImmutableList.Builder<SqlTypeName> list = ImmutableList.builder();
    for (SqlTypeName argType : argTypes) {
      // Interval types are always mapped to BIGINT types.
      if (SqlTypeName.INTERVAL_TYPES.contains(argType)) {
        list.add(SqlTypeName.BIGINT);
      } else {
        list.add(argType);
      }
    }
    return list.build();
  }

  @Override
  public int compareTo(ImpalaFunctionSignature other) {
    if (this.retType == null) {
      return -1;
    }

    if (other.retType == null) {
      return 1;
    }

    if (this.argTypes.size() == 0) {
      if (other.argTypes.size() == 0) {
        return 0;
      }
      return -1;
    }

    if (other.argTypes.size() == 0) {
      return 1;
    }

    int thisOrdinal = SqlTypeOrdering.valueOf(this.argTypes.get(0).toString()).ordinal();
    int otherOrdinal = SqlTypeOrdering.valueOf(other.argTypes.get(0).toString()).ordinal();
    return Integer.compare(thisOrdinal, otherOrdinal);
  }

  private static String getFunctionName(String funcName,
      List<SqlTypeName> sqlTypes) {
    if (!funcName.equals("+") && !funcName.equals("-")) {
      return funcName;
    }
    String opType = funcName.equals("+") ? "add" : "sub";
    // We only need to support months* and milliseconds*. Days intervals and anything less all
    // get translated into milliseconds within calcite.  Months and years both get translated into
    // months.
    for (SqlTypeName argType : sqlTypes) {
      if (SqlTypeName.YEAR_INTERVAL_TYPES.contains(argType)) {
        return "months_" + opType;
      }
      if (SqlTypeName.DAY_INTERVAL_TYPES.contains(argType)) {
        return "milliseconds_" + opType;
      }
    }
    return funcName;
  }

  private static boolean verifyCaseParams(SqlTypeName typeToMatch, List<SqlTypeName> inputs) {
    // For loop constructs this signature's arguments in pairs.
    for (int i = 0; i < inputs.size() / 2; ++i) {
      // all the even args should match the return type.
      int currentArg = 2 * i + 1;
      if (typeToMatch != inputs.get(currentArg)) {
        return false;
      }
    }

    // If there is an "else" parameter, that argument needs to match too.
    if (inputs.size() % 2 == 1) {
      return typeToMatch == inputs.get(inputs.size() - 1);
    }
    return true;
  }

  public static ImpalaFunctionSignature fetch(
      Map<ImpalaFunctionSignature, ? extends FunctionDetails> functionDetailsMap,
      String func, List<SqlTypeName> argTypes, SqlTypeName returnType) {
    String lowerCaseFunc = func.toLowerCase();

    SqlOperator op = ImpalaOperatorTable.IMPALA_OPERATOR_MAP.get(func.toUpperCase());

    SqlKind kind = op != null ? op.getKind() : SqlKind.OTHER;

    ImpalaFunctionSignature ifs;
    switch(kind) {
      case CASE:
        // Extra check for verification of case statement. In function resolver mode,
        // the return type will always be null and this will return null. At Impala
        // translation time (after CBO), the parameters need to be simplified, since
        // the case statement in Impala is of the form <TYPE> CASE(<TYPE>).
        if (!verifyCaseParams(returnType, argTypes)) {
          return null;
        }
        ifs = new ImpalaFunctionSignature(lowerCaseFunc, Lists.newArrayList(returnType),
            returnType, false);
        break;
      case EXTRACT:
        // Extract can come in two different forms. From the function resolver, it comes in
        // as YEAR(TIMESTAMP). From Calcite, it comes in  the form YEAR(SYMBOL(YEAR), TIMESTAMP).
        // So if we see two parameters, we only need the second one.
        List<SqlTypeName> extractArgs = argTypes.size() > 1 ? argTypes.subList(1,2) : argTypes;
        ifs = new ImpalaFunctionSignature(lowerCaseFunc, extractArgs, returnType);
        break;
      default:
        ifs = new ImpalaFunctionSignature(lowerCaseFunc, argTypes, returnType);
    }
    // Check within the given map that this signature exists. Even though this static function
    // created a signature, the returned signature will be a different object as retrieved from
    // the map.
    return ifs.getMatchingSignature(functionDetailsMap);
  }

  public static ImpalaFunctionSignature create(String func, List<SqlTypeName> argTypes, SqlTypeName retType,
      boolean hasVarArgs) {
    return new ImpalaFunctionSignature(func, argTypes, retType, hasVarArgs);
  }

  public static boolean canCastUp(SqlTypeName castFrom, SqlTypeName castTo) {
    if (castFrom == SqlTypeName.NULL) {
      return true;
    }
    if (castFrom.equals(castTo)) {
      return true;
    }
    ImpalaFunctionSignature castSig =
        new ImpalaFunctionSignature("cast", Lists.newArrayList(castFrom), castTo);
    ScalarFunctionDetails details = ScalarFunctionDetails.SCALAR_BUILTINS_INSTANCE.get(castSig);
    return details != null && details.castUp;
  }
}
