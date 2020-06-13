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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;
import org.apache.impala.catalog.Type;

import java.io.InputStreamReader;
import java.io.Reader;
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
    INT,
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
    java.lang.reflect.Type scalarFuncDetailsType = new TypeToken<ArrayList<ScalarFunctionDetails>>(){}.getType();
    List<ScalarFunctionDetails> scalarDetails = gson.fromJson(reader, scalarFuncDetailsType);

    reader =
        new InputStreamReader(ImpalaFunctionSignature.class.getResourceAsStream("/impala_aggs.json"));
    java.lang.reflect.Type aggFuncDetailsType = new TypeToken<ArrayList<AggFunctionDetails>>(){}.getType();
    List<AggFunctionDetails> aggDetails = gson.fromJson(reader, aggFuncDetailsType);

    for (ScalarFunctionDetails sfd : scalarDetails) {
      List<Type> argTypes = Lists.newArrayList();
      ImpalaFunctionSignature ifs = new ImpalaFunctionSignature(sfd.fnName, sfd.getArgTypes(),
          sfd.getRetType(), sfd.hasVarArgs);
      List<ImpalaFunctionSignature> castIfsList =
          CAST_CHECK_BUILTINS_INSTANCE.computeIfAbsent(sfd.fnName, k -> Lists.newArrayList());
      castIfsList.add(ifs);
    }

    for (AggFunctionDetails afd : aggDetails) {
      ImpalaFunctionSignature ifs = new ImpalaFunctionSignature(afd.fnName, afd.getArgTypes(),
          afd.getRetType(), false);
      List<ImpalaFunctionSignature> castIfsList =
          CAST_CHECK_BUILTINS_INSTANCE.computeIfAbsent(afd.fnName, k -> Lists.newArrayList());
      castIfsList.add(ifs);
    }

    for (String fnName : CAST_CHECK_BUILTINS_INSTANCE.keySet()) {
      List<ImpalaFunctionSignature> ifsList = CAST_CHECK_BUILTINS_INSTANCE.get(fnName);
      Collections.sort(ifsList);
    }
  }

  private final String func;

  private final List<Type> argTypes;

  private final Type retType;

  private final boolean hasVarArgs;

  private final RelDataType retRelDataType;

  private final List<RelDataType> argRelDataTypes;

  private ImpalaFunctionSignature(String func, List<Type> argTypes, Type retType,
      boolean hasVarArgs) {
    Preconditions.checkNotNull(func);
    this.func = func;
    this.argTypes = ImmutableList.copyOf(argTypes);
    this.argRelDataTypes = ImpalaTypeConverter.getRelDataTypes(this.argTypes);
    this.retType = retType;
    this.retRelDataType =
        (this.retType == null) ? null : ImpalaTypeConverter.getRelDataType(this.retType);
    this.hasVarArgs = hasVarArgs;
  }

  private ImpalaFunctionSignature(String func, List<RelDataType> argTypes, RelDataType retType) {
    Preconditions.checkNotNull(func);
    this.func = func;
    this.argTypes = ImpalaTypeConverter.getNormalizedImpalaTypes(argTypes);
    this.argRelDataTypes = ImpalaTypeConverter.getRelDataTypes(this.argTypes);
    this.retType = retType != null ? ImpalaTypeConverter.getNormalizedImpalaType(retType) : null;
    this.retRelDataType =
        (this.retType == null) ? null : ImpalaTypeConverter.getRelDataType(this.retType);
    this.hasVarArgs = false;
  }

  public String getFunc() {
     return func;
  }

  public RelDataType getRetType() {
    return ImpalaTypeConverter.getRelDataType(retType);
  }

  public List<RelDataType> getArgTypes() {
    return ImpalaTypeConverter.getRelDataTypes(argTypes);
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
      // if either arg type is null, the signature will match because nulls can be
      // used for any datatype.
      if (this.argTypes.get(i) == Type.NULL || other.argTypes.get(i) == Type.NULL) {
        continue;
      }
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

    // Check that the types match.
    // The pattern of the ordinal string for the type needs to be normalized.
    // It is possible the type can show up like "DECIMAL(32,8)", and we only
    // want the DECIMAL part.
    String thisOrdinalString = this.argTypes.get(0).toString().split("\\(")[0];
    String otherOrdinalString = other.argTypes.get(0).toString().split("\\(")[0];
    int thisOrdinal = SqlTypeOrdering.valueOf(thisOrdinalString).ordinal();
    int otherOrdinal = SqlTypeOrdering.valueOf(otherOrdinalString).ordinal();
    return Integer.compare(thisOrdinal, otherOrdinal);
  }

  private static boolean verifyCaseParams(RelDataType typeToMatch, List<RelDataType> inputs) {
    // For loop constructs this signature's arguments in pairs.
    for (int i = 0; i < inputs.size() / 2; ++i) {
      // all the even args should match the return type.
      int currentArg = 2 * i + 1;
      if (!areCompatibleDataTypes(typeToMatch, inputs.get(currentArg))) {
        return false;
      }
    }

    // If there is an "else" parameter, that argument needs to match too.
    if (inputs.size() % 2 == 1) {
      return areCompatibleDataTypes(typeToMatch,
          inputs.get(inputs.size() - 1));
    }
    return true;
  }

  private static ImpalaFunctionSignature createTimeIntervalOpSignature(
      SqlKind kind, List<RelDataType> argTypes) {
    String funcName = TimeIntervalOpFunctionResolver.getFunctionName(kind, argTypes);
    // The time interval operations will always take timestamp and bigint as their two
    // arguments.  If the first operand is of type "date", it will be cast into a timestamp.
    List<Type> timeArgTypes = ImmutableList.of(Type.TIMESTAMP, Type.BIGINT);
    List<RelDataType> timeArgRelDataTypes = ImpalaTypeConverter.getRelDataTypes(timeArgTypes);
    Type timeRetType = Type.TIMESTAMP;
    RelDataType timeRetRelDataType = ImpalaTypeConverter.getRelDataType(timeRetType);
    return new ImpalaFunctionSignature(funcName, timeArgRelDataTypes, timeRetRelDataType);
  }

  /**
   * Returns true if datatypes are compatible within the Impala function. In the case of character,
   * data, char, varchar, and string are all compatible.  Nulls are compatible with everything
   * since the function signature will never take a null type and if the data is null, it can
   * go into any Impala function.
   */
  public static boolean areCompatibleDataTypes(RelDataType dt1, RelDataType dt2) {
    if (SqlTypeName.CHAR_TYPES.contains(dt1.getSqlTypeName()) &&
        SqlTypeName.CHAR_TYPES.contains(dt2.getSqlTypeName())) {
      return true;
    }

    if (dt1.getSqlTypeName() == SqlTypeName.NULL || dt2.getSqlTypeName() == SqlTypeName.NULL) {
      return true;
    }

    return dt1.getSqlTypeName() == dt2.getSqlTypeName();
  }

  public static ImpalaFunctionSignature fetch(
      Map<ImpalaFunctionSignature, ? extends FunctionDetails> functionDetailsMap,
      String func, List<RelDataType> argTypes, RelDataType returnType) {
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
            returnType);
        break;
      case EXTRACT:
        // Extract can come in two different forms. From the function resolver, it comes in
        // as YEAR(TIMESTAMP). From Calcite, it comes in  the form YEAR(SYMBOL(YEAR), TIMESTAMP).
        // So if we see two parameters, we only need the second one.
        List<RelDataType> extractArgs = argTypes.size() > 1 ? argTypes.subList(1,2) : argTypes;
        ifs = new ImpalaFunctionSignature(lowerCaseFunc, extractArgs, returnType);
        break;
      case PLUS:
      case MINUS:
        ifs = TimeIntervalOpFunctionResolver.isTimeIntervalOp(argTypes)
            ? createTimeIntervalOpSignature(kind, argTypes)
            : new ImpalaFunctionSignature(lowerCaseFunc, argTypes, returnType);
	break;
      default:
        ifs = new ImpalaFunctionSignature(lowerCaseFunc, argTypes, returnType);
    }
    // Check within the given map that this signature exists. Even though this static function
    // created a signature, the returned signature will be a different object as retrieved from
    // the map.
    return ifs.getMatchingSignature(functionDetailsMap);
  }

  public static ImpalaFunctionSignature create(String func, List<Type> argTypes, Type retType,
      boolean hasVarArgs) {
    return new ImpalaFunctionSignature(func, argTypes, retType, hasVarArgs);
  }

  public static boolean canCastUp(RelDataType castFrom, RelDataType castTo) {
    if (castFrom.getSqlTypeName() == SqlTypeName.NULL) {
      return true;
    }

    if (areCompatibleDataTypes(castFrom, castTo)) {
      return true;
    }

    ImpalaFunctionSignature castSig =
        new ImpalaFunctionSignature("cast", Lists.newArrayList(castFrom), castTo);
    ScalarFunctionDetails details = ScalarFunctionDetails.SCALAR_BUILTINS_INSTANCE.get(castSig);
    return details != null && details.castUp;
  }
}
