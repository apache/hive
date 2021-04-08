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

package org.apache.hadoop.hive.impala.funcmapper;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;
import org.apache.impala.analysis.TypesUtil;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImpalaFunctionSignature {
  protected static final Logger LOG = LoggerFactory.getLogger(ImpalaFunctionSignature.class);

  private enum SqlTypeOrdering {
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    CHAR,
    VARCHAR,
    STRING,
    DECIMAL,
    DATE,
    TIMESTAMP
  }

  // A map of the function name to a list of possible signatures.
  // For instance, the "sum" function has an instance where BIGINT is an operand and has an instance
  // where DOUBLE is an operand.
  static Map<String, List<ImpalaFunctionSignature>> CAST_CHECK_BUILTINS_INSTANCE = Maps.newHashMap();

  // List of functions where Impala does not handle CHAR or VARCHAR types but handles a STRING type.
  public static List<String> STRING_ONLY_FUNCTIONS =
      ImmutableList.of("coalesce", "in", "substr", "substring", "upper", "lower", "like");

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
      ImpalaFunctionSignature ifs = new ImpalaFunctionSignature(sfd.fnName, sfd.getArgTypes(),
          sfd.getRetType(), sfd.hasVarArgs, sfd.retTypeAlwaysNullable);
      List<ImpalaFunctionSignature> castIfsList =
          CAST_CHECK_BUILTINS_INSTANCE.computeIfAbsent(sfd.fnName, k -> Lists.newArrayList());
      castIfsList.add(ifs);
    }

    for (AggFunctionDetails afd : aggDetails) {
      ImpalaFunctionSignature ifs = new ImpalaFunctionSignature(afd.fnName, afd.getArgTypes(),
          afd.getRetType(), false, false);
      List<ImpalaFunctionSignature> castIfsList =
          CAST_CHECK_BUILTINS_INSTANCE.computeIfAbsent(afd.fnName, k -> Lists.newArrayList());
      castIfsList.add(ifs);
    }

    for (String fnName : CAST_CHECK_BUILTINS_INSTANCE.keySet()) {
      List<ImpalaFunctionSignature> ifsList = CAST_CHECK_BUILTINS_INSTANCE.get(fnName);
      Collections.sort(ifsList, new SignatureComparator());
    }
  }

  private final String func;

  private final List<Type> argTypes;

  private final Type retType;

  private final boolean hasVarArgs;

  private final RelDataType retRelDataType;

  private final List<RelDataType> argRelDataTypes;

  // flag to specify if the function
  private final Boolean retTypeAlwaysNullable;

  private ImpalaFunctionSignature(String func, List<Type> argTypes, Type retType,
      boolean hasVarArgs, Boolean retTypeAlwaysNullable) {
    Preconditions.checkNotNull(func);
    this.func = func;
    this.argTypes = ImmutableList.copyOf(argTypes);
    this.argRelDataTypes = ImpalaTypeConverter.getRelDataTypesForArgs(this.argTypes);
    this.retType = retType;
    this.retRelDataType =
        (this.retType == null) ? null : ImpalaTypeConverter.getRelDataType(this.retType, true);
    this.hasVarArgs = hasVarArgs;
    // "from_timestamp" is a function that changes the input into a null if it cannot be processed.
    // If we find other functions that can turn non-nulls into nulls, we probably should make
    // this an attribute in the json file.
    this.retTypeAlwaysNullable = retTypeAlwaysNullable;
  }

  private ImpalaFunctionSignature(String func, List<RelDataType> argTypes, RelDataType retType) {
    Preconditions.checkNotNull(func);
    this.func = func;
    this.argTypes = ImpalaTypeConverter.getNormalizedImpalaTypes(argTypes);
    this.argRelDataTypes = ImpalaTypeConverter.getRelDataTypesForArgs(this.argTypes);
    this.retType = retType != null ? ImpalaTypeConverter.getNormalizedImpalaType(retType) : null;
    this.retRelDataType =
        (this.retType == null) ? null : ImpalaTypeConverter.getRelDataType(this.retType, true);
    this.hasVarArgs = false;
    this.retTypeAlwaysNullable = null;
  }

  public String getFunc() {
     return func;
  }

  public RelDataType getRetType() {
    return ImpalaTypeConverter.getRelDataType(retType, true);
  }

  public List<RelDataType> getArgTypes() {
    return ImpalaTypeConverter.getRelDataTypesForArgs(argTypes);
  }

  public boolean hasVarArgs() {
    return hasVarArgs;
  }

  public boolean retTypeAlwaysNullable() {
    Preconditions.checkNotNull(retTypeAlwaysNullable);
    return retTypeAlwaysNullable;
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
      return areCompatibleDataTypes(typeToMatch, inputs.get(inputs.size() - 1));
    }
    return true;
  }

  private static ImpalaFunctionSignature createTimeIntervalOpSignature(
      String funcName, SqlKind kind, List<RelDataType> argTypes) {
    String timeOpFuncName =
        TimeIntervalOpFunctionResolver.getFunctionName(funcName, kind, argTypes);
    boolean isNullable = ImpalaTypeConverter.areAnyTypesNullable(argTypes);
    // The time interval operations will always take timestamp and bigint as their two
    // arguments.  If the first operand is of type "date", it will be cast into a timestamp.
    List<Type> timeArgTypes = ImmutableList.of(Type.TIMESTAMP, Type.BIGINT);
    List<RelDataType> timeArgRelDataTypes =
        ImpalaTypeConverter.getRelDataTypesForArgs(timeArgTypes);
    Type timeRetType = Type.TIMESTAMP;
    RelDataType timeRetRelDataType = ImpalaTypeConverter.getRelDataType(timeRetType, isNullable);
    return new ImpalaFunctionSignature(timeOpFuncName, timeArgRelDataTypes, timeRetRelDataType);
  }

  // Create the Impala Function Signature for STRING_ONLY_FUNCTIONS, the functions that
  // handle STRING types but do not handle CHAR and VARCHAR types.
  private static ImpalaFunctionSignature createStringSignature(
      String funcName, List<RelDataType> argTypes, RelDataType retType) {
    List<RelDataType> inArgTypes = Lists.newArrayList();
    for (RelDataType argType : argTypes) {
      if (SqlTypeName.CHAR_TYPES.contains(argType.getSqlTypeName())) {
        inArgTypes.add(ImpalaTypeConverter.getRelDataType(Type.STRING, argType.isNullable()));
      } else {
        inArgTypes.add(argType);
      }
    }
    boolean isNullable = ImpalaTypeConverter.areAnyTypesNullable(argTypes);
    RelDataType inRetType = retType;
    if (retType != null && SqlTypeName.CHAR_TYPES.contains(retType.getSqlTypeName())) {
      retType = ImpalaTypeConverter.getRelDataType(Type.STRING, isNullable);
    }
    return new ImpalaFunctionSignature(funcName, inArgTypes, inRetType);
  }


  public static boolean areCompatibleDataTypes(RelDataType dt1, RelDataType dt2) {
    return areCompatibleDataTypes(dt1, dt2, false);
  }

  /**
   * Returns true if datatypes are compatible within the Impala function. In the case of character,
   * data, char, varchar, and string are all compatible.  Nulls are compatible with everything
   * since the function signature will never take a null type and if the data is null, it can
   * go into any Impala function.
   * If isStrictDecimal is true, then the precision and scale of the reldatatypes need to match.
   * If isStrictDecimal is false, then any two decimal types will be compatible.
   */
  public static boolean areCompatibleDataTypes(RelDataType dt1, RelDataType dt2,
      boolean isStrictDecimal) {
    if (SqlTypeName.CHAR_TYPES.contains(dt1.getSqlTypeName()) &&
        SqlTypeName.CHAR_TYPES.contains(dt2.getSqlTypeName())) {
      // char types are always compatible from a compilation point of view, so no
      // casting is needed.  At runtime, however, different char sizes will not
      // be equal to each other.
      // TODO: If we do have different chars, the optimizer should be able to remove the
      // clause.
      if (dt1.getSqlTypeName() == SqlTypeName.CHAR &&
          dt2.getSqlTypeName() == SqlTypeName.CHAR) {
        return true;
      }

      if (dt1.getSqlTypeName() == SqlTypeName.VARCHAR &&
          dt2.getSqlTypeName() == SqlTypeName.VARCHAR) {
        // Impala treaats a string type different from a varchar type. So varchar(5) is compatible
        // So if varchars have the same precision, they're compatible.
        // If varchars have different precisions, they are compabible unless it's a string type, and
        // string types are denoted by a precision of Integer.MAX_VALUE
        if (dt1.getPrecision() == dt2.getPrecision()) {
          return true;
        }
        if (dt1.getPrecision() != Integer.MAX_VALUE && dt2.getPrecision() != Integer.MAX_VALUE) {
          return true;
        }
      }
      return false;
    }

    if (dt1.getSqlTypeName() == SqlTypeName.DECIMAL && dt2.getSqlTypeName() == SqlTypeName.DECIMAL) {
      if (!isStrictDecimal) {
        return true;
      }
      return dt1.getPrecision() == dt2.getPrecision() && dt1.getScale() == dt2.getScale();
    }

    if (dt1.getSqlTypeName() == SqlTypeName.NULL || dt2.getSqlTypeName() == SqlTypeName.NULL) {
      return true;
    }

    if (dt1.getStructKind() != dt2.getStructKind()) {
      return false;
    }

    if (dt1.isStruct()) {
      List<RelDataTypeField> dt1Fields = dt1.getFieldList();
      List<RelDataTypeField> dt2Fields = dt2.getFieldList();
      if (dt1Fields.size() != dt2Fields.size()) {
        return false;
      }
      for (int i = 0; i < dt1Fields.size() && i < dt2Fields.size(); ++i) {
        if (!areCompatibleDataTypes(dt1Fields.get(i).getType(),
                                    dt2Fields.get(i).getType())) {
          return false;
        }
      }
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

    if (STRING_ONLY_FUNCTIONS.contains(lowerCaseFunc)) {
      ifs = createStringSignature(lowerCaseFunc, argTypes, returnType);
    } else {
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
          ifs = TimeIntervalOpFunctionResolver.argTypesHaveTimeIntervalOp(argTypes)
              ? createTimeIntervalOpSignature(lowerCaseFunc, kind, argTypes)
              : new ImpalaFunctionSignature(lowerCaseFunc, argTypes, returnType);
          break;
        default:
          ifs = new ImpalaFunctionSignature(lowerCaseFunc, argTypes, returnType);
      }
    }
    // Check within the given map that this signature exists. Even though this static function
    // created a signature, the returned signature will be a different object as retrieved from
    // the map.
    return ifs.getMatchingSignature(functionDetailsMap);
  }

  public static ImpalaFunctionSignature create(String func, List<Type> argTypes, Type retType,
      boolean hasVarArgs, Boolean retTypeAlwaysNullable) {
    return new ImpalaFunctionSignature(func, argTypes, retType, hasVarArgs, retTypeAlwaysNullable);
  }

  public static RelDataType getCastType(RelDataType dt1, RelDataType dt2, RelDataTypeFactory typeFactory) {
    if (dt1.getSqlTypeName() == SqlTypeName.NULL) {
      return dt2;
    }
    if (dt2.getSqlTypeName() == SqlTypeName.NULL) {
      return dt1;
    }

    if (dt1.getStructKind() == dt2.getStructKind()) {
      if (dt1.isStruct()) {
        List<RelDataTypeField> outputFields = Lists.newArrayList();
        List<RelDataTypeField> dt1Fields = dt1.getFieldList();
        List<RelDataTypeField> dt2Fields = dt2.getFieldList();
        for (int i = 0; i < dt1Fields.size() && i < dt2Fields.size(); ++i) {
          RelDataType fieldType =
            getCastType(dt1Fields.get(i).getType(), dt2Fields.get(i).getType(), typeFactory);
          outputFields.add(new RelDataTypeFieldImpl(
              dt1Fields.get(i).getName(), i, fieldType));
        }

        List<RelDataTypeField> smallerDtFields =
            dt1Fields.size() < dt2Fields.size() ? dt1Fields : dt2Fields;
        while (outputFields.size() < smallerDtFields.size()) {
          int fieldNum = outputFields.size();
          RelDataType fieldType = smallerDtFields.get(fieldNum).getType();
          outputFields.add(new RelDataTypeFieldImpl(
                smallerDtFields.get(fieldNum).getName(), fieldNum, fieldType));
        }
        return new RelRecordType(outputFields);
      } else if (canCastUp(dt1, dt2)) {
        // canCastUp checks castability across different datatypes. If the datatypes
        // are the same, it will return true. The precision and scale of the datatype
        // (if decimal) are ignored in the canCastUp function and are dealt with in
        // the adjustCastType method.
        return adjustCastType(dt1, dt2, typeFactory);
      } else if (canCastUp(dt2, dt1)) {
        return adjustCastType(dt2, dt1, typeFactory);
      }
    }


    throw new RuntimeException("Cannot derive common cast type for " + dt1.getFullTypeString() + " and " + dt2.getFullTypeString());
  }

  // Handle cases where the return type must be different than the from and to type
  // i.e. integer->decimal(1,0).  If either the from or to type is a decimal, then the least common
  // shared datatype will be a decimal as defined by Impala's typecasting rules.
  static RelDataType adjustCastType(RelDataType fromType, RelDataType toType, RelDataTypeFactory typeFactory) {
    return toType.getSqlTypeName() == SqlTypeName.DECIMAL
        ? getDecimalAssignmentCompatibleType(fromType, toType, typeFactory)
        : toType;
  }

  static RelDataType getDecimalAssignmentCompatibleType(RelDataType dt1, RelDataType dt2,
      RelDataTypeFactory typeFactory) {
    boolean isNullable = dt1.isNullable() || dt2.isNullable();
    ScalarType impalaType1 = (ScalarType) ImpalaTypeConverter.createImpalaType(dt1);
    ScalarType decimalType1 = impalaType1.getMinResolutionDecimal();
    ScalarType impalaType2 = (ScalarType) ImpalaTypeConverter.createImpalaType(dt2);
    ScalarType decimalType2 = impalaType2.getMinResolutionDecimal();
    ScalarType commonScalarType =
        TypesUtil.getDecimalAssignmentCompatibleType(decimalType1, decimalType2, false);
    RelDataType retType = typeFactory.createSqlType(SqlTypeName.DECIMAL,
        commonScalarType.decimalPrecision(), commonScalarType.decimalScale());
    return typeFactory.createTypeWithNullability(retType, isNullable);
  }


  // Check to see if the "from" type can be cast up to the "to" type. In the case
  // where both are decimals, this will return true.
  public static boolean canCastUp(RelDataType castFrom, RelDataType castTo) {
    if (castFrom.getSqlTypeName() == SqlTypeName.NULL) {
      return true;
    }

    if (areCompatibleDataTypes(castFrom, castTo)) {
      return true;
    }

    ImpalaFunctionSignature castSig =
        new ImpalaFunctionSignature("cast", Lists.newArrayList(castFrom), castTo);
    ScalarFunctionDetails details = ScalarFunctionDetails.SCALAR_BUILTINS_MAP.get(castSig);
    return details != null && details.castUp;
  }

  // Comparator for ImpalaFunctionSignature.  This comparator is only used to determine
  // the order of usage when the function name matches for casting purposes.  The order
  // is needed for instances when we could cast to both a SMALLINT and INT, but we would
  // prefer to use the SMALLINT function, so we put the SMALLINT function before the INT
  // function.
  public static class SignatureComparator implements Comparator<ImpalaFunctionSignature> {
    @Override
    public int compare(ImpalaFunctionSignature o1, ImpalaFunctionSignature o2) {
      Preconditions.checkState(o1.func.equals(o2.func));

      // In the cases where the prototypes have different argument sizes, the
      // ordering doesn't really matter, because the function with the wrong
      // number of parameters will ultimately be rejected anyway.
      if (o1.argTypes.size() != o2.argTypes.size()) {
        return o2.argTypes.size() - o1.argTypes.size();
      }

      if (o1.argTypes.size() == 0) {
        return 0;
      }

      // Check that the types match.
      // The pattern of the ordinal string for the type needs to be normalized.
      // It is possible the type can show up like "DECIMAL(32,8)", and we only
      // want the DECIMAL part.
      String thisOrdinalString = o1.argTypes.get(0).toString().split("\\(")[0];
      String otherOrdinalString = o2.argTypes.get(0).toString().split("\\(")[0];
      int thisOrdinal = SqlTypeOrdering.valueOf(thisOrdinalString).ordinal();
      int otherOrdinal = SqlTypeOrdering.valueOf(otherOrdinalString).ordinal();
      return Integer.compare(thisOrdinal, otherOrdinal);
    }
  }
}
