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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.functions.CalciteUDFInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSqlFunction;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;
import org.apache.impala.analysis.TypesUtil;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class ImpalaFunctionResolverImpl implements ImpalaFunctionResolver {

  protected final String func;

  protected final ImmutableList<RelDataType> argTypes;

  protected final FunctionHelper helper;

  protected final RexBuilder rexBuilder;

  protected final List<RexNode> inputNodes;

  protected final SqlOperator op;

  public ImpalaFunctionResolverImpl(FunctionHelper helper, String func,
      List<RexNode> inputNodes) {
    this(helper, null, func, inputNodes);
  }

  public ImpalaFunctionResolverImpl(FunctionHelper helper, SqlOperator op,
      List<RexNode> inputNodes) {
    this(helper, op, op.getName().toLowerCase(), inputNodes);
  }

  public ImpalaFunctionResolverImpl(FunctionHelper helper, SqlOperator op, String func,
      List<RexNode> inputNodes) {
    this.helper = helper;
    this.rexBuilder = helper.getRexNodeExprFactory().getRexBuilder();
    this.inputNodes = inputNodes;
    // op can be null for FunctionResolvers that are special and don't map to
    // anything in Calcite.  One example is the InternalIntervalFunctionResolver
    // where it maps the function "internal_interval" into a RexLiteral.
    this.func = func.toLowerCase();
    this.argTypes = ImmutableList.copyOf(RexUtil.types(inputNodes));
    this.op = op;
  }

  public ImpalaFunctionSignature getFunction(
      Map<ImpalaFunctionSignature, ? extends FunctionDetails> functionDetailsMap)
      throws SemanticException {

    // First try to retrieve a signature that directly matches the input with no casting,
    // if possible. The "derived" return type is null in most cases, but can have a
    // value if it is important for the function to pick the right signature. This happens
    // with arithmetic operators.
    ImpalaFunctionSignature candidate =
        ImpalaFunctionSignature.fetch(functionDetailsMap, func, argTypes, null);

    // if the candidate has varargs, we always want to check if the arguments need to be
    // cast.  The signature fetched will match the first <n> arguments, but we don't know
    // if the variable part of the signature needs casting at this point.
    if (candidate != null && !candidate.hasVarArgs()) {
      return candidate;
    }

    // No directly matching, so we try to retrieve a function to which we can cast.
    return getFunctionWithCasts();
  }

  public List<RexNode> getConvertedInputs(ImpalaFunctionSignature candidate) throws HiveException {
    List<RelDataType> castTypes = getCastOperandTypes(candidate);
    Preconditions.checkNotNull(castTypes);
    // The number of cast types match 1:1 with the input nodes.
    Preconditions.checkState(castTypes.size() == inputNodes.size());
    return castInputs(inputNodes, castTypes);
  }

  /**
   * Derive the return type from the given signature and operands.
   */
  public RelDataType getRetType(ImpalaFunctionSignature funcSig, List<RexNode> operands) {
    RelDataType relDataType = funcSig.getRetType();
    // handle the return types with precision and scale separately.
    switch (relDataType.getSqlTypeName()) {
      case VARCHAR: {
        boolean isNullable = funcSig.retTypeAlwaysNullable();
        for (RexNode operand : operands) {
          isNullable = isNullable || operand.getType().isNullable();
        }

        int precision = relDataType.getPrecision();
        RelDataType rdt = rexBuilder.getTypeFactory().createTypeWithCharsetAndCollation(
              rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, precision),
              Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT);
        return rexBuilder.getTypeFactory().createTypeWithNullability(rdt, isNullable);
      }
      case CHAR:
      case DECIMAL:
        // This could fail if there are Impala functions which are not mapped into Calcite
        // and have a return type of char or decimal. We will have to figure out some way
        // to handle these types, since it might not be wise to pick a default.
        Preconditions.checkNotNull(op);
        return op.inferReturnType(rexBuilder.getTypeFactory(), RexUtil.types(operands));
      // For the default case, we can just create a RelDataType.
      default: {
        boolean isNullable = funcSig.retTypeAlwaysNullable();
        for (RexNode operand : operands) {
          isNullable = isNullable || operand.getType().isNullable();
        }
        RelDataType rdt = funcSig.getRetType();
        return rexBuilder.getTypeFactory().createTypeWithNullability(rdt, isNullable);
      }
    }
  }

  public RexNode createRexNode(ImpalaFunctionSignature candidate, List<RexNode> inputs,
      RelDataType returnDataType) {
    SqlOperator opToUse = op;
    if (opToUse == null) {
      CalciteUDFInfo udfInfo = CalciteUDFInfo.createUDFInfo(func, argTypes, returnDataType);
      boolean deterministic =
          helper.isConsistentWithinQuery(new ImpalaFunctionInfo(candidate.getFunc()));
      opToUse = new HiveSqlFunction(func, SqlKind.OTHER_FUNCTION, udfInfo.returnTypeInference,
          udfInfo.operandTypeInference, udfInfo.operandTypeChecker,
          SqlFunctionCategory.USER_DEFINED_FUNCTION, deterministic, false);
    }
    return rexBuilder.makeCall(returnDataType, opToUse, inputs);
  }

  @Override
  public String toString() {
    return func + "(" + StringUtils.join(argTypes, ", ") + ")";
  }

  /**
   * Return the most optimal function signature that can resolve "this" object if
   * casting is allowed.
   */
  protected ImpalaFunctionSignature getFunctionWithCasts() throws SemanticException {
    // castCandidates contains a list of potential functions that matches the name.
    // These candidates will have the same function name, but different operand/return types.
    List<ImpalaFunctionSignature> castCandidates = getCastCandidates(func);
        ImpalaFunctionSignature.CAST_CHECK_BUILTINS_INSTANCE.get(func);
    if (castCandidates == null) {
      throw new SemanticException("Could not find function name " + func +
          " in resource file");
    }

    // Check if we should favor a precise numeric function (e.g. decimal) over a
    // non-precise one.  Non-precise functions appear before precise functions in
    // the candidate list.
    boolean favorPreciseFunction = favorPreciseFunction(argTypes);

    // iterate through list of potential functions which we could cast.  These functions should be
    // in a pre-determined optimal order.
    // For instance:  we have sum(BIGINT), sum(DOUBLE), and sum(DECIMAL) in that order.  If we are
    // trying to map a sum(tinyint_col), we would choose sum(BIGINT).  If we are trying to map
    // a sum(float_col), we would choose sum(DOUBLE).
    List<ImpalaFunctionSignature> matchingCandidates = Lists.newArrayList();
    for (ImpalaFunctionSignature castCandidate : castCandidates) {
      if (canCast(castCandidate)) {
        // if we are not favoring the precise function, we can return the first function found.
        if (!favorPreciseFunction) {
          return castCandidate;
        }
        matchingCandidates.add(castCandidate);
      }
    }
    if (matchingCandidates.isEmpty()) {
      throw new SemanticException("Could not cast for function name " + func);
    }
    return pickPreferredCastCandidate(matchingCandidates);
  }

  protected List<ImpalaFunctionSignature> getCastCandidates(String func) throws SemanticException {
    // castCandidates contains a list of potential functions that matches the name.
    // These candidates will have the same function name, but different operand/return types.
    List<ImpalaFunctionSignature> castCandidates =
        ImpalaFunctionSignature.CAST_CHECK_BUILTINS_INSTANCE.get(func);
    if (castCandidates == null) {
      throw new SemanticException("Could not find function name " + func +
          " in resource file");
    }
    return castCandidates;
  }

  /**
   * Returns true if all of the given operands can be cast to the given function
   * signature.
   **/
  protected boolean canCast(ImpalaFunctionSignature candidate) {
    // If the number of arguments is different and the signature doesn't
    // allow a variable number of arguments, we cannot cast to this
    // signature.
    if (!candidate.hasVarArgs() &&
        argTypes.size() != candidate.getArgTypes().size()) {
      return false;
    }

    RelDataType castTo = null;
    // loop through all the arguments to make sure they all can be cast.
    for (int i = 0; i < argTypes.size(); ++i) {
      RelDataType castFrom = argTypes.get(i);
      // Interval types won't be cast and will be left as/is.
      if (SqlTypeName.INTERVAL_TYPES.contains(castFrom.getSqlTypeName())) {
        continue;
      }
      // if we have var args, the last arg will be repeating, so we don't have to
      // get the last one.
      if (i < candidate.getArgTypes().size()) {
        castTo = candidate.getArgTypes().get(i);
      }
      // if they are equal, it doesn't need to be cast, so we move to the next
      // argument
      if (!ImpalaFunctionSignature.areCompatibleDataTypes(castFrom, castTo) &&
          !ImpalaFunctionSignature.canCastUp(castFrom, castTo)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Gets all the cast operand types.
   * It is possible that the cast candidate has a variable number of operands, and thus
   * can have less than the number of operands in "this" object. But in any case, the
   * number of operands returned should match the number of operands in "this" object.
   **/
  protected List<RelDataType> getCastOperandTypes(
      ImpalaFunctionSignature castCandidate) {
    // We need the arguments from the cast candidate signature, but "this" may contain
    // more arguments than the cast in the case where the cast candidate has a variable
    // number of arguments.  So we call "getAllVarArgs" with "this" number of arguments
    // so that the returned number of arguments matches what is expected.
    return getAllVarArgs(castCandidate.getArgTypes(), argTypes.size());
  }

  protected List<RexNode> castInputs(List<RexNode> inputs, List<RelDataType> castTypes) {
    List<RexNode> newOperands = Lists.newArrayList();
    Preconditions.checkState(inputs.size() == castTypes.size());
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

    // Some functions (e.g. IN) require all decimal parameters to use the same precision
    // and scale. The commonDecimalType will hold a value only for these functions.
    RelDataType commonDecimalType =
        getCommonDecimalTypeIfNeeded(typeFactory, inputs, castTypes);

    for (int i = 0; i < inputs.size(); ++i) {
      RelDataType preCastDataType =  inputs.get(i).getType();
      boolean strictDecimalComparison = (commonDecimalType != null);
      RelDataType postCastDataType =
          (strictDecimalComparison && castTypes.get(i).getSqlTypeName() == SqlTypeName.DECIMAL)
              ? commonDecimalType : castTypes.get(i);
      // If the datatypes are compatible, we don't want to cast it.
      // If the datatype is null, they are compatible, but we always want to cast it
      // to its proper datatype.
      // The strictDecimalComparison boolean is false (normal case) when we don't want to cast
      // decimal types with different precisions (e.g. for multiplication, it's ok to multiply
      // dec(3,2) * dec(8,1). For "IN" clauses, the params need a compatible decimal
      // precision/scale.
      if (preCastDataType.getSqlTypeName() != SqlTypeName.NULL &&
          ImpalaFunctionSignature.areCompatibleDataTypes(preCastDataType, postCastDataType,
              strictDecimalComparison)) {
        newOperands.add(inputs.get(i));
      } else {
        RelDataType castedRelDataType = (castTypes.get(i).getSqlTypeName() == SqlTypeName.DECIMAL)
            ? getCastedDecimalDataType(typeFactory, commonDecimalType, preCastDataType)
            : getCastedDataType(typeFactory, castTypes.get(i), preCastDataType);
        // cast to appropriate type
        newOperands.add(rexBuilder.makeCast(castedRelDataType, inputs.get(i), true));
      }
    }
    return newOperands;
  }

  /**
   * needsCommonDecimalType returns true if the function requires all decimal parameters
   * to have the same precision and scale. The default is that it's ok to have different
   * precisions and scales. A derived FunctionResolver can override this method.
   */
  protected boolean needsCommonDecimalType(List<RelDataType> castTypes) {
    return false;
  }

  public RexNode castToType(RexNode node, RelDataType toType) {
    if (node.getType().getStructKind() != toType.getStructKind()) {
      throw new RuntimeException(
          "Cannot convert " + node.getType().getFullTypeString() +
          " to " + toType.getFullTypeString());
    }
    if (node.getType().equals(toType)) {
      return node;
    }
    if (toType.isStruct()) {
      RexCall call = (RexCall)node;
      List<RexNode> operands = call.getOperands();
      List<RexNode> newOperands = Lists.newArrayList();
      List<RelDataTypeField> dtFields = toType.getFieldList();
      for (int i = 0; i < dtFields.size() && i < operands.size(); ++i) {
        RexNode newOp = castToType(operands.get(i), dtFields.get(i).getType());
        newOperands.add(newOp);
      }
      while (newOperands.size() < dtFields.size()) {
        RelDataType fieldType = dtFields.get(newOperands.size()).getType();
        newOperands.add(rexBuilder.makeNullLiteral(fieldType));
      }
      return rexBuilder.makeCall(toType, call.getOperator(), newOperands);
    } else {
      RelDataType castedRelDataType = getCastedDataType(
          rexBuilder.getTypeFactory(), toType, node.getType());
      return rexBuilder.makeCast(toType, node, true);
    }
  }

  protected List<RexNode> castToCompatibleType(List<RexNode> inputs) {
    List<RexNode> newOperands = Lists.newArrayList();
    RelDataType widestType = null;
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    for (RexNode node : inputs) {
      widestType = (widestType == null) ? node.getType() :
          ImpalaFunctionSignature.getCastType(widestType, node.getType(), typeFactory);
    }
    for (RexNode node : inputs) {
      newOperands.add(castToType(node, widestType));
    }
    return newOperands;
  }

  /**
   * getCommonDecimalTypeIfNeeded returns a common decimal RelDataType across all the
   * decimal input parameters.  It is only required for functions that need a common
   * data type.
   */
  private RelDataType getCommonDecimalTypeIfNeeded(RelDataTypeFactory typeFactory,
      List<RexNode> inputs, List<RelDataType> castTypes) {
    if (!needsCommonDecimalType(castTypes)) {
      return null;
    }
    return getCommonDecimalType(typeFactory, inputs);
  }

  /**
   * getCommonDecimalType returns a common decimal RelDataType across all the
   * decimal input parameters.
   */
  protected RelDataType getCommonDecimalType(RelDataTypeFactory typeFactory,
      List<RexNode> inputs) {
    ScalarType commonScalarType = null;
    boolean isNullable = false;
    for (RexNode input : getCommonDecimalInputs(inputs)) {
      RelDataType dt = input.getType();
      isNullable |= dt.isNullable();
      ScalarType impalaType = (ScalarType) ImpalaTypeConverter.createImpalaType(dt);
      // get minimum resolution decimal (e.g. tinyint returns dec(3,0))
      ScalarType decimalType = impalaType.getMinResolutionDecimal();
      // nulls do not change the requirements of the common decimal type
      if (decimalType.isNull()) {
        continue;
      }
      commonScalarType = (commonScalarType == null)
          ? decimalType
          : TypesUtil.getDecimalAssignmentCompatibleType(commonScalarType, decimalType, false);
    }
    if (commonScalarType == null) {
      return null;
    }

    RelDataType retType = typeFactory.createSqlType(SqlTypeName.DECIMAL,
        commonScalarType.decimalPrecision(), commonScalarType.decimalScale());
    return typeFactory.createTypeWithNullability(retType, isNullable);
  }

  protected List<RexNode> getCommonDecimalInputs(List<RexNode> inputs) {
    return inputs;
  }

  private RelDataType getCastedDecimalDataType(RelDataTypeFactory dtFactory,
      RelDataType commonDecimalType, RelDataType preCastRelDataType) {
    // if there is no common decimal type, the appropriate size decimal will be created based
    // on the datatype that is going to be case.
    // If there is a common datatype, the common datatype is created with the appropriate
    // nullability.
    return commonDecimalType == null
        ? ImpalaTypeConverter.createDecimalType(dtFactory, preCastRelDataType)
        : dtFactory.createTypeWithNullability(commonDecimalType, preCastRelDataType.isNullable());
  }

  /**
   * Return the casted RelDatatype of the provided postCastSqlTypeName
   */
  private RelDataType getCastedDataType(RelDataTypeFactory dtFactory,
      RelDataType postCastRelType, RelDataType preCastRelDataType) {
    SqlTypeName postCastSqlTypeName = postCastRelType.getSqlTypeName();

    if (postCastSqlTypeName == SqlTypeName.DECIMAL) {
      return ImpalaTypeConverter.createDecimalType(dtFactory, preCastRelDataType);
    }

    RelDataType rdt;
    if (SqlTypeName.CHAR_TYPES.contains(postCastSqlTypeName)) {
      rdt = dtFactory.createTypeWithCharsetAndCollation(
              dtFactory.createSqlType(postCastSqlTypeName, postCastRelType.getPrecision()),
              Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT);
    } else {
      rdt = dtFactory.createSqlType(postCastSqlTypeName);
    }
    return dtFactory.createTypeWithNullability(rdt, preCastRelDataType.isNullable());
  }

  /**
   * Helper method when var args exist.  If there are no var args, no action is
   * needed and we immediately pass back the args structure.  If there are varargs,
   * we repeat the last args until we have the exact number of args that we need.
   */
  private List<RelDataType> getAllVarArgs(List<RelDataType> currArgTypes, int numArgs) {
    // if the function didn't have var args, we expect it to return right away.
    if (currArgTypes.size() == numArgs) {
      return currArgTypes;
    }

    // Fill in the remaining arguments with the last argument so the number of arguments
    // match what is expected.
    List<RelDataType> result = Lists.newArrayList(currArgTypes);
    RelDataType lastArg = result.get(result.size() - 1);
    while (result.size() < numArgs) {
      result.add(lastArg);
    }
    return result;
  }

  // Return true if we have a case where we would prefer a precise function to a non-precise
  // function. Impala has rules that state that if one of the argTypes is a DECIMAL, and all
  // the other types are precise, we should use precise types.
  // should use precise types.
  private boolean favorPreciseFunction(List<RelDataType> types) {
    return hasDecimalType(types) && !hasFloatingType(types);
  }

  private boolean hasDecimalType(List<RelDataType> types) {
    for (RelDataType type : types) {
      if (type.getSqlTypeName().equals(SqlTypeName.DECIMAL)) {
        return true;
      }
    }
    return false;
  }

  private boolean hasFloatingType(List<RelDataType> types) {
    for (RelDataType type : types) {
      if (SqlTypeName.APPROX_TYPES.contains(type.getSqlTypeName())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Follow the Impala rules to pick the preferred candidate from a list of
   * matching candidates. The original list is in sorted order of preferability, for the
   * most part. However, Impala has a rule that if one of the operands is a decimal
   * we would prefer to cast to a decimal and keep the function precise.
   * However, if we can't find a precise function, it's ok to use a non-precise one.
   */
  private ImpalaFunctionSignature pickPreferredCastCandidate (
      List<ImpalaFunctionSignature> matchingCandidates) {
    for (ImpalaFunctionSignature ifs : matchingCandidates) {
      if (!hasFloatingType(ifs.getArgTypes())) {
        return ifs;
      }
    }
    // If all functions are floating type, just return the first one.
    Preconditions.checkState(!matchingCandidates.isEmpty());
    return matchingCandidates.get(0);
  }

  private static ImpalaFunctionResolver createGenericFuncResolver(FunctionHelper helper,
      String func, List<RexNode> inputs) throws CalciteSemanticException {
    if (func.equals("internal_interval")) {
      return new InternalIntervalFunctionResolver(helper, inputs);
    }
    if (func.equals("inline")) {
      return new InlineFunctionResolver(helper, inputs);
    }

    return new ImpalaFunctionResolverImpl(helper, func, inputs);
  }

  public static ImpalaFunctionResolver create(FunctionHelper helper, String func,
      List<RexNode> inputs, RelDataType retType) throws HiveException {

    SqlOperator op = ImpalaOperatorTable.IMPALA_OPERATOR_MAP.get(func.toUpperCase());

    if (op == null) {
      // handle the special case where there is no mapped operator
      return createGenericFuncResolver(helper, func, inputs);
    }

    switch(op.getKind()) {
      case CAST:
        // if the return type is not passed in, it is derived from the function name.
        if (retType == null) {
          String adjustedFunc = func.toUpperCase().equals("INT") ? "INTEGER" : func.toUpperCase();
          // Use the Impala normalized type.
          Type impalaType = Type.parseColumnType(adjustedFunc);
          Preconditions.checkState(inputs.size() == 1);
          // When casting a timestamp, it is possible to get a null value
          // (e.g cast('1' as timestamp)). There may be other examples as well
          // so to be safe, isNullable is set to true always.
          boolean isNullable = true;
          retType = ImpalaTypeConverter.getRelDataType(impalaType, isNullable);
        }
        return new CastFunctionResolver(helper, op, inputs, retType);
      case CASE:
        // case statements can come in as "when" or "case", see cthe Case*Resolver comment
        // for more information.
        if (func.toLowerCase().equals("when")) {
          return new CaseWhenFunctionResolver(helper, op, inputs);
        }
        return new CaseFunctionResolver(helper, op, inputs);
      case IN:
        return new InFunctionResolver(helper, op, inputs);
      case EXTRACT:
        return new ExtractFunctionResolver(helper, op, inputs);
      case GROUPING:
        return new GroupingFunctionResolver(helper, op, inputs);
      case ARRAY_VALUE_CONSTRUCTOR:
      case ARRAY_QUERY_CONSTRUCTOR:
        return new ArrayFunctionResolver(helper, op, inputs);
      case ROW:
        return new StructFunctionResolver(helper, op, inputs);
      case PLUS:
      case MINUS:
        return TimeIntervalOpFunctionResolver.rexNodesHaveTimeIntervalOp(inputs)
            ? new TimeIntervalOpFunctionResolver(helper, func, op.getKind(), inputs)
            : new ArithmeticFunctionResolver(helper, op, inputs);
      case TIMES:
      case DIVIDE:
        return new ArithmeticFunctionResolver(helper, op, inputs);
      case OTHER:
      case OTHER_FUNCTION:
        // Functions like "round" come in as an "OTHER" operator kind.
        if (AdjustScaleFunctionResolver.isAdjustScaleFunction(op.getName().toLowerCase())) {
          return new AdjustScaleFunctionResolver(helper, op, inputs);
        }
        return new ImpalaFunctionResolverImpl(helper, op, inputs);
      default:
        return new ImpalaFunctionResolverImpl(helper, op, inputs);
    }
  }
}
