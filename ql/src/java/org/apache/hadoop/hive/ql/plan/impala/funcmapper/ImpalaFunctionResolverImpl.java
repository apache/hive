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
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.NlsString;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;
import org.apache.hadoop.hive.ql.parse.type.RexNodeExprFactory;
import org.apache.hadoop.hive.ql.parse.type.RexNodeExprFactory.HiveNlsString;
import org.apache.hadoop.hive.ql.parse.type.RexNodeExprFactory.HiveNlsString.Interpretation;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class ImpalaFunctionResolverImpl implements ImpalaFunctionResolver {

  protected final String func;

  protected final List<RelDataType> argTypes;

  protected final RelDataType retType;

  protected final FunctionHelper helper;

  protected final RexBuilder rexBuilder;

  protected final List<RexNode> inputNodes;

  protected final SqlOperator op;


  public ImpalaFunctionResolverImpl(FunctionHelper helper, SqlOperator op, List<RexNode> inputNodes,
      RelDataType returnType) {
    this.helper = helper;
    this.rexBuilder = helper.getRexNodeExprFactory().getRexBuilder();
    this.inputNodes = inputNodes;
    this.func = op.getName().toLowerCase();
    this.argTypes = RexUtil.types(inputNodes);
    this.retType = returnType;
    this.op = op;
  }

  public ImpalaFunctionSignature getFunction(
      Map<ImpalaFunctionSignature, ? extends FunctionDetails> functionDetailsMap)
      throws SemanticException {

    // First try to retrieve a signature that directly matches the input with no casting,
    // if possible.
    RelDataType returnType = getReturnType();
    ImpalaFunctionSignature candidate =
        ImpalaFunctionSignature.fetch(functionDetailsMap, func, argTypes, returnType);
    if (candidate != null) {
      return candidate;
    }

    // No directly matching, so we try to retrieve a function to which we can cast.
    return getCastFunction();
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
    // If the return type was passed in, we just return it.
    if (retType != null) {
      return retType;
    }

    RelDataType relDataType = funcSig.getRetType();
    // handle the return types with precision and scale separately.
    switch (relDataType.getSqlTypeName()) {
      case VARCHAR: {
        boolean isNullable = false;
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
        return op.inferReturnType(rexBuilder.getTypeFactory(), RexUtil.types(operands));
      // For the default case, we can just create a RelDataType.
      default: {
        boolean isNullable = false;
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
    return rexBuilder.makeCall(returnDataType, op, inputs);
  }

  @Override
  public String toString() {
    return func + "(" + StringUtils.join(argTypes, ", ") + ")";
  }

  protected ImpalaFunctionSignature getCastFunction() throws SemanticException {
    // castCandidates contains a list of potential functions that matches the name.
    // These candidates will have the same function name, but different operand/return types.
    List<ImpalaFunctionSignature> castCandidates =
        ImpalaFunctionSignature.CAST_CHECK_BUILTINS_INSTANCE.get(func);
    if (castCandidates == null) {
      throw new SemanticException("Could not find function name " + func +
          " in resource file");
    }

    // iterate through list of potential functions which we could cast.  These functions should be
    // in a pre-determined optimal order.
    // For instance:  we have sum(BIGINT), sum(DECIMAL), and sum(DOUBLE) in that order.  If the arg
    // is a TINYINT, we should return a RexNode of "sum(CAST arg AS BIGINT)"
    for (ImpalaFunctionSignature castCandidate : castCandidates) {
      if (canCast(castCandidate)) {
        return castCandidate;
      }
    }
    throw new SemanticException("Could not cast for function name " + func);
  }

  /**
   * Returns true if all of the given operands can be cast to the given function
   * signature.
   **/
  protected boolean canCast(ImpalaFunctionSignature candidate) {
    RelDataType retType = getReturnType();
    // If the return type is not given (set to null), it is ignored. If it is
    // given, we can only match if the given return type matches exactly (no
    // casting allowed on the return type).
    if (retType != null &&
          !ImpalaFunctionSignature.areCompatibleDataTypes(retType, candidate.getRetType())) {
      return false;
    }

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
    for (int i = 0; i < inputs.size(); ++i) {
      RelDataType preCastDataType =  inputs.get(i).getType();
      if (ImpalaFunctionSignature.areCompatibleDataTypes(preCastDataType, castTypes.get(i))) {
        newOperands.add(inputs.get(i));
      } else {
        RelDataType castedRelDataType = getCastedDataType(typeFactory, castTypes.get(i), preCastDataType);
        // cast to appropriate type
        newOperands.add(rexBuilder.makeCast(castedRelDataType, inputs.get(i), true));
      }
    }
    return newOperands;
  }

  protected RelDataType getReturnType() {
    if (retType != null) {
      return retType;
    }
    return null;
  }

  /**
   * Return the casted RelDatatype of the provided postCastSqlTypeName
   */
  private RelDataType getCastedDataType(RelDataTypeFactory dtFactory,
      RelDataType postCastRelType, RelDataType preCastRelDataType) {
    SqlTypeName postCastSqlTypeName = postCastRelType.getSqlTypeName();
    // In the case where we are casting to Decimal, we need to provide a precision
    // and scale.  The Calcite method provides the DecimalRelDataType with the
    // appropriate precision and scale with the provided datatype that will be casted.
    if (postCastSqlTypeName == SqlTypeName.DECIMAL) {
      return Calcite2302.decimalOf(dtFactory, preCastRelDataType);
    }
    RelDataType rdt;
    if (SqlTypeName.CHAR_TYPES.contains(postCastSqlTypeName)) {
      rdt = dtFactory.createSqlType(postCastSqlTypeName, postCastRelType.getPrecision());
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

  public static ImpalaFunctionResolver create(FunctionHelper helper, String func,
      List<RexNode> inputs, RelDataType returnType) throws HiveException {

    SqlOperator op = ImpalaOperatorTable.IMPALA_OPERATOR_MAP.get(func.toUpperCase());

    if (op == null) {
      throw new RuntimeException("Could not find op for " + func);
    }

    switch(op.getKind()) {
      case CAST:
        // The return type is passed in when the cast has precision, e.g. DECIMAL(15,2)
        if (returnType != null) {
          return new CastFunctionResolver(helper, op, inputs, returnType);
        }
        // When there is no return type, the cast is passed in via the function name.
        String adjustedFunc = func.toUpperCase().equals("INT") ? "INTEGER" : func.toUpperCase();
        SqlTypeName retSqlType = SqlTypeName.valueOf(adjustedFunc);
        RexBuilder rexBuilder = helper.getRexNodeExprFactory().getRexBuilder();
        RelDataType newReturnType = rexBuilder.getTypeFactory().createSqlType(retSqlType);

        return new CastFunctionResolver(helper, op, inputs, newReturnType);
      case CASE:
        // case statements can come in as "when" or "case", see cthe Case*Resolver comment
        // for more information.
        if (func.equals("when")) {
          return new CaseWhenFunctionResolver(helper, op, inputs, returnType);
        }
        return new CaseFunctionResolver(helper, op, inputs, returnType);
      case EXTRACT:
        return new ExtractFunctionResolver(helper, op, inputs, returnType);
      case PLUS:
      case MINUS:
        if (inputs.size() == 2 &&
            (SqlTypeName.INTERVAL_TYPES.contains(inputs.get(0).getType().getSqlTypeName()) ||
            SqlTypeName.INTERVAL_TYPES.contains(inputs.get(1).getType().getSqlTypeName()))) {
          return new TimeIntervalOpFunctionResolver(helper, op, inputs, returnType);
        }
        return new ArithmeticFunctionResolver(helper, op, inputs);
      case TIMES:
      case DIVIDE:
        return new ArithmeticFunctionResolver(helper, op, inputs);
      case OTHER:
      case OTHER_FUNCTION:
        // Functions like "round" come in as an "OTHER" operator kind.
        if (AdjustScaleFunctionResolver.isAdjustScaleFunction(op.getName().toLowerCase())) {
          return new AdjustScaleFunctionResolver(helper, op, inputs, returnType);
        }
        return new ImpalaFunctionResolverImpl(helper, op, inputs, returnType);
      default:
        return new ImpalaFunctionResolverImpl(helper, op, inputs, returnType);
    }
  }
}
