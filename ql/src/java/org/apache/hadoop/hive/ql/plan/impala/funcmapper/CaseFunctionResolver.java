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

import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;
import org.apache.impala.catalog.Type;

import java.util.List;
import java.util.Map;

/**
 * Case function resolver.  This specific case function resolver resolves case statements
 * of the form:
 * CASE [expr] WHEN expr THEN expr [WHEN whenexpr THEN rettype ...] [ELSE elseexpr] END
 *
 * In Calcite, the "WHEN" expression is always of type <boolean> and the THEN expression
 * is always the same type as the return type.
 *
 * In this "case", the first expression (i.e. CASE [expr]) is given. This will get rewritten
 * into Calcite so that all WHEN expressions are of type <boolean>
 * (e.g.WHEN expr == whenexpr)
 *
 * The ELSE condition in Calcite will also always be the same type as the return type.
 *
 * Impala simplified this signature for the function to be case(<rettype>) since all other
 * arguments are optional or derived.
 *
 */
public class CaseFunctionResolver extends ImpalaFunctionResolverImpl {

  public CaseFunctionResolver(FunctionHelper helper, SqlOperator op,
      List<RexNode> inputNodes) {
    super(helper, op, inputNodes);
  }

  @Override
  public ImpalaFunctionSignature getFunction(
      Map<ImpalaFunctionSignature, ? extends FunctionDetails> functionDetailsMap)
      throws SemanticException {
    // For the pure "case" function, extra processing needs to be done to turn
    // this into a "case-when" function.  So we can't find a matching signature
    // directly as we do in the base class.
    return getCastFunction();
  }

  @Override
  public List<RexNode> getConvertedInputs(ImpalaFunctionSignature candidate) throws HiveException {
    // Convert the inputs to get rid of the first expression in the case statement (see class comment
    // for details).
    List<RexNode> newInputs = convertCaseToWhenFormat(inputNodes);
    List<RelDataType> castTypes = getCastOperandTypes(candidate);
    if (castTypes == null) {
      throw new HiveException("getCastOperandTypes() for case statement failed.");
    }
    // if there are less input nodes than cast types, then we have an implicit "else null" that
    // we need to create.
    if (newInputs.size() < castTypes.size()) {
      newInputs.add(rexBuilder.makeNullLiteral(castTypes.get(castTypes.size() - 1)));
    }
    return castInputs(newInputs, castTypes);
  }

  @Override
  protected boolean canCast(ImpalaFunctionSignature candidate) {
    RelDataType castTo = candidate.getArgTypes().get(0);
    // For loop constructs this signature's arguments in pairs.
    for (int i = 0; i < (this.argTypes.size()-1) / 2; ++i) {
      int currentArg = 2 * (i + 1);
      // Check to see if the second argument can be cast.
      RelDataType castFrom = this.argTypes.get(currentArg);
      if (!ImpalaFunctionSignature.canCastUp(castFrom, castTo)) {
        return false;
      }
    }

    // If there is an "else" parameter, we see if that parameter can be cast.
    if (this.argTypes.size() % 2 == 0) {
      RelDataType castFrom = this.argTypes.get(this.argTypes.size() - 1);

      return ImpalaFunctionSignature.canCastUp(castFrom, castTo);
    }
    return true;
  }

  @Override
  protected List<RelDataType> getCastOperandTypes(ImpalaFunctionSignature candidate) {
    List<RelDataType> castArgTypes = Lists.newArrayList();

    // For loop constructs this signature's arguments in pairs.
    for (int i = 0; i < (this.argTypes.size() - 1)/ 2; ++i) {
      // first argument in pair is always a boolean
      castArgTypes.add(ImpalaTypeConverter.getRelDataType(Type.BOOLEAN, false));
      // second argument is the candidate type.
      castArgTypes.add(candidate.getArgTypes().get(0));
    }
    // Handle "else" argument.
    castArgTypes.add(candidate.getArgTypes().get(0));

    return castArgTypes;
  }

  /**
   * Convert the 'case <expr> when' to a 'case when' by setting each when
   * clause to a boolean argument. See class comment for more details.
   */
  private List<RexNode> convertCaseToWhenFormat(List<RexNode> inputs) throws HiveException {
    List<RexNode> whenFormatInputs = Lists.newArrayList();
    RexNode firstPred = inputs.get(0);
    for (int i = 0; i < (this.argTypes.size()-1) / 2; ++i) {
      int currentArg = 2 * i + 1;
      List<RexNode> equalsArgs = Lists.newArrayList(firstPred, inputs.get(currentArg));
      try {
        whenFormatInputs.add(createWhenExpression(equalsArgs));
      } catch (SemanticException e) {
        throw new HiveException("Could not convert 'case' statement to 'when' statement, " +
            "could not set equivalent args: " + equalsArgs.get(0) + ", " + equalsArgs.get(1));
      }
      currentArg += 1;
      whenFormatInputs.add(inputs.get(currentArg));
    }

    if (this.argTypes.size() % 2 == 0) {
      whenFormatInputs.add(inputs.get(inputs.size() - 1));
    }
    return whenFormatInputs;
  }

  private RexNode createWhenExpression(List<RexNode> equalsInputs) throws SemanticException {
    FunctionInfo functionInfo = helper.getFunctionInfo("=");
    RelDataType retType = helper.getReturnType(functionInfo, equalsInputs);
    List<RexNode> convertedInputs = helper.convertInputs(functionInfo, equalsInputs, retType);
    return helper.getExpression("=", functionInfo, convertedInputs, retType);
  }
}
