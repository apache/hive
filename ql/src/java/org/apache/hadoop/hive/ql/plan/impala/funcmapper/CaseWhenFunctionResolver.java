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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;
import org.apache.impala.catalog.Type;

import java.util.List;
import java.util.Map;

/**
 * Case function resolver.  This specific case function resolver resolves case statements
 * of the form:
 * CASE WHEN expr THEN expr [WHEN whenexpr THEN rettype ...] [ELSE elseexpr] END
 *
 * In Calcite, the "WHEN" expression is always of type <boolean> and the THEN expression
 * is always the same type as the return type.
 *
 * The ELSE condition in Calcite will also always be the same type as the return type.
 *
 * Impala simplified this signature for the function to be case(<rettype>) since all other
 * arguments are optional or derived.
 *
 * The purpose of this class is to help map the Calcite signature at runtime to the Impala
 * signature.
 */
public class CaseWhenFunctionResolver extends ImpalaFunctionResolverImpl {

  CaseWhenFunctionResolver(FunctionHelper helper, SqlOperator op, List<RexNode> inputNodes, RelDataType returnType) {
    super(helper, op, inputNodes, returnType);
  }

  @Override
  public ImpalaFunctionSignature getFunction(
      Map<ImpalaFunctionSignature, ? extends FunctionDetails> functionDetailsMap)
      throws SemanticException {
    // First try to retrieve a signature that directly matches the input with no casting,
    // if possible.
    // If there is a matching signature, the return type will be the first even parameter.
    RelDataType retType = inputNodes.get(1).getType();
    ImpalaFunctionSignature candidate =
        ImpalaFunctionSignature.fetch(functionDetailsMap, func, argTypes, retType);
    if (candidate != null) {
      return candidate;
    }

    // No directly matching, so we try to retrieve a function to which we can cast.
    return getCastFunction();
  }


  @Override
  public List<RexNode> getConvertedInputs(ImpalaFunctionSignature candidate) throws HiveException {
    List<RelDataType> castTypes = getCastOperandTypes(candidate);
    if (castTypes == null) {
      throw new HiveException("getCastOperandTypes() for case statement failed.");
    }
    List<RexNode> inputNodesCopy = Lists.newArrayList(inputNodes);
    // if there are less input nodes than cast types, then we have an implicit "else null" that
    // we need to create.
    if (inputNodes.size() < castTypes.size()) {
      inputNodesCopy.add(rexBuilder.makeNullLiteral(castTypes.get(castTypes.size() - 1)));
    }
    return castInputs(inputNodesCopy, castTypes);
  }

  @Override
  protected boolean canCast(ImpalaFunctionSignature candidate) {
    RelDataType castTo = candidate.getArgTypes().get(0);
    // For loop constructs this signature's arguments in pairs.
    for (int i = 0; i < this.argTypes.size() / 2; ++i) {
      int currentArg = 2 * i;
      // First argument of pair should always be a boolean
      Preconditions.checkState(
          this.argTypes.get(currentArg).getSqlTypeName().equals(SqlTypeName.BOOLEAN));
      currentArg += 1;
      // Check to see if the second argument can be cast.
      RelDataType castFrom = this.argTypes.get(currentArg);
      if (!ImpalaFunctionSignature.canCastUp(castFrom, castTo)) {
        return false;
      }
    }
    // If there is an "else" parameter, we see if that parameter can be cast.
    if (this.argTypes.size() % 2 == 1) {
      RelDataType castFrom = this.argTypes.get(this.argTypes.size() - 1);
      return ImpalaFunctionSignature.canCastUp(castFrom, castTo);
    }
    return true;
  }

  @Override
  public List<RelDataType> getCastOperandTypes(ImpalaFunctionSignature candidate) {
    List<RelDataType> castArgTypes = Lists.newArrayList();

    // For loop constructs this signature's arguments in pairs.
    for (int i = 0; i < this.argTypes.size() / 2; ++i) {
      // first argument in pair is always a boolean
      castArgTypes.add(ImpalaTypeConverter.getRelDataType(Type.BOOLEAN));
      // second argument is the candidate type.
      castArgTypes.add(candidate.getArgTypes().get(0));
    }
    // Handle "else" argument.
    castArgTypes.add(candidate.getArgTypes().get(0));

    return castArgTypes;
  }
}
