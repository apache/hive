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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;

import java.util.List;
import java.util.Map;

/**
 * Special override for arithmetic functions.
 */
public class ArithmeticFunctionResolver extends ImpalaFunctionResolverImpl {

  /**
   * The calculated return type based on the operator and operands.
   */
  private final RelDataType arithRetType;

  ArithmeticFunctionResolver(FunctionHelper helper, SqlOperator op, List<RexNode> inputNodes) {
    super(helper, op, inputNodes);
    List<RelDataType> relDataTypes =
        Lists.newArrayList(inputNodes.get(0).getType(), inputNodes.get(1).getType());
    // calculate return type up front.
    arithRetType =
        ImpalaArithmeticOperators.deriveReturnType(rexBuilder.getTypeFactory(), op, relDataTypes);
  }

  @Override
  public ImpalaFunctionSignature getFunction(
      Map<ImpalaFunctionSignature, ? extends FunctionDetails> functionDetailsMap)
      throws SemanticException {

    // First try to retrieve a signature that directly matches the input with no casting,
    // if possible. The "derived" return type is null in most cases, but can have a
    // value if it is important for the function to pick the right signature. This happens
    // with arithmetic operators.
    ImpalaFunctionSignature candidate =
        ImpalaFunctionSignature.fetch(functionDetailsMap, func, argTypes, arithRetType);
    if (candidate != null) {
      return candidate;
    }

    // No directly matching, so we try to retrieve a function to which we can cast.
    return getFunctionWithCasts();
  }

  /**
   * Return the precalculated return type.
   */
  @Override
  public RelDataType getRetType(ImpalaFunctionSignature funcSig, List<RexNode> nodes) {
    return arithRetType;
  }

  @Override
  protected List<ImpalaFunctionSignature> getCastCandidates(String func) throws SemanticException {
    List<ImpalaFunctionSignature> castCandidates = super.getCastCandidates(func);

    // Since the return type for this resolver has been resolved, only one candidate should match.
    for (ImpalaFunctionSignature ifs : castCandidates) {
      if (ifs.getRetType().getSqlTypeName() == arithRetType.getSqlTypeName()) {
	return ImmutableList.of(ifs);
      }
    }

    throw new SemanticException("Could not find function name " + func +
          " in resource file with type " + arithRetType);
  }
}
