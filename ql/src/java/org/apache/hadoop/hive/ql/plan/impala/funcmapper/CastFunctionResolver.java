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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;

import java.util.List;
import java.util.Map;

/**
 * A cast function resolver created from a RexCall.
 */
public class CastFunctionResolver extends ImpalaFunctionResolverImpl {

  private final SqlTypeName retSqlType;

  CastFunctionResolver(FunctionHelper helper, SqlOperator op, List<RexNode> inputNodes,
      RelDataType retType) {
    super(helper, op, inputNodes, retType);
    this.retSqlType = retType.getSqlTypeName();
  }

  CastFunctionResolver(FunctionHelper helper, SqlOperator op, List<RexNode> inputNodes,
      SqlTypeName retSqlType) {
    super(helper, op, inputNodes, null);
    this.retSqlType = retSqlType;
  }

  /**
   * Retrieve the function signature for the cast function. The reason to override
   * the default method is because when the default method doesn't find a matching
   * signature, it attempts to find a signature which can be used via casting. Here,
   * we never want to cast a cast function.
   */
  @Override
  public ImpalaFunctionSignature getFunction(Map<ImpalaFunctionSignature,
      ? extends FunctionDetails> functionDetailsMap) {
    return ImpalaFunctionSignature.fetch(functionDetailsMap, func, argTypes, retSqlType);
  }

  @Override
  public List<RexNode> getConvertedInputs(ImpalaFunctionSignature candidate) {
    return inputNodes;
  }

  @Override
  public RelDataType getRetType(ImpalaFunctionSignature funcSig, List<RexNode> operands) {
    if (retType != null) {
      return retType;
    }
    return rexBuilder.getTypeFactory().createSqlType(retSqlType);
  }

  @Override
  public RexNode createRexNode(ImpalaFunctionSignature candidate, List<RexNode> inputs,
      RelDataType retType) {
    return rexBuilder.makeCast(retType, inputs.get(0));
  }
}
