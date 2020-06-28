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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;

import java.util.List;
import java.util.Map;

/**
 * Function resolving for the Impala "struct"
 */
public class InlineFunctionResolver extends ImpalaFunctionResolverImpl {

  public InlineFunctionResolver(FunctionHelper helper, List<RexNode> inputNodes) {
    super(helper, "inline", inputNodes);
  }

  @Override
  public ImpalaFunctionSignature getFunction(
      Map<ImpalaFunctionSignature, ? extends FunctionDetails> functionDetailsMap)
      throws SemanticException {
    // there is no function signature associated with the struct function so just return null
    return null;
  }

  public List<RexNode> getConvertedInputs(ImpalaFunctionSignature function) throws HiveException {
    return inputNodes;
  }

  @Override
  public RelDataType getRetType(ImpalaFunctionSignature function, List<RexNode> inputs) {
    RexCall array = (RexCall) inputs.get(0);
    return array.getOperands().get(0).getType();
  }

  @Override
  public RexNode createRexNode(ImpalaFunctionSignature candidate, List<RexNode> inputs,
      RelDataType returnDataType) {
    throw new RuntimeException("InlineFunctionResolver.createRexNode not implemented.");
  }
}
