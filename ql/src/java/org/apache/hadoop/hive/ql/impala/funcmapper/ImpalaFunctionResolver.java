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

package org.apache.hadoop.hive.ql.impala.funcmapper;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.List;
import java.util.Map;

public interface ImpalaFunctionResolver {

  /**
   * Retrieves the Impala function associated with this function resolver. The
   * given map specifies whether to check the scalar functions or the agg functions.
   */
  ImpalaFunctionSignature getFunction(
      Map<ImpalaFunctionSignature, ? extends FunctionDetails> functionDetailsMap)
      throws SemanticException;

  /**
   * Retrieve the converted inputs for this function resolver given the function.
   * The most common conversion is casting of the operands from the original operand
   * to an operand that the function can handle.
   */
  List<RexNode> getConvertedInputs(ImpalaFunctionSignature function) throws HiveException;

  /**
   * Derive the return type from the given signature and operands.
   */
  RelDataType getRetType(ImpalaFunctionSignature function, List<RexNode> operands);

  /**
   * Create the RexNode. This should be the last call made to this interface as the
   * whole purpose of the interface is to produce a RexNode using Impala semnatics.
   */
  RexNode createRexNode(ImpalaFunctionSignature function, List<RexNode> inputs,
      RelDataType returnDataType);
}
