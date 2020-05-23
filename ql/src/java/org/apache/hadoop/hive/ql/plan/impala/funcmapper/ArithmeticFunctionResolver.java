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
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;

import java.util.List;

/**
 * Special override for arithmetic functions.
 */
public class ArithmeticFunctionResolver extends ImpalaFunctionResolverImpl {

  /**
   * The calculated return type based on the operator and operands.
   */
  private final RelDataType arithRetType;

  ArithmeticFunctionResolver(FunctionHelper helper, SqlOperator op, List<RexNode> inputNodes) {
    super(helper, op, inputNodes, null);
    List<RelDataType> relDataTypes =
        Lists.newArrayList(inputNodes.get(0).getType(), inputNodes.get(1).getType());
    // calculate return type up front.
    arithRetType =
        ImpalaArithmeticOperators.deriveReturnType(rexBuilder.getTypeFactory(), op, relDataTypes);
  }

  /**
   * Return the precalculated return type.
   */
  @Override
  protected RelDataType getReturnType() {
    return arithRetType;
  }

  /**
   * Return the precalculated return type.
   */
  @Override
  public RelDataType getRetType(ImpalaFunctionSignature funcSig, List<RexNode> nodes) {
    return arithRetType;
  }
}
