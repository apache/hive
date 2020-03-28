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

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

public class ImpalaFunctionMapper {

  private final ImpalaFunctionSignature funcSig;

  public ImpalaFunctionMapper(ImpalaFunctionSignature funcSig) {
    this.funcSig = funcSig;
  }

  /*
   * For "this" signature, check if there exists a function signature which is compatible.
   * Returns a list of the operand types.
   */
  public List<SqlTypeName> mapOperands(
      Map<ImpalaFunctionSignature, ? extends FunctionDetails> functionDetailsMap) {
    // if there is an exact match of name, args, ans return type existing in the builtins, we can
    // use the rexCall as/is.
    if (this.funcSig.useSignatureTypes(functionDetailsMap)) {
      return this.funcSig.getArgTypes();
    }

    // castCandidates contains a list of potential functions that can match "this" signature.
    // These candidates will have the same function name, but different operand/return types.
    List<ImpalaFunctionSignature> castCandidates =
        ImpalaFunctionSignature.CAST_CHECK_BUILTINS_INSTANCE.get(this.funcSig.getFunc());
    if (castCandidates == null) {
      throw new RuntimeException("Could not find function name " +
          this.funcSig.getFunc()+ " in resource file");
    }

    // iterate through list of potential functions which we could cast.  These functions should be
    // in a pre-determined optimal order.
    // For instance:  we have sum(BIGINT), sum(DOUBLE), and sum(DECIMAL) in that order.  If the arg
    // is a TINYINT, we should return a RexNode of "sum(CAST arg AS BIGINT)"
    for (ImpalaFunctionSignature castCandidate : castCandidates) {
      if (this.funcSig.canCastToCandidate(castCandidate)) {
        return this.funcSig.getCastOperandTypes(castCandidate);
      }
    }
    throw new RuntimeException("Could not cast for function name " + this.funcSig.getFunc());
  }
}
