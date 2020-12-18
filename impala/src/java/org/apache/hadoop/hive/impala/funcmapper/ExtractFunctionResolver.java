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
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.RexNodeConverter;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;

import java.util.List;

/**
 * Extract function resolver.  This is used for date specific Calcite
 * signatures that need mapping into Impala. The Calcite signatures have
 * a signature of "EXTRACT(FLAG(YEAR):SYMBOL, DATE)" while the Impala
 * signatures are like "YEAR(DATE)". The "SYMBOL" Calcite parameter needs
 * to be stripped when comparing or mapping to Impala signatures.
 * The function coming into Calcite also has the form of "YEAR(DATE)", so
 * the appopriate translation into Calcite is needed.
 */
public class ExtractFunctionResolver extends ImpalaFunctionResolverImpl {

  ExtractFunctionResolver(FunctionHelper helper, SqlOperator op, List<RexNode> inputNodes
      ) throws HiveException {
    super(helper, op, inputNodes);
    if (argTypes.size() != 1) {
      throw new HiveException("Function " + func + " should have exactly 1 argument");
    }
  }

  @Override
  public List<RexNode> getConvertedInputs(ImpalaFunctionSignature candidate) {
    return RexNodeConverter.rewriteExtractDateChildren(op, inputNodes, rexBuilder);
  }

  @Override
  public List<RelDataType> getCastOperandTypes(
      ImpalaFunctionSignature castCandidate) {
    Preconditions.checkState(castCandidate.getArgTypes().size() == 1,
        "Num of arguments for " + this.func + " expected to be 1.");
    // first argument in candidate is "SYMBOL" and does not have to be cast, so we
    // grab it from this Calcite signature. The second argument needs to be
    // grabbed from the first argument of the cast candidate.
    return Lists.newArrayList(argTypes.get(0), castCandidate.getArgTypes().get(0));
  }
}
