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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.impala.operator.InIterateOperator;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;

import java.util.List;

/**
 * InFunctionResolver is the special function resolver for IN methods.  There are two
 * special attributes to the "IN" function.
 * 1) If any of the parameters to the in function has an input ref, then we need to use
 * the "iterate" algorithm (cannot use a lookup table because every row can have different
 * values to check)
 * 2) If the decimal version of the IN function is used, all decimal parameters needs to
 * have the same precision/scale.
 */
public class InFunctionResolver extends ImpalaFunctionResolverImpl {

  public InFunctionResolver(FunctionHelper helper, SqlOperator defaultOp,
      List<RexNode> inputNodes) {
    super(helper, getInOperator(defaultOp, inputNodes), inputNodes);
  }

  public static SqlOperator getInOperator(SqlOperator defaultOp, List<RexNode> inputNodes) {
    // if any param other than the first (which is outside of the 'in'), has an
    // inputref, Impala needs the in_iterate algorithm.
    for (int i = 1; i < inputNodes.size(); ++i) {
      if (RexUtil.containsInputRef(inputNodes.get(i))) {
        return InIterateOperator.IN_ITERATE;
      }
    }
    return defaultOp;
  }

  @Override
  protected boolean needsCommonDecimalType(List<RelDataType> castTypes) {
    Preconditions.checkArgument(castTypes.size() > 1);
    return castTypes.get(1).getSqlTypeName() == SqlTypeName.DECIMAL;
  }
}
