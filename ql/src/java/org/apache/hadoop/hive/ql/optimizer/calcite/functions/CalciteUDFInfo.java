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
package org.apache.hadoop.hive.ql.optimizer.calcite.functions;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.Util;

/**
 * Holds information about UDF needed to create a Calcite function.
 */
public class CalciteUDFInfo {
  public final String udfName;
  public final SqlReturnTypeInference returnTypeInference;
  public final SqlOperandTypeInference operandTypeInference;
  public final SqlOperandTypeChecker operandTypeChecker;

  private CalciteUDFInfo(String udfName, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker) {
    this.udfName = udfName;
    this.returnTypeInference = returnTypeInference;
    this.operandTypeInference = operandTypeInference;
    this.operandTypeChecker = operandTypeChecker;
  }

  public static CalciteUDFInfo createUDFInfo(String udfName,
      List<RelDataType> calciteArgTypes, RelDataType calciteRetType) {
    ImmutableList.Builder<SqlTypeFamily> typeFamilyBuilder = new ImmutableList.Builder<SqlTypeFamily>();
    for (RelDataType at : calciteArgTypes) {
      typeFamilyBuilder.add(Util.first(at.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
    }
    return new CalciteUDFInfo(udfName, ReturnTypes.explicit(calciteRetType),
        InferTypes.explicit(calciteArgTypes), OperandTypes.family(typeFamilyBuilder.build()));
  }
}
