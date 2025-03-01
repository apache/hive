/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.functions;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.Optionality;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

public class HiveSqlAggFunction extends SqlAggFunction {

  protected HiveSqlAggFunction(
      String name,
      SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker,
      SqlFunctionCategory funcType) {
    this(
        name,
        null,
        kind,
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker,
        funcType,
        false,
        false,
        Optionality.FORBIDDEN);
  }

  protected HiveSqlAggFunction(
      String name,
      @Nullable SqlIdentifier sqlIdentifier,
      SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker,
      SqlFunctionCategory funcType,
      boolean requiresOrder,
      boolean requiresOver,
      Optionality requiresGroupOrder) {
    super(name, sqlIdentifier, kind, returnTypeInference, operandTypeInference, operandTypeChecker, funcType,
        requiresOrder, requiresOver, requiresGroupOrder);
  }

  @Override
  public @Nullable SqlAggFunction getRollup() {
    return HiveRelBuilder.getRollup(this);
  }
}
