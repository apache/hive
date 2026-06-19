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
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

public class HiveSqlFunction extends SqlFunction {
  private final boolean deterministic;
  private final boolean runtimeConstant;

  public HiveSqlFunction(String name, SqlKind kind, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker,
      SqlFunctionCategory category, boolean deterministic, boolean runtimeConstant) {
    super(name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, category);
    this.deterministic = deterministic;
    this.runtimeConstant = runtimeConstant;
  }

  @Override
  public boolean isDeterministic() {
    return deterministic;
  }

  /**
   * Whether it is safe to cache or materialize plans containing this operator.
   * We do not rely on {@link SqlFunction#isDynamicFunction()} because it has
   * different implications, e.g., a dynamic function will not be reduced in
   * Calcite since plans may be cached in the context of prepared statements.
   * In our case, we check whether a plan contains runtime constants before
   * constant folding happens, hence we can let Calcite reduce these functions.
   *
   * @return true iff it is unsafe to cache or materialized query plans
   * referencing this operator
   */
  public boolean isRuntimeConstant() {
    return runtimeConstant;
  }
}
