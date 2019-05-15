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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

/**
 * Aggregation function to represent: stddev_pop, stddev_samp, var_pop, var_samp.
 */
public class HiveSqlVarianceAggFunction extends SqlAggFunction {

  public HiveSqlVarianceAggFunction(String name, SqlKind kind, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker) {
    super(name, null, kind, returnTypeInference, operandTypeInference,
        operandTypeChecker, SqlFunctionCategory.NUMERIC, false, false);
    assert kind == SqlKind.STDDEV_POP || kind == SqlKind.STDDEV_SAMP ||
        kind == SqlKind.VAR_POP || kind == SqlKind.VAR_SAMP;
  }

}
