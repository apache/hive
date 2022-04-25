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

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

/**
 * Mergeable aggregate.
 *
 * A mergeable aggregate is:
 * - accepts the same kind as inputs as the output (an X^n -&gt; X function)
 *
 * Example: the SUM function is a great example; since SUM of SUM -s is the overall sum.
 */
public class HiveMergeableAggregate extends SqlAggFunction  {

  private SqlAggFunction mergeAgg;

  public HiveMergeableAggregate(String string, SqlKind kind, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    this(string, kind, returnTypeInference, operandTypeInference, operandTypeChecker, null);
  }

  public HiveMergeableAggregate(String string, SqlKind kind, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker,
      SqlAggFunction unionFn) {
    super(
        string, kind,
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker,
        SqlFunctionCategory.NUMERIC);
    if (unionFn == null) {
      this.mergeAgg = this;
    } else {
      this.mergeAgg = unionFn;
    }

  }

  public SqlAggFunction getMergeAggFunction() {
    return mergeAgg;
  }

}
