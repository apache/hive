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
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;

import java.util.Arrays;

/**
 * Calcite SQL operator mapping to FROM_UNIXTIME Hive UDF.
 * <p>
 * The return type of the function is declared as {@code VARCHAR(100)} since it is highly unlikely that a user will 
 * request a timestamp format that requires more than 100 characters.
 * </p>
 */
public final class HiveFromUnixTimeSqlOperator {
  public static final SqlFunction INSTANCE = new SqlFunction("FROM_UNIXTIME",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.explicit(SqlTypeName.VARCHAR, 100).andThen(SqlTypeTransforms.TO_NULLABLE),
      null,
      OperandTypes.family(Arrays.asList(SqlTypeFamily.INTEGER, SqlTypeFamily.STRING), number -> number == 1),
      SqlFunctionCategory.STRING);

  private HiveFromUnixTimeSqlOperator() {
  }
}
