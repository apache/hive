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

import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveBetween;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExtractDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnixTimestampSqlOperator;
import org.apache.hadoop.hive.ql.plan.impala.operator.InIterateOperator;

import java.util.Collections;
import java.util.Map;

/**
 * Class containing all the Impala operators. Most are grabbed from the
 * standard Calcite operator table, but some of the operators are overridden.
 */
public class ImpalaOperatorTable {

  protected static final Map<String, SqlOperator> IMPALA_OPERATOR_MAP;

  static {
    SqlStdOperatorTable table = SqlStdOperatorTable.instance();
    Map<String, SqlOperator> map = Maps.newHashMap();
    for (SqlOperator op : table.getOperatorList()) {
      map.put(op.getName(), op);
    }

    // Need our own arithmetic operators since Impala derives the
    // return type.
    map.put("+", ImpalaArithmeticOperators.PLUS);
    map.put("-", ImpalaArithmeticOperators.MINUS);
    map.put("*", ImpalaArithmeticOperators.MULTIPLY);
    map.put("/", ImpalaArithmeticOperators.DIVIDE);

    map.put("BETWEEN", HiveBetween.INSTANCE);
    // Calcite uses "||" as the concat name, we just need to
    // make sure we understand "concat" as well.
    map.put("CONCAT", SqlStdOperatorTable.CONCAT);
    // Need to handle versions without the underscore in is_not_false, etc...
    map.put("ISNOTFALSE", SqlStdOperatorTable.IS_NOT_FALSE);
    map.put("ISNOTNULL", SqlStdOperatorTable.IS_NOT_NULL);
    map.put("ISNOTTRUE", SqlStdOperatorTable.IS_NOT_TRUE);
    map.put("ISFALSE", SqlStdOperatorTable.IS_FALSE);
    map.put("ISNULL", SqlStdOperatorTable.IS_NULL);
    map.put("ISTRUE", SqlStdOperatorTable.IS_TRUE);
    // Unary minus comes in as "negative"
    map.put("NEGATIVE", SqlStdOperatorTable.UNARY_MINUS);
    // substr works instead of substring
    map.put("STRUCT", SqlStdOperatorTable.ROW);
    map.put("SUBSTR", SqlStdOperatorTable.SUBSTRING);
    map.put("UNIX_TIMESTAMP", HiveUnixTimestampSqlOperator.INSTANCE);
    // there is a "when" version of case we need to handle.
    map.put("WHEN", SqlStdOperatorTable.CASE);

    // special extract operators
    map.put("YEAR", HiveExtractDate.YEAR);
    map.put("QUARTER", HiveExtractDate.QUARTER);
    map.put("MONTH", HiveExtractDate.MONTH);
    map.put("WEEK", HiveExtractDate.WEEK);
    map.put("DAY", HiveExtractDate.DAY);
    map.put("HOUR", HiveExtractDate.HOUR);
    map.put("MINUTE", HiveExtractDate.MINUTE);
    map.put("SECOND", HiveExtractDate.SECOND);

    // special cast operators
    map.put("BIGINT", SqlStdOperatorTable.CAST);
    map.put("BOOLEAN", SqlStdOperatorTable.CAST);
    map.put("CHAR", SqlStdOperatorTable.CAST);
    map.put("DATE", SqlStdOperatorTable.CAST);
    map.put("DECIMAL", SqlStdOperatorTable.CAST);
    map.put("FLOAT", SqlStdOperatorTable.CAST);
    map.put("INT", SqlStdOperatorTable.CAST);
    map.put("INTEGER", SqlStdOperatorTable.CAST);
    map.put("SMALLINT", SqlStdOperatorTable.CAST);
    map.put("STRING", SqlStdOperatorTable.CAST);
    map.put("VARCHAR", SqlStdOperatorTable.CAST);
    map.put("TIMESTAMP", SqlStdOperatorTable.CAST);
    map.put("TINYINT", SqlStdOperatorTable.CAST);

    map.put("IN_ITERATE", InIterateOperator.IN_ITERATE);

    IMPALA_OPERATOR_MAP = Collections.unmodifiableMap(map);
  }
}
