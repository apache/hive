/**
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

import java.util.Set;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import com.google.common.collect.Sets;

public class HiveDateGranularity extends SqlFunction {

  public static final SqlFunction YEAR = new HiveDateGranularity("YEAR");
  public static final SqlFunction QUARTER = new HiveDateGranularity("QUARTER");
  public static final SqlFunction MONTH = new HiveDateGranularity("MONTH");
  public static final SqlFunction WEEK = new HiveDateGranularity("WEEK");
  public static final SqlFunction DAY = new HiveDateGranularity("DAY");
  public static final SqlFunction HOUR = new HiveDateGranularity("HOUR");
  public static final SqlFunction MINUTE = new HiveDateGranularity("MINUTE");
  public static final SqlFunction SECOND = new HiveDateGranularity("SECOND");

  public static final Set<SqlFunction> ALL_FUNCTIONS =
          Sets.newHashSet(YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND);

  private HiveDateGranularity(String name) {
    super(
        name,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIME_NULLABLE,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE);
  }

}
