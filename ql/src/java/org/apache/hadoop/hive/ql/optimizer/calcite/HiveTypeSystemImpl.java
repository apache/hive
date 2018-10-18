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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;

public class HiveTypeSystemImpl extends RelDataTypeSystemImpl {
  // TODO: This should come from type system; Currently there is no definition
  // in type system for this.
  private static final int MAX_DECIMAL_PRECISION     = 38;
  private static final int MAX_DECIMAL_SCALE         = 38;
  private static final int DEFAULT_DECIMAL_PRECISION = 10;
  // STRING type in Hive is represented as VARCHAR with precision Integer.MAX_VALUE.
  // In turn, the max VARCHAR precision should be 65535. However, the value is not
  // used for validation, but rather only internally by the optimizer to know the max
  // precision supported by the system. Thus, no VARCHAR precision should fall between
  // 65535 and Integer.MAX_VALUE; the check for VARCHAR precision is done in Hive.
  private static final int MAX_CHAR_PRECISION        = Integer.MAX_VALUE;
  private static final int DEFAULT_VARCHAR_PRECISION = 65535;
  private static final int DEFAULT_CHAR_PRECISION    = 255;
  private static final int MAX_BINARY_PRECISION      = Integer.MAX_VALUE;
  private static final int MAX_TIMESTAMP_PRECISION   = 9;
  private static final int MAX_TIMESTAMP_WITH_LOCAL_TIME_ZONE_PRECISION = 15; // Up to nanos

  @Override
  public int getMaxScale(SqlTypeName typeName) {
    switch (typeName) {
    case DECIMAL:
      return getMaxNumericScale();
    case INTERVAL_YEAR:
    case INTERVAL_MONTH:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return SqlTypeName.MAX_INTERVAL_FRACTIONAL_SECOND_PRECISION;
    default:
      return -1;
    }
  }

  @Override
  public int getDefaultPrecision(SqlTypeName typeName) {
    switch (typeName) {
    // Hive will always require user to specify exact sizes for char, varchar;
    // Binary doesn't need any sizes; Decimal has the default of 10.
    case BINARY:
    case VARBINARY:
    case TIME:
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return getMaxPrecision(typeName);
    case CHAR:
      return DEFAULT_CHAR_PRECISION;
    case VARCHAR:
      return DEFAULT_VARCHAR_PRECISION;
    case DECIMAL:
      return DEFAULT_DECIMAL_PRECISION;
    case INTERVAL_YEAR:
    case INTERVAL_MONTH:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return SqlTypeName.DEFAULT_INTERVAL_START_PRECISION;
    default:
      return -1;
    }
  }

  @Override
  public int getMaxPrecision(SqlTypeName typeName) {
    switch (typeName) {
    case DECIMAL:
      return getMaxNumericPrecision();
    case VARCHAR:
    case CHAR:
      return MAX_CHAR_PRECISION;
    case VARBINARY:
    case BINARY:
      return MAX_BINARY_PRECISION;
    case TIME:
    case TIMESTAMP:
      return MAX_TIMESTAMP_PRECISION;
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      return MAX_TIMESTAMP_WITH_LOCAL_TIME_ZONE_PRECISION;
    case INTERVAL_YEAR:
    case INTERVAL_MONTH:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return SqlTypeName.MAX_INTERVAL_START_PRECISION;
    default:
      return -1;
    }
  }

  @Override
  public int getMaxNumericScale() {
    return MAX_DECIMAL_SCALE;
  }

  @Override
  public int getMaxNumericPrecision() {
    return MAX_DECIMAL_PRECISION;
  }
}
