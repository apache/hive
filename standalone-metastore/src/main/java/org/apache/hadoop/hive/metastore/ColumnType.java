/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.utils.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Constants and utility functions for column types.  This is explicitly done as constants in the
 * class rather than an enum in order to interoperate with Hive's old serdeConstants.  All type
 * names in this class match the type names in Hive's serdeConstants class.  They must continue
 * to do so.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ColumnType {
  public static final String VOID_TYPE_NAME = "void";

  public static final String BOOLEAN_TYPE_NAME = "boolean";

  public static final String TINYINT_TYPE_NAME = "tinyint";

  public static final String SMALLINT_TYPE_NAME = "smallint";

  public static final String INT_TYPE_NAME = "int";

  public static final String BIGINT_TYPE_NAME = "bigint";

  public static final String FLOAT_TYPE_NAME = "float";

  public static final String DOUBLE_TYPE_NAME = "double";

  public static final String STRING_TYPE_NAME = "string";

  public static final String CHAR_TYPE_NAME = "char";

  public static final String VARCHAR_TYPE_NAME = "varchar";

  public static final String DATE_TYPE_NAME = "date";

  public static final String DATETIME_TYPE_NAME = "datetime";

  public static final String TIMESTAMP_TYPE_NAME = "timestamp";

  public static final String DECIMAL_TYPE_NAME = "decimal";

  public static final String BINARY_TYPE_NAME = "binary";

  public static final String INTERVAL_YEAR_MONTH_TYPE_NAME = "interval_year_month";

  public static final String INTERVAL_DAY_TIME_TYPE_NAME = "interval_day_time";

  public static final String TIMESTAMPTZ_TYPE_NAME = "timestamp with time zone";

  public static final String LIST_TYPE_NAME = "array";

  public static final String MAP_TYPE_NAME = "map";

  public static final String STRUCT_TYPE_NAME = "struct";

  public static final String UNION_TYPE_NAME = "uniontype";

  public static final String LIST_COLUMNS = "columns";

  public static final String LIST_COLUMN_TYPES = "columns.types";

  public static final String COLUMN_NAME_DELIMITER = "column.name.delimiter";

  public static final String SERIALIZATION_FORMAT = "serialization.format";

  public static final Set<String> PrimitiveTypes = StringUtils.asSet(
    VOID_TYPE_NAME,
    BOOLEAN_TYPE_NAME,
    TINYINT_TYPE_NAME,
    SMALLINT_TYPE_NAME,
    INT_TYPE_NAME,
    BIGINT_TYPE_NAME,
    FLOAT_TYPE_NAME,
    DOUBLE_TYPE_NAME,
    STRING_TYPE_NAME,
    VARCHAR_TYPE_NAME,
    CHAR_TYPE_NAME,
    DATE_TYPE_NAME,
    DATETIME_TYPE_NAME,
    TIMESTAMP_TYPE_NAME,
    INTERVAL_YEAR_MONTH_TYPE_NAME,
    INTERVAL_DAY_TIME_TYPE_NAME,
    DECIMAL_TYPE_NAME,
    BINARY_TYPE_NAME,
    TIMESTAMPTZ_TYPE_NAME);

  public static final Set<String> StringTypes = StringUtils.asSet(
      STRING_TYPE_NAME,
      VARCHAR_TYPE_NAME,
      CHAR_TYPE_NAME
  );

  public static final Set<String> NumericTypes = StringUtils.asSet(
      TINYINT_TYPE_NAME,
      SMALLINT_TYPE_NAME,
      INT_TYPE_NAME,
      BIGINT_TYPE_NAME,
      FLOAT_TYPE_NAME,
      DOUBLE_TYPE_NAME,
      DECIMAL_TYPE_NAME
  );

  // This intentionally does not include interval types.
  public static final Set<String> DateTimeTypes = StringUtils.asSet(
      DATE_TYPE_NAME,
      DATETIME_TYPE_NAME,
      TIMESTAMP_TYPE_NAME,
      TIMESTAMPTZ_TYPE_NAME
  );

  // This map defines the progression of up casts in numeric types.
  public static final Map<String, Integer> NumericCastOrder = new HashMap<>();

  static {
    NumericCastOrder.put(TINYINT_TYPE_NAME, 1);
    NumericCastOrder.put(SMALLINT_TYPE_NAME, 2);
    NumericCastOrder.put(INT_TYPE_NAME, 3);
    NumericCastOrder.put(BIGINT_TYPE_NAME, 4);
    NumericCastOrder.put(DECIMAL_TYPE_NAME, 5);
    NumericCastOrder.put(FLOAT_TYPE_NAME, 6);
    NumericCastOrder.put(DOUBLE_TYPE_NAME, 7);
  }

  private static final Map<String, String> alternateTypeNames = new HashMap<>();

  static {
    alternateTypeNames.put("integer", INT_TYPE_NAME);
    alternateTypeNames.put("numeric", DECIMAL_TYPE_NAME);
  }

  public static final Set<String> CollectionTypes = StringUtils.asSet(
    LIST_TYPE_NAME,
    MAP_TYPE_NAME);

  public static final Set<String> IntegralTypes = StringUtils.asSet(
    TINYINT_TYPE_NAME,
    SMALLINT_TYPE_NAME,
    INT_TYPE_NAME,
    BIGINT_TYPE_NAME);

  public static final Set<String> AllTypes = StringUtils.asSet(
    VOID_TYPE_NAME,
    BOOLEAN_TYPE_NAME,
    TINYINT_TYPE_NAME,
    SMALLINT_TYPE_NAME,
    INT_TYPE_NAME,
    BIGINT_TYPE_NAME,
    FLOAT_TYPE_NAME,
    DOUBLE_TYPE_NAME,
    STRING_TYPE_NAME,
    CHAR_TYPE_NAME,
    VARCHAR_TYPE_NAME,
    DATE_TYPE_NAME,
    DATETIME_TYPE_NAME,
    TIMESTAMP_TYPE_NAME,
    DECIMAL_TYPE_NAME,
    BINARY_TYPE_NAME,
    INTERVAL_YEAR_MONTH_TYPE_NAME,
    INTERVAL_DAY_TIME_TYPE_NAME,
    TIMESTAMPTZ_TYPE_NAME,
    LIST_TYPE_NAME,
    MAP_TYPE_NAME,
    STRUCT_TYPE_NAME,
    UNION_TYPE_NAME,
    LIST_COLUMNS,
    LIST_COLUMN_TYPES,
    COLUMN_NAME_DELIMITER
  );

  /**
   * Given a type string return the type name.  For example, passing in the type string
   * <tt>varchar(256)</tt> will return <tt>varchar</tt>.
   * @param typeString Type string
   * @return type name, guaranteed to be in lower case
   */
  public static String getTypeName(String typeString) {
    if (typeString == null) return null;
    String protoType = typeString.toLowerCase().split("\\W")[0];
    String realType = alternateTypeNames.get(protoType);
    return realType == null ? protoType : realType;
  }

  public static boolean areColTypesCompatible(String from, String to) {
    if (from.equals(to)) return true;

    if (PrimitiveTypes.contains(from) && PrimitiveTypes.contains(to)) {
      // They aren't the same, but we may be able to do a cast

      // If they are both types of strings, that should be fine
      if (StringTypes.contains(from) && StringTypes.contains(to)) return true;

      // If both are numeric, make sure the new type is larger than the old.
      if (NumericTypes.contains(from) && NumericTypes.contains(to)) {
        return NumericCastOrder.get(from) < NumericCastOrder.get(to);
      }

      // Allow string to double conversion
      if (StringTypes.contains(from) && to.equals(DOUBLE_TYPE_NAME)) return true;

      // Void can go to anything
      if (from.equals(VOID_TYPE_NAME)) return true;

      // Allow date to string casts.  NOTE: I suspect this is the reverse of what we actually
      // want, but it matches the code in o.a.h.h.serde2.typeinfo.TypeInfoUtils.  I can't see how
      // users would be altering date columns into string columns.  The other I easily see since
      // Hive did not originally support datetime types.  Also, the comment in the Hive code
      // says string to date, even though the code does the opposite.  But for now I'm keeping
      // this as is so the functionality matches.
      if (DateTimeTypes.contains(from) && StringTypes.contains(to)) return true;

      // Allow numeric to string
      if (NumericTypes.contains(from) && StringTypes.contains(to)) return true;

    }
    return false;
  }
}
