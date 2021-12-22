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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
  public static final Map<String, Integer> NumericCastOrder;

  static {
    Map<String, Integer> map = new HashMap<>();
    map.put(TINYINT_TYPE_NAME, 1);
    map.put(SMALLINT_TYPE_NAME, 2);
    map.put(INT_TYPE_NAME, 3);
    map.put(BIGINT_TYPE_NAME, 4);
    map.put(DECIMAL_TYPE_NAME, 5);
    map.put(FLOAT_TYPE_NAME, 6);
    map.put(DOUBLE_TYPE_NAME, 7);
    NumericCastOrder = Collections.unmodifiableMap(map);
  }

  private static final Set<String> decoratedTypeNames = new HashSet<>();

  static {
    decoratedTypeNames.add("char");
    decoratedTypeNames.add("decimal");
    decoratedTypeNames.add("varchar");
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
    if (decoratedTypeNames.contains(protoType)) {
      return protoType;
    }
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

  // These aren't column types, they are info for how things are stored in thrift.
  // It didn't seem useful to create another Constants class just for these though.
  public static final String SERIALIZATION_FORMAT = "serialization.format";

  public static final String SERIALIZATION_LIB = "serialization.lib";

  public static final String SERIALIZATION_DDL = "serialization.ddl";

  public static final char COLUMN_COMMENTS_DELIMITER = '\0';

  private static HashMap<String, String> typeToThriftTypeMap;
  static {
    typeToThriftTypeMap = new HashMap<>();
    typeToThriftTypeMap.put(BOOLEAN_TYPE_NAME, "bool");
    typeToThriftTypeMap.put(TINYINT_TYPE_NAME, "byte");
    typeToThriftTypeMap.put(SMALLINT_TYPE_NAME, "i16");
    typeToThriftTypeMap.put(INT_TYPE_NAME, "i32");
    typeToThriftTypeMap.put(BIGINT_TYPE_NAME, "i64");
    typeToThriftTypeMap.put(DOUBLE_TYPE_NAME, "double");
    typeToThriftTypeMap.put(FLOAT_TYPE_NAME, "float");
    typeToThriftTypeMap.put(LIST_TYPE_NAME, "list");
    typeToThriftTypeMap.put(MAP_TYPE_NAME, "map");
    typeToThriftTypeMap.put(STRING_TYPE_NAME, "string");
    typeToThriftTypeMap.put(BINARY_TYPE_NAME, "binary");
    // These 4 types are not supported yet.
    // We should define a complex type date in thrift that contains a single int
    // member, and DynamicSerDe
    // should convert it to date type at runtime. (note: DynamicSerDe has been removed)
    typeToThriftTypeMap.put(DATE_TYPE_NAME, "date");
    typeToThriftTypeMap.put(DATETIME_TYPE_NAME, "datetime");
    typeToThriftTypeMap.put(TIMESTAMP_TYPE_NAME, "timestamp");
    typeToThriftTypeMap.put(DECIMAL_TYPE_NAME, "decimal");
    typeToThriftTypeMap.put(INTERVAL_YEAR_MONTH_TYPE_NAME, INTERVAL_YEAR_MONTH_TYPE_NAME);
    typeToThriftTypeMap.put(INTERVAL_DAY_TIME_TYPE_NAME, INTERVAL_DAY_TIME_TYPE_NAME);
  }

  /**
   * Convert type to ThriftType. We do that by tokenizing the type and convert
   * each token.
   */
  public static String typeToThriftType(String type) {
    StringBuilder thriftType = new StringBuilder();
    int last = 0;
    boolean lastAlphaDigit = Character.isLetterOrDigit(type.charAt(last));
    for (int i = 1; i <= type.length(); i++) {
      if (i == type.length()
          || Character.isLetterOrDigit(type.charAt(i)) != lastAlphaDigit) {
        String token = type.substring(last, i);
        last = i;
        String thriftToken = typeToThriftTypeMap.get(token);
        thriftType.append(thriftToken == null ? token : thriftToken);
        lastAlphaDigit = !lastAlphaDigit;
      }
    }
    return thriftType.toString();
  }

  public static String getListType(String t) {
    return "array<" + t + ">";
  }
}
