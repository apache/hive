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

package org.apache.hive.jdbc;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.thrift.Type;


/**
 * Column metadata.
 */
public class JdbcColumn {
  private final String columnName;
  private final String tableName;
  private final String tableCatalog;
  private final String type;
  private final String comment;
  private final int ordinalPos;

  JdbcColumn(String columnName, String tableName, String tableCatalog
          , String type, String comment, int ordinalPos) {
    this.columnName = columnName;
    this.tableName = tableName;
    this.tableCatalog = tableCatalog;
    this.type = type;
    this.comment = comment;
    this.ordinalPos = ordinalPos;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTableCatalog() {
    return tableCatalog;
  }

  public String getType() {
    return type;
  }

  static String columnClassName(Type hiveType, JdbcColumnAttributes columnAttributes)
      throws SQLException {
    int columnType = hiveTypeToSqlType(hiveType);
    switch(columnType) {
      case Types.NULL:
        return "null";
      case Types.BOOLEAN:
        return Boolean.class.getName();
      case Types.CHAR:
      case Types.VARCHAR:
        return String.class.getName();
      case Types.TINYINT:
        return Byte.class.getName();
      case Types.SMALLINT:
        return Short.class.getName();
      case Types.INTEGER:
        return Integer.class.getName();
      case Types.BIGINT:
        return Long.class.getName();
      case Types.DATE:
        return Date.class.getName();
      case Types.FLOAT:
        return Float.class.getName();
      case Types.DOUBLE:
        return Double.class.getName();
      case  Types.TIMESTAMP:
        return Timestamp.class.getName();
      case Types.TIMESTAMP_WITH_TIMEZONE:
        return TimestampTZ.class.getName();
      case Types.DECIMAL:
        return BigInteger.class.getName();
      case Types.BINARY:
        return byte[].class.getName();
      case Types.OTHER:
      case Types.JAVA_OBJECT: {
        switch (hiveType) {
          case INTERVAL_YEAR_MONTH_TYPE:
            return HiveIntervalYearMonth.class.getName();
          case INTERVAL_DAY_TIME_TYPE:
            return HiveIntervalDayTime.class.getName();
          default:
            return String.class.getName();
        }
      }
      case Types.ARRAY:
      case Types.STRUCT:
        return String.class.getName();
      default:
        throw new SQLException("Invalid column type: " + columnType);
    }
  }

  static Type typeStringToHiveType(String type) throws SQLException {
    if ("string".equalsIgnoreCase(type)) {
      return Type.STRING_TYPE;
    } else if ("varchar".equalsIgnoreCase(type)) {
      return Type.VARCHAR_TYPE;
    } else if ("char".equalsIgnoreCase(type)) {
      return Type.CHAR_TYPE;
    } else if ("float".equalsIgnoreCase(type)) {
      return Type.FLOAT_TYPE;
    } else if ("double".equalsIgnoreCase(type)) {
      return Type.DOUBLE_TYPE;
    } else if ("boolean".equalsIgnoreCase(type)) {
      return Type.BOOLEAN_TYPE;
    } else if ("tinyint".equalsIgnoreCase(type)) {
      return Type.TINYINT_TYPE;
    } else if ("smallint".equalsIgnoreCase(type)) {
      return Type.SMALLINT_TYPE;
    } else if ("int".equalsIgnoreCase(type)) {
      return Type.INT_TYPE;
    } else if ("bigint".equalsIgnoreCase(type)) {
      return Type.BIGINT_TYPE;
    } else if ("date".equalsIgnoreCase(type)) {
      return Type.DATE_TYPE;
    } else if ("timestamp".equalsIgnoreCase(type)) {
      return Type.TIMESTAMP_TYPE;
    } else if (serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME.equalsIgnoreCase(type)) {
      return Type.TIMESTAMPLOCALTZ_TYPE;
    } else if ("interval_year_month".equalsIgnoreCase(type)) {
      return Type.INTERVAL_YEAR_MONTH_TYPE;
    } else if ("interval_day_time".equalsIgnoreCase(type)) {
      return Type.INTERVAL_DAY_TIME_TYPE;
    } else if ("decimal".equalsIgnoreCase(type)) {
      return Type.DECIMAL_TYPE;
    } else if ("binary".equalsIgnoreCase(type)) {
      return Type.BINARY_TYPE;
    } else if ("map".equalsIgnoreCase(type)) {
      return Type.MAP_TYPE;
    } else if ("array".equalsIgnoreCase(type)) {
      return Type.ARRAY_TYPE;
    } else if ("struct".equalsIgnoreCase(type)) {
      return Type.STRUCT_TYPE;
    } else if ("uniontype".equalsIgnoreCase(type)) {
      return Type.UNION_TYPE;
    } else if ("void".equalsIgnoreCase(type) || "null".equalsIgnoreCase(type)) {
      return Type.NULL_TYPE;
    }
    throw new SQLException("Unrecognized column type: " + type);
  }

  public static int hiveTypeToSqlType(Type hiveType) throws SQLException {
    return hiveType.toJavaSQLType();
  }

  public static int hiveTypeToSqlType(String type) throws SQLException {
    return hiveTypeToSqlType(typeStringToHiveType(type));
  }

  static String getColumnTypeName(String type) throws SQLException {
    // we need to convert the Hive type to the SQL type name
    // TODO: this would be better handled in an enum
    if ("string".equalsIgnoreCase(type)) {
      return serdeConstants.STRING_TYPE_NAME;
    } else if ("varchar".equalsIgnoreCase(type)) {
      return serdeConstants.VARCHAR_TYPE_NAME;
    } else if ("char".equalsIgnoreCase(type)) {
      return serdeConstants.CHAR_TYPE_NAME;
    } else if ("float".equalsIgnoreCase(type)) {
      return serdeConstants.FLOAT_TYPE_NAME;
    } else if ("double".equalsIgnoreCase(type)) {
      return serdeConstants.DOUBLE_TYPE_NAME;
    } else if ("boolean".equalsIgnoreCase(type)) {
      return serdeConstants.BOOLEAN_TYPE_NAME;
    } else if ("tinyint".equalsIgnoreCase(type)) {
      return serdeConstants.TINYINT_TYPE_NAME;
    } else if ("smallint".equalsIgnoreCase(type)) {
      return serdeConstants.SMALLINT_TYPE_NAME;
    } else if ("int".equalsIgnoreCase(type)) {
      return serdeConstants.INT_TYPE_NAME;
    } else if ("bigint".equalsIgnoreCase(type)) {
      return serdeConstants.BIGINT_TYPE_NAME;
    } else if ("timestamp".equalsIgnoreCase(type)) {
      return serdeConstants.TIMESTAMP_TYPE_NAME;
    } else if (serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME.equalsIgnoreCase(type)) {
      return serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME;
    } else if ("date".equalsIgnoreCase(type)) {
      return serdeConstants.DATE_TYPE_NAME;
    } else if ("interval_year_month".equalsIgnoreCase(type)) {
      return serdeConstants.INTERVAL_YEAR_MONTH_TYPE_NAME;
    } else if ("interval_day_time".equalsIgnoreCase(type)) {
      return serdeConstants.INTERVAL_DAY_TIME_TYPE_NAME;
    } else if ("decimal".equalsIgnoreCase(type)) {
      return serdeConstants.DECIMAL_TYPE_NAME;
    } else if ("binary".equalsIgnoreCase(type)) {
      return serdeConstants.BINARY_TYPE_NAME;
    } else if ("void".equalsIgnoreCase(type) || "null".equalsIgnoreCase(type)) {
      return serdeConstants.VOID_TYPE_NAME;
    } else if (type.equalsIgnoreCase("map")) {
      return serdeConstants.MAP_TYPE_NAME;
    } else if (type.equalsIgnoreCase("array")) {
      return serdeConstants.LIST_TYPE_NAME;
    } else if (type.equalsIgnoreCase("struct")) {
      return serdeConstants.STRUCT_TYPE_NAME;
    }

    throw new SQLException("Unrecognized column type: " + type);
  }

  static int columnDisplaySize(Type hiveType, JdbcColumnAttributes columnAttributes)
      throws SQLException {
    // according to hiveTypeToSqlType possible options are:
    int columnType = hiveTypeToSqlType(hiveType);
    switch(columnType) {
    case Types.NULL:
      return 4; // "NULL"
    case Types.BOOLEAN:
      return columnPrecision(hiveType, columnAttributes);
    case Types.CHAR:
    case Types.VARCHAR:
      return columnPrecision(hiveType, columnAttributes);
    case Types.BINARY:
      return Integer.MAX_VALUE; // hive has no max limit for binary
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
    case Types.BIGINT:
      return columnPrecision(hiveType, columnAttributes) + 1; // allow +/-
    case Types.DATE:
      return 10;
    case Types.TIMESTAMP:
    case Types.TIMESTAMP_WITH_TIMEZONE:
      return columnPrecision(hiveType, columnAttributes);

    // see http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Float.MAX_EXPONENT
    case Types.FLOAT:
      return 24; // e.g. -(17#).e-###
    // see http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Double.MAX_EXPONENT
    case Types.DOUBLE:
      return 25; // e.g. -(17#).e-####
    case Types.DECIMAL:
      return columnPrecision(hiveType, columnAttributes) + 2;  // '-' sign and '.'
    case Types.OTHER:
    case Types.JAVA_OBJECT:
      return columnPrecision(hiveType, columnAttributes);
    case Types.ARRAY:
    case Types.STRUCT:
      return Integer.MAX_VALUE;
    default:
      throw new SQLException("Invalid column type: " + columnType);
    }
  }

  static int columnPrecision(Type hiveType, JdbcColumnAttributes columnAttributes)
      throws SQLException {
    int columnType = hiveTypeToSqlType(hiveType);
    // according to hiveTypeToSqlType possible options are:
    switch(columnType) {
    case Types.NULL:
      return 0;
    case Types.BOOLEAN:
      return 1;
    case Types.CHAR:
    case Types.VARCHAR:
      if (columnAttributes != null) {
        return columnAttributes.precision;
      }
      return Integer.MAX_VALUE; // hive has no max limit for strings
    case Types.BINARY:
      return Integer.MAX_VALUE; // hive has no max limit for binary
    case Types.TINYINT:
      return 3;
    case Types.SMALLINT:
      return 5;
    case Types.INTEGER:
      return 10;
    case Types.BIGINT:
      return 19;
    case Types.FLOAT:
      return 7;
    case Types.DOUBLE:
      return 15;
    case Types.DATE:
      return 10;
    case Types.TIMESTAMP:
      return 29;
    case Types.TIMESTAMP_WITH_TIMEZONE:
      return 31;
    case Types.DECIMAL:
      return columnAttributes.precision;
    case Types.OTHER:
    case Types.JAVA_OBJECT: {
      switch (hiveType) {
        case INTERVAL_YEAR_MONTH_TYPE:
          // -yyyyyyy-mm  : should be more than enough
          return 11;
        case INTERVAL_DAY_TIME_TYPE:
          // -ddddddddd hh:mm:ss.nnnnnnnnn
          return 29;
        default:
          return Integer.MAX_VALUE;
      }
    }
    case Types.ARRAY:
    case Types.STRUCT:
      return Integer.MAX_VALUE;
    default:
      throw new SQLException("Invalid column type: " + columnType);
    }
  }

  static int columnScale(Type hiveType, JdbcColumnAttributes columnAttributes)
      throws SQLException {
    int columnType = hiveTypeToSqlType(hiveType);
    // according to hiveTypeToSqlType possible options are:
    switch(columnType) {
    case Types.NULL:
    case Types.BOOLEAN:
    case Types.CHAR:
    case Types.VARCHAR:
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
    case Types.BIGINT:
    case Types.DATE:
    case Types.BINARY:
      return 0;
    case Types.FLOAT:
      return 7;
    case Types.DOUBLE:
      return 15;
    case  Types.TIMESTAMP:
    case Types.TIMESTAMP_WITH_TIMEZONE:
      return 9;
    case Types.DECIMAL:
      return columnAttributes.scale;
    case Types.OTHER:
    case Types.JAVA_OBJECT:
    case Types.ARRAY:
    case Types.STRUCT:
      return 0;
    default:
      throw new SQLException("Invalid column type: " + columnType);
    }
  }

  public Integer getNumPrecRadix() {
    if (type.equalsIgnoreCase("tinyint")) {
      return 10;
    } else if (type.equalsIgnoreCase("smallint")) {
      return 10;
    } else if (type.equalsIgnoreCase("int")) {
      return 10;
    } else if (type.equalsIgnoreCase("bigint")) {
      return 10;
    } else if (type.equalsIgnoreCase("float")) {
      return 10;
    } else if (type.equalsIgnoreCase("double")) {
      return 10;
    } else if (type.equalsIgnoreCase("decimal")) {
      return 10;
    } else { // anything else including boolean and string is null
      return null;
    }
  }

  public String getComment() {
    return comment;
  }

  public int getOrdinalPos() {
    return ordinalPos;
  }
}
