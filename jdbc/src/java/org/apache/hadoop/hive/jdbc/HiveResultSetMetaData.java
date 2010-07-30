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

package org.apache.hadoop.hive.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import org.apache.hadoop.hive.serde.Constants;

/**
 * HiveResultSetMetaData.
 *
 */
public class HiveResultSetMetaData implements java.sql.ResultSetMetaData {
  private List<String> columnNames;
  private List<String> columnTypes;

  public HiveResultSetMetaData(List<String> columnNames,
      List<String> columnTypes) {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
  }

  public String getCatalogName(int column) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public String getColumnClassName(int column) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public int getColumnCount() throws SQLException {
    return columnNames.size();
  }

  public int getColumnDisplaySize(int column) throws SQLException {
    // taking a stab at appropriate values
    switch (getColumnType(column)) {
    case Types.VARCHAR:
    case Types.BIGINT:
      return 32;
    case Types.TINYINT:
      return 2;
    case Types.BOOLEAN:
      return 8;
    case Types.DOUBLE:
    case Types.INTEGER:
      return 16;
    default:
      return 32;
    }
  }

  public String getColumnLabel(int column) throws SQLException {
    return columnNames.get(column - 1);
  }

  public String getColumnName(int column) throws SQLException {
    return columnNames.get(column - 1);
  }

  public int getColumnType(int column) throws SQLException {
    if (columnTypes == null) {
      throw new SQLException(
          "Could not determine column type name for ResultSet");
    }

    if (column < 1 || column > columnTypes.size()) {
      throw new SQLException("Invalid column value: " + column);
    }

    // we need to convert the thrift type to the SQL type
    String type = columnTypes.get(column - 1);

    // we need to convert the thrift type to the SQL type
    return hiveTypeToSqlType(type);
  }

  /**
   * Convert hive types to sql types.
   * @param type
   * @return Integer java.sql.Types values
   * @throws SQLException
   */
  public static Integer hiveTypeToSqlType(String type) throws SQLException {
    if ("string".equalsIgnoreCase(type)) {
      return Types.VARCHAR;
    } else if ("bool".equalsIgnoreCase(type) || "boolean".equalsIgnoreCase(type)) {
      return Types.BOOLEAN;
    } else if ("float".equalsIgnoreCase(type)) {
      return Types.FLOAT;
    } else if ("double".equalsIgnoreCase(type)) {
      return Types.DOUBLE;
    } else if ("byte".equalsIgnoreCase(type) || "tinyint".equalsIgnoreCase(type)) {
      return Types.TINYINT;
    } else if ("i32".equalsIgnoreCase(type) || "int".equalsIgnoreCase(type)) {
      return Types.INTEGER;
    } else if ("i64".equalsIgnoreCase(type) || "bigint".equalsIgnoreCase(type)) {
      return Types.BIGINT;
    }
     throw new SQLException("Unrecognized column type: " + type);
  }


  public String getColumnTypeName(int column) throws SQLException {
    if (columnTypes == null) {
      throw new SQLException(
          "Could not determine column type name for ResultSet");
    }

    if (column < 1 || column > columnTypes.size()) {
      throw new SQLException("Invalid column value: " + column);
    }

    // we need to convert the thrift type to the SQL type name
    // TODO: this would be better handled in an enum
    String type = columnTypes.get(column - 1);
    if ("string".equalsIgnoreCase(type)) {
      return Constants.STRING_TYPE_NAME;
    } else if ("double".equalsIgnoreCase(type)) {
      return Constants.DOUBLE_TYPE_NAME;
    } else if ("bool".equalsIgnoreCase(type)) {
      return Constants.BOOLEAN_TYPE_NAME;
    } else if ("byte".equalsIgnoreCase(type)) {
      return Constants.TINYINT_TYPE_NAME;
    } else if ("i32".equalsIgnoreCase(type)) {
      return Constants.INT_TYPE_NAME;
    } else if ("i64".equalsIgnoreCase(type)) {
      return Constants.BIGINT_TYPE_NAME;
    }

    throw new SQLException("Inrecognized column type: " + type);
  }

  public int getPrecision(int column) throws SQLException {
    if (Types.DOUBLE == getColumnType(column)) {
      return -1; // Do we have a precision limit?
    }

    return 0;
  }

  public int getScale(int column) throws SQLException {
    if (Types.DOUBLE == getColumnType(column)) {
      return -1; // Do we have a scale limit?
    }

    return 0;
  }

  public String getSchemaName(int column) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public String getTableName(int column) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean isAutoIncrement(int column) throws SQLException {
    // Hive doesn't have an auto-increment concept
    return false;
  }

  public boolean isCaseSensitive(int column) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean isCurrency(int column) throws SQLException {
    // Hive doesn't support a currency type
    return false;
  }

  public boolean isDefinitelyWritable(int column) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public int isNullable(int column) throws SQLException {
    // Hive doesn't have the concept of not-null
    return ResultSetMetaData.columnNullable;
  }

  public boolean isReadOnly(int column) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean isSearchable(int column) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean isSigned(int column) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean isWritable(int column) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException("Method not supported");
  }

}
