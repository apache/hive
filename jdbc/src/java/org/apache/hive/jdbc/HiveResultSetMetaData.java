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

package org.apache.hive.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

import org.apache.hadoop.hive.serde2.thrift.Type;

/**
 * HiveResultSetMetaData.
 *
 */
public class HiveResultSetMetaData implements java.sql.ResultSetMetaData {
  private final List<String> columnNames;
  private final List<String> columnTypes;
  private final List<JdbcColumnAttributes> columnAttributes;

  public HiveResultSetMetaData(List<String> columnNames,
      List<String> columnTypes,
      List<JdbcColumnAttributes> columnAttributes) {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.columnAttributes = columnAttributes;
  }

  public String getCatalogName(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  private Type getHiveType(int column) throws SQLException {
    return JdbcColumn.typeStringToHiveType(columnTypes.get(toZeroIndex(column)));
  }

  public String getColumnClassName(int column) throws SQLException {
    return JdbcColumn.columnClassName(getHiveType(column),
        columnAttributes.get(toZeroIndex(column)));
  }

  public int getColumnCount() throws SQLException {
    return columnNames.size();
  }

  public int getColumnDisplaySize(int column) throws SQLException {
    return JdbcColumn.columnDisplaySize(getHiveType(column),
        columnAttributes.get(toZeroIndex(column)));
  }

  public String getColumnLabel(int column) throws SQLException {
    return columnNames.get(toZeroIndex(column));
  }

  public String getColumnName(int column) throws SQLException {
    return columnNames.get(toZeroIndex(column));
  }

  public int getColumnType(int column) throws SQLException {
    // we need to convert the thrift type to the SQL type
    String type = columnTypes.get(toZeroIndex(column));

    // we need to convert the thrift type to the SQL type
    return JdbcColumn.hiveTypeToSqlType(type);
  }

  public String getColumnTypeName(int column) throws SQLException {
    return JdbcColumn.getColumnTypeName(columnTypes.get(toZeroIndex(column)));
  }

  public int getPrecision(int column) throws SQLException {
    return JdbcColumn.columnPrecision(getHiveType(column),
        columnAttributes.get(toZeroIndex(column)));
  }

  public int getScale(int column) throws SQLException {
    return JdbcColumn.columnScale(getHiveType(column),
        columnAttributes.get(toZeroIndex(column)));
  }

  public String getSchemaName(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public String getTableName(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean isAutoIncrement(int column) throws SQLException {
    // Hive doesn't have an auto-increment concept
    return false;
  }

  public boolean isCaseSensitive(int column) throws SQLException {
    // we need to convert the Hive type to the SQL type name
    // TODO: this would be better handled in an enum
    String type = columnTypes.get(toZeroIndex(column));

    if("string".equalsIgnoreCase(type)) {
      return true;
    } else {
      return false;
    }
  }

  public boolean isCurrency(int column) throws SQLException {
    // Hive doesn't support a currency type
    return false;
  }

  public boolean isDefinitelyWritable(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public int isNullable(int column) throws SQLException {
    // Hive doesn't have the concept of not-null
    return ResultSetMetaData.columnNullable;
  }

  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  public boolean isSearchable(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean isSigned(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean isWritable(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  protected int toZeroIndex(int column) throws SQLException {
    if (columnTypes == null) {
      throw new SQLException(
          "Could not determine column type name for ResultSet");
    }
    if (column < 1 || column > columnTypes.size()) {
      throw new SQLException("Invalid column value: " + column);
    }
    return column - 1;
  }
}
