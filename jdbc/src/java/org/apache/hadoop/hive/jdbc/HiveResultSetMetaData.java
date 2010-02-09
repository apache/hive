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
  List<String> columnNames;
  List<String> columnTypes;

  public HiveResultSetMetaData(List<String> columnNames,
      List<String> columnTypes) {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getCatalogName(int)
   */

  public String getCatalogName(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getColumnClassName(int)
   */

  public String getColumnClassName(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getColumnCount()
   */

  public int getColumnCount() throws SQLException {
    return columnNames.size();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getColumnDisplaySize(int)
   */

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

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getColumnLabel(int)
   */

  public String getColumnLabel(int column) throws SQLException {
    // TODO Auto-generated method stub
    return columnNames.get(column - 1);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getColumnName(int)
   */

  public String getColumnName(int column) throws SQLException {
    return columnNames.get(column - 1);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getColumnType(int)
   */

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
    // TODO: this would be better handled in an enum
    if ("string".equals(type)) {
      return Types.VARCHAR;
    } else if ("bool".equals(type)) {
      return Types.BOOLEAN;
    } else if ("double".equals(type)) {
      return Types.DOUBLE;
    } else if ("byte".equals(type)) {
      return Types.TINYINT;
    } else if ("i32".equals(type)) {
      return Types.INTEGER;
    } else if ("i64".equals(type)) {
      return Types.BIGINT;
    }

    throw new SQLException("Inrecognized column type: " + type);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getColumnTypeName(int)
   */

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
    if ("string".equals(type)) {
      return Constants.STRING_TYPE_NAME;
    } else if ("double".equals(type)) {
      return Constants.DOUBLE_TYPE_NAME;
    } else if ("bool".equals(type)) {
      return Constants.BOOLEAN_TYPE_NAME;
    } else if ("byte".equals(type)) {
      return Constants.TINYINT_TYPE_NAME;
    } else if ("i32".equals(type)) {
      return Constants.INT_TYPE_NAME;
    } else if ("i64".equals(type)) {
      return Constants.BIGINT_TYPE_NAME;
    }

    throw new SQLException("Inrecognized column type: " + type);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getPrecision(int)
   */

  public int getPrecision(int column) throws SQLException {
    if (Types.DOUBLE == getColumnType(column)) {
      return -1; // Do we have a precision limit?
    }

    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getScale(int)
   */

  public int getScale(int column) throws SQLException {
    if (Types.DOUBLE == getColumnType(column)) {
      return -1; // Do we have a scale limit?
    }

    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getSchemaName(int)
   */

  public String getSchemaName(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#getTableName(int)
   */

  public String getTableName(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isAutoIncrement(int)
   */

  public boolean isAutoIncrement(int column) throws SQLException {
    // Hive doesn't have an auto-increment concept
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isCaseSensitive(int)
   */

  public boolean isCaseSensitive(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isCurrency(int)
   */

  public boolean isCurrency(int column) throws SQLException {
    // Hive doesn't support a currency type
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isDefinitelyWritable(int)
   */

  public boolean isDefinitelyWritable(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isNullable(int)
   */

  public int isNullable(int column) throws SQLException {
    // Hive doesn't have the concept of not-null
    return ResultSetMetaData.columnNullable;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isReadOnly(int)
   */

  public boolean isReadOnly(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isSearchable(int)
   */

  public boolean isSearchable(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isSigned(int)
   */

  public boolean isSigned(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.ResultSetMetaData#isWritable(int)
   */

  public boolean isWritable(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
   */

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.sql.Wrapper#unwrap(java.lang.Class)
   */

  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

}
