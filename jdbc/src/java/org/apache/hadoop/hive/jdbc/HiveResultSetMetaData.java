/**
 *
 */
package org.apache.hadoop.hive.jdbc;

import java.sql.SQLException;
import java.util.List;

public class HiveResultSetMetaData implements java.sql.ResultSetMetaData {
  List<String> columnNames;
  List<String> columnTypes;
  
  public HiveResultSetMetaData(List<String> columnNames, List<String> columnTypes) {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getCatalogName(int)
   */

  public String getCatalogName(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnClassName(int)
   */

  public String getColumnClassName(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnCount()
   */

  public int getColumnCount() throws SQLException {
    return columnNames.size();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnDisplaySize(int)
   */

  public int getColumnDisplaySize(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnLabel(int)
   */

  public String getColumnLabel(int column) throws SQLException {
    // TODO Auto-generated method stub
    return columnNames.get(column-1);
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnName(int)
   */

  public String getColumnName(int column) throws SQLException {
    return columnNames.get(column-1);
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnType(int)
   */

  public int getColumnType(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getColumnTypeName(int)
   */

  public String getColumnTypeName(int column) throws SQLException {
    return columnTypes.get(column-1);
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getPrecision(int)
   */

  public int getPrecision(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getScale(int)
   */

  public int getScale(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getSchemaName(int)
   */

  public String getSchemaName(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#getTableName(int)
   */

  public String getTableName(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isAutoIncrement(int)
   */

  public boolean isAutoIncrement(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isCaseSensitive(int)
   */

  public boolean isCaseSensitive(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isCurrency(int)
   */

  public boolean isCurrency(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isDefinitelyWritable(int)
   */

  public boolean isDefinitelyWritable(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isNullable(int)
   */

  public int isNullable(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isReadOnly(int)
   */

  public boolean isReadOnly(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isSearchable(int)
   */

  public boolean isSearchable(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isSigned(int)
   */

  public boolean isSigned(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSetMetaData#isWritable(int)
   */

  public boolean isWritable(int column) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
   */

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.Wrapper#unwrap(java.lang.Class)
   */

  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

}
