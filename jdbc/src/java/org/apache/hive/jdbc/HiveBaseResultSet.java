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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hive.service.cli.TableSchema;

/**
 * Data independent base class which implements the common part of all Hive
 * result sets.
 */
public abstract class HiveBaseResultSet implements ResultSet {

  protected Statement statement = null;
  protected SQLWarning warningChain = null;
  protected boolean wasNull = false;
  protected Object[] row;
  protected List<String> columnNames;
  protected List<String> normalizedColumnNames;
  protected List<String> columnTypes;
  protected List<JdbcColumnAttributes> columnAttributes;
  private final Map<String, Integer> columnNameIndexCache = new HashMap<>();

  private TableSchema schema;

  @Override
  public boolean absolute(int row) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void afterLast() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void beforeFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int findColumn(final String columnName) throws SQLException {
    if (columnName == null) {
      throw new SQLException("null column name not supported");
    }
    final String lcColumnName = columnName.toLowerCase();
    final Integer result = this.columnNameIndexCache.computeIfAbsent(lcColumnName, cn -> {
      int columnIndex = 0;
      for (final String normalizedColumnName : normalizedColumnNames) {
        ++columnIndex;
        final int idx = normalizedColumnName.lastIndexOf('.');
        final String name = (idx == -1) ? normalizedColumnName : normalizedColumnName.substring(1 + idx);
        if (name.equals(cn) || normalizedColumnName.equals(cn)) {
          return columnIndex;
        }
      }
      return null;
    });
    if (result == null) {
      throw new SQLException("Could not find " + columnName + " in " + normalizedColumnNames);
    }
    return result.intValue();
  }

  @Override
  public boolean first() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Array getArray(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Array getArray(String colName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public InputStream getAsciiStream(String columnName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    final Object val = getObject(columnIndex);
    if (val == null) {
      return null;
    }
    if (val instanceof BigDecimal) {
      return (BigDecimal) val;
    }
    throw new SQLException("Illegal conversion to BigDecimal from column " + columnIndex + " [" + val + "]");
  }

  @Override
  public BigDecimal getBigDecimal(String columnName) throws SQLException {
    return getBigDecimal(findColumn(columnName));
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    MathContext mc = new MathContext(scale);
    return getBigDecimal(columnIndex).round(mc);
  }

  @Override
  public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
    return getBigDecimal(findColumn(columnName), scale);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }
    if (obj instanceof InputStream) {
      return (InputStream) obj;
    }
    if (obj instanceof byte[]) {
      byte[] byteArray = (byte[]) obj;
      return new ByteArrayInputStream(byteArray);
    }
    if (obj instanceof String) {
      final String str = (String) obj;
      return new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));
    }
    throw new SQLException("Illegal conversion to binary stream from column " + columnIndex + " [" + obj + "]");
  }

  @Override
  public InputStream getBinaryStream(String columnName) throws SQLException {
    return getBinaryStream(findColumn(columnName));
  }

  @Override
  public Blob getBlob(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Blob getBlob(String colName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /**
   * Retrieves the value of the designated column in the current row of this
   * ResultSet object as a boolean in the Java programming language. If the
   * designated column has a datatype of CHAR or VARCHAR and contains a "0" or
   * has a datatype of BIT, TINYINT, SMALLINT, INTEGER or BIGINT and contains a
   * 0, a value of false is returned.
   *
   * For a true value, this implementation relaxes the JDBC specs. If the
   * designated column has a datatype of CHAR or VARCHAR and contains a values
   * other than "0" or has a datatype of BIT, TINYINT, SMALLINT, INTEGER or
   * BIGINT and contains a value other than 0, a value of true is returned.
   *
   * @param columnIndex the first column is 1, the second is 2, ...
   * @return the column value; if the value is SQL NULL, the value returned is
   *         false
   * @throws if the columnIndex is not valid; if a database access error occurs
   *           or this method is called on a closed result set
   * @see ResultSet#getBoolean(int)
   */
  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return false;
    }
    if (obj instanceof Boolean) {
      return (Boolean) obj;
    }
    if (obj instanceof Number) {
      return ((Number) obj).intValue() != 0;
    }
    if (obj instanceof String) {
      return !((String) obj).equals("0");
    }
    throw new SQLException("Illegal conversion to boolean from column " + columnIndex + " [" + obj + "]");
  }

  /**
   * Retrieves the value of the designated column in the current row of this
   * ResultSet object as a boolean in the Java programming language. If the
   * designated column has a datatype of CHAR or VARCHAR and contains a "0" or
   * has a datatype of BIT, TINYINT, SMALLINT, INTEGER or BIGINT and contains a
   * 0, a value of false is returned.
   *
   * For a true value, this implementation relaxes the JDBC specs. If the
   * designated column has a datatype of CHAR or VARCHAR and contains a values
   * other than "0" or has a datatype of BIT, TINYINT, SMALLINT, INTEGER or
   * BIGINT and contains a value other than 0, a value of true is returned.
   *
   * @param columnLabel the label for the column specified with the SQL AS
   *          clause. If the SQL AS clause was not specified, then the label is
   *          the name of the column
   * @return the column value; if the value is SQL NULL, the value returned is
   *         false
   * @throws if the columnIndex is not valid; if a database access error occurs
   *           or this method is called on a closed result set
   * @see ResultSet#getBoolean(String)
   */
  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return getBoolean(findColumn(columnLabel));
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return 0;
    }
    if (obj instanceof Number) {
      return ((Number) obj).byteValue();
    }
    throw new SQLException("Illegal conversion to byte from column " + columnIndex + " [" + obj + "]");
  }

  @Override
  public byte getByte(String columnName) throws SQLException {
    return getByte(findColumn(columnName));
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public byte[] getBytes(String columnName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Reader getCharacterStream(String columnName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Clob getClob(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Clob getClob(String colName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }
    if (obj instanceof Date) {
      return (Date) obj;
    }
    if (obj instanceof String) {
      try {
        return Date.valueOf((String) obj);
      } catch (Exception e) {
        throw new SQLException("Illegal conversion to Date from column " + columnIndex + " [" + obj + "]", e);
      }
    }
    throw new SQLException("Illegal conversion to Date from column " + columnIndex + " [" + obj + "]");
  }

  @Override
  public Date getDate(String columnName) throws SQLException {
    return getDate(findColumn(columnName));
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Date getDate(String columnName, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return 0.0;
    }
    if (obj instanceof Number) {
      return ((Number) obj).doubleValue();
    }
    if (obj instanceof String) {
      try {
        return Double.parseDouble((String) obj);
      } catch (Exception e) {
        throw new SQLException("Illegal conversion to double from column " + columnIndex + " [" + obj + "]", e);
      }
    }
    throw new SQLException("Illegal conversion to double from column " + columnIndex + " [" + obj + "]");
  }

  @Override
  public double getDouble(String columnName) throws SQLException {
    return getDouble(findColumn(columnName));
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return 0.0f;
    }
    if (obj instanceof Number) {
      return ((Number) obj).floatValue();
    }
    if (obj instanceof String) {
      try {
        return Float.parseFloat((String) obj);
      } catch (Exception e) {
        throw new SQLException("Illegal conversion to float from column " + columnIndex + " [" + obj + "]", e);
      }
    }
    throw new SQLException("Illegal conversion to float from column " + columnIndex + " [" + obj + "]");
  }

  @Override
  public float getFloat(String columnName) throws SQLException {
    return getFloat(findColumn(columnName));
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return 0;
    }
    if (obj instanceof Number) {
      return ((Number) obj).intValue();
    }
    if (obj instanceof String) {
      try {
        return Integer.parseInt((String) obj);
      } catch (Exception e) {
        throw new SQLException("Illegal conversion to int from column " + columnIndex + " [" + obj + "]", e);
      }
    }
    throw new SQLException("Illegal conversion to int from column " + columnIndex + " [" + obj + "]");
  }

  @Override
  public int getInt(String columnName) throws SQLException {
    return getInt(findColumn(columnName));
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return 0L;
    }
    if (obj instanceof Number) {
      return ((Number) obj).longValue();
    }
    if (obj instanceof String) {
      try {
        return Long.parseLong((String) obj);
      } catch (Exception e) {
        throw new SQLException("Illegal conversion to long from column " + columnIndex + " [" + obj + "]", e);
      }
    }
    throw new SQLException("Illegal conversion to long from column " + columnIndex + " [" + obj + "]");
  }

  @Override
  public long getLong(String columnName) throws SQLException {
    return getLong(findColumn(columnName));
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return new HiveResultSetMetaData(columnNames, columnTypes, columnAttributes);
  }

  @Override
  public Reader getNCharacterStream(int arg0) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Reader getNCharacterStream(String arg0) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public NClob getNClob(int arg0) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  private Object getColumnValue(int columnIndex) throws SQLException {
    if (row == null) {
      throw new SQLException("No row found");
    }
    if (row.length == 0) {
      throw new SQLException("RowSet does not contain any columns");
    }
    // the first column is 1, the second is 2, ...
    if (columnIndex <= 0 || columnIndex > row.length) {
      throw new SQLException("Invalid columnIndex: " + columnIndex);
    }

    final Type columnType = getSchema().getColumnDescriptorAt(columnIndex - 1).getType();
    final Object value = row[columnIndex - 1];

    try {
      final Object evaluated = evaluate(columnType, value);
      wasNull = evaluated == null;
      return evaluated;
    } catch (Exception e) {
      throw new SQLException("Failed to evaluate " + columnType + " [" + value + "]", e);
    }
  }

  private Object evaluate(Type type, Object value) {
    if (value == null) {
      return null;
    }
    switch (type) {
    case BINARY_TYPE:
      if (value instanceof String) {
        return ((String) value).getBytes(StandardCharsets.UTF_8);
      }
      return value;
    case TIMESTAMP_TYPE:
      return Timestamp.valueOf((String) value);
    case TIMESTAMPLOCALTZ_TYPE:
      return TimestampTZUtil.parse((String) value);
    case DECIMAL_TYPE:
      return new BigDecimal((String) value);
    case DATE_TYPE:
      return Date.valueOf((String) value);
    case INTERVAL_YEAR_MONTH_TYPE:
      return HiveIntervalYearMonth.valueOf((String) value);
    case INTERVAL_DAY_TIME_TYPE:
      return HiveIntervalDayTime.valueOf((String) value);
    case ARRAY_TYPE:
    case MAP_TYPE:
    case STRUCT_TYPE:
      // todo: returns json string. should recreate object from it?
      return value;
    default:
      return value;
    }
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return getColumnValue(columnIndex);
  }

  @Override
  public Object getObject(String columnName) throws SQLException {
    return getObject(findColumn(columnName));
  }

  @Override
  public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Object getObject(String colName, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Ref getRef(int i) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Ref getRef(String colName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    final Object obj = getObject(columnIndex);

    if (Number.class.isInstance(obj)) {
      return ((Number) obj).shortValue();
    } else if (obj == null) {
      return 0;
    } else if (String.class.isInstance(obj)) {
      try {
        return Short.parseShort((String) obj);
      } catch (Exception e) {
        throw new SQLException("Illegal conversion to int from column " + columnIndex + " [" + obj + "]", e);
      }
    } else {
      throw new SQLException("Illegal conversion to int from column " + columnIndex + " [" + obj + "]");
    }
  }

  @Override
  public short getShort(String columnName) throws SQLException {
    return getShort(findColumn(columnName));
  }

  @Override
  public Statement getStatement() throws SQLException {
    return this.statement;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    final Object value = getColumnValue(columnIndex);
    if (value == null) {
      return null;
    }
    if (value instanceof byte[]) {
      return new String((byte[]) value, StandardCharsets.UTF_8);
    }
    return value.toString();
  }

  @Override
  public String getString(String columnName) throws SQLException {
    return getString(findColumn(columnName));
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Time getTime(String columnName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Time getTime(String columnName, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    final Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }
    if (obj instanceof Timestamp) {
      return (Timestamp) obj;
    }
    if (obj instanceof String) {
      try {
        return Timestamp.valueOf((String) obj);
      } catch (Exception e) {
        throw new SQLException("Illegal conversion to int from column " + columnIndex + " [" + obj + "]", e);
      }
    }
    throw new SQLException("Illegal conversion to int from column " + columnIndex + " [" + obj + "]");
  }

  @Override
  public Timestamp getTimestamp(String columnName) throws SQLException {
    return getTimestamp(findColumn(columnName));
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public URL getURL(String columnName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public InputStream getUnicodeStream(String columnName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void insertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean isClosed() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean isFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean isLast() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean last() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean previous() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateArray(String columnName, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBlob(String columnName, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBoolean(String columnName, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateByte(String columnName, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateBytes(String columnName, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateCharacterStream(String columnName, Reader reader, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateClob(String columnName, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateDate(String columnName, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateDouble(String columnName, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateFloat(String columnName, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateInt(String columnName, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateLong(String columnName, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNClob(int columnIndex, NClob clob) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNClob(String columnLabel, NClob clob) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNString(int columnIndex, String string) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNString(String columnLabel, String string) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateNull(String columnName) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateObject(String columnName, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateObject(String columnName, Object x, int scale) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateRef(String columnName, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateShort(String columnName, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateString(String columnName, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateTime(String columnName, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  @Override
  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  @Override
  public void close() throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public boolean wasNull() throws SQLException {
    return wasNull;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  protected void setSchema(TableSchema schema) {
    this.schema = schema;
  }

  protected TableSchema getSchema() {
    return schema;
  }
}
