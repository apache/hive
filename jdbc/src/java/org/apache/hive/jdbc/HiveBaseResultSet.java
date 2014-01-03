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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.net.URL;
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
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.Type;

/**
 * Data independent base class which implements the common part of
 * all Hive result sets.
 */
public abstract class HiveBaseResultSet implements ResultSet {

  protected Statement statement = null;
  protected SQLWarning warningChain = null;
  protected boolean wasNull = false;
  protected Object[] row;
  protected List<String> columnNames;
  protected List<String> columnTypes;
  protected List<JdbcColumnAttributes> columnAttributes;

  private TableSchema schema;

  public boolean absolute(int row) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void afterLast() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void beforeFirst() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void cancelRowUpdates() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void deleteRow() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public int findColumn(String columnName) throws SQLException {
    int columnIndex = columnNames.indexOf(columnName);
    if (columnIndex==-1) {
      throw new SQLException();
    } else {
      return ++columnIndex;
    }
  }

  public boolean first() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Array getArray(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Array getArray(String colName) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public InputStream getAsciiStream(String columnName) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    Object val = getObject(columnIndex);

    if (val == null || val instanceof BigDecimal) {
      return (BigDecimal)val;
    }

    throw new SQLException("Illegal conversion");
  }

  public BigDecimal getBigDecimal(String columnName) throws SQLException {
    return getBigDecimal(findColumn(columnName));
  }

  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    MathContext mc = new MathContext(scale);
    return getBigDecimal(columnIndex).round(mc);
  }

  public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
    return getBigDecimal(findColumn(columnName), scale);
  }

  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public InputStream getBinaryStream(String columnName) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Blob getBlob(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Blob getBlob(String colName) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean getBoolean(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (Boolean.class.isInstance(obj)) {
      return (Boolean) obj;
    } else if (obj == null) {
      return false;
    } else if (Number.class.isInstance(obj)) {
      return ((Number) obj).intValue() != 0;
    } else if (String.class.isInstance(obj)) {
      return !((String) obj).equals("0");
    }
    throw new SQLException("Cannot convert column " + columnIndex + " to boolean");
  }

  public boolean getBoolean(String columnName) throws SQLException {
    return getBoolean(findColumn(columnName));
  }

  public byte getByte(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (Number.class.isInstance(obj)) {
      return ((Number) obj).byteValue();
    } else if (obj == null) {
      return 0;
    }
    throw new SQLException("Cannot convert column " + columnIndex + " to byte");
  }

  public byte getByte(String columnName) throws SQLException {
    return getByte(findColumn(columnName));
  }

  public byte[] getBytes(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public byte[] getBytes(String columnName) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Reader getCharacterStream(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Reader getCharacterStream(String columnName) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Clob getClob(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Clob getClob(String colName) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public int getConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  public String getCursorName() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Date getDate(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }
    if (obj instanceof Date) {
      return (Date) obj;
    }
    try {
      if (obj instanceof String) {
        return Date.valueOf((String)obj);
      }
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex
              + " to date: " + e.toString(), e);
    }
    // If we fell through to here this is not a valid type conversion
    throw new SQLException("Cannot convert column " + columnIndex
        + " to date: Illegal conversion");
  }

  public Date getDate(String columnName) throws SQLException {
    return getDate(findColumn(columnName));
  }

  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Date getDate(String columnName, Calendar cal) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public double getDouble(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).doubleValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Double.valueOf((String)obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex
              + " to double: " + e.toString(), e);
    }
  }

  public double getDouble(String columnName) throws SQLException {
    return getDouble(findColumn(columnName));
  }

  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  public int getFetchSize() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public float getFloat(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).floatValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Float.valueOf((String)obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex
              + " to float: " + e.toString(), e);
    }
  }

  public float getFloat(String columnName) throws SQLException {
    return getFloat(findColumn(columnName));
  }

  public int getHoldability() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public int getInt(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).intValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Integer.valueOf((String)obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException(
          "Cannot convert column " + columnIndex + " to integer" + e.toString(),
          e);
    }
  }

  public int getInt(String columnName) throws SQLException {
    return getInt(findColumn(columnName));
  }

  public long getLong(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).longValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Long.valueOf((String)obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException(
          "Cannot convert column " + columnIndex + " to long: " + e.toString(),
          e);
    }
  }

  public long getLong(String columnName) throws SQLException {
    return getLong(findColumn(columnName));
  }

  public ResultSetMetaData getMetaData() throws SQLException {
    return new HiveResultSetMetaData(columnNames, columnTypes, columnAttributes);
  }

  public Reader getNCharacterStream(int arg0) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Reader getNCharacterStream(String arg0) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public NClob getNClob(int arg0) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public NClob getNClob(String columnLabel) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public String getNString(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public String getNString(String columnLabel) throws SQLException {
    throw new SQLException("Method not supported");
  }

  private Object getColumnValue(int columnIndex) throws SQLException {
    if (row == null) {
      throw new SQLException("No row found.");
    }
    if (row.length == 0) {
      throw new SQLException("RowSet does not contain any columns!");
    }
    if (columnIndex > row.length) {
      throw new SQLException("Invalid columnIndex: " + columnIndex);
    }
    Type columnType = getSchema().getColumnDescriptorAt(columnIndex - 1).getType();

    try {
      Object evaluated = evaluate(columnType, row[columnIndex - 1]);
      wasNull = evaluated == null;
      return evaluated;
    } catch (Exception e) {
      e.printStackTrace();
      throw new SQLException("Unrecognized column type:" + columnType, e);
    }
  }

  private Object evaluate(Type type, Object value) {
    if (value == null) {
      return null;
    }
    switch (type) {
      case BINARY_TYPE:
        if (value instanceof String) {
          return ((String) value).getBytes();
        }
        return value;
      case TIMESTAMP_TYPE:
        return Timestamp.valueOf((String) value);
      case DECIMAL_TYPE:
        return new BigDecimal((String)value);
      case DATE_TYPE:
        return Date.valueOf((String) value);
      default:
        return value;
    }
  }

  public Object getObject(int columnIndex) throws SQLException {
    return getColumnValue(columnIndex);
  }

  public Object getObject(String columnName) throws SQLException {
    return getObject(findColumn(columnName));
  }

  public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Object getObject(String colName, Map<String, Class<?>> map) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Ref getRef(int i) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Ref getRef(String colName) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public int getRow() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public RowId getRowId(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public RowId getRowId(String columnLabel) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public short getShort(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).shortValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Short.valueOf((String)obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex
              + " to short: " + e.toString(), e);
    }
  }

  public short getShort(String columnName) throws SQLException {
    return getShort(findColumn(columnName));
  }

  public Statement getStatement() throws SQLException {
    return this.statement;
  }

  /**
   * @param columnIndex - the first column is 1, the second is 2, ...
   * @see java.sql.ResultSet#getString(int)
   */
  public String getString(int columnIndex) throws SQLException {
    Object value = getColumnValue(columnIndex);
    if (wasNull) {
      return null;
    }
    if (value instanceof byte[]){
      return new String((byte[])value);
    }
    return value.toString();
  }

  public String getString(String columnName) throws SQLException {
    return getString(findColumn(columnName));
  }

  public Time getTime(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Time getTime(String columnName) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Time getTime(String columnName, Calendar cal) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }
    if (obj instanceof Timestamp) {
      return (Timestamp) obj;
    }
    if (obj instanceof String) {
      return Timestamp.valueOf((String)obj);
    }
    throw new SQLException("Illegal conversion");
  }

  public Timestamp getTimestamp(String columnName) throws SQLException {
    return getTimestamp(findColumn(columnName));
  }

  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  public URL getURL(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public URL getURL(String columnName) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public InputStream getUnicodeStream(String columnName) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void insertRow() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean isAfterLast() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean isBeforeFirst() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean isClosed() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean isFirst() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean isLast() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean last() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void moveToCurrentRow() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void moveToInsertRow() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean previous() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void refreshRow() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean relative(int rows) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean rowDeleted() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean rowInserted() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean rowUpdated() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void setFetchSize(int rows) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateArray(String columnName, Array x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateAsciiStream(int columnIndex, InputStream x, int length)
          throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateAsciiStream(String columnName, InputStream x, int length)
          throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateAsciiStream(int columnIndex, InputStream x, long length)
          throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateAsciiStream(String columnLabel, InputStream x, long length)
          throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBinaryStream(int columnIndex, InputStream x, int length)
          throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBinaryStream(String columnName, InputStream x, int length)
          throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBinaryStream(int columnIndex, InputStream x, long length)
          throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBinaryStream(String columnLabel, InputStream x, long length)
          throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBlob(String columnName, Blob x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBlob(int columnIndex, InputStream inputStream, long length)
          throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBlob(String columnLabel, InputStream inputStream,
                         long length) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBoolean(String columnName, boolean x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateByte(String columnName, byte x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateBytes(String columnName, byte[] x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateCharacterStream(int columnIndex, Reader x, int length)
          throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateCharacterStream(String columnName, Reader reader, int length)
          throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateCharacterStream(int columnIndex, Reader x, long length)
          throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateCharacterStream(String columnLabel, Reader reader,
                                    long length) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateClob(String columnName, Clob x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateDate(String columnName, Date x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateDouble(String columnName, double x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateFloat(String columnName, float x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateInt(String columnName, int x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateLong(String columnName, long x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateNCharacterStream(String columnLabel, Reader reader,
                                     long length) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateNClob(int columnIndex, NClob clob) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateNClob(String columnLabel, NClob clob) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateNString(int columnIndex, String string) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateNString(String columnLabel, String string) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateNull(int columnIndex) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateNull(String columnName) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateObject(String columnName, Object x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateObject(String columnName, Object x, int scale) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateRef(String columnName, Ref x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateRow() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateShort(String columnName, short x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateString(int columnIndex, String x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateString(String columnName, String x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateTime(String columnName, Time x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  public void close() throws SQLException {
    throw new SQLException("Method not supported");
  }

  public boolean wasNull() throws SQLException {
    return wasNull;
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException("Method not supported");
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException("Method not supported");
  }

  protected void setSchema(TableSchema schema) {
    this.schema = schema;
  }

  protected TableSchema getSchema() {
    return schema;
  }
}
