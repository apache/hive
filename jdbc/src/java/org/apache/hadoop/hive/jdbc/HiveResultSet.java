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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.io.BytesWritable;


public class HiveResultSet implements java.sql.ResultSet {
  HiveInterface client;
  ArrayList<?> row;
  DynamicSerDe ds;
  List<String> columnNames;
  List<String> columnTypes;

  SQLWarning warningChain = null;
  boolean wasNull = false;
  int maxRows = 0;
  int rowsFetched = 0;

  /**
   *
   */
  @SuppressWarnings("unchecked")
  public HiveResultSet(HiveInterface client, int maxRows) throws SQLException {
    this.client = client;
    this.row = new ArrayList();
    this.maxRows = maxRows;
    initDynamicSerde();
  }

  @SuppressWarnings("unchecked")
  public HiveResultSet(HiveInterface client) throws SQLException {
    this(client, 0);
  }

  /**
   * Instantiate the dynamic serde used to deserialize the result row
   */
  public void initDynamicSerde() throws SQLException {
    try {
      Schema fullSchema = client.getThriftSchema();
      List<FieldSchema> schema = fullSchema.getFieldSchemas();
      columnNames = new ArrayList<String>();
      columnTypes = new ArrayList<String>();
      
      String serDDL;

      if ((schema != null) && (!schema.isEmpty())) {
        serDDL = new String("struct result { ");
        for (int pos = 0; pos < schema.size(); pos++) {
          if (pos != 0)
            serDDL = serDDL.concat(",");
          columnTypes.add(schema.get(pos).getType());
          columnNames.add(schema.get(pos).getName());
          serDDL = serDDL.concat(schema.get(pos).getType());
          serDDL = serDDL.concat(" ");
          serDDL = serDDL.concat(schema.get(pos).getName());
        }
        serDDL = serDDL.concat("}");
      }
      else
        serDDL = new String("struct result { string empty }");

      ds = new DynamicSerDe();
      Properties dsp = new Properties();
      dsp.setProperty(Constants.SERIALIZATION_FORMAT, org.apache.hadoop.hive.serde2.thrift.TCTLSeparatedProtocol.class.getName());
      dsp.setProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME, "result");
      dsp.setProperty(Constants.SERIALIZATION_DDL, serDDL);
      dsp.setProperty(Constants.SERIALIZATION_LIB, ds.getClass().toString());
      dsp.setProperty(Constants.FIELD_DELIM, "9");
      ds.initialize(new Configuration(), dsp);
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Could not create ResultSet: " + ex.getMessage());
    }
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#absolute(int)
   */

  public boolean absolute(int row) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#afterLast()
   */

  public void afterLast() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#beforeFirst()
   */

  public void beforeFirst() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#cancelRowUpdates()
   */

  public void cancelRowUpdates() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#clearWarnings()
   */

  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#close()
   */

  public void close() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#deleteRow()
   */

  public void deleteRow() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#findColumn(java.lang.String)
   */

  public int findColumn(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#first()
   */

  public boolean first() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getArray(int)
   */

  public Array getArray(int i) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getArray(java.lang.String)
   */

  public Array getArray(String colName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getAsciiStream(int)
   */

  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getAsciiStream(java.lang.String)
   */

  public InputStream getAsciiStream(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getBigDecimal(int)
   */

  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getBigDecimal(java.lang.String)
   */

  public BigDecimal getBigDecimal(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getBigDecimal(int, int)
   */

  public BigDecimal getBigDecimal(int columnIndex, int scale)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getBigDecimal(java.lang.String, int)
   */

  public BigDecimal getBigDecimal(String columnName, int scale)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getBinaryStream(int)
   */

  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getBinaryStream(java.lang.String)
   */

  public InputStream getBinaryStream(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getBlob(int)
   */

  public Blob getBlob(int i) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getBlob(java.lang.String)
   */

  public Blob getBlob(String colName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getBoolean(int)
   */

  public boolean getBoolean(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (Number.class.isInstance(obj)) {
      return ((Number)obj).intValue() != 0;
    }
    throw new SQLException("Cannot convert column " + columnIndex + " to boolean");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getBoolean(java.lang.String)
   */

  public boolean getBoolean(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getByte(int)
   */

  public byte getByte(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (Number.class.isInstance(obj)) {
      return ((Number)obj).byteValue();
    }
    throw new SQLException("Cannot convert column " + columnIndex + " to byte");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getByte(java.lang.String)
   */

  public byte getByte(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getBytes(int)
   */

  public byte[] getBytes(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getBytes(java.lang.String)
   */

  public byte[] getBytes(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getCharacterStream(int)
   */

  public Reader getCharacterStream(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getCharacterStream(java.lang.String)
   */

  public Reader getCharacterStream(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getClob(int)
   */

  public Clob getClob(int i) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getClob(java.lang.String)
   */

  public Clob getClob(String colName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getConcurrency()
   */

  public int getConcurrency() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getCursorName()
   */

  public String getCursorName() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getDate(int)
   */

  public Date getDate(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (obj == null) return null;

    try {
      return Date.valueOf((String)obj);
    }
    catch (Exception e) {
    throw new SQLException("Cannot convert column " + columnIndex + " to date: " + e.toString());
    }
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getDate(java.lang.String)
   */

  public Date getDate(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getDate(int, java.util.Calendar)
   */

  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getDate(java.lang.String, java.util.Calendar)
   */

  public Date getDate(String columnName, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getDouble(int)
   */

  public double getDouble(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number)obj).doubleValue();
      }
      throw new Exception("Illegal conversion");
    }
    catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex + " to double: " + e.toString());
    }
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getDouble(java.lang.String)
   */

  public double getDouble(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getFetchDirection()
   */

  public int getFetchDirection() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getFetchSize()
   */

  public int getFetchSize() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getFloat(int)
   */

  public float getFloat(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number)obj).floatValue();
      }
      throw new Exception("Illegal conversion");
    }
    catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex + " to float: " + e.toString());
    }
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getFloat(java.lang.String)
   */

  public float getFloat(String columnName) throws SQLException {
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getHoldability()
   */

  public int getHoldability() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getInt(int)
   */

  public int getInt(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number)obj).intValue();
      }
      throw new Exception("Illegal conversion");
    }
    catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex + " to integer" + e.toString());
    }
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getInt(java.lang.String)
   */

  public int getInt(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getLong(int)
   */

  public long getLong(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number)obj).longValue();
      }
      throw new Exception("Illegal conversion");
    }
    catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex + " to long: " + e.toString());
    }
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getLong(java.lang.String)
   */

  public long getLong(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getMetaData()
   */

  public ResultSetMetaData getMetaData() throws SQLException {
    return new HiveResultSetMetaData(columnNames, columnTypes);
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getNCharacterStream(int)
   */

  public Reader getNCharacterStream(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getNCharacterStream(java.lang.String)
   */

  public Reader getNCharacterStream(String arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getNClob(int)
   */

  public NClob getNClob(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getNClob(java.lang.String)
   */

  public NClob getNClob(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getNString(int)
   */

  public String getNString(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getNString(java.lang.String)
   */

  public String getNString(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getObject(int)
   */

  public Object getObject(int columnIndex) throws SQLException {
    if (row == null) {
      throw new SQLException("No row found.");
    }

    if (columnIndex > row.size()) {
      throw new SQLException("Invalid columnIndex: " + columnIndex);
    }

    try {
      this.wasNull = false;
      if (row.get(columnIndex-1) == null) this.wasNull = true;

      return row.get(columnIndex-1);
    }
    catch (Exception e) {
      throw new SQLException(e.toString());
    }
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getObject(java.lang.String)
   */

  public Object getObject(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getObject(int, java.util.Map)
   */

  public Object getObject(int i, Map<String, Class<?>> map)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getObject(java.lang.String, java.util.Map)
   */

  public Object getObject(String colName, Map<String, Class<?>> map)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getRef(int)
   */

  public Ref getRef(int i) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getRef(java.lang.String)
   */

  public Ref getRef(String colName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getRow()
   */

  public int getRow() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getRowId(int)
   */

  public RowId getRowId(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getRowId(java.lang.String)
   */

  public RowId getRowId(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getSQLXML(int)
   */

  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getSQLXML(java.lang.String)
   */

  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getShort(int)
   */

  public short getShort(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number)obj).shortValue();
      }
      throw new Exception("Illegal conversion");
    }
    catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex + " to short: " + e.toString());
    }
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getShort(java.lang.String)
   */

  public short getShort(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getStatement()
   */

  public Statement getStatement() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /**
   * @param columnIndex - the first column is 1, the second is 2, ...
   * @see java.sql.ResultSet#getString(int)
   */

  public String getString(int columnIndex) throws SQLException {
    // Column index starts from 1, not 0.
    Object obj = getObject(columnIndex);
    if (obj == null) return null;

    return obj.toString();
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getString(java.lang.String)
   */

  public String getString(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getTime(int)
   */

  public Time getTime(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getTime(java.lang.String)
   */

  public Time getTime(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getTime(int, java.util.Calendar)
   */

  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getTime(java.lang.String, java.util.Calendar)
   */

  public Time getTime(String columnName, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getTimestamp(int)
   */

  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getTimestamp(java.lang.String)
   */

  public Timestamp getTimestamp(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getTimestamp(int, java.util.Calendar)
   */

  public Timestamp getTimestamp(int columnIndex, Calendar cal)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getTimestamp(java.lang.String, java.util.Calendar)
   */

  public Timestamp getTimestamp(String columnName, Calendar cal)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getType()
   */

  public int getType() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getURL(int)
   */

  public URL getURL(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getURL(java.lang.String)
   */

  public URL getURL(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getUnicodeStream(int)
   */

  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getUnicodeStream(java.lang.String)
   */

  public InputStream getUnicodeStream(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#getWarnings()
   */

  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#insertRow()
   */

  public void insertRow() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#isAfterLast()
   */

  public boolean isAfterLast() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#isBeforeFirst()
   */

  public boolean isBeforeFirst() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#isClosed()
   */

  public boolean isClosed() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#isFirst()
   */

  public boolean isFirst() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#isLast()
   */

  public boolean isLast() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#last()
   */

  public boolean last() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#moveToCurrentRow()
   */

  public void moveToCurrentRow() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#moveToInsertRow()
   */

  public void moveToInsertRow() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /**
   * Moves the cursor down one row from its current position.
   *
   * @see java.sql.ResultSet#next()
   * @throws SQLException if a database access error occurs.
   */

  public boolean next() throws SQLException {
    if (this.maxRows > 0 && this.rowsFetched >= this.maxRows) return false;

    String row_str = "";
    try {
      row_str = (String)client.fetchOne();
      this.rowsFetched++;
      if (!row_str.equals("")) {
        Object o = ds.deserialize(new BytesWritable(row_str.getBytes()));
        row = (ArrayList<?>)o;
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Error retrieving next row");
    }
    // NOTE: fetchOne dosn't throw new SQLException("Method not supported").
    return !row_str.equals("");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#previous()
   */

  public boolean previous() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#refreshRow()
   */

  public void refreshRow() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#relative(int)
   */

  public boolean relative(int rows) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#rowDeleted()
   */

  public boolean rowDeleted() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#rowInserted()
   */

  public boolean rowInserted() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#rowUpdated()
   */

  public boolean rowUpdated() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#setFetchDirection(int)
   */

  public void setFetchDirection(int direction) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#setFetchSize(int)
   */

  public void setFetchSize(int rows) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateArray(int, java.sql.Array)
   */

  public void updateArray(int columnIndex, Array x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateArray(java.lang.String, java.sql.Array)
   */

  public void updateArray(String columnName, Array x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream)
   */

  public void updateAsciiStream(int columnIndex, InputStream x)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream)
   */

  public void updateAsciiStream(String columnLabel, InputStream x)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, int)
   */

  public void updateAsciiStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream, int)
   */

  public void updateAsciiStream(String columnName, InputStream x, int length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, long)
   */

  public void updateAsciiStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream, long)
   */

  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBigDecimal(int, java.math.BigDecimal)
   */

  public void updateBigDecimal(int columnIndex, BigDecimal x)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBigDecimal(java.lang.String, java.math.BigDecimal)
   */

  public void updateBigDecimal(String columnName, BigDecimal x)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream)
   */

  public void updateBinaryStream(int columnIndex, InputStream x)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream)
   */

  public void updateBinaryStream(String columnLabel, InputStream x)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, int)
   */

  public void updateBinaryStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream, int)
   */

  public void updateBinaryStream(String columnName, InputStream x, int length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, long)
   */

  public void updateBinaryStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream, long)
   */

  public void updateBinaryStream(String columnLabel, InputStream x,
      long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBlob(int, java.sql.Blob)
   */

  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBlob(java.lang.String, java.sql.Blob)
   */

  public void updateBlob(String columnName, Blob x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBlob(int, java.io.InputStream)
   */

  public void updateBlob(int columnIndex, InputStream inputStream)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBlob(java.lang.String, java.io.InputStream)
   */

  public void updateBlob(String columnLabel, InputStream inputStream)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBlob(int, java.io.InputStream, long)
   */

  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBlob(java.lang.String, java.io.InputStream, long)
   */

  public void updateBlob(String columnLabel, InputStream inputStream,
      long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBoolean(int, boolean)
   */

  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBoolean(java.lang.String, boolean)
   */

  public void updateBoolean(String columnName, boolean x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateByte(int, byte)
   */

  public void updateByte(int columnIndex, byte x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateByte(java.lang.String, byte)
   */

  public void updateByte(String columnName, byte x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBytes(int, byte[])
   */

  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateBytes(java.lang.String, byte[])
   */

  public void updateBytes(String columnName, byte[] x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader)
   */

  public void updateCharacterStream(int columnIndex, Reader x)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader)
   */

  public void updateCharacterStream(String columnLabel, Reader reader)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, int)
   */

  public void updateCharacterStream(int columnIndex, Reader x, int length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader, int)
   */

  public void updateCharacterStream(String columnName, Reader reader,
      int length) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, long)
   */

  public void updateCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader, long)
   */

  public void updateCharacterStream(String columnLabel, Reader reader,
      long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateClob(int, java.sql.Clob)
   */

  public void updateClob(int columnIndex, Clob x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateClob(java.lang.String, java.sql.Clob)
   */

  public void updateClob(String columnName, Clob x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateClob(int, java.io.Reader)
   */

  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateClob(java.lang.String, java.io.Reader)
   */

  public void updateClob(String columnLabel, Reader reader)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateClob(int, java.io.Reader, long)
   */

  public void updateClob(int columnIndex, Reader reader, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateClob(java.lang.String, java.io.Reader, long)
   */

  public void updateClob(String columnLabel, Reader reader, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateDate(int, java.sql.Date)
   */

  public void updateDate(int columnIndex, Date x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateDate(java.lang.String, java.sql.Date)
   */

  public void updateDate(String columnName, Date x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateDouble(int, double)
   */

  public void updateDouble(int columnIndex, double x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateDouble(java.lang.String, double)
   */

  public void updateDouble(String columnName, double x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateFloat(int, float)
   */

  public void updateFloat(int columnIndex, float x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateFloat(java.lang.String, float)
   */

  public void updateFloat(String columnName, float x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateInt(int, int)
   */

  public void updateInt(int columnIndex, int x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateInt(java.lang.String, int)
   */

  public void updateInt(String columnName, int x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateLong(int, long)
   */

  public void updateLong(int columnIndex, long x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateLong(java.lang.String, long)
   */

  public void updateLong(String columnName, long x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateNCharacterStream(int, java.io.Reader)
   */

  public void updateNCharacterStream(int columnIndex, Reader x)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateNCharacterStream(java.lang.String, java.io.Reader)
   */

  public void updateNCharacterStream(String columnLabel, Reader reader)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateNCharacterStream(int, java.io.Reader, long)
   */

  public void updateNCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateNCharacterStream(java.lang.String, java.io.Reader, long)
   */

  public void updateNCharacterStream(String columnLabel, Reader reader,
      long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateNClob(int, java.sql.NClob)
   */

  public void updateNClob(int columnIndex, NClob clob) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateNClob(java.lang.String, java.sql.NClob)
   */

  public void updateNClob(String columnLabel, NClob clob) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateNClob(int, java.io.Reader)
   */

  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateNClob(java.lang.String, java.io.Reader)
   */

  public void updateNClob(String columnLabel, Reader reader)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateNClob(int, java.io.Reader, long)
   */

  public void updateNClob(int columnIndex, Reader reader, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateNClob(java.lang.String, java.io.Reader, long)
   */

  public void updateNClob(String columnLabel, Reader reader, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateNString(int, java.lang.String)
   */

  public void updateNString(int columnIndex, String string)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateNString(java.lang.String, java.lang.String)
   */

  public void updateNString(String columnLabel, String string)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateNull(int)
   */

  public void updateNull(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateNull(java.lang.String)
   */

  public void updateNull(String columnName) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateObject(int, java.lang.Object)
   */

  public void updateObject(int columnIndex, Object x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object)
   */

  public void updateObject(String columnName, Object x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateObject(int, java.lang.Object, int)
   */

  public void updateObject(int columnIndex, Object x, int scale)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object, int)
   */

  public void updateObject(String columnName, Object x, int scale)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateRef(int, java.sql.Ref)
   */

  public void updateRef(int columnIndex, Ref x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateRef(java.lang.String, java.sql.Ref)
   */

  public void updateRef(String columnName, Ref x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateRow()
   */

  public void updateRow() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateRowId(int, java.sql.RowId)
   */

  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateRowId(java.lang.String, java.sql.RowId)
   */

  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateSQLXML(int, java.sql.SQLXML)
   */

  public void updateSQLXML(int columnIndex, SQLXML xmlObject)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateSQLXML(java.lang.String, java.sql.SQLXML)
   */

  public void updateSQLXML(String columnLabel, SQLXML xmlObject)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateShort(int, short)
   */

  public void updateShort(int columnIndex, short x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateShort(java.lang.String, short)
   */

  public void updateShort(String columnName, short x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateString(int, java.lang.String)
   */

  public void updateString(int columnIndex, String x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateString(java.lang.String, java.lang.String)
   */

  public void updateString(String columnName, String x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateTime(int, java.sql.Time)
   */

  public void updateTime(int columnIndex, Time x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateTime(java.lang.String, java.sql.Time)
   */

  public void updateTime(String columnName, Time x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateTimestamp(int, java.sql.Timestamp)
   */

  public void updateTimestamp(int columnIndex, Timestamp x)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#updateTimestamp(java.lang.String, java.sql.Timestamp)
   */

  public void updateTimestamp(String columnName, Timestamp x)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.ResultSet#wasNull()
   */

  public boolean wasNull() throws SQLException {
    return this.wasNull;
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
