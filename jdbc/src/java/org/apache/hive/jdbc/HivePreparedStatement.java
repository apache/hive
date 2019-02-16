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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TSessionHandle;

/**
 * HivePreparedStatement.
 *
 */
public class HivePreparedStatement extends HiveStatement implements PreparedStatement {
  private final String sql;

  /**
   * save the SQL parameters {paramLoc:paramValue}
   */
  private final HashMap<Integer, String> parameters=new HashMap<Integer, String>();

  public HivePreparedStatement(HiveConnection connection, TCLIService.Iface client,
      TSessionHandle sessHandle, String sql) {
    super(connection, client, sessHandle);
    this.sql = sql;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#addBatch()
   */

  public void addBatch() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#clearParameters()
   */

  public void clearParameters() throws SQLException {
    this.parameters.clear();
  }

  /**
   *  Invokes executeQuery(sql) using the sql provided to the constructor.
   *
   *  @return boolean Returns true if a resultSet is created, false if not.
   *                  Note: If the result set is empty a true is returned.
   *
   *  @throws SQLException
   */

  public boolean execute() throws SQLException {
    return super.execute(updateSql(sql, parameters));
  }

  /**
   *  Invokes executeQuery(sql) using the sql provided to the constructor.
   *
   *  @return ResultSet
   *  @throws SQLException
   */

  public ResultSet executeQuery() throws SQLException {
    return super.executeQuery(updateSql(sql, parameters));
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#executeUpdate()
   */

  public int executeUpdate() throws SQLException {
    return super.executeUpdate(updateSql(sql, parameters));
  }

  /**
   * update the SQL string with parameters set by setXXX methods of {@link PreparedStatement}
   *
   * @param sql
   * @param parameters
   * @return updated SQL string
   * @throws SQLException 
   */
  private String updateSql(final String sql, HashMap<Integer, String> parameters) throws SQLException {
    List<String>  parts=splitSqlStatement(sql);
    
    StringBuilder newSql = new StringBuilder(parts.get(0));
    for(int i=1;i<parts.size();i++){
      if(!parameters.containsKey(i)){
        throw new SQLException("Parameter #"+i+" is unset");
      }
      newSql.append(parameters.get(i));
      newSql.append(parts.get(i));
    }
    return newSql.toString();

  }
  
  /**
   * Splits the parametered sql statement at parameter boundaries.
   * 
   * taking into account ' and \ escaping.
   * 
   * output for: 'select 1 from ? where a = ?'
   *  ['select 1 from ',' where a = ','']
   * 
   * @param sql
   * @return
   */
  private List<String> splitSqlStatement(String sql) {
    List<String> parts=new ArrayList<>();
    int apCount=0;
    int off=0;
    boolean skip=false;

    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if(skip){
        skip=false;
        continue;
      }
      switch (c) {
      case '\'':
        apCount++;
        break;
      case '\\':
        skip = true;
        break;
      case '?':
        if ((apCount & 1) == 0) {
          parts.add(sql.substring(off,i));
          off=i+1;
        }
        break;
      default:
        break;
      }
    }
    parts.add(sql.substring(off,sql.length()));
    return parts;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#getMetaData()
   */

  public ResultSetMetaData getMetaData() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#getParameterMetaData()
   */

  public ParameterMetaData getParameterMetaData() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setArray(int, java.sql.Array)
   */

  public void setArray(int i, Array x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream)
   */

  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream,
   * int)
   */

  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream,
   * long)
   */

  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setBigDecimal(int, java.math.BigDecimal)
   */

  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    this.parameters.put(parameterIndex, x.toString());
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream)
   */

  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    String str = new Scanner(x, "UTF-8").useDelimiter("\\A").next();
    setString(parameterIndex, str);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream,
   * int)
   */

  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream,
   * long)
   */

  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setBlob(int, java.sql.Blob)
   */

  public void setBlob(int i, Blob x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setBlob(int, java.io.InputStream)
   */

  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setBlob(int, java.io.InputStream, long)
   */

  public void setBlob(int parameterIndex, InputStream inputStream, long length)
          throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setBoolean(int, boolean)
   */

  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    this.parameters.put(parameterIndex, ""+x);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setByte(int, byte)
   */

  public void setByte(int parameterIndex, byte x) throws SQLException {
    this.parameters.put(parameterIndex, ""+x);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setBytes(int, byte[])
   */

  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader)
   */

  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader,
   * int)
   */

  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader,
   * long)
   */

  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setClob(int, java.sql.Clob)
   */

  public void setClob(int i, Clob x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setClob(int, java.io.Reader)
   */

  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setClob(int, java.io.Reader, long)
   */

  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setDate(int, java.sql.Date)
   */

  public void setDate(int parameterIndex, Date x) throws SQLException {
    this.parameters.put(parameterIndex, "'" + x.toString() + "'");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setDate(int, java.sql.Date,
   * java.util.Calendar)
   */

  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setDouble(int, double)
   */

  public void setDouble(int parameterIndex, double x) throws SQLException {
    this.parameters.put(parameterIndex,""+x);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setFloat(int, float)
   */

  public void setFloat(int parameterIndex, float x) throws SQLException {
    this.parameters.put(parameterIndex,""+x);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setInt(int, int)
   */

  public void setInt(int parameterIndex, int x) throws SQLException {
    this.parameters.put(parameterIndex,""+x);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setLong(int, long)
   */

  public void setLong(int parameterIndex, long x) throws SQLException {
    this.parameters.put(parameterIndex,""+x);
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setNCharacterStream(int, java.io.Reader)
   */

  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setNCharacterStream(int, java.io.Reader,
   * long)
   */

  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setNClob(int, java.sql.NClob)
   */

  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setNClob(int, java.io.Reader)
   */

  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setNClob(int, java.io.Reader, long)
   */

  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setNString(int, java.lang.String)
   */

  public void setNString(int parameterIndex, String value) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setNull(int, int)
   */

  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    this.parameters.put(parameterIndex, "NULL");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setNull(int, int, java.lang.String)
   */

  public void setNull(int paramIndex, int sqlType, String typeName) throws SQLException {
    this.parameters.put(paramIndex, "NULL");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setObject(int, java.lang.Object)
   */

  public void setObject(int parameterIndex, Object x) throws SQLException {
    if (x == null) {
      setNull(parameterIndex, Types.NULL);
    } else if (x instanceof String) {
      setString(parameterIndex, (String) x);
    } else if (x instanceof Short) {
      setShort(parameterIndex, ((Short) x).shortValue());
    } else if (x instanceof Integer) {
      setInt(parameterIndex, ((Integer) x).intValue());
    } else if (x instanceof Long) {
      setLong(parameterIndex, ((Long) x).longValue());
    } else if (x instanceof Float) {
      setFloat(parameterIndex, ((Float) x).floatValue());
    } else if (x instanceof Double) {
      setDouble(parameterIndex, ((Double) x).doubleValue());
    } else if (x instanceof Boolean) {
      setBoolean(parameterIndex, ((Boolean) x).booleanValue());
    } else if (x instanceof Byte) {
      setByte(parameterIndex, ((Byte) x).byteValue());
    } else if (x instanceof Character) {
      setString(parameterIndex, x.toString());
    } else if (x instanceof Timestamp) {
      setTimestamp(parameterIndex, (Timestamp) x);
    } else if (x instanceof BigDecimal) {
      setString(parameterIndex, x.toString());
    } else {
      // Can't infer a type.
      throw new SQLException(
          MessageFormat
              .format(
                  "Can''t infer the SQL type to use for an instance of {0}. Use setObject() with an explicit Types value to specify the type to use.",
                  x.getClass().getName()));
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int)
   */

  public void setObject(int parameterIndex, Object x, int targetSqlType)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int, int)
   */

  public void setObject(int parameterIndex, Object x, int targetSqlType, int scale)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setRef(int, java.sql.Ref)
   */

  public void setRef(int i, Ref x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setRowId(int, java.sql.RowId)
   */

  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setSQLXML(int, java.sql.SQLXML)
   */

  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setShort(int, short)
   */

  public void setShort(int parameterIndex, short x) throws SQLException {
    this.parameters.put(parameterIndex,""+x);
  }

  private String replaceBackSlashSingleQuote(String x) {
    // scrutinize escape pair, specifically, replace \' to '
    StringBuffer newX = new StringBuffer();
    for (int i = 0; i < x.length(); i++) {
      char c = x.charAt(i);
      if (c == '\\' && i < x.length()-1) {
        char c1 = x.charAt(i+1);
        if (c1 == '\'') {
          newX.append(c1);
        } else {
          newX.append(c);
          newX.append(c1);
        }
        i++;
      } else {
        newX.append(c);
      }
    }
    return newX.toString();
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setString(int, java.lang.String)
   */

  public void setString(int parameterIndex, String x) throws SQLException {
    x = replaceBackSlashSingleQuote(x);
    x=x.replace("'", "\\'");
    this.parameters.put(parameterIndex, "'"+x+"'");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setTime(int, java.sql.Time)
   */

  public void setTime(int parameterIndex, Time x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setTime(int, java.sql.Time,
   * java.util.Calendar)
   */

  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp)
   */

  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    this.parameters.put(parameterIndex, "'" + x.toString() + "'");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp,
   * java.util.Calendar)
   */

  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setURL(int, java.net.URL)
   */

  public void setURL(int parameterIndex, URL x) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.PreparedStatement#setUnicodeStream(int, java.io.InputStream,
   * int)
   */

  public void setUnicodeStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLFeatureNotSupportedException("Method not supported");
  }
}
