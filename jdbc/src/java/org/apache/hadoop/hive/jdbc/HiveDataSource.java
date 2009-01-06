/**
 *
 */
package org.apache.hadoop.hive.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;


public class HiveDataSource implements DataSource {

  /**
   *
   */
  public HiveDataSource() {
    // TODO Auto-generated constructor stub
  }

  /* (non-Javadoc)
   * @see javax.sql.DataSource#getConnection()
   */

  public Connection getConnection() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
   */

  public Connection getConnection(String username, String password)
      throws SQLException {
    try {
      return new HiveConnection("", null);
    } catch (Exception ex) {
      throw new SQLException();
    }
  }

  /* (non-Javadoc)
   * @see javax.sql.CommonDataSource#getLogWriter()
   */

  public PrintWriter getLogWriter() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see javax.sql.CommonDataSource#getLoginTimeout()
   */

  public int getLoginTimeout() throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see javax.sql.CommonDataSource#setLogWriter(java.io.PrintWriter)
   */

  public void setLogWriter(PrintWriter arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see javax.sql.CommonDataSource#setLoginTimeout(int)
   */

  public void setLoginTimeout(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
   */

  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /* (non-Javadoc)
   * @see java.sql.Wrapper#unwrap(java.lang.Class)
   */

  public <T> T unwrap(Class<T> arg0) throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

}
