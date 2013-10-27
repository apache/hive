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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.HashMap;
import java.util.Map;

import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCancelOperationReq;
import org.apache.hive.service.cli.thrift.TCancelOperationResp;
import org.apache.hive.service.cli.thrift.TCloseOperationReq;
import org.apache.hive.service.cli.thrift.TCloseOperationResp;
import org.apache.hive.service.cli.thrift.TExecuteStatementReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementResp;
import org.apache.hive.service.cli.thrift.TGetOperationStatusReq;
import org.apache.hive.service.cli.thrift.TGetOperationStatusResp;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TSessionHandle;

/**
 * HiveStatement.
 *
 */
public class HiveStatement implements java.sql.Statement {
  private final HiveConnection connection;
  private TCLIService.Iface client;
  private TOperationHandle stmtHandle = null;
  private final TSessionHandle sessHandle;
  Map<String,String> sessConf = new HashMap<String,String>();
  private int fetchSize = 50;
  /**
   * We need to keep a reference to the result set to support the following:
   * <code>
   * statement.execute(String sql);
   * statement.getResultSet();
   * </code>.
   */
  private ResultSet resultSet = null;

  /**
   * Sets the limit for the maximum number of rows that any ResultSet object produced by this
   * Statement can contain to the given number. If the limit is exceeded, the excess rows
   * are silently dropped. The value must be >= 0, and 0 means there is not limit.
   */
  private int maxRows = 0;

  /**
   * Add SQLWarnings to the warningChain if needed.
   */
  private SQLWarning warningChain = null;

  /**
   * Keep state so we can fail certain calls made after close().
   */
  private boolean isClosed = false;

  /**
   *
   */
  public HiveStatement(HiveConnection connection, TCLIService.Iface client,
      TSessionHandle sessHandle) {
    this.connection = connection;
    this.client = client;
    this.sessHandle = sessHandle;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#addBatch(java.lang.String)
   */

  public void addBatch(String sql) throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#cancel()
   */

  public void cancel() throws SQLException {
    if (isClosed) {
      throw new SQLException("Can't cancel after statement has been closed");
    }

    if (stmtHandle == null) {
      return;
    }

    TCancelOperationReq cancelReq = new TCancelOperationReq();
    cancelReq.setOperationHandle(stmtHandle);
    try {
      TCancelOperationResp cancelResp = client.CancelOperation(cancelReq);
      Utils.verifySuccessWithInfo(cancelResp.getStatus());
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException(e.toString(), "08S01", e);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#clearBatch()
   */

  public void clearBatch() throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#clearWarnings()
   */

  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  void closeClientOperation() throws SQLException {
    try {
      if (stmtHandle != null) {
        TCloseOperationReq closeReq = new TCloseOperationReq();
        closeReq.setOperationHandle(stmtHandle);
        TCloseOperationResp closeResp = client.CloseOperation(closeReq);
        Utils.verifySuccessWithInfo(closeResp.getStatus());
      }
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException(e.toString(), "08S01", e);
    }
    stmtHandle = null;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#close()
   */
  public void close() throws SQLException {
    if (isClosed) {
      return;
    }
    if (stmtHandle != null) {
      closeClientOperation();
    }
    client = null;
    resultSet = null;
    isClosed = true;
  }

  public void closeOnCompletion() throws SQLException {
    // JDK 1.7
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#execute(java.lang.String)
   */

  public boolean execute(String sql) throws SQLException {
    if (isClosed) {
      throw new SQLException("Can't execute after statement has been closed");
    }

    try {
      if (stmtHandle != null) {
        closeClientOperation();
      }

      TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, sql);
      execReq.setConfOverlay(sessConf);
      TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
      Utils.verifySuccessWithInfo(execResp.getStatus());
      stmtHandle = execResp.getOperationHandle();
    } catch (SQLException eS) {
      throw eS;
    } catch (Exception ex) {
      throw new SQLException(ex.toString(), "08S01", ex);
    }

    if (!stmtHandle.isHasResultSet()) {
      // Poll until the query has completed one way or another. DML queries will not return a result
      // set, but we should not return from this method until the query has completed to avoid
      // racing with possible subsequent session shutdown, or queries that depend on the results
      // materialised here.
      TGetOperationStatusReq statusReq = new TGetOperationStatusReq(stmtHandle);
      boolean requestComplete = false;
      while (!requestComplete) {
        try {
          TGetOperationStatusResp statusResp = client.GetOperationStatus(statusReq);
          Utils.verifySuccessWithInfo(statusResp.getStatus());
          if (statusResp.isSetOperationState()) {
            switch (statusResp.getOperationState()) {
            case CLOSED_STATE:
            case FINISHED_STATE:
              return false;
            case CANCELED_STATE:
              // 01000 -> warning
              throw new SQLException("Query was cancelled", "01000");
            case ERROR_STATE:
              // HY000 -> general error
              throw new SQLException("Query failed", "HY000");
            case UKNOWN_STATE:
              throw new SQLException("Unknown query", "HY000");
            case INITIALIZED_STATE:
            case RUNNING_STATE:
              break;
            }
          }
        } catch (Exception ex) {
          throw new SQLException(ex.toString(), "08S01", ex);
        }

        try {
          Thread.sleep(100);
        } catch (InterruptedException ex) {
          // Ignore
        }
      }
      return false;
    }
    resultSet =  new HiveQueryResultSet.Builder(this).setClient(client).setSessionHandle(sessHandle)
        .setStmtHandle(stmtHandle).setMaxRows(maxRows).setFetchSize(fetchSize)
        .build();
    return true;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#execute(java.lang.String, int)
   */

  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#execute(java.lang.String, int[])
   */

  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
   */

  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#executeBatch()
   */

  public int[] executeBatch() throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#executeQuery(java.lang.String)
   */

  public ResultSet executeQuery(String sql) throws SQLException {
    if (!execute(sql)) {
      throw new SQLException("The query did not generate a result set!");
    }
    return resultSet;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#executeUpdate(java.lang.String)
   */

  public int executeUpdate(String sql) throws SQLException {
    execute(sql);
    return 0;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#executeUpdate(java.lang.String, int)
   */

  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
   */

  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#executeUpdate(java.lang.String, java.lang.String[])
   */

  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#getConnection()
   */

  public Connection getConnection() throws SQLException {
    return this.connection;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#getFetchDirection()
   */

  public int getFetchDirection() throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#getFetchSize()
   */

  public int getFetchSize() throws SQLException {
    return fetchSize;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#getGeneratedKeys()
   */

  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#getMaxFieldSize()
   */

  public int getMaxFieldSize() throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#getMaxRows()
   */

  public int getMaxRows() throws SQLException {
    return maxRows;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#getMoreResults()
   */

  public boolean getMoreResults() throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#getMoreResults(int)
   */

  public boolean getMoreResults(int current) throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#getQueryTimeout()
   */

  public int getQueryTimeout() throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#getResultSet()
   */

  public ResultSet getResultSet() throws SQLException {
    return resultSet;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#getResultSetConcurrency()
   */

  public int getResultSetConcurrency() throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#getResultSetHoldability()
   */

  public int getResultSetHoldability() throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#getResultSetType()
   */

  public int getResultSetType() throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#getUpdateCount()
   */

  public int getUpdateCount() throws SQLException {
    return 0;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#getWarnings()
   */

  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#isClosed()
   */

  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  public boolean isCloseOnCompletion() throws SQLException {
    // JDK 1.7
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#isPoolable()
   */

  public boolean isPoolable() throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#setCursorName(java.lang.String)
   */

  public void setCursorName(String name) throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#setEscapeProcessing(boolean)
   */

  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#setFetchDirection(int)
   */

  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#setFetchSize(int)
   */

  public void setFetchSize(int rows) throws SQLException {
    fetchSize = rows;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#setMaxFieldSize(int)
   */

  public void setMaxFieldSize(int max) throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#setMaxRows(int)
   */

  public void setMaxRows(int max) throws SQLException {
    if (max < 0) {
      throw new SQLException("max must be >= 0");
    }
    maxRows = max;
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#setPoolable(boolean)
   */

  public void setPoolable(boolean poolable) throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Statement#setQueryTimeout(int)
   */

  public void setQueryTimeout(int seconds) throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
   */

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException("Method not supported");
  }

  /*
   * (non-Javadoc)
   *
   * @see java.sql.Wrapper#unwrap(java.lang.Class)
   */

  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException("Method not supported");
  }

}
