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

package org.apache.hive.hplsql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;

import org.antlr.v4.runtime.ParserRuleContext;

public class Query {
  String sql;
  ParserRuleContext sqlExpr;
  ParserRuleContext sqlSelect;  
  
  Connection conn;
  Statement stmt;
  PreparedStatement pstmt;
  ResultSet rs;
  Exception exception;

  public enum State { OPEN, FETCHED_OK, FETCHED_NODATA, CLOSE };
  State state = State.CLOSE;
  
  boolean withReturn = false;
  
  Query() {
  }
  
  Query(String sql) {
    this.sql = sql;
  }  
 
  /** 
   * Set query objects
   */
  public void set(Connection conn, Statement stmt, ResultSet rs) {
    this.conn = conn;
    this.stmt = stmt;
    this.rs = rs;
    if (rs != null) {
      state = State.OPEN;
    }
  }
  
  public void set(Connection conn, PreparedStatement pstmt) {
    this.conn = conn;
    this.pstmt = pstmt;
  }
  
  /**
   * Set the fetch status
   */
  public void setFetch(boolean ok) {
    if (ok == true) {
      state = State.FETCHED_OK;
    }
    else {
      state = State.FETCHED_NODATA;
    }
  }
  
  /**
   * Get the number of rows
   */
  public int getRowCount() {
    if (!error() && stmt != null) {
      try {
        return stmt.getUpdateCount();
      } catch (SQLException e) {}
    }
    return -1;
  }
  
  /**
   * Check if the cursor is open
   */
  public boolean isOpen() {
    if (rs != null) {
      return true;
    }
    return false;
  }
  
  /**
   * Check if the cursor was fetched and a row was returned
   */
  public Boolean isFound() {
    if (state == State.OPEN || state == State.CLOSE) {
      return null;
    }
    if (state == State.FETCHED_OK) {
      return Boolean.valueOf(true);
    } 
    return Boolean.valueOf(false);    
  }
  
  /**
   * Check if the cursor was fetched and no row was returned
   */
  public Boolean isNotFound() {
    if (state == State.OPEN || state == State.CLOSE) {
      return null;
    }
    if (state == State.FETCHED_NODATA) {
      return Boolean.valueOf(true);
    }
    return Boolean.valueOf(false);
  }
  
  /**
   * Close statement results
   */
  public void closeStatement() {
    try {
      if(rs != null) {
        rs.close();
        rs = null;
      }
      if(stmt != null) {
        stmt.close();
        stmt = null;
      }
      if(pstmt != null) {
        pstmt.close();
        pstmt = null;
      }
      state = State.CLOSE;
    } catch (SQLException e) {
      e.printStackTrace();
    }   
  }
  
  /**
   * Set SQL statement
   */
  public void setSql(String sql) {
    this.sql = sql;
  }
  
  /**
   * Set expression context
   */
  public void setExprCtx(ParserRuleContext sqlExpr) {
    this.sqlExpr = sqlExpr;
  }

  /**
   * Set SELECT statement context
   */
  public void setSelectCtx(ParserRuleContext sqlSelect) {
    this.sqlSelect = sqlSelect;
  }
  
  /**
   * Set whether the cursor is returned to the caller
   */
  public void setWithReturn(boolean withReturn) {
    this.withReturn = withReturn;
  }
  
  /**
   * Set an execution error
   */
  public void setError(Exception e) {
    exception = e;
  }
  
  /**
   * Print error stack trace
   */
  public void printStackTrace() {
    if(exception != null) {
      exception.printStackTrace();
    }
  }
  
  /**
   * Get the result set object
   */
  public ResultSet getResultSet() {
    return rs;
  }
  
  /**
   * Get the prepared statement object
   */
  public PreparedStatement getPreparedStatement() {
    return pstmt;
  }
  
  /**
   * Get the connection object
   */
  public Connection getConnection() {
    return conn;
  }
  
  /**
   * Check if the cursor defined as a return cursor to client
   */
  public boolean getWithReturn() {
    return withReturn;
  }
  
  /**
   * Return error information
   */
  public boolean error() {
    return exception != null;
  }
  
  public String errorText() {
    if(exception != null) {
      if(exception instanceof ClassNotFoundException) {
        return "ClassNotFoundException: " + exception.getMessage();
      }
      return exception.getMessage();
    }
    return "";
  }
  
  public Exception getException() {
    return exception;
  }
}
