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

package org.apache.hive.hplsql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.antlr.v4.runtime.ParserRuleContext;

public class Query {
  String sql;
  ParserRuleContext sqlExpr;
  ParserRuleContext sqlSelect;  
  
  Connection conn;
  Statement stmt;
  ResultSet rs;
  Exception exception;
  
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
