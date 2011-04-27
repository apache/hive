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

package org.apache.hadoop.hive.ql.stats.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.ql.stats.StatsSetupConst;

public class JDBCStatsPublisher implements StatsPublisher {

  private Connection conn;
  private String connectionString;
  private Configuration hiveconf;
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
  private PreparedStatement selStmt, updStmt, insStmt;
  private int timeout; // default timeout in sec. for JDBC connection and statements
  // SQL comment that identifies where the SQL statement comes from
  private final String comment = "Hive stats publishing: " + this.getClass().getName();
  private int maxRetries, waitWindow;
  private final Random r;

  public JDBCStatsPublisher() {
    r = new Random();
  }

  @Override
  public boolean connect(Configuration hiveconf) {
    this.hiveconf = hiveconf;
    maxRetries = HiveConf.getIntVar(hiveconf, HiveConf.ConfVars.HIVE_STATS_RETRIES_MAX);
    waitWindow = HiveConf.getIntVar(hiveconf, HiveConf.ConfVars.HIVE_STATS_RETRIES_WAIT);
    connectionString = HiveConf.getVar(hiveconf, HiveConf.ConfVars.HIVESTATSDBCONNECTIONSTRING);
    timeout = HiveConf.getIntVar(hiveconf, HiveConf.ConfVars.HIVE_STATS_JDBC_TIMEOUT);
    String driver = HiveConf.getVar(hiveconf, HiveConf.ConfVars.HIVESTATSJDBCDRIVER);

    try {
      Class.forName(driver).newInstance();
    } catch (Exception e) {
      LOG.error("Error during instantiating JDBC driver " + driver + ". ", e);
      return false;
    }

    // prepare the SELECT/UPDATE/INSERT statements
    String select =
      "SELECT /* " + comment + " */ " + JDBCStatsSetupConstants.PART_STAT_ROW_COUNT_COLUMN_NAME +
      " FROM " + JDBCStatsSetupConstants.PART_STAT_TABLE_NAME +
      " WHERE " + JDBCStatsSetupConstants.PART_STAT_ID_COLUMN_NAME + " = ?";

    String update =
      "UPDATE /* " + comment + " */ "+ JDBCStatsSetupConstants.PART_STAT_TABLE_NAME +
      " SET " +  JDBCStatsSetupConstants.PART_STAT_ROW_COUNT_COLUMN_NAME + "= ? " +
      " WHERE " + JDBCStatsSetupConstants.PART_STAT_ID_COLUMN_NAME + " = ?";

    String insert =
      "INSERT INTO /* " + comment + " */ " + JDBCStatsSetupConstants.PART_STAT_TABLE_NAME +
      " VALUES (?, ?)";

    DriverManager.setLoginTimeout(timeout); // stats is non-blocking

    // function pointer for executeWithRetry to setQueryTimeout
    Utilities.SQLCommand<Void> setQueryTimeout = new Utilities.SQLCommand<Void>() {
      @Override
      public Void run(PreparedStatement stmt) throws SQLException {
        stmt.setQueryTimeout(timeout);
        return null;
      }
    };

    for (int failures = 0; ; failures++) {
      try {
        conn = Utilities.connectWithRetry(connectionString, waitWindow, maxRetries);

        // prepare statements
        selStmt = Utilities.prepareWithRetry(conn, select, waitWindow, maxRetries);
        updStmt = Utilities.prepareWithRetry(conn, update, waitWindow, maxRetries);
        insStmt = Utilities.prepareWithRetry(conn, insert, waitWindow, maxRetries);

        // set query timeout
        Utilities.executeWithRetry(setQueryTimeout, selStmt, waitWindow, maxRetries);
        Utilities.executeWithRetry(setQueryTimeout, updStmt, waitWindow, maxRetries);
        Utilities.executeWithRetry(setQueryTimeout, insStmt, waitWindow, maxRetries);

        return true;
      } catch (SQLRecoverableException e) {
        if (failures >= maxRetries) {
          LOG.error("Error during JDBC connection to " + connectionString + ". ", e);
          return false;  // just return false without fail the task
        }
        long waitTime = Utilities.getRandomWaitTime(waitWindow, failures, r);
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException e1) {
        }
      } catch (SQLException e) {
        // for SQLTransientException (maxRetries already achieved at Utilities retry functions
        // or SQLNonTransientException, declare a real failure
        return false;
      }
    }
  }

  @Override
  public boolean publishStat(String fileID, String statType, String value) {

    if (conn == null) {
      LOG.error("JDBC connection is null. Cannot publish stats without JDBC connection.");
      return false;
    }

    if (statType != StatsSetupConst.ROW_COUNT) {
      LOG.warn("Warning. Invalid statistic. Currently " +
          "row count is the only supported statistic");
      return false;
    }
    LOG.info("Stats publishing for key " + fileID + ". Value = " + value);

    Utilities.SQLCommand<ResultSet> execQuery = new Utilities.SQLCommand<ResultSet>() {
      @Override
      public ResultSet run(PreparedStatement stmt) throws SQLException {
        return stmt.executeQuery();
      }
    };

    Utilities.SQLCommand<Void> execUpdate = new Utilities.SQLCommand<Void>() {
      @Override
      public Void run(PreparedStatement stmt) throws SQLException {
        stmt.executeUpdate();
        return null;
      }
    };

    for (int failures = 0; ; failures++) {
      try {

        // Check to see if a previous task (mapper attempt) had published a previous stat
        selStmt.setString(1, fileID);
        ResultSet result = Utilities.executeWithRetry(execQuery, selStmt, waitWindow, maxRetries);

        if (result.next()) {
          long currval = result.getLong(1);
          // Only update if the previous value is smaller (i.e. the previous attempt was a fail and
          // hopefully this attempt is a success (as it has a greater value).
          if (currval < Long.parseLong(value)) {
            updStmt.setString(1, value);
            updStmt.setString(2, fileID);
            Utilities.executeWithRetry(execUpdate, updStmt, waitWindow, maxRetries);
          }
        } else {
          // No previous attempts.
          insStmt.setString(1, fileID);
          insStmt.setString(2, value);
          Utilities.executeWithRetry(execUpdate, insStmt, waitWindow, maxRetries);
        }
        return true;
      } catch (SQLRecoverableException e) {
        // need to start from scratch (connection)
        if (failures >= maxRetries) {
          return false;
        }
        // close the current connection
        closeConnection();
        long waitTime = Utilities.getRandomWaitTime(waitWindow, failures, r);
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException iex) {
        }
        // get a new connection
        if (!connect(hiveconf)) {
          // if cannot reconnect, just fail because connect() already handles retries.
          LOG.error("Error during publishing aggregation. " + e);
          return false;
        }
      } catch (SQLException e) {
        LOG.error("Error during publishing statistics. ", e);
        return false;
      }
    }
  }

  @Override
  public boolean closeConnection() {
    if (conn == null) {
      return true;
    }
    try {
      if (insStmt != null) {
        insStmt.close();
      }
      if (updStmt != null) {
        updStmt.close();
      }
      if (selStmt != null) {
        selStmt.close();
      }

      conn.close();

      // In case of derby, explicitly shutdown the database otherwise it reports error when
      // trying to connect to the same JDBC connection string again.
      if(HiveConf.getVar(hiveconf, HiveConf.ConfVars.HIVESTATSDBCLASS).equalsIgnoreCase("jdbc:derby")) {
        try {
          // The following closes the derby connection. It throws an exception that has to be caught and ignored.
          DriverManager.getConnection(connectionString + ";shutdown=true");
        }
        catch (Exception e) {
          // Do nothing because we know that an exception is thrown anyway.
        }
      }
      return true;
    } catch (SQLException e) {
      LOG.error("Error during JDBC termination. ", e);
      return false;
    }
  }

  /**
   * Initialize the intermediate stats DB for the first time it is running (e.g.,
   * creating tables.).
   */
  @Override
  public boolean init(Configuration hconf) {
    try {
      this.hiveconf = hconf;
      connectionString = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVESTATSDBCONNECTIONSTRING);
      String driver = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVESTATSJDBCDRIVER);
      Class.forName(driver).newInstance();
      DriverManager.setLoginTimeout(timeout);
      conn = DriverManager.getConnection(connectionString);

      Statement stmt = conn.createStatement();
      stmt.setQueryTimeout(timeout);

      // Check if the table exists
      DatabaseMetaData dbm = conn.getMetaData();
      ResultSet rs = dbm.getTables(null, null, JDBCStatsSetupConstants.PART_STAT_TABLE_NAME, null);
      boolean tblExists = rs.next();
      if (!tblExists) { // Table does not exist, create it
        String createTable =
          "CREATE TABLE " + JDBCStatsSetupConstants.PART_STAT_TABLE_NAME + " (" +
          JDBCStatsSetupConstants.PART_STAT_ID_COLUMN_NAME + " VARCHAR(255), " +
          JDBCStatsSetupConstants.PART_STAT_ROW_COUNT_COLUMN_NAME + " BIGINT)";

        stmt.executeUpdate(createTable);
        stmt.close();
      }

      closeConnection();
    } catch (Exception e) {
      LOG.error("Error during JDBC initialization. ", e);
      return false;
    }
    return true;
  }
}
