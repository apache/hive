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
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;

public class JDBCStatsPublisher implements StatsPublisher {

  private Connection conn;
  private String connectionString;
  private Configuration hiveconf;
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
  private PreparedStatement updStmt, insStmt;
  private int timeout; // default timeout in sec. for JDBC connection and statements
  // SQL comment that identifies where the SQL statement comes from
  private final String comment = "Hive stats publishing: " + this.getClass().getName();
  private int maxRetries;
  private long waitWindow;
  private final Random r;

  public JDBCStatsPublisher() {
    r = new Random();
  }

  @Override
  public boolean connect(Configuration hiveconf) {
    this.hiveconf = hiveconf;
    maxRetries = HiveConf.getIntVar(hiveconf, HiveConf.ConfVars.HIVE_STATS_RETRIES_MAX);
    waitWindow = HiveConf.getTimeVar(
        hiveconf, HiveConf.ConfVars.HIVE_STATS_RETRIES_WAIT, TimeUnit.MILLISECONDS);
    connectionString = HiveConf.getVar(hiveconf, HiveConf.ConfVars.HIVESTATSDBCONNECTIONSTRING);
    timeout = (int) HiveConf.getTimeVar(
        hiveconf, HiveConf.ConfVars.HIVE_STATS_JDBC_TIMEOUT, TimeUnit.SECONDS);
    String driver = HiveConf.getVar(hiveconf, HiveConf.ConfVars.HIVESTATSJDBCDRIVER);

    try {
      Class.forName(driver).newInstance();
    } catch (Exception e) {
      LOG.error("Error during instantiating JDBC driver " + driver + ". ", e);
      return false;
    }

    DriverManager.setLoginTimeout(timeout); // stats is non-blocking

    // function pointer for executeWithRetry to setQueryTimeout
    Utilities.SQLCommand<Void> setQueryTimeout = new Utilities.SQLCommand<Void>() {
      @Override
      public Void run(PreparedStatement stmt) throws SQLException {
        stmt.setQueryTimeout(timeout);
        return null;
      }
    };

    for (int failures = 0;; failures++) {
      try {
        conn = Utilities.connectWithRetry(connectionString, waitWindow, maxRetries);

        // prepare statements
        updStmt = Utilities.prepareWithRetry(conn, JDBCStatsUtils.getUpdate(comment), waitWindow,
            maxRetries);
        insStmt = Utilities.prepareWithRetry(conn, JDBCStatsUtils.getInsert(comment), waitWindow,
            maxRetries);

        // set query timeout
        Utilities.executeWithRetry(setQueryTimeout, updStmt, waitWindow, maxRetries);
        Utilities.executeWithRetry(setQueryTimeout, insStmt, waitWindow, maxRetries);


        return true;
      } catch (SQLRecoverableException e) {
        if (failures >= maxRetries) {
          LOG.error("Error during JDBC connection to " + connectionString + ". ", e);
          return false; // just return false without fail the task
        }
        long waitTime = Utilities.getRandomWaitTime(waitWindow, failures, r);
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException e1) {
        }
      } catch (SQLException e) {
        // for SQLTransientException (maxRetries already achieved at Utilities retry functions
        // or SQLNonTransientException, declare a real failure
        LOG.error("Error during JDBC connection to " + connectionString + ". ", e);
        return false;
      }
    }
  }

  @Override
  public boolean publishStat(String fileID, Map<String, String> stats) {

    if (stats.isEmpty()) {
      // If there are no stats to publish, nothing to do.
      return true;
    }

    if (conn == null) {
      LOG.error("JDBC connection is null. Cannot publish stats without JDBC connection.");
      return false;
    }

    if (!JDBCStatsUtils.isValidStatisticSet(stats.keySet())) {
      LOG.warn("Invalid statistic:" + stats.keySet().toString() + ", supported "
          + " stats: " + JDBCStatsUtils.getSupportedStatistics());
      return false;
    }
    JDBCStatsUtils.validateRowId(fileID);
    if (LOG.isInfoEnabled()) {
      LOG.info("Stats publishing for key " + fileID);
    }

    Utilities.SQLCommand<Void> execUpdate = new Utilities.SQLCommand<Void>() {
      @Override
      public Void run(PreparedStatement stmt) throws SQLException {
        stmt.executeUpdate();
        return null;
      }
    };

    List<String> supportedStatistics = JDBCStatsUtils.getSupportedStatistics();

    for (int failures = 0;; failures++) {
      try {
        insStmt.setString(1, fileID);
        for (int i = 0; i < JDBCStatsUtils.getSupportedStatistics().size(); i++) {
          insStmt.setString(i + 2, stats.get(supportedStatistics.get(i)));
        }
        Utilities.executeWithRetry(execUpdate, insStmt, waitWindow, maxRetries);
        return true;
      } catch (SQLIntegrityConstraintViolationException e) {

        // We assume that the table used for partial statistics has a primary key declared on the
        // "fileID". The exception will be thrown if two tasks report results for the same fileID.
        // In such case, we either update the row, or abandon changes depending on which statistic
        // is newer.

        for (int updateFailures = 0;; updateFailures++) {
          try {
            int i;
            for (i = 0; i < JDBCStatsUtils.getSupportedStatistics().size(); i++) {
              updStmt.setString(i + 1, stats.get(supportedStatistics.get(i)));
            }
            updStmt.setString(supportedStatistics.size() + 1, fileID);
            updStmt.setString(supportedStatistics.size() + 2,
                stats.get(JDBCStatsUtils.getBasicStat()));
            updStmt.setString(supportedStatistics.size() + 3, fileID);
            Utilities.executeWithRetry(execUpdate, updStmt, waitWindow, maxRetries);
            return true;
          } catch (SQLRecoverableException ue) {
            // need to start from scratch (connection)
            if (!handleSQLRecoverableException(ue, updateFailures)) {
              return false;
            }
          } catch (SQLException ue) {
            LOG.error("Error during publishing statistics. ", e);
            return false;
          }
        }

      } catch (SQLRecoverableException e) {
        // need to start from scratch (connection)
        if (!handleSQLRecoverableException(e, failures)) {
          return false;
        }
      } catch (SQLException e) {
        LOG.error("Error during publishing statistics. ", e);
        return false;
      }
    }
  }

  private boolean handleSQLRecoverableException(Exception e, int failures) {
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
    return true;
  }

  @Override
  public boolean closeConnection() {
    if (conn == null) {
      return true;
    }
    try {
      if (updStmt != null) {
        updStmt.close();
      }
      if (insStmt != null) {
        insStmt.close();
      }

      conn.close();

      // In case of derby, explicitly shutdown the database otherwise it reports error when
      // trying to connect to the same JDBC connection string again.
      if (HiveConf.getVar(hiveconf, HiveConf.ConfVars.HIVESTATSDBCLASS).equalsIgnoreCase(
          "jdbc:derby")) {
        try {
          // The following closes the derby connection. It throws an exception that has to be caught
          // and ignored.
          synchronized(DriverManager.class) {
            DriverManager.getConnection(connectionString + ";shutdown=true");
          }
        } catch (Exception e) {
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
    Statement stmt = null;
    ResultSet rs = null;
    try {
      this.hiveconf = hconf;
      connectionString = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVESTATSDBCONNECTIONSTRING);
      String driver = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVESTATSJDBCDRIVER);
      Class.forName(driver).newInstance();
      synchronized(DriverManager.class) {
        DriverManager.setLoginTimeout(timeout);
        conn = DriverManager.getConnection(connectionString);

        stmt = conn.createStatement();
        stmt.setQueryTimeout(timeout);

        // TODO: why is this not done using Hive db scripts?
        // Check if the table exists
        DatabaseMetaData dbm = conn.getMetaData();
        String tableName = JDBCStatsUtils.getStatTableName();
        rs = dbm.getTables(null, null, tableName, null);
        boolean tblExists = rs.next();
        if (!tblExists) { // Table does not exist, create it
          String createTable = JDBCStatsUtils.getCreate("");
          stmt.executeUpdate(createTable);
        } else {
          // Upgrade column name to allow for longer paths.
          String idColName = JDBCStatsUtils.getIdColumnName();
          int colSize = -1;
          try {
            rs.close();
            rs = dbm.getColumns(null, null, tableName, idColName);
            if (rs.next()) {
              colSize = rs.getInt("COLUMN_SIZE");
              if (colSize < JDBCStatsSetupConstants.ID_COLUMN_VARCHAR_SIZE) {
                String alterTable = JDBCStatsUtils.getAlterIdColumn();
                  stmt.executeUpdate(alterTable);
              }
            } else {
              LOG.warn("Failed to update " + idColName + " - column not found");
            }
          } catch (Throwable t) {
            LOG.warn("Failed to update " + idColName + " (size "
                + (colSize == -1 ? "unknown" : colSize) + ")", t);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Error during JDBC initialization. ", e);
      return false;
    } finally {
      if(rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          // do nothing
        }
      }
      if(stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          // do nothing
        }
      }
      closeConnection();
    }
    return true;
  }

}
