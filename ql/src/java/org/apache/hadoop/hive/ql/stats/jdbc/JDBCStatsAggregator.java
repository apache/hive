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
import org.apache.hadoop.hive.ql.stats.StatsAggregator;
import org.apache.hadoop.hive.ql.stats.StatsSetupConst;

public class JDBCStatsAggregator implements StatsAggregator {

  private Connection conn;
  private String connectionString;
  private Configuration hiveconf;
  private PreparedStatement selStmt, delStmt;
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
  private int timeout = 30;
  private final String comment = "Hive stats aggregation: " + this.getClass().getName();
  private int maxRetries, waitWindow;
  private final Random r;

  public JDBCStatsAggregator() {
    r = new Random();
  }

  @Override
  public boolean connect(Configuration hiveconf) {
    this.hiveconf = hiveconf;
    timeout = HiveConf.getIntVar(hiveconf, HiveConf.ConfVars.HIVE_STATS_JDBC_TIMEOUT);
    connectionString = HiveConf.getVar(hiveconf, HiveConf.ConfVars.HIVESTATSDBCONNECTIONSTRING);
    String driver = HiveConf.getVar(hiveconf, HiveConf.ConfVars.HIVESTATSJDBCDRIVER);
    maxRetries = HiveConf.getIntVar(hiveconf, HiveConf.ConfVars.HIVE_STATS_RETRIES_MAX);
    waitWindow = HiveConf.getIntVar(hiveconf, HiveConf.ConfVars.HIVE_STATS_RETRIES_WAIT);

    try {
      Class.forName(driver).newInstance();
    } catch (Exception e) {
      LOG.error("Error during instantiating JDBC driver " + driver + ". ", e);
      return false;
    }

    // prepare the SELECT/DELETE statements
    String select =
      "SELECT /* " + comment + " */ " +
      " SUM(" + JDBCStatsSetupConstants.PART_STAT_ROW_COUNT_COLUMN_NAME + ")" +
      " FROM " + JDBCStatsSetupConstants.PART_STAT_TABLE_NAME +
      " WHERE " + JDBCStatsSetupConstants.PART_STAT_ID_COLUMN_NAME +
      " LIKE ? ESCAPE ?";

    /* Automatic Cleaning:
    IMPORTANT: Since we publish and aggregate only 1 value (1 column) which is the row count, it
    is valid to delete the row after aggregation (automatic cleaning) because we know that there is no
    other values to aggregate.
    If ;in the future; other values are aggregated and published, then we cannot do cleaning except
    when we are sure that all values are aggregated, or we can separate the implementation of cleaning
    through a separate method which the developer has to call it manually in the code.
    */
    String delete =
      "DELETE /* " + comment + " */ " +
      " FROM " + JDBCStatsSetupConstants.PART_STAT_TABLE_NAME +
      " WHERE " + JDBCStatsSetupConstants.PART_STAT_ID_COLUMN_NAME +
      " LIKE ? ESCAPE ?";

    // stats is non-blocking -- throw an exception when timeout
    DriverManager.setLoginTimeout(timeout);
    // function pointer for executeWithRetry to setQueryTimeout
    Utilities.SQLCommand<Void> setQueryTimeout = new Utilities.SQLCommand<Void>() {
      @Override
      public Void run(PreparedStatement stmt) throws SQLException {
        stmt.setQueryTimeout(timeout);
        return null;
      }
    };

    // retry connection and statement preparations
    for (int failures = 0; ;failures++) {
      try {
        conn = Utilities.connectWithRetry(connectionString, waitWindow, maxRetries);

        // prepare statements
        selStmt = Utilities.prepareWithRetry(conn, select, waitWindow, maxRetries);
        delStmt = Utilities.prepareWithRetry(conn, delete, waitWindow, maxRetries);

        // set query timeout
        Utilities.executeWithRetry(setQueryTimeout, selStmt, waitWindow, failures);
        Utilities.executeWithRetry(setQueryTimeout, delStmt, waitWindow, failures);

        return true;
      } catch (SQLRecoverableException e) {
        if (failures > maxRetries) {
          LOG.error("Error during JDBC connection and preparing statement: " + e);
        	return false;
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
  public String aggregateStats(String fileID, String statType) {

    LOG.info("Stats Aggregator for key " + fileID);

    if (statType != StatsSetupConst.ROW_COUNT) {
      LOG.warn("Warning. Invalid statistic. Currently " +
      "row count is the only supported statistic");
      return null;
    }

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

    String keyPrefix = Utilities.escapeSqlLike(fileID) + "%";
    for (int failures = 0; ; failures++) {
      try {
        long retval = 0;

        selStmt.setString(1, keyPrefix);
        selStmt.setString(2, Character.toString(Utilities.sqlEscapeChar));
        ResultSet result = Utilities.executeWithRetry(execQuery, selStmt, waitWindow, maxRetries);
        if (result.next()) {
          retval = result.getLong(1);
        } else {
          LOG.warn("Warning. Nothing published. Nothing aggregated.");
          return "";
        }

        /* Automatic Cleaning:
            IMPORTANT: Since we publish and aggregate only 1 value (1 column) which is the row count, it
            is valid to delete the row after aggregation (automatic cleaning) because we know that there is no
            other values to aggregate.
            If in the future; other values are aggregated and published, then we cannot do cleaning except
            when we are sure that all values are aggregated, or we can separate the implementation of cleaning
            through a separate method which the developer has to call it manually in the code.
         */
        delStmt.setString(1, keyPrefix);
        delStmt.setString(2, Character.toString(Utilities.sqlEscapeChar));
        Utilities.executeWithRetry(execUpdate, delStmt, waitWindow, maxRetries);

        LOG.info("Stats aggregator got " + retval);

        return Long.toString(retval);
      } catch (SQLRecoverableException e) {
        // need to start from scratch (connection)
        if (failures >= maxRetries) {
          return null;
        }
        // close the current connection
        closeConnection();
        long waitTime = Utilities.getRandomWaitTime(waitWindow, failures, r);
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException iex) {
        }
        // getting a new connection
        if (!connect(hiveconf)) {
          // if cannot reconnect, just fail because connect() already handles retries.
          LOG.error("Error during publishing aggregation. " + e);
          return null;
        }
      } catch (SQLException e) {
        // for SQLTransientException (already handled by Utilities.*WithRetries() functions
        // and SQLNonTransientException, just declare failure.
        LOG.error("Error during publishing aggregation. " + e);
        return null;
      }
    }
  }

  @Override
  public boolean closeConnection() {

    if (conn == null) {
      return true;
    }

    try {
      conn.close();
      // In case of derby, explicitly close the database connection
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
      LOG.error("Error during JDBC termination. " + e);
      return false;
    }
  }

  @Override
  public boolean cleanUp(String rowID) {
    try {
      Statement stmt = conn.createStatement();

      String delete =
        "DELETE /* " + comment + " */ " +
        " FROM " + JDBCStatsSetupConstants.PART_STAT_TABLE_NAME +
        " WHERE " + JDBCStatsSetupConstants.PART_STAT_ID_COLUMN_NAME + " LIKE '" + rowID + "%'";
      stmt.executeUpdate(delete);
      stmt.close();
      return closeConnection();
    } catch (SQLException e) {
      LOG.error("Error during publishing aggregation. " + e);
      return false;
    }
  }
}
