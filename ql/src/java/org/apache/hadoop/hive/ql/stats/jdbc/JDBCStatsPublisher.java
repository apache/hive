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
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.ql.stats.StatsSetupConst;

public class JDBCStatsPublisher implements StatsPublisher {

  private Connection conn;
  private String connectionString;
  private Configuration hiveconf;
  private final Log LOG = LogFactory.getLog(this.getClass().getName());
  private PreparedStatement selStmt, updStmt, insStmt;

  public JDBCStatsPublisher() {
    selStmt = updStmt = insStmt = null;
  }

  public boolean connect(Configuration hiveconf) {
    try {
      this.hiveconf = hiveconf;
      connectionString = HiveConf.getVar(hiveconf, HiveConf.ConfVars.HIVESTATSDBCONNECTIONSTRING);
      String driver = HiveConf.getVar(hiveconf, HiveConf.ConfVars.HIVESTATSJDBCDRIVER);
      Class.forName(driver).newInstance();
      DriverManager.setLoginTimeout(3); // stats should not block
      conn = DriverManager.getConnection(connectionString);

      // prepare the SELECT/UPDATE/INSERT statements
      String select =
        "SELECT " + JDBCStatsSetupConstants.PART_STAT_ROW_COUNT_COLUMN_NAME +
        " FROM " + JDBCStatsSetupConstants.PART_STAT_TABLE_NAME +
        " WHERE " + JDBCStatsSetupConstants.PART_STAT_ID_COLUMN_NAME + " = ?";

      String update =
        "UPDATE " + JDBCStatsSetupConstants.PART_STAT_TABLE_NAME +
        " SET " +  JDBCStatsSetupConstants.PART_STAT_ROW_COUNT_COLUMN_NAME + "= ? " +
        " WHERE " + JDBCStatsSetupConstants.PART_STAT_ID_COLUMN_NAME + " = ?";

      String insert =
        "INSERT INTO " + JDBCStatsSetupConstants.PART_STAT_TABLE_NAME +
        " VALUES (?, ?)";

      selStmt = conn.prepareStatement(select);
      updStmt = conn.prepareStatement(update);
      insStmt = conn.prepareStatement(insert);

      // make the statements non-blocking
      selStmt.setQueryTimeout(5);
      updStmt.setQueryTimeout(5);
      insStmt.setQueryTimeout(5);

      return true;
    } catch (Exception e) {
      LOG.error("Error during JDBC connection to " + connectionString + ". ", e);
      return false;
    }
  }

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

    try {

      // Check to see if a previous task (mapper attempt) had published a previous stat
      selStmt.setString(1, fileID);
      ResultSet result = selStmt.executeQuery();

      if (result.next()) {
        long currval = result.getLong(1);
        // Only update if the previous value is smaller (i.e. the previous attempt was a fail and
        // hopefully this attempt is a success (as it has a greater value).
        if (currval < Long.parseLong(value)) {
          updStmt.setString(1, value);
          updStmt.setString(2, fileID);
          updStmt.executeUpdate();
        }
      } else {
        // No previous attempts.
        insStmt.setString(1, fileID);
        insStmt.setString(2, value);
        insStmt.executeUpdate();
      }
      return true;
    } catch (SQLException e) {
      LOG.error("Error during publishing statistics. ", e);
      return false;
    }
  }

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

  public boolean init(Configuration hconf) {
    try {
      this.hiveconf = hconf;
      connectionString = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVESTATSDBCONNECTIONSTRING);
      String driver = HiveConf.getVar(hconf, HiveConf.ConfVars.HIVESTATSJDBCDRIVER);
      Class.forName(driver).newInstance();
      conn = DriverManager.getConnection(connectionString);

      Statement stmt = conn.createStatement();

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
