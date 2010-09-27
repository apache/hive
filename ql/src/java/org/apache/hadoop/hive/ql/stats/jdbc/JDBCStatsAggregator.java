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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.stats.StatsAggregator;
import org.apache.hadoop.hive.ql.stats.StatsSetupConst;

public class JDBCStatsAggregator implements StatsAggregator {

  private Connection conn;
  private String connectionString;
  private Configuration hiveconf;
  private final Log LOG = LogFactory.getLog(this.getClass().getName());

  public boolean connect(Configuration hiveconf) {
    try {
      this.hiveconf = hiveconf;
      connectionString = HiveConf.getVar(hiveconf, HiveConf.ConfVars.HIVESTATSDBCONNECTIONSTRING);
      String driver = HiveConf.getVar(hiveconf, HiveConf.ConfVars.HIVESTATSJDBCDRIVER);
      Class.forName(driver).newInstance();
      DriverManager.setLoginTimeout(3); // stats should not block
      conn = DriverManager.getConnection(connectionString);
      return true;
    } catch (Exception e) {
      LOG.error("Error during JDBC connection. " + e);
      return false;
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

    try {
      long retval = 0;
      Statement stmt = conn.createStatement();
      String select =
        "SELECT SUM" + "(" + JDBCStatsSetupConstants.PART_STAT_ROW_COUNT_COLUMN_NAME + ")" +
        " FROM " + JDBCStatsSetupConstants.PART_STAT_TABLE_NAME +
        " WHERE " + JDBCStatsSetupConstants.PART_STAT_ID_COLUMN_NAME + " LIKE '" + fileID + "%'";

      ResultSet result = stmt.executeQuery(select);
      if (result.next()) {
        retval = result.getLong(1);
      } else {
        LOG.warn("Warning. Nothing published. Nothing aggregated.");
        return "";
      }
      stmt.clearBatch();

      /* Automatic Cleaning:
          IMPORTANT: Since we publish and aggregate only 1 value (1 column) which is the row count, it
          is valid to delete the row after aggregation (automatic cleaning) because we know that there is no
          other values to aggregate.
          If ;in the future; other values are aggregated and published, then we cannot do cleaning except
          when we are sure that all values are aggregated, or we can separate the implementation of cleaning
          through a separate method which the developer has to call it manually in the code.
       */
      String delete =
        "DELETE FROM " + JDBCStatsSetupConstants.PART_STAT_TABLE_NAME +
        " WHERE " + JDBCStatsSetupConstants.PART_STAT_ID_COLUMN_NAME + " LIKE '" + fileID + "%'";
      stmt.executeUpdate(delete);
      stmt.close();

      LOG.info("Stats aggregator got " + retval);

      return Long.toString(retval);
    } catch (SQLException e) {
      LOG.error("Error during publishing aggregation. " + e);
      return null;
    }
  }

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

  public boolean cleanUp(String rowID) {
    try {
      Statement stmt = conn.createStatement();

      String delete =
        "DELETE FROM " + JDBCStatsSetupConstants.PART_STAT_TABLE_NAME +
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
