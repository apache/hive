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

package org.apache.hadoop.hive.ql.profiler;

import java.util.Map;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Connection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;

public class HiveProfilePublisherInfo implements HiveProfilerConnectionInfo {
  final private Log LOG = LogFactory.getLog(this.getClass().getName());
  private String statsDbClass;
  private final String tableName = "PROFILER_STATS";
  private int maxRetries, waitWindow, timeout;
  private Connection conn;
  private String connectionString;
  private PreparedStatement insStmt;

  public String getDbClass() { return statsDbClass; }

  public int getTimeout() { return timeout; }

  public int getMaxRetries() { return maxRetries; }

  public int getWaitWindow() { return waitWindow; }

  public String getConnectionString() { return connectionString; }

  public String getTableName() { return tableName; }

  public Connection getConnection() { return conn; }

  protected PreparedStatement getInsertStatement() {
    return insStmt;
  }

  private String getInsert() {
    String colNames = "";
    String val = "";
    int numCols = HiveProfilerStats.COLUMN_NAMES.length;
    for (int i = 0; i < numCols; i++) {
      colNames += HiveProfilerStats.COLUMN_NAMES[i];
      val += "?";

      if (i < numCols - 1) {
        colNames += ",";
        val += ",";
      }
    }
    return "INSERT INTO " + tableName + " (" + colNames + ") VALUES (" + val + ")";
  }

  public HiveProfilePublisherInfo (Configuration conf) throws Exception{
    maxRetries = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_PROFILER_RETRIES_MAX);
    waitWindow = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_PROFILER_RETRIES_WAIT);
    connectionString = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEPROFILERDBCONNECTIONSTRING);
    timeout = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_PROFILER_JDBC_TIMEOUT);
    String driver = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEPROFILERJDBCDRIVER);
    statsDbClass = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEPROFILERDBCLASS);
    try {
      Class.forName(driver).newInstance();
    } catch (Exception e) {
      LOG.error("Error during instantiating JDBC driver " + driver + ". ", e);
    }
    DriverManager.setLoginTimeout(timeout); // stats is non-blocking
    conn = Utilities.connectWithRetry(connectionString, waitWindow, maxRetries);
  }

  protected void prepareInsert() throws SQLException {
    insStmt = Utilities.prepareWithRetry(conn, getInsert(), waitWindow, maxRetries);
  }
  protected void closeInsertStatement() throws SQLException {
    insStmt.close();
  }

  protected PreparedStatement getInsert(Map<String, String> stats) throws SQLException {
    for (int i = 0; i < HiveProfilerStats.COLUMN_NAMES.length; i++) {
      insStmt.setString(i + 1, stats.get(HiveProfilerStats.COLUMN_NAMES[i]));
    }
    return insStmt;
  }

}

