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
package org.apache.hadoop.hive.metastore.txn;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.ShimLoader;

/**
 * Utility methods for creating and destroying txn database/schema.
 * Placed here in a separate class so it can be shared across unit tests.
 */
public final class TxnDbUtil {

  static final private Log LOG = LogFactory.getLog(TxnDbUtil.class.getName());
  private static final String TXN_MANAGER = "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager";

  private static int deadlockCnt = 0;

  private TxnDbUtil() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

  /**
   * Set up the configuration so it will use the DbTxnManager, concurrency will be set to true,
   * and the JDBC configs will be set for putting the transaction and lock info in the embedded
   * metastore.
   *
   * @param conf HiveConf to add these values to
   */
  public static void setConfValues(HiveConf conf) {
    conf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, TXN_MANAGER);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
  }

  public static void prepDb() throws Exception {
    // This is a bogus hack because it copies the contents of the SQL file
    // intended for creating derby databases, and thus will inexorably get
    // out of date with it.  I'm open to any suggestions on how to make this
    // read the file in a build friendly way.

    Connection conn = null;
    Statement stmt = null;
    try {
      conn = getConnection();
      stmt = conn.createStatement();
      stmt.execute("CREATE TABLE TXNS (" +
          "  TXN_ID bigint PRIMARY KEY," +
          "  TXN_STATE char(1) NOT NULL," +
          "  TXN_STARTED bigint NOT NULL," +
          "  TXN_LAST_HEARTBEAT bigint NOT NULL," +
          "  TXN_USER varchar(128) NOT NULL," +
          "  TXN_HOST varchar(128) NOT NULL)");

      stmt.execute("CREATE TABLE TXN_COMPONENTS (" +
          "  TC_TXNID bigint REFERENCES TXNS (TXN_ID)," +
          "  TC_DATABASE varchar(128) NOT NULL," +
          "  TC_TABLE varchar(128)," +
          "  TC_PARTITION varchar(767))");
      stmt.execute("CREATE TABLE COMPLETED_TXN_COMPONENTS (" +
          "  CTC_TXNID bigint," +
          "  CTC_DATABASE varchar(128) NOT NULL," +
          "  CTC_TABLE varchar(128)," +
          "  CTC_PARTITION varchar(767))");
      stmt.execute("CREATE TABLE NEXT_TXN_ID (" + "  NTXN_NEXT bigint NOT NULL)");
      stmt.execute("INSERT INTO NEXT_TXN_ID VALUES(1)");
      stmt.execute("CREATE TABLE HIVE_LOCKS (" +
          " HL_LOCK_EXT_ID bigint NOT NULL," +
          " HL_LOCK_INT_ID bigint NOT NULL," +
          " HL_TXNID bigint," +
          " HL_DB varchar(128) NOT NULL," +
          " HL_TABLE varchar(128)," +
          " HL_PARTITION varchar(767)," +
          " HL_LOCK_STATE char(1) NOT NULL," +
          " HL_LOCK_TYPE char(1) NOT NULL," +
          " HL_LAST_HEARTBEAT bigint NOT NULL," +
          " HL_ACQUIRED_AT bigint," +
          " HL_USER varchar(128) NOT NULL," +
          " HL_HOST varchar(128) NOT NULL," +
          " PRIMARY KEY(HL_LOCK_EXT_ID, HL_LOCK_INT_ID))");
      stmt.execute("CREATE INDEX HL_TXNID_INDEX ON HIVE_LOCKS (HL_TXNID)");

      stmt.execute("CREATE TABLE NEXT_LOCK_ID (" + " NL_NEXT bigint NOT NULL)");
      stmt.execute("INSERT INTO NEXT_LOCK_ID VALUES(1)");

      stmt.execute("CREATE TABLE COMPACTION_QUEUE (" +
          " CQ_ID bigint PRIMARY KEY," +
          " CQ_DATABASE varchar(128) NOT NULL," +
          " CQ_TABLE varchar(128) NOT NULL," +
          " CQ_PARTITION varchar(767)," +
          " CQ_STATE char(1) NOT NULL," +
          " CQ_TYPE char(1) NOT NULL," +
          " CQ_WORKER_ID varchar(128)," +
          " CQ_START bigint," +
          " CQ_RUN_AS varchar(128))");

      stmt.execute("CREATE TABLE NEXT_COMPACTION_QUEUE_ID (NCQ_NEXT bigint NOT NULL)");
      stmt.execute("INSERT INTO NEXT_COMPACTION_QUEUE_ID VALUES(1)");

      conn.commit();
    } catch (SQLException e) {
      // This might be a deadlock, if so, let's retry
      conn.rollback();
      if (e instanceof SQLTransactionRollbackException && deadlockCnt++ < 5) {
        LOG.warn("Caught deadlock, retrying db creation");
        prepDb();
      } else {
        throw e;
      }
    } finally {
      deadlockCnt = 0;
      closeResources(conn, stmt, null);
    }
  }

  public static void cleanDb() throws Exception {
    Connection conn = null;
    Statement stmt = null;
    try {
      conn = getConnection();
      stmt = conn.createStatement();

      // We want to try these, whether they succeed or fail.
      try {
        stmt.execute("DROP INDEX HL_TXNID_INDEX");
      } catch (Exception e) {
        System.err.println("Unable to drop index HL_TXNID_INDEX " + e.getMessage());
      }

      dropTable(stmt, "TXN_COMPONENTS");
      dropTable(stmt, "COMPLETED_TXN_COMPONENTS");
      dropTable(stmt, "TXNS");
      dropTable(stmt, "NEXT_TXN_ID");
      dropTable(stmt, "HIVE_LOCKS");
      dropTable(stmt, "NEXT_LOCK_ID");
      dropTable(stmt, "COMPACTION_QUEUE");
      dropTable(stmt, "NEXT_COMPACTION_QUEUE_ID");

      conn.commit();
    } finally {
      closeResources(conn, stmt, null);
    }
  }

  private static void dropTable(Statement stmt, String name) {
    try {
      stmt.execute("DROP TABLE " + name);
    } catch (Exception e) {
      System.err.println("Unable to drop table " + name + ": " + e.getMessage());
    }
  }

  /**
   * A tool to count the number of partitions, tables,
   * and databases locked by a particular lockId.
   *
   * @param lockId lock id to look for lock components
   *
   * @return number of components, or 0 if there is no lock
   */
  public static int countLockComponents(long lockId) throws Exception {
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      conn = getConnection();
      stmt = conn.prepareStatement("SELECT count(*) FROM hive_locks WHERE hl_lock_ext_id = ?");
      stmt.setLong(1, lockId);
      rs = stmt.executeQuery();
      if (!rs.next()) {
        return 0;
      }
      return rs.getInt(1);
    } finally {
      closeResources(conn, stmt, rs);
    }
  }

  public static int findNumCurrentLocks() throws Exception {
    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      conn = getConnection();
      stmt = conn.createStatement();
      rs = stmt.executeQuery("select count(*) from hive_locks");
      if (!rs.next()) {
        return 0;
      }
      return rs.getInt(1);
    } finally {
      closeResources(conn, stmt, rs);
    }
  }

  private static Connection getConnection() throws Exception {
    HiveConf conf = new HiveConf();
    String jdbcDriver = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER);
    Driver driver = (Driver) Class.forName(jdbcDriver).newInstance();
    Properties prop = new Properties();
    String driverUrl = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORECONNECTURLKEY);
    String user = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME);
    String passwd =
      ShimLoader.getHadoopShims().getPassword(conf, HiveConf.ConfVars.METASTOREPWD.varname);
    prop.setProperty("user", user);
    prop.setProperty("password", passwd);
    return driver.connect(driverUrl, prop);
  }

  private static void closeResources(Connection conn, Statement stmt, ResultSet rs) {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException e) {
        System.err.println("Error closing ResultSet: " + e.getMessage());
      }
    }

    if (stmt != null) {
      try {
        stmt.close();
      } catch (SQLException e) {
        System.err.println("Error closing Statement: " + e.getMessage());
      }
    }

    if (conn != null) {
      try {
        conn.rollback();
      } catch (SQLException e) {
        System.err.println("Error rolling back: " + e.getMessage());
      }
      try {
        conn.close();
      } catch (SQLException e) {
        System.err.println("Error closing Connection: " + e.getMessage());
      }
    }
  }
}
