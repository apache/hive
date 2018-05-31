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
package org.apache.hadoop.hive.metastore.txn;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.sql.Statement;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for creating and destroying txn database/schema, plus methods for
 * querying against metastore tables.
 * Placed here in a separate class so it can be shared across unit tests.
 */
public final class TxnDbUtil {

  static final private Logger LOG = LoggerFactory.getLogger(TxnDbUtil.class.getName());
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
  public static void setConfValues(Configuration conf) {
    MetastoreConf.setVar(conf, ConfVars.HIVE_TXN_MANAGER, TXN_MANAGER);
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
  }

  public static void prepDb(Configuration conf) throws Exception {
    // This is a bogus hack because it copies the contents of the SQL file
    // intended for creating derby databases, and thus will inexorably get
    // out of date with it.  I'm open to any suggestions on how to make this
    // read the file in a build friendly way.

    Connection conn = null;
    Statement stmt = null;
    try {
      conn = getConnection(conf);
      stmt = conn.createStatement();
      stmt.execute("CREATE TABLE TXNS (" +
          "  TXN_ID bigint PRIMARY KEY," +
          "  TXN_STATE char(1) NOT NULL," +
          "  TXN_STARTED bigint NOT NULL," +
          "  TXN_LAST_HEARTBEAT bigint NOT NULL," +
          "  TXN_USER varchar(128) NOT NULL," +
          "  TXN_HOST varchar(128) NOT NULL)");

      stmt.execute("CREATE TABLE TXN_COMPONENTS (" +
          "  TC_TXNID bigint NOT NULL REFERENCES TXNS (TXN_ID)," +
          "  TC_DATABASE varchar(128) NOT NULL," +
          "  TC_TABLE varchar(128)," +
          "  TC_PARTITION varchar(767)," +
          "  TC_OPERATION_TYPE char(1) NOT NULL," +
          "  TC_WRITEID bigint)");
      stmt.execute("CREATE TABLE COMPLETED_TXN_COMPONENTS (" +
          "  CTC_TXNID bigint NOT NULL," +
          "  CTC_DATABASE varchar(128) NOT NULL," +
          "  CTC_TABLE varchar(128)," +
          "  CTC_PARTITION varchar(767)," +
          "  CTC_ID bigint GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) NOT NULL," +
          "  CTC_TIMESTAMP timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL," +
          "  CTC_WRITEID bigint)");
      stmt.execute("CREATE TABLE NEXT_TXN_ID (" + "  NTXN_NEXT bigint NOT NULL)");
      stmt.execute("INSERT INTO NEXT_TXN_ID VALUES(1)");

      stmt.execute("CREATE TABLE TXN_TO_WRITE_ID (" +
          " T2W_TXNID bigint NOT NULL," +
          " T2W_DATABASE varchar(128) NOT NULL," +
          " T2W_TABLE varchar(256) NOT NULL," +
          " T2W_WRITEID bigint NOT NULL)");
      stmt.execute("CREATE TABLE NEXT_WRITE_ID (" +
          " NWI_DATABASE varchar(128) NOT NULL," +
          " NWI_TABLE varchar(256) NOT NULL," +
          " NWI_NEXT bigint NOT NULL)");

      stmt.execute("CREATE TABLE MIN_HISTORY_LEVEL (" +
          " MHL_TXNID bigint NOT NULL," +
          " MHL_MIN_OPEN_TXNID bigint NOT NULL," +
          " PRIMARY KEY(MHL_TXNID))");

      stmt.execute("CREATE TABLE HIVE_LOCKS (" +
          " HL_LOCK_EXT_ID bigint NOT NULL," +
          " HL_LOCK_INT_ID bigint NOT NULL," +
          " HL_TXNID bigint NOT NULL," +
          " HL_DB varchar(128) NOT NULL," +
          " HL_TABLE varchar(128)," +
          " HL_PARTITION varchar(767)," +
          " HL_LOCK_STATE char(1) NOT NULL," +
          " HL_LOCK_TYPE char(1) NOT NULL," +
          " HL_LAST_HEARTBEAT bigint NOT NULL," +
          " HL_ACQUIRED_AT bigint," +
          " HL_USER varchar(128) NOT NULL," +
          " HL_HOST varchar(128) NOT NULL," +
          " HL_HEARTBEAT_COUNT integer," +
          " HL_AGENT_INFO varchar(128)," +
          " HL_BLOCKEDBY_EXT_ID bigint," +
          " HL_BLOCKEDBY_INT_ID bigint," +
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
          " CQ_TBLPROPERTIES varchar(2048)," +
          " CQ_WORKER_ID varchar(128)," +
          " CQ_START bigint," +
          " CQ_RUN_AS varchar(128)," +
          " CQ_HIGHEST_WRITE_ID bigint," +
          " CQ_META_INFO varchar(2048) for bit data," +
          " CQ_HADOOP_JOB_ID varchar(32))");

      stmt.execute("CREATE TABLE NEXT_COMPACTION_QUEUE_ID (NCQ_NEXT bigint NOT NULL)");
      stmt.execute("INSERT INTO NEXT_COMPACTION_QUEUE_ID VALUES(1)");

      stmt.execute("CREATE TABLE COMPLETED_COMPACTIONS (" +
          " CC_ID bigint PRIMARY KEY," +
          " CC_DATABASE varchar(128) NOT NULL," +
          " CC_TABLE varchar(128) NOT NULL," +
          " CC_PARTITION varchar(767)," +
          " CC_STATE char(1) NOT NULL," +
          " CC_TYPE char(1) NOT NULL," +
          " CC_TBLPROPERTIES varchar(2048)," +
          " CC_WORKER_ID varchar(128)," +
          " CC_START bigint," +
          " CC_END bigint," +
          " CC_RUN_AS varchar(128)," +
          " CC_HIGHEST_WRITE_ID bigint," +
          " CC_META_INFO varchar(2048) for bit data," +
          " CC_HADOOP_JOB_ID varchar(32))");

      stmt.execute("CREATE TABLE AUX_TABLE (" +
        " MT_KEY1 varchar(128) NOT NULL," +
        " MT_KEY2 bigint NOT NULL," +
        " MT_COMMENT varchar(255)," +
        " PRIMARY KEY(MT_KEY1, MT_KEY2))");

      stmt.execute("CREATE TABLE WRITE_SET (" +
        " WS_DATABASE varchar(128) NOT NULL," +
        " WS_TABLE varchar(128) NOT NULL," +
        " WS_PARTITION varchar(767)," +
        " WS_TXNID bigint NOT NULL," +
        " WS_COMMIT_ID bigint NOT NULL," +
        " WS_OPERATION_TYPE char(1) NOT NULL)"
      );

      stmt.execute("CREATE TABLE REPL_TXN_MAP (" +
          " RTM_REPL_POLICY varchar(256) NOT NULL, " +
          " RTM_SRC_TXN_ID bigint NOT NULL, " +
          " RTM_TARGET_TXN_ID bigint NOT NULL, " +
          " PRIMARY KEY (RTM_REPL_POLICY, RTM_SRC_TXN_ID))"
      );

      try {
        stmt.execute("CREATE TABLE \"APP\".\"SEQUENCE_TABLE\" (\"SEQUENCE_NAME\" VARCHAR(256) NOT " +

                "NULL, \"NEXT_VAL\" BIGINT NOT NULL)"
        );
      } catch (SQLException e) {
        if (e.getMessage() != null && e.getMessage().contains("already exists")) {
          LOG.info("SEQUENCE_TABLE table already exist, ignoring");
        } else {
          throw e;
        }
      }

      try {
        stmt.execute("CREATE TABLE \"APP\".\"NOTIFICATION_SEQUENCE\" (\"NNI_ID\" BIGINT NOT NULL, " +

                "\"NEXT_EVENT_ID\" BIGINT NOT NULL)"
        );
      } catch (SQLException e) {
        if (e.getMessage() != null && e.getMessage().contains("already exists")) {
          LOG.info("NOTIFICATION_SEQUENCE table already exist, ignoring");
        } else {
          throw e;
        }
      }

      try {
        stmt.execute("CREATE TABLE \"APP\".\"NOTIFICATION_LOG\" (\"NL_ID\" BIGINT NOT NULL, " +
                "\"DB_NAME\" VARCHAR(128), \"EVENT_ID\" BIGINT NOT NULL, \"EVENT_TIME\" INTEGER NOT" +

                " NULL, \"EVENT_TYPE\" VARCHAR(32) NOT NULL, \"MESSAGE\" CLOB, \"TBL_NAME\" " +
                "VARCHAR" +
                "(256), \"MESSAGE_FORMAT\" VARCHAR(16))"
        );
      } catch (SQLException e) {
        if (e.getMessage() != null && e.getMessage().contains("already exists")) {
          LOG.info("NOTIFICATION_LOG table already exist, ignoring");
        } else {
          throw e;
        }
      }

      stmt.execute("INSERT INTO \"APP\".\"SEQUENCE_TABLE\" (\"SEQUENCE_NAME\", \"NEXT_VAL\") " +
              "SELECT * FROM (VALUES ('org.apache.hadoop.hive.metastore.model.MNotificationLog', " +
              "1)) tmp_table WHERE NOT EXISTS ( SELECT \"NEXT_VAL\" FROM \"APP\"" +
              ".\"SEQUENCE_TABLE\" WHERE \"SEQUENCE_NAME\" = 'org.apache.hadoop.hive.metastore" +
              ".model.MNotificationLog')");

      stmt.execute("INSERT INTO \"APP\".\"NOTIFICATION_SEQUENCE\" (\"NNI_ID\", \"NEXT_EVENT_ID\")" +
              " SELECT * FROM (VALUES (1,1)) tmp_table WHERE NOT EXISTS ( SELECT " +
              "\"NEXT_EVENT_ID\" FROM \"APP\".\"NOTIFICATION_SEQUENCE\")");
    } catch (SQLException e) {
      try {
        conn.rollback();
      } catch (SQLException re) {
        LOG.error("Error rolling back: " + re.getMessage());
      }

      // Another thread might have already created these tables.
      if (e.getMessage() != null && e.getMessage().contains("already exists")) {
        LOG.info("Txn tables already exist, returning");
        return;
      }

      // This might be a deadlock, if so, let's retry
      if (e instanceof SQLTransactionRollbackException && deadlockCnt++ < 5) {
        LOG.warn("Caught deadlock, retrying db creation");
        prepDb(conf);
      } else {
        throw e;
      }
    } finally {
      deadlockCnt = 0;
      closeResources(conn, stmt, null);
    }
  }

  public static void cleanDb(Configuration conf) throws Exception {
    int retryCount = 0;
    while(++retryCount <= 3) {
      boolean success = true;
      Connection conn = null;
      Statement stmt = null;
      try {
        conn = getConnection(conf);
        stmt = conn.createStatement();

        // We want to try these, whether they succeed or fail.
        try {
          stmt.execute("DROP INDEX HL_TXNID_INDEX");
        } catch (SQLException e) {
          if(!("42X65".equals(e.getSQLState()) && 30000 == e.getErrorCode())) {
            //42X65/3000 means index doesn't exist
            LOG.error("Unable to drop index HL_TXNID_INDEX " + e.getMessage() +
              "State=" + e.getSQLState() + " code=" + e.getErrorCode() + " retryCount=" + retryCount);
            success = false;
          }
        }

        success &= dropTable(stmt, "TXN_COMPONENTS", retryCount);
        success &= dropTable(stmt, "COMPLETED_TXN_COMPONENTS", retryCount);
        success &= dropTable(stmt, "TXNS", retryCount);
        success &= dropTable(stmt, "NEXT_TXN_ID", retryCount);
        success &= dropTable(stmt, "TXN_TO_WRITE_ID", retryCount);
        success &= dropTable(stmt, "NEXT_WRITE_ID", retryCount);
        success &= dropTable(stmt, "MIN_HISTORY_LEVEL", retryCount);
        success &= dropTable(stmt, "HIVE_LOCKS", retryCount);
        success &= dropTable(stmt, "NEXT_LOCK_ID", retryCount);
        success &= dropTable(stmt, "COMPACTION_QUEUE", retryCount);
        success &= dropTable(stmt, "NEXT_COMPACTION_QUEUE_ID", retryCount);
        success &= dropTable(stmt, "COMPLETED_COMPACTIONS", retryCount);
        success &= dropTable(stmt, "AUX_TABLE", retryCount);
        success &= dropTable(stmt, "WRITE_SET", retryCount);
        success &= dropTable(stmt, "REPL_TXN_MAP", retryCount);
        /*
         * Don't drop NOTIFICATION_LOG, SEQUENCE_TABLE and NOTIFICATION_SEQUENCE as its used by other
         * table which are not txn related to generate primary key. So if these tables are dropped
         *  and other tables are not dropped, then it will create key duplicate error while inserting
         *  to other table.
         */
      } finally {
        closeResources(conn, stmt, null);
      }
      if(success) {
        return;
      }
    }
    throw new RuntimeException("Failed to clean up txn tables");
  }

  private static boolean dropTable(Statement stmt, String name, int retryCount) throws SQLException {
    for (int i = 0; i < 3; i++) {
      try {
        stmt.execute("DROP TABLE " + name);
        LOG.debug("Successfully dropped table " + name);
        return true;
      } catch (SQLException e) {
        if ("42Y55".equals(e.getSQLState()) && 30000 == e.getErrorCode()) {
          LOG.debug("Not dropping " + name + " because it doesn't exist");
          //failed because object doesn't exist
          return true;
        }
        if ("X0Y25".equals(e.getSQLState()) && 30000 == e.getErrorCode()) {
          // Intermittent failure
          LOG.warn("Intermittent drop failure, retrying, try number " + i);
          continue;
        }
        LOG.error("Unable to drop table " + name + ": " + e.getMessage() +
            " State=" + e.getSQLState() + " code=" + e.getErrorCode() + " retryCount=" + retryCount);
      }
    }
    LOG.error("Failed to drop table, don't know why");
    return false;
  }

  /**
   * A tool to count the number of partitions, tables,
   * and databases locked by a particular lockId.
   *
   * @param lockId lock id to look for lock components
   *
   * @return number of components, or 0 if there is no lock
   */
  public static int countLockComponents(Configuration conf, long lockId) throws Exception {
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      conn = getConnection(conf);
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

  /**
   * Utility method used to run COUNT queries like "select count(*) from ..." against metastore tables
   * @param countQuery countQuery text
   * @return count countQuery result
   * @throws Exception
   */
  public static int countQueryAgent(Configuration conf, String countQuery) throws Exception {
    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      conn = getConnection(conf);
      stmt = conn.createStatement();
      rs = stmt.executeQuery(countQuery);
      if (!rs.next()) {
        return 0;
      }
      return rs.getInt(1);
    } finally {
      closeResources(conn, stmt, rs);
    }
  }
  @VisibleForTesting
  public static String queryToString(Configuration conf, String query) throws Exception {
    return queryToString(conf, query, true);
  }
  public static String queryToString(Configuration conf, String query, boolean includeHeader)
      throws Exception {
    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    StringBuilder sb = new StringBuilder();
    try {
      conn = getConnection(conf);
      stmt = conn.createStatement();
      rs = stmt.executeQuery(query);
      ResultSetMetaData rsmd = rs.getMetaData();
      if(includeHeader) {
        for (int colPos = 1; colPos <= rsmd.getColumnCount(); colPos++) {
          sb.append(rsmd.getColumnName(colPos)).append("   ");
        }
        sb.append('\n');
      }
      while(rs.next()) {
        for (int colPos = 1; colPos <= rsmd.getColumnCount(); colPos++) {
          sb.append(rs.getObject(colPos)).append("   ");
        }
        sb.append('\n');
      }
    } finally {
      closeResources(conn, stmt, rs);
    }
    return sb.toString();
  }

  static Connection getConnection(Configuration conf) throws Exception {
    String jdbcDriver = MetastoreConf.getVar(conf, ConfVars.CONNECTION_DRIVER);
    Driver driver = (Driver) Class.forName(jdbcDriver).newInstance();
    Properties prop = new Properties();
    String driverUrl = MetastoreConf.getVar(conf, ConfVars.CONNECT_URL_KEY);
    String user = MetastoreConf.getVar(conf, ConfVars.CONNECTION_USER_NAME);
    String passwd = MetastoreConf.getPassword(conf, MetastoreConf.ConfVars.PWD);
    prop.setProperty("user", user);
    prop.setProperty("password", passwd);
    Connection conn = driver.connect(driverUrl, prop);
    conn.setAutoCommit(true);
    return conn;
  }

  static void closeResources(Connection conn, Statement stmt, ResultSet rs) {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException e) {
        LOG.error("Error closing ResultSet: " + e.getMessage());
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
