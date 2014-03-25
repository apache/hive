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

import org.apache.hadoop.hive.conf.HiveConf;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Utility methods for creating and destroying txn database/schema.  Placed
 * here in a separate class so it can be shared across unit tests.
 */
public class TxnDbUtil {
  private final static String txnMgr = "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager";

  /**
   * Set up the configuration so it will use the DbTxnManager, concurrency will be set to true,
   * and the JDBC configs will be set for putting the transaction and lock info in the embedded
   * metastore.
   * @param conf HiveConf to add these values to.
   */
  public static void setConfValues(HiveConf conf) {
    conf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, txnMgr);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
  }

  public static void prepDb() throws Exception {
    // This is a bogus hack because it copies the contents of the SQL file
    // intended for creating derby databases, and thus will inexorably get
    // out of date with it.  I'm open to any suggestions on how to make this
    // read the file in a build friendly way.
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("CREATE TABLE TXNS (" +
        "  TXN_ID bigint PRIMARY KEY," +
        "  TXN_STATE char(1) NOT NULL," +
        "  TXN_STARTED bigint NOT NULL," +
        "  TXN_LAST_HEARTBEAT bigint NOT NULL," +
        "  TXN_USER varchar(128) NOT NULL," +
        "  TXN_HOST varchar(128) NOT NULL)");

        s.execute("CREATE TABLE TXN_COMPONENTS (" +
        "  TC_TXNID bigint REFERENCES TXNS (TXN_ID)," +
        "  TC_DATABASE varchar(128) NOT NULL," +
        "  TC_TABLE varchar(128)," +
        "  TC_PARTITION varchar(767))");
    s.execute("CREATE TABLE COMPLETED_TXN_COMPONENTS (" +
        "  CTC_TXNID bigint," +
        "  CTC_DATABASE varchar(128) NOT NULL," +
        "  CTC_TABLE varchar(128)," +
        "  CTC_PARTITION varchar(767))");
    s.execute("CREATE TABLE NEXT_TXN_ID (" +
        "  NTXN_NEXT bigint NOT NULL)");
    s.execute("INSERT INTO NEXT_TXN_ID VALUES(1)");
    s.execute("CREATE TABLE HIVE_LOCKS (" +
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
    s.execute("CREATE INDEX HL_TXNID_INDEX ON HIVE_LOCKS (HL_TXNID)");

    s.execute("CREATE TABLE NEXT_LOCK_ID (" +
        " NL_NEXT bigint NOT NULL)");
    s.execute("INSERT INTO NEXT_LOCK_ID VALUES(1)");

    s.execute("CREATE TABLE COMPACTION_QUEUE (" +
        " CQ_ID bigint PRIMARY KEY," +
        " CQ_DATABASE varchar(128) NOT NULL," +
        " CQ_TABLE varchar(128) NOT NULL," +
        " CQ_PARTITION varchar(767)," +
        " CQ_STATE char(1) NOT NULL," +
        " CQ_TYPE char(1) NOT NULL," +
        " CQ_WORKER_ID varchar(128)," +
        " CQ_START bigint," +
        " CQ_RUN_AS varchar(128))");

    s.execute("CREATE TABLE NEXT_COMPACTION_QUEUE_ID (NCQ_NEXT bigint NOT NULL)");
    s.execute("INSERT INTO NEXT_COMPACTION_QUEUE_ID VALUES(1)");

    conn.commit();
    conn.close();
  }

  public static void cleanDb() throws  Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    // We want to try these, whether they succeed or fail.
    try {
      s.execute("DROP INDEX HL_TXNID_INDEX");
    } catch (Exception e) {
      System.err.println("Unable to drop index HL_TXNID_INDEX " +
          e.getMessage());
    }
    try {
      s.execute("DROP TABLE TXN_COMPONENTS");
    } catch (Exception e) {
      System.err.println("Unable to drop table TXN_COMPONENTS " +
          e.getMessage());
    }
    try {
      s.execute("DROP TABLE COMPLETED_TXN_COMPONENTS");
    } catch (Exception e) {
      System.err.println("Unable to drop table COMPLETED_TXN_COMPONENTS " +
          e.getMessage());
    }
    try {
      s.execute("DROP TABLE TXNS");
    } catch (Exception e) {
      System.err.println("Unable to drop table TXNS " +
          e.getMessage());
    }
    try {
      s.execute("DROP TABLE NEXT_TXN_ID");
    } catch (Exception e) {
      System.err.println("Unable to drop table NEXT_TXN_ID " +
          e.getMessage());
    }
    try {
      s.execute("DROP TABLE HIVE_LOCKS");
    } catch (Exception e) {
      System.err.println("Unable to drop table HIVE_LOCKS " +
          e.getMessage());
    }
    try {
      s.execute("DROP TABLE NEXT_LOCK_ID");
    } catch (Exception e) {
    }
    try {
      s.execute("DROP TABLE COMPACTION_QUEUE");
    } catch (Exception e) {
    }
    try {
      s.execute("DROP TABLE NEXT_COMPACTION_QUEUE_ID");
    } catch (Exception e) {
    }
    conn.commit();
    conn.close();
  }

  /**
   * A tool to count the number of partitions, tables,
   * and databases locked by a particular lockId.
   * @param lockId lock id to look for lock components
   * @return number of components, or 0 if there is no lock
   */
  public static int countLockComponents(long lockId) throws  Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    ResultSet rs = s.executeQuery("select count(*) from hive_locks where " +
        "hl_lock_ext_id = " + lockId);
    if (!rs.next()) return 0;
    int rc = rs.getInt(1);
    conn.rollback();
    conn.close();
    return rc;
  }

  public static int findNumCurrentLocks() throws Exception {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    ResultSet rs = s.executeQuery("select count(*) from hive_locks");
    if (!rs.next()) return 0;
    int rc = rs.getInt(1);
    conn.rollback();
    conn.close();
    return rc;
  }

  private static Connection getConnection() throws Exception {
    HiveConf conf = new HiveConf();
    String jdbcDriver = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER);
    Driver driver = (Driver)Class.forName(jdbcDriver).newInstance();
    Properties prop = new Properties();
    String driverUrl = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORECONNECTURLKEY);
    String user = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME);
    String passwd = HiveConf.getVar(conf, HiveConf.ConfVars.METASTOREPWD);
    prop.put("user", user);
    prop.put("password", passwd);
    return driver.connect(driverUrl, prop);
  }

}
