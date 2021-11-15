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
package org.apache.hive.jdbc;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ddl.process.kill.KillQueriesOperation;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.registry.impl.ZkRegistryBase;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test JDBC driver when two HS2 instance is running with service discovery enabled.
 */
@Ignore("unstable HIVE-23528")
public class TestJdbcWithServiceDiscovery {

  private static final Logger LOG = LoggerFactory.getLogger(TestJdbcWithServiceDiscovery.class);
  private static final String TABLE_NAME = "testJdbcMinihs2Tbl";
  private static final String DB_NAME = "testJdbcMinihs2";
  private static final String REMOTE_ERROR_MESSAGE = "Unable to kill query locally or on remote servers.";

  private static TestingServer zkServer;
  private static MiniHS2 miniHS2server1;
  private static MiniHS2 miniHS2server2;
  private static String miniHS2directUrl1;
  private static String miniHS2directUrl2;
  private static Path kvDataFilePath;

  @BeforeClass
  public static void setup() throws Exception {
    MiniHS2.cleanupLocalDir();
    zkServer = new TestingServer();

    // Create one MiniHS2 with miniMRCluster and one with Local FS only
    HiveConf hiveConf1 = loadConf();
    HiveConf hiveConf2 = new HiveConf();

    setSDConfigs(hiveConf1);
    setSDConfigs(hiveConf2);

    miniHS2server1 = new MiniHS2.Builder().withConf(hiveConf1).withMiniMR().build();
    miniHS2server2 = new MiniHS2.Builder().withConf(hiveConf2).cleanupLocalDirOnStartup(false).build();

    Class.forName(MiniHS2.getJdbcDriverName());
    String instanceId1 = UUID.randomUUID().toString();
    miniHS2server1.start(getConfOverlay(instanceId1));
    miniHS2directUrl1 =
        "jdbc:hive2://" + miniHS2server1.getHost() + ":" + miniHS2server1.getBinaryPort() + "/" + DB_NAME;
    String instanceId2 = UUID.randomUUID().toString();
    miniHS2server2.start(getConfOverlay(instanceId2));
    miniHS2directUrl2 =
        "jdbc:hive2://" + miniHS2server2.getHost() + ":" + miniHS2server2.getBinaryPort() + "/" + DB_NAME;

    String dataFileDir = hiveConf1.get("test.data.files").replace('\\', '/').replace("c:", "");
    kvDataFilePath = new Path(dataFileDir, "kv1.txt");

    setupDb();
  }

  /**
   * SleepMsUDF.
   */
  public static class SleepMsUDF extends UDF {
    public Integer evaluate(int value, int ms) {
      try {
        Thread.sleep(ms);
      } catch (InterruptedException e) {
        // No-op
      }
      return value;
    }
  }

  public static void setupDb() throws Exception {
    Connection conDefault = DriverManager
        .getConnection("jdbc:hive2://" + miniHS2server1.getHost() + ":" + miniHS2server1.getBinaryPort() + "/default",
            System.getProperty("user.name"), "bar");
    Statement stmt = conDefault.createStatement();
    String tblName = DB_NAME + "." + TABLE_NAME;
    stmt.execute("drop database if exists " + DB_NAME + " cascade");
    stmt.execute("create database " + DB_NAME);
    stmt.execute("use " + DB_NAME);
    stmt.execute("create table " + tblName + " (int_col int, value string) ");
    stmt.execute("load data local inpath '" + kvDataFilePath.toString() + "' into table " + tblName);
    stmt.execute("grant select on table " + tblName + " to role public");

    stmt.close();
    conDefault.close();
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if ((miniHS2server1 != null) && miniHS2server1.isStarted()) {
      try {
        miniHS2server1.stop();
      } catch (Exception e) {
        LOG.warn("Error why shutting down Hs2", e);
      }
    }
    if ((miniHS2server2 != null) && miniHS2server2.isStarted()) {
      try {
        miniHS2server2.stop();
      } catch (Exception e) {
        LOG.warn("Error why shutting down Hs2", e);
      }
    }
    if (zkServer != null) {
      zkServer.close();
      zkServer = null;
    }
    MiniHS2.cleanupLocalDir();
  }

  private static HiveConf loadConf() throws Exception {
    String confDir = "../../data/conf/";
    HiveConf.setHiveSiteLocation(new URL("file://" + new File(confDir).toURI().getPath() + "/hive-site.xml"));
    System.out.println("Setting hive-site: " + HiveConf.getHiveSiteLocation());
    HiveConf defaultConf = new HiveConf();
    return defaultConf;
  }

  private static void setSDConfigs(HiveConf conf) {
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY, true);
    conf.setVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM, zkServer.getConnectString());
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_ENABLE, false);
    conf.setTimeVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_TIMEOUT, 2, TimeUnit.SECONDS);
    conf.setTimeVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME, 100, TimeUnit.MILLISECONDS);
    conf.setIntVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES, 1);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_KILLQUERY_ENABLE, true);
  }

  private static Map<String, String> getConfOverlay(final String instanceId) {
    Map<String, String> confOverlay = new HashMap<>();
    confOverlay.put("hive.server2.zookeeper.publish.configs", "true");
    confOverlay.put(ZkRegistryBase.UNIQUE_IDENTIFIER, instanceId);
    return confOverlay;
  }

  private static class ExceptionHolder {
    Throwable throwable;
  }

  private void executeQueryAndKill(Connection con1, Connection con2, ExceptionHolder tExecuteHolder,
      ExceptionHolder tKillHolder) throws SQLException, InterruptedException {
    final HiveStatement stmt = (HiveStatement) con1.createStatement();
    final Statement stmt2 = con2.createStatement();
    final StringBuffer stmtQueryId = new StringBuffer();

    // Thread executing the query
    Thread tExecute = new Thread(() -> {
      try {
        LOG.info("Executing waiting query.");
        // The test table has 500 rows, so total query time should be ~ 500*500ms
        stmt.executeAsync(
            "select sleepMsUDF(t1.int_col, 1), t1.int_col, t2.int_col " + "from " + TABLE_NAME + " t1 join "
                + TABLE_NAME + " t2 on t1.int_col = t2.int_col");
        stmtQueryId.append(stmt.getQueryId());
        stmt.getUpdateCount();
      } catch (SQLException e) {
        tExecuteHolder.throwable = e;
      }
    });

    tExecute.start();

    // wait for other thread to create the stmt handle
    int count = 0;
    while (count < 10) {
      try {
        Thread.sleep(500);
        String queryId;
        if (stmtQueryId.length() != 0) {
          queryId = stmtQueryId.toString();
        } else {
          count++;
          continue;
        }

        LOG.info("Killing query: " + queryId);
        stmt2.execute("kill query '" + queryId + "'");
        stmt2.close();
        break;
      } catch (SQLException e) {
        LOG.warn("Exception when kill query", e);
        tKillHolder.throwable = e;
        break;
      }
    }

    tExecute.join();
    try {
      stmt.close();
      con1.close();
      con2.close();
    } catch (Exception e) {
      LOG.warn("Exception when close stmt and con", e);
    }
  }

  @Test
  public void testKillQueryWithSameServer() throws Exception {
    Connection con1 = DriverManager.getConnection(miniHS2directUrl1, System.getProperty("user.name"), "bar");
    Connection con2 = DriverManager.getConnection(miniHS2directUrl1, System.getProperty("user.name"), "bar");

    Statement stmt = con1.createStatement();
    stmt.execute("create temporary function sleepMsUDF as '" + SleepMsUDF.class.getName() + "'");
    stmt.close();

    ExceptionHolder tExecuteHolder = new ExceptionHolder();
    ExceptionHolder tKillHolder = new ExceptionHolder();

    executeQueryAndKill(con1, con2, tExecuteHolder, tKillHolder);

    assertNotNull("tExecute", tExecuteHolder.throwable);
    assertEquals("Query was cancelled. User invoked KILL QUERY", tExecuteHolder.throwable.getMessage());
    assertNull("tCancel", tKillHolder.throwable);
  }

  @Test
  public void testKillQueryWithDifferentServer() throws Exception {
    Connection con1 = DriverManager.getConnection(miniHS2directUrl1, System.getProperty("user.name"), "bar");
    Connection con2 = DriverManager.getConnection(miniHS2directUrl2, System.getProperty("user.name"), "bar");

    Statement stmt = con1.createStatement();
    stmt.execute("create temporary function sleepMsUDF as '" + SleepMsUDF.class.getName() + "'");
    stmt.close();

    ExceptionHolder tExecuteHolder = new ExceptionHolder();
    ExceptionHolder tKillHolder = new ExceptionHolder();

    executeQueryAndKill(con1, con2, tExecuteHolder, tKillHolder);

    assertNotNull("tExecute", tExecuteHolder.throwable);
    assertEquals(HiveStatement.QUERY_CANCELLED_MESSAGE + " " + KillQueriesOperation.KILL_QUERY_MESSAGE,
        tExecuteHolder.throwable.getMessage());
    assertNull("tCancel", tKillHolder.throwable);
  }

  @Test
  public void testKillQueryWithDifferentServerZKTurnedOff() throws Exception {
    Connection con1 = DriverManager.getConnection(miniHS2directUrl1, System.getProperty("user.name"), "bar");
    Connection con2 = DriverManager.getConnection(miniHS2directUrl2, System.getProperty("user.name"), "bar");

    Statement stmt = con1.createStatement();
    stmt.execute("create temporary function sleepMsUDF as '" + SleepMsUDF.class.getName() + "'");
    stmt.close();

    stmt = con2.createStatement();
    stmt.execute("set hive.zookeeper.killquery.enable = false");
    stmt.close();

    ExceptionHolder tExecuteHolder = new ExceptionHolder();
    ExceptionHolder tKillHolder = new ExceptionHolder();

    executeQueryAndKill(con1, con2, tExecuteHolder, tKillHolder);

    assertNull("tExecute", tExecuteHolder.throwable);
    assertNull("tCancel", tKillHolder.throwable);
  }

  @Test
  public void testKillQueryWithRandomId() throws Exception {
    Connection con1 = DriverManager.getConnection(miniHS2directUrl1, System.getProperty("user.name"), "bar");
    ExceptionHolder tKillHolder = new ExceptionHolder();

    Statement stmt = con1.createStatement();
    String queryId = "randomId123";
    try {
      LOG.info("Killing query: " + queryId);
      stmt.execute("kill query '" + queryId + "'");
      stmt.close();
    } catch (SQLException e) {
      LOG.warn("Exception when kill query", e);
      tKillHolder.throwable = e;
    }
    try {
      con1.close();
    } catch (Exception e) {
      LOG.warn("Exception when close stmt and con", e);
    }

    assertNotNull("tCancel", tKillHolder.throwable);
    assertTrue(tKillHolder.throwable.getMessage(), tKillHolder.throwable.getMessage().contains(REMOTE_ERROR_MESSAGE));
  }
}
