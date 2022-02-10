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

package org.apache.hadoop.hive.ql.reexec;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.hive.jdbc.TestJdbcWithMiniLlapArrow;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestReExecuteKilledTezAMQueryPlugin {
  protected static final Logger LOG = LoggerFactory.getLogger(TestReExecuteKilledTezAMQueryPlugin.class);

  private static MiniHS2 miniHS2 = null;
  private static final String tableName = "testKillTezAmTbl";
  private static String dataFileDir;
  private static final String testDbName = "testKillTezAmDb";
  private static HiveConf conf;

  private static class ExceptionHolder {
    Throwable throwable;
  }

  static HiveConf defaultConf() throws Exception {
    String confDir = "../../data/conf/llap/";
    if (confDir != null && !confDir.isEmpty()) {
      HiveConf.setHiveSiteLocation(new URL("file://"+ new File(confDir).toURI().getPath() + "/hive-site.xml"));
      System.out.println("Setting hive-site: " + HiveConf.getHiveSiteLocation());
    }
    HiveConf defaultConf = new HiveConf();
    defaultConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    defaultConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    defaultConf.addResource(new URL("file://" + new File(confDir).toURI().getPath() + "/tez-site.xml"));
    return defaultConf;
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    conf = defaultConf();
    conf.setVar(HiveConf.ConfVars.USERS_IN_ADMIN_ROLE, System.getProperty("user.name"));
    conf.set(HiveConf.ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES.varname, "recompile_without_cbo,reexecute_lost_am");
    MiniHS2.cleanupLocalDir();
    Class.forName(MiniHS2.getJdbcDriverName());
    miniHS2 = new MiniHS2(conf, MiniHS2.MiniClusterType.LLAP);
    dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    Map<String, String> confOverlay = new HashMap<String, String>();
    miniHS2.start(confOverlay);
    miniHS2.getDFS().getFileSystem().mkdirs(new Path("/apps_staging_dir/anonymous"));

    Connection conDefault = getConnection(miniHS2.getJdbcURL(),
    System.getProperty("user.name"), "bar");
    Statement stmt = conDefault.createStatement();
    String tblName = testDbName + "." + tableName;
    Path dataFilePath = new Path(dataFileDir, "kv1.txt");
    String udfName = TestJdbcWithMiniLlapArrow.SleepMsUDF.class.getName();
    stmt.execute("drop database if exists " + testDbName + " cascade");
    stmt.execute("create database " + testDbName);
    stmt.execute("set role admin");
    stmt.execute("dfs -put " + dataFilePath.toString() + " " + "kv1.txt");
    stmt.execute("use " + testDbName);
    stmt.execute("create table " + tblName + " (int_col int, value string) ");
    stmt.execute("load data inpath 'kv1.txt' into table " + tblName);
    stmt.execute("create function sleepMsUDF as '" + udfName + "'");
    stmt.execute("grant select on table " + tblName + " to role public");

    stmt.close();
    conDefault.close();
  }

  @AfterClass
  public static void afterTest() {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  public static Connection getConnection(String jdbcURL, String user, String pwd) throws SQLException {
    Connection conn = DriverManager.getConnection(jdbcURL, user, pwd);
    conn.createStatement().execute("set hive.support.concurrency = false");
    return conn;
  }

  @Test
  public void testKillQueryById() throws Exception {
    String user = System.getProperty("user.name");
    Connection con1 = getConnection(miniHS2.getJdbcURL(testDbName), user, "bar");

    final HiveStatement stmt = (HiveStatement)con1.createStatement();
    final StringBuffer stmtQueryId = new StringBuffer();
    ExceptionHolder originalQExHolder = new ExceptionHolder();
    originalQExHolder.throwable = null;

    // Thread executing the query
    Thread tExecute = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          System.out.println("Executing query: ");
          stmt.execute("set hive.llap.execution.mode = none");

          // The test table has 500 rows, so total query time should be ~ 500*100ms
          stmt.executeAsync("select sleepMsUDF(t1.int_col, 100), t1.int_col, t2.int_col " +
          "from " + tableName + " t1 join " + tableName + " t2 on t1.int_col = t2.int_col");
          stmtQueryId.append(stmt.getQueryId());
          stmt.getUpdateCount();
        } catch (SQLException e) {
          originalQExHolder.throwable = e;
        }
      }
    });

    tExecute.start();

    // wait for other thread to create the stmt handle
    int count = 0;
    while (++count <= 10) {
      Thread.sleep(2000);
      if (stmtQueryId.length() != 0) {
        String queryId = stmtQueryId.toString();
        System.out.println("Killing query: " + queryId);
        killAMForQueryId(queryId);
        break;
      }
    }

    tExecute.join();
    try {
      stmt.close();
      con1.close();
    } catch (Exception e) {
      // ignore error
      LOG.warn("Exception when close stmt and con", e);
    }
    Assert.assertEquals(originalQExHolder.throwable, null);
  }

  private void killAMForQueryId(String queryId) throws Exception {
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    List<ApplicationReport> applicationReports = yarnClient.getApplications();
    String diagnosticsMessage = "AM Container for %s exited with exitCode: -100";
    for (ApplicationReport ar : applicationReports) {
      LOG.info("Killing application: " + ar.getApplicationId());
      if (ar.getApplicationTags().contains(queryId)) {
        yarnClient.killApplication(ar.getApplicationId(), String.format(diagnosticsMessage, ar.getCurrentApplicationAttemptId()));
        ApplicationReport updated = yarnClient.getApplicationReport(ar.getApplicationId());
        Assert.assertEquals(updated.getYarnApplicationState(), YarnApplicationState.KILLED);
      }
    }
  }
}