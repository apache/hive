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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ddl.process.kill.KillQueriesOperation;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@org.junit.Ignore("HIVE-25389")
public class TestKillQueryWithAuthorizationDisabled {
  private static final Logger LOG = LoggerFactory.getLogger(TestKillQueryWithAuthorizationDisabled.class);

  private static MiniHS2 miniHS2 = null;
  private static final String tableName = "testKillQueryMinihs2Tbl";
  private static final String testDbName = "testKillQueryMinihs2";
  private static final String tag = "killTag";

  private static class ExceptionHolder {
    Throwable throwable;
  }

  static class FakeGroupAuthenticator extends SessionStateUserAuthenticator {
    @Override public List<String> getGroupNames() {
      List<String> groups = new ArrayList<String>();
      if (getUserName().equals("user1")) {
        groups.add("group_a");
      } else if (getUserName().equals("user2")) {
        groups.add("group_a");
        groups.add("group_b");
      } else if (getUserName().equals(System.getProperty("user.name"))) {
        groups.add("group_b");
        groups.add("group_c");
      }
      return groups;
    }
  }

  public static class SleepMsUDF extends UDF {
    private static final Logger LOG = LoggerFactory.getLogger(TestKillQueryWithAuthorizationDisabled.class);

    public Integer evaluate(final Integer value, final Integer ms) {
      try {
        LOG.info("Sleeping for " + ms + " milliseconds");
        Thread.sleep(ms);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted Exception");
        // No-op
      }
      return value;
    }
  }

  static HiveConf defaultConf() throws Exception {
    String confDir = "../../data/conf/llap/";
    if (StringUtils.isNotBlank(confDir)) {
      HiveConf.setHiveSiteLocation(new URL("file://" + new File(confDir).toURI().getPath() + "/hive-site.xml"));
      System.out.println("Setting hive-site: " + HiveConf.getHiveSiteLocation());
    }
    HiveConf defaultConf = new HiveConf();
    defaultConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    defaultConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    defaultConf.setBoolVar(HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_ENABLED, false);
    defaultConf.addResource(new URL("file://" + new File(confDir).toURI().getPath() + "/tez-site.xml"));
    return defaultConf;
  }

  @BeforeClass public static void beforeTest() throws Exception {
    HiveConf conf = defaultConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    conf.set(HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER.varname, FakeGroupAuthenticator.class.getName());
    // Disable Hive Authorization
    conf.setBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED, false);
    MiniHS2.cleanupLocalDir();
    Class.forName(MiniHS2.getJdbcDriverName());
    miniHS2 = new MiniHS2(conf, MiniHS2.MiniClusterType.LLAP);
    Map<String, String> confOverlay = new HashMap<String, String>();
    miniHS2.start(confOverlay);
    miniHS2.getDFS().getFileSystem().mkdirs(new Path("/apps_staging_dir/anonymous"));

    Connection conDefault =
        BaseJdbcWithMiniLlap.getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
    Statement stmt = conDefault.createStatement();
    String tblName = testDbName + "." + tableName;
    String dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    Path dataFilePath = new Path(dataFileDir, "kv1.txt");
    String udfName = SleepMsUDF.class.getName();
    stmt.execute("drop database if exists " + testDbName + " cascade");
    stmt.execute("create database " + testDbName);
    stmt.execute("dfs -put " + dataFilePath.toString() + " " + "kv1.txt");
    stmt.execute("use " + testDbName);
    stmt.execute("create table " + tblName + " (int_col int, value string) ");
    stmt.execute("load data inpath 'kv1.txt' into table " + tblName);
    stmt.execute("create function sleepMsUDF as '" + udfName + "'");

    stmt.close();
    conDefault.close();
  }

  @AfterClass public static void afterTest() {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  /**
   * Test CLI kill command of a query that is running.
   * We spawn 2 threads - one running the query and
   * the other attempting to cancel.
   * We're using a dummy udf to simulate a query,
   * that runs for a sufficiently long time.
   * @throws Exception
   */
  private void testKillQueryInternal(String user, String killUser, boolean useTag, final ExceptionHolder stmtHolder,
      final ExceptionHolder tKillHolder) throws Exception {
    Connection con1 = BaseJdbcWithMiniLlap.getConnection(miniHS2.getJdbcURL(testDbName), user, "bar");
    Connection con2 = BaseJdbcWithMiniLlap.getConnection(miniHS2.getJdbcURL(testDbName), killUser, "bar");

    final Statement stmt2 = con2.createStatement();
    final HiveStatement stmt = (HiveStatement) con1.createStatement();
    final StringBuffer stmtQueryId = new StringBuffer();

    // Thread executing the query
    Thread tExecute = new Thread(() -> {
      try {
        String query =
            "select sleepMsUDF(t1.int_col, 10), t1.int_col, t2.int_col " + "from " + tableName + " t1 join " + tableName
                + " t2 on t1.int_col = t2.int_col";
        LOG.info("Executing query: " + query);
        stmt.execute("set hive.llap.execution.mode = none");
        stmt.execute("use " + testDbName);

        if (useTag) {
          stmt.execute("set hive.query.tag = " + tag);
        }
        // The test table has 500 rows, so total query time should be ~ 500*100ms = 50 seconds
        stmt.executeAsync(query);
        Thread.sleep(1000);
        stmtQueryId.append(stmt.getQueryId());
        stmt.getUpdateCount();
      } catch (SQLException | InterruptedException e) {
        stmtHolder.throwable = e;
      }
    });

    tExecute.start();

    // wait for other thread to create the stmt handle
    int count = 0;
    while (++count <= 10) {
      try {
        tKillHolder.throwable = null;
        Thread.sleep(2000);
        String queryId;
        if (useTag) {
          queryId = tag;
        } else {
          if (stmtQueryId.length() != 0) {
            queryId = stmtQueryId.toString();
          } else {
            continue;
          }
        }
        System.out.println("Killing query: " + queryId);
        stmt2.execute("kill query '" + queryId + "'");
        stmt2.close();
        break;
      } catch (SQLException e) {
        LOG.warn("Exception when kill query", e);
        tKillHolder.throwable = e;
      }
    }

    tExecute.join();
    try {
      stmt.close();
      con1.close();
      con2.close();
    } catch (Exception e) {
      // ignore error
      LOG.warn("Exception when close stmt and con", e);
    }
  }

  @Test public void testKillQueryByIdOwner() throws Exception {
    ExceptionHolder tExecuteHolder = new ExceptionHolder();
    ExceptionHolder tKillHolder = new ExceptionHolder();
    testKillQueryInternal("user1", "user1", false, tExecuteHolder, tKillHolder);
    assertNotNull("tExecute", tExecuteHolder.throwable);
    assertEquals(HiveStatement.QUERY_CANCELLED_MESSAGE + " " + KillQueriesOperation.KILL_QUERY_MESSAGE,
        tExecuteHolder.throwable.getMessage());
    assertNull("tCancel", tKillHolder.throwable);
  }

  @Test public void testKillQueryByIdAdmin() throws Exception {
    ExceptionHolder tExecuteHolder = new ExceptionHolder();
    ExceptionHolder tKillHolder = new ExceptionHolder();
    testKillQueryInternal("user1", System.getProperty("user.name"), false, tExecuteHolder, tKillHolder);
    assertNotNull("tExecute", tExecuteHolder.throwable);
    assertEquals(HiveStatement.QUERY_CANCELLED_MESSAGE + " " + KillQueriesOperation.KILL_QUERY_MESSAGE,
        tExecuteHolder.throwable.getMessage());
    assertNull("tCancel", tKillHolder.throwable);
  }

  @Test public void testKillQueryByIdNegative() throws Exception {
    ExceptionHolder tExecuteHolder = new ExceptionHolder();
    ExceptionHolder tKillHolder = new ExceptionHolder();
    testKillQueryInternal("user1", "user2", false, tExecuteHolder, tKillHolder);
    assertNull("tExecute", tExecuteHolder.throwable);
    assertNotNull("tCancel", tKillHolder.throwable);
    assertTrue(tKillHolder.throwable.getMessage(), tKillHolder.throwable.getMessage().contains("No privilege"));
  }

  @Test public void testKillQueryByTagAdmin() throws Exception {
    ExceptionHolder tExecuteHolder = new ExceptionHolder();
    ExceptionHolder tKillHolder = new ExceptionHolder();
    testKillQueryInternal("user1", System.getProperty("user.name"), true, tExecuteHolder, tKillHolder);
    assertNotNull("tExecute", tExecuteHolder.throwable);
    assertEquals(HiveStatement.QUERY_CANCELLED_MESSAGE + " " + KillQueriesOperation.KILL_QUERY_MESSAGE,
        tExecuteHolder.throwable.getMessage());
    assertNull("tCancel", tKillHolder.throwable);
  }

}
