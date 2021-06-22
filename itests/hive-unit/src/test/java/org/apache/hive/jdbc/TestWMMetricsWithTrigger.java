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

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.common.metrics.metrics2.MetricsReporting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.tez.WorkloadManager;
import org.apache.hadoop.hive.ql.wm.*;
import org.apache.hadoop.metrics2.AbstractMetric;
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
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@org.junit.Ignore("HIVE-25247")
public class TestWMMetricsWithTrigger {

  private final Logger LOG = LoggerFactory.getLogger(getClass().getName());
  private static MiniHS2 miniHS2 = null;
  private static List<Iterable<AbstractMetric>> metricValues = new ArrayList<>();
  private static final String tableName = "testWmMetricsTriggerTbl";
  private static final String testDbName = "testWmMetricsTrigger";
  private static String wmPoolName = "llap";

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

  private static class ExceptionHolder {
    Throwable throwable;
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
    defaultConf.setVar(HiveConf.ConfVars.HIVE_AUTHENTICATOR_MANAGER,
        "org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator");
    defaultConf.addResource(new URL("file://" + new File(confDir).toURI().getPath() + "/tez-site.xml"));
    defaultConf.setTimeVar(HiveConf.ConfVars.HIVE_TRIGGER_VALIDATION_INTERVAL, 100, TimeUnit.MILLISECONDS);
    defaultConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_INTERACTIVE_QUEUE, "default");
    defaultConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED, true);
    defaultConf.setVar(HiveConf.ConfVars.HIVE_METRICS_REPORTER, MetricsReporting.JSON_FILE.name());
    // don't want cache hits from llap io for testing filesystem bytes read counters
    defaultConf.setVar(HiveConf.ConfVars.LLAP_IO_MEMORY_MODE, "none");
    return defaultConf;
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    HiveConf conf = defaultConf();

    Class.forName(MiniHS2.getJdbcDriverName());
    miniHS2 = new MiniHS2(conf, MiniHS2.MiniClusterType.LLAP);
    Map<String, String> confOverlay = new HashMap<>();
    miniHS2.start(confOverlay);
    miniHS2.getDFS().getFileSystem().mkdirs(new Path("/apps_staging_dir/anonymous"));

    Connection conDefault =
        BaseJdbcWithMiniLlap.getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
    Statement stmt = conDefault.createStatement();
    String tblName = testDbName + "." + tableName;
    String dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    Path dataFilePath = new Path(dataFileDir, "kv1.txt");
    String udfName = TestKillQueryWithAuthorizationDisabled.SleepMsUDF.class.getName();
    stmt.execute("drop database if exists " + testDbName + " cascade");
    stmt.execute("create database " + testDbName);
    stmt.execute("dfs -put " + dataFilePath.toString() + " " + "kv1.txt");
    stmt.execute("use " + testDbName);
    stmt.execute("create table " + tblName + " (int_col int, value string) ");
    stmt.execute("load data inpath 'kv1.txt' into table " + tblName);
    stmt.execute("create function sleep as '" + udfName + "'");
    stmt.close();
    conDefault.close();
    setupPlanAndTrigger();
  }

  private static void setupPlanAndTrigger() throws Exception {
    WorkloadManager wm = WorkloadManager.getInstance();
    WMPool wmPool = new WMPool("test_plan", wmPoolName);
    wmPool.setAllocFraction(1.0f);
    wmPool.setQueryParallelism(1);
    WMFullResourcePlan resourcePlan = new WMFullResourcePlan(new WMResourcePlan("rp"), Lists.newArrayList(wmPool));
    resourcePlan.getPlan().setDefaultPoolPath(wmPoolName);
    Expression expression = ExpressionFactory.fromString("EXECUTION_TIME > 10000");
    Trigger trigger = new ExecutionTrigger("kill_query", expression, new Action(Action.Type.KILL_QUERY));
    WMTrigger wmTrigger = wmTriggerFromTrigger(trigger);
    resourcePlan.addToTriggers(wmTrigger);
    resourcePlan.addToPoolTriggers(new WMPoolTrigger("llap", trigger.getName()));
    wm.updateResourcePlanAsync(resourcePlan).get(10, TimeUnit.SECONDS);
  }

  @AfterClass
  public static void afterTest() {
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
    metricValues.clear();
    metricValues = null;
  }

  void runQueryWithTrigger(int queryTimeoutSecs) throws Exception {
    LOG.info("Starting test");
    String query = "select sleep(t1.int_col + t2.int_col, 500), t1.value from " + tableName + " t1 join " + tableName
        + " t2 on t1.int_col>=t2.int_col";
    long start = System.currentTimeMillis();
    Connection conn =
        BaseJdbcWithMiniLlap.getConnection(miniHS2.getJdbcURL(testDbName), System.getProperty("user.name"), "bar");
    final Statement selStmt = conn.createStatement();
    Throwable throwable = null;
    try {
      if (queryTimeoutSecs > 0) {
        selStmt.setQueryTimeout(queryTimeoutSecs);
      }
      selStmt.execute(query);
    } catch (SQLException e) {
      throwable = e;
    }
    selStmt.close();
    assertNotNull("Expected non-null throwable", throwable);
    assertEquals(SQLException.class, throwable.getClass());
    assertTrue("Query was killed due to " + throwable.getMessage() + " and not because of trigger violation",
        throwable.getMessage().contains("violated"));
    long end = System.currentTimeMillis();
    LOG.info("time taken: {} ms", (end - start));
  }

  private static WMTrigger wmTriggerFromTrigger(Trigger trigger) {
    WMTrigger result = new WMTrigger("rp", trigger.getName());
    result.setTriggerExpression(trigger.getExpression().toString());
    result.setActionExpression(trigger.getAction().toString());
    return result;
  }

  @Test(timeout = 30000)
  public void testWmPoolMetricsAfterKillTrigger() throws Exception {
    verifyMetrics(0, 4, 1, 0);

    ExceptionHolder stmtHolder = new ExceptionHolder();
    // Run Query with Kill Trigger in place in a separate thread
    Thread tExecute = new Thread(() -> {
      try {
        runQueryWithTrigger(10);
      } catch (Exception e) {
        LOG.error("Exception while executing runQueryWithTrigger", e);
        stmtHolder.throwable = e;
      }
    });
    tExecute.start();

    //Wait for Workload Manager main thread to update the metrics after query enters processing.
    Thread.sleep(5000);
    verifyMetrics(4, 4, 1, 1);

    tExecute.join();
    assertNull("Exception while executing statement", stmtHolder.throwable);

    //Wait for Workload Manager main thread to update the metrics after kill query succeeded.
    Thread.sleep(10000);

    //Metrics should reset to original value after query is killed
    verifyMetrics(0, 4, 1, 0);

  }

  private static void verifyMetrics(int numExecutors, int numExecutorsMax, int numParallelQueries, int numRunningQueries)
      throws Exception {
    CodahaleMetrics metrics = (CodahaleMetrics) MetricsFactory.getInstance();
    String json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, "WM_llap_numExecutors", numExecutors);
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, "WM_llap_numExecutorsMax", numExecutorsMax);
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, "WM_llap_numParallelQueries", numParallelQueries);
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, "WM_llap_numRunningQueries", numRunningQueries);
  }

}
