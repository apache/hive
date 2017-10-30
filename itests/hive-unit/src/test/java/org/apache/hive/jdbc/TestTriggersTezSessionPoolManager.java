/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionPoolManager;
import org.apache.hadoop.hive.ql.wm.ExecutionTrigger;
import org.apache.hadoop.hive.ql.wm.Expression;
import org.apache.hadoop.hive.ql.wm.ExpressionFactory;
import org.apache.hadoop.hive.ql.wm.MetastoreGlobalTriggersFetcher;
import org.apache.hadoop.hive.ql.wm.Trigger;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.jdbc.miniHS2.MiniHS2.MiniClusterType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestTriggersTezSessionPoolManager {
  protected static MiniHS2 miniHS2 = null;
  protected static String dataFileDir;
  static Path kvDataFilePath;
  private static String tableName = "testtab1";

  protected static HiveConf conf = null;
  protected Connection hs2Conn = null;

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());

    String confDir = "../../data/conf/llap/";
    HiveConf.setHiveSiteLocation(new URL("file://" + new File(confDir).toURI().getPath() + "/hive-site.xml"));
    System.out.println("Setting hive-site: " + HiveConf.getHiveSiteLocation());

    conf = new HiveConf();
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    conf.setVar(ConfVars.HIVE_SERVER2_TEZ_DEFAULT_QUEUES, "default");
    conf.setTimeVar(ConfVars.HIVE_TRIGGER_VALIDATION_INTERVAL_MS, 100, TimeUnit.MILLISECONDS);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_TEZ_INITIALIZE_DEFAULT_SESSIONS, true);
    conf.setBoolVar(ConfVars.TEZ_EXEC_SUMMARY, true);
    conf.setBoolVar(ConfVars.HIVE_STRICT_CHECKS_CARTESIAN, false);
    // don't want cache hits from llap io for testing filesystem bytes read counters
    conf.setVar(ConfVars.LLAP_IO_MEMORY_MODE, "none");

    conf.addResource(new URL("file://" + new File(confDir).toURI().getPath()
      + "/tez-site.xml"));

    miniHS2 = new MiniHS2(conf, MiniClusterType.LLAP);
    dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    kvDataFilePath = new Path(dataFileDir, "kv1.txt");

    Map<String, String> confOverlay = new HashMap<>();
    miniHS2.start(confOverlay);
    miniHS2.getDFS().getFileSystem().mkdirs(new Path("/apps_staging_dir/anonymous"));
  }

  @Before
  public void setUp() throws Exception {
    hs2Conn = TestJdbcWithMiniLlap.getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
  }

  @After
  public void tearDown() throws Exception {
    LlapBaseInputFormat.closeAll();
    hs2Conn.close();
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  @Test(timeout = 60000)
  public void testTriggerSlowQueryElapsedTime() throws Exception {
    Expression expression = ExpressionFactory.fromString("ELAPSED_TIME > 20000");
    Trigger trigger = new ExecutionTrigger("slow_query", expression, Trigger.Action.KILL_QUERY);
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select sleep(t1.under_col, 500), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, null, "Query was cancelled");
  }

  @Test(timeout = 60000)
  public void testTriggerSlowQueryExecutionTime() throws Exception {
    Expression expression = ExpressionFactory.fromString("EXECUTION_TIME > 1000");
    Trigger trigger = new ExecutionTrigger("slow_query", expression, Trigger.Action.KILL_QUERY);
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, null, "Query was cancelled");
  }

  @Test(timeout = 60000)
  public void testTriggerHighShuffleBytes() throws Exception {
    Expression expression = ExpressionFactory.fromString("SHUFFLE_BYTES > 100");
    Trigger trigger = new ExecutionTrigger("big_shuffle", expression, Trigger.Action.KILL_QUERY);
    setupTriggers(Lists.newArrayList(trigger));
    List<String> setCmds = new ArrayList<>();
    // reducer phase starts only after 75% of mappers
    // this will start 15 tasks, shuffle starts after 11 tasks
    setCmds.add("set hive.exec.dynamic.partition.mode=nonstrict");
    setCmds.add("set mapred.min.split.size=400");
    setCmds.add("set mapred.max.split.size=400");
    setCmds.add("set tez.grouping.min-size=400");
    setCmds.add("set tez.grouping.max-size=400");
    String query = "select t1.under_col, t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col order by t1.under_col";
    runQueryWithTrigger(query, setCmds, "Query was cancelled");
  }

  @Test(timeout = 60000)
  public void testTriggerHighBytesRead() throws Exception {
    Expression expression = ExpressionFactory.fromString("HDFS_BYTES_READ > 100");
    Trigger trigger = new ExecutionTrigger("big_read", expression, Trigger.Action.KILL_QUERY);
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, null, "Query was cancelled");
  }

  @Test(timeout = 60000)
  public void testTriggerHighBytesWrite() throws Exception {
    Expression expression = ExpressionFactory.fromString("FILE_BYTES_WRITTEN > 100");
    Trigger trigger = new ExecutionTrigger("big_write", expression, Trigger.Action.KILL_QUERY);
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, null, "Query was cancelled");
  }

  @Test(timeout = 60000)
  public void testTriggerTotalTasks() throws Exception {
    Expression expression = ExpressionFactory.fromString("TOTAL_TASKS > 50");
    Trigger trigger = new ExecutionTrigger("highly_parallel", expression, Trigger.Action.KILL_QUERY);
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, getConfigs(), "Query was cancelled");
  }

  @Test(timeout = 60000)
  public void testTriggerCustomReadOps() throws Exception {
    Expression expression = ExpressionFactory.fromString("HDFS_READ_OPS > 50");
    Trigger trigger = new ExecutionTrigger("high_read_ops", expression, Trigger.Action.KILL_QUERY);
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, getConfigs(), "Query was cancelled");
  }

  @Test(timeout = 120000)
  public void testTriggerCustomCreatedFiles() throws Exception {
    List<String> cmds = getConfigs();

    Expression expression = ExpressionFactory.fromString("CREATED_FILES > 5");
    Trigger trigger = new ExecutionTrigger("high_read_ops", expression, Trigger.Action.KILL_QUERY);
    setupTriggers(Lists.newArrayList(trigger));
    String query = "create table testtab2 as select * from " + tableName;
    runQueryWithTrigger(query, cmds, "Query was cancelled");

    // partitioned insert
    expression = ExpressionFactory.fromString("CREATED_FILES > 10");
    trigger = new ExecutionTrigger("high_read_ops", expression, Trigger.Action.KILL_QUERY);
    setupTriggers(Lists.newArrayList(trigger));
    cmds.add("drop table src3");
    cmds.add("create table src3 (key int) partitioned by (value string)");
    query = "insert overwrite table src3 partition (value) select sleep(under_col, 10), value from " + tableName +
      " where under_col < 100";
    runQueryWithTrigger(query, cmds, "Query was cancelled");
  }

  @Test(timeout = 240000)
  public void testTriggerCustomCreatedDynamicPartitions() throws Exception {
    List<String> cmds = getConfigs();
    cmds.add("drop table src2");
    cmds.add("create table src2 (key int) partitioned by (value string)");

    // query will get cancelled before creating 57 partitions
    String query =
      "insert overwrite table src2 partition (value) select * from " + tableName + " where under_col < 100";
    Expression expression = ExpressionFactory.fromString("CREATED_DYNAMIC_PARTITIONS > 20");
    Trigger trigger = new ExecutionTrigger("high_read_ops", expression, Trigger.Action.KILL_QUERY);
    setupTriggers(Lists.newArrayList(trigger));
    runQueryWithTrigger(query, cmds, "Query was cancelled");

    cmds = getConfigs();
    // let it create 57 partitions without any triggers
    query = "insert overwrite table src2 partition (value) select under_col, value from " + tableName +
      " where under_col < 100";
    setupTriggers(Lists.newArrayList());
    runQueryWithTrigger(query, cmds, null);

    // query will try to add 64 more partitions to already existing 57 partitions but will get cancelled for violation
    query = "insert into table src2 partition (value) select * from " + tableName + " where under_col < 200";
    expression = ExpressionFactory.fromString("CREATED_DYNAMIC_PARTITIONS > 30");
    trigger = new ExecutionTrigger("high_read_ops", expression, Trigger.Action.KILL_QUERY);
    setupTriggers(Lists.newArrayList(trigger));
    runQueryWithTrigger(query, cmds, "Query was cancelled");

    // let it create 64 more partitions (total 57 + 64 = 121) without any triggers
    query = "insert into table src2 partition (value) select * from " + tableName + " where under_col < 200";
    setupTriggers(Lists.newArrayList());
    runQueryWithTrigger(query, cmds, null);

    // re-run insert into but this time no new partitions will be created, so there will be no violation
    query = "insert into table src2 partition (value) select * from " + tableName + " where under_col < 200";
    expression = ExpressionFactory.fromString("CREATED_DYNAMIC_PARTITIONS > 10");
    trigger = new ExecutionTrigger("high_read_ops", expression, Trigger.Action.KILL_QUERY);
    setupTriggers(Lists.newArrayList(trigger));
    runQueryWithTrigger(query, cmds, null);
  }

  @Test(timeout = 60000)
  public void testTriggerCustomCreatedDynamicPartitionsMultiInsert() throws Exception {
    List<String> cmds = getConfigs();
    cmds.add("drop table src2");
    cmds.add("drop table src3");
    cmds.add("create table src2 (key int) partitioned by (value string)");
    cmds.add("create table src3 (key int) partitioned by (value string)");

    String query =
      "from " + tableName +
        " insert overwrite table src2 partition (value) select * where under_col < 100 " +
        " insert overwrite table src3 partition (value) select * where under_col >= 100 and under_col < 200";
    Expression expression = ExpressionFactory.fromString("CREATED_DYNAMIC_PARTITIONS > 70");
    Trigger trigger = new ExecutionTrigger("high_partitions", expression, Trigger.Action.KILL_QUERY);
    setupTriggers(Lists.newArrayList(trigger));
    runQueryWithTrigger(query, cmds, "Query was cancelled");
  }

  @Test(timeout = 60000)
  public void testTriggerCustomCreatedDynamicPartitionsUnionAll() throws Exception {
    List<String> cmds = getConfigs();
    cmds.add("drop table src2");
    cmds.add("create table src2 (key int) partitioned by (value string)");

    // query will get cancelled before creating 57 partitions
    String query =
      "insert overwrite table src2 partition (value) " +
        "select temps.* from (" +
        "select * from " + tableName + " where under_col < 100 " +
        "union all " +
        "select * from " + tableName + " where under_col >= 100 and under_col < 200) temps";
    Expression expression = ExpressionFactory.fromString("CREATED_DYNAMIC_PARTITIONS > 70");
    Trigger trigger = new ExecutionTrigger("high_partitions", expression, Trigger.Action.KILL_QUERY);
    setupTriggers(Lists.newArrayList(trigger));
    runQueryWithTrigger(query, cmds, "Query was cancelled");
  }

  @Test(timeout = 60000)
  public void testTriggerCustomNonExistent() throws Exception {
    Expression expression = ExpressionFactory.fromString("OPEN_FILES > 50");
    Trigger trigger = new ExecutionTrigger("non_existent", expression, Trigger.Action.KILL_QUERY);
    setupTriggers(Lists.newArrayList(trigger));
    String query =
      "select l.under_col, l.value from " + tableName + " l join " + tableName + " r on l.under_col>=r.under_col";
    runQueryWithTrigger(query, null, null);
  }

  @Test(timeout = 60000)
  public void testMultipleTriggers1() throws Exception {
    Expression shuffleExpression = ExpressionFactory.fromString("HDFS_BYTES_READ > 1000000");
    Trigger shuffleTrigger = new ExecutionTrigger("big_shuffle", shuffleExpression, Trigger.Action.KILL_QUERY);
    Expression execTimeExpression = ExpressionFactory.fromString("EXECUTION_TIME > 1000");
    Trigger execTimeTrigger = new ExecutionTrigger("slow_query", execTimeExpression, Trigger.Action.KILL_QUERY);
    setupTriggers(Lists.newArrayList(shuffleTrigger, execTimeTrigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, null, "Query was cancelled");
  }

  @Test(timeout = 60000)
  public void testMultipleTriggers2() throws Exception {
    Expression shuffleExpression = ExpressionFactory.fromString("HDFS_BYTES_READ > 100");
    Trigger shuffleTrigger = new ExecutionTrigger("big_shuffle", shuffleExpression, Trigger.Action.KILL_QUERY);
    Expression execTimeExpression = ExpressionFactory.fromString("EXECUTION_TIME > 100000");
    Trigger execTimeTrigger = new ExecutionTrigger("slow_query", execTimeExpression, Trigger.Action.KILL_QUERY);
    setupTriggers(Lists.newArrayList(shuffleTrigger, execTimeTrigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, null, "Query was cancelled");
  }

  private void createSleepUDF() throws SQLException {
    String udfName = TestJdbcWithMiniHS2.SleepMsUDF.class.getName();
    Connection con = hs2Conn;
    Statement stmt = con.createStatement();
    stmt.execute("create temporary function sleep as '" + udfName + "'");
    stmt.close();
  }

  private void runQueryWithTrigger(final String query, final List<String> setCmds,
    final String expect)
    throws Exception {

    Connection con = hs2Conn;
    TestJdbcWithMiniLlap.createTestTable(con, null, tableName, kvDataFilePath.toString());
    createSleepUDF();

    final Statement selStmt = con.createStatement();
    final Throwable[] throwable = new Throwable[1];
    Thread queryThread = new Thread(() -> {
      try {
        if (setCmds != null) {
          for (String setCmd : setCmds) {
            selStmt.execute(setCmd);
          }
        }
        selStmt.execute(query);
      } catch (SQLException e) {
        throwable[0] = e;
      }
    });
    queryThread.start();

    queryThread.join();
    selStmt.close();

    if (expect == null) {
      assertNull("Expected query to succeed", throwable[0]);
    } else {
      assertNotNull("Expected non-null throwable", throwable[0]);
      assertEquals(SQLException.class, throwable[0].getClass());
      assertTrue(expect + " is not contained in " + throwable[0].getMessage(),
        throwable[0].getMessage().contains(expect));
    }
  }

  protected void setupTriggers(final List<Trigger> triggers) throws Exception {
    MetastoreGlobalTriggersFetcher triggersFetcher = mock(MetastoreGlobalTriggersFetcher.class);
    when(triggersFetcher.fetch()).thenReturn(triggers);
    TezSessionPoolManager.getInstance().setGlobalTriggersFetcher(triggersFetcher);
  }

  private List<String> getConfigs(String... more) {
    List<String> setCmds = new ArrayList<>();
    setCmds.add("set hive.exec.dynamic.partition.mode=nonstrict");
    setCmds.add("set mapred.min.split.size=100");
    setCmds.add("set mapred.max.split.size=100");
    setCmds.add("set tez.grouping.min-size=100");
    setCmds.add("set tez.grouping.max-size=100");
    if (more != null) {
      setCmds.addAll(Arrays.asList(more));
    }
    return setCmds;
  }
}