/*
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

import static org.apache.hadoop.hive.ql.exec.tez.TestWorkloadManager.plan;
import static org.apache.hadoop.hive.ql.exec.tez.TestWorkloadManager.pool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMPoolTrigger;
import org.apache.hadoop.hive.ql.exec.tez.WorkloadManager;
import org.apache.hadoop.hive.ql.wm.Action;
import org.apache.hadoop.hive.ql.wm.ExecutionTrigger;
import org.apache.hadoop.hive.ql.wm.Expression;
import org.apache.hadoop.hive.ql.wm.ExpressionFactory;
import org.apache.hadoop.hive.ql.wm.Trigger;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.jdbc.miniHS2.MiniHS2.MiniClusterType;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestTriggersMoveWorkloadManager extends AbstractJdbcTriggersTest {

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());

    String confDir = "../../data/conf/llap/";
    HiveConf.setHiveSiteLocation(new URL("file://" + new File(confDir).toURI().getPath() + "/hive-site.xml"));
    System.out.println("Setting hive-site: " + HiveConf.getHiveSiteLocation());

    conf = new HiveConf();
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    conf.setTimeVar(ConfVars.HIVE_TRIGGER_VALIDATION_INTERVAL_MS, 100, TimeUnit.MILLISECONDS);
    conf.setVar(ConfVars.HIVE_SERVER2_TEZ_INTERACTIVE_QUEUE, "default");
    conf.setBoolean("hive.test.workload.management", true);
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

  @Test(timeout = 60000)
  public void testTriggerMoveAndKill() throws Exception {
    Expression moveExpression = ExpressionFactory.fromString("EXECUTION_TIME > 1000");
    Expression killExpression = ExpressionFactory.fromString("EXECUTION_TIME > 5000");
    Trigger moveTrigger = new ExecutionTrigger("slow_query_move", moveExpression,
      new Action(Action.Type.MOVE_TO_POOL, "ETL"));
    Trigger killTrigger = new ExecutionTrigger("slow_query_kill", killExpression,
      new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(moveTrigger, killTrigger), Lists.newArrayList(killTrigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, null, killTrigger + " violated");
  }

  @Test(timeout = 60000)
  public void testTriggerMoveEscapeKill() throws Exception {
    Expression moveExpression = ExpressionFactory.fromString("HDFS_BYTES_READ > 100");
    Expression killExpression = ExpressionFactory.fromString("EXECUTION_TIME > 5000");
    Trigger moveTrigger = new ExecutionTrigger("move_big_read", moveExpression,
      new Action(Action.Type.MOVE_TO_POOL, "ETL"));
    Trigger killTrigger = new ExecutionTrigger("slow_query_kill", killExpression,
      new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(moveTrigger, killTrigger), Lists.newArrayList());
    String query = "select sleep(t1.under_col, 1), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col==t2.under_col";
    runQueryWithTrigger(query, null, null);
  }

  @Test(timeout = 60000)
  public void testTriggerMoveConflictKill() throws Exception {
    Expression moveExpression = ExpressionFactory.fromString("HDFS_BYTES_READ > 100");
    Expression killExpression = ExpressionFactory.fromString("HDFS_BYTES_READ > 100");
    Trigger moveTrigger = new ExecutionTrigger("move_big_read", moveExpression,
      new Action(Action.Type.MOVE_TO_POOL, "ETL"));
    Trigger killTrigger = new ExecutionTrigger("kill_big_read", killExpression,
      new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(moveTrigger, killTrigger), Lists.newArrayList());
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, null, killTrigger + " violated");
  }

  @Override
  protected void setupTriggers(final List<Trigger> triggers) throws Exception {
    setupTriggers(triggers, new ArrayList<>());
  }

  private void setupTriggers(final List<Trigger> biTriggers, final List<Trigger> etlTriggers) throws Exception {
    WorkloadManager wm = WorkloadManager.getInstance();
    WMPool biPool = pool("BI", 1, 0.8f);
    WMPool etlPool = pool("ETL", 1, 0.2f);
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(), Lists.newArrayList(biPool, etlPool));
    plan.getPlan().setDefaultPoolPath("BI");

    for (Trigger trigger : biTriggers) {
      plan.addToTriggers(wmTriggerFromTrigger(trigger));
      plan.addToPoolTriggers(new WMPoolTrigger("BI", trigger.getName()));
    }

    for (Trigger trigger : etlTriggers) {
      plan.addToTriggers(wmTriggerFromTrigger(trigger));
      plan.addToPoolTriggers(new WMPoolTrigger("ETL", trigger.getName()));
    }
    wm.updateResourcePlanAsync(plan).get(10, TimeUnit.SECONDS);
  }
}