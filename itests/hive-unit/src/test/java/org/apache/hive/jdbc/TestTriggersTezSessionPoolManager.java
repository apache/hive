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

import org.apache.hadoop.hive.metastore.api.WMTrigger;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.ql.exec.tez.TezSessionPoolManager;
import org.apache.hadoop.hive.ql.wm.Action;
import org.apache.hadoop.hive.ql.wm.ExecutionTrigger;
import org.apache.hadoop.hive.ql.wm.Expression;
import org.apache.hadoop.hive.ql.wm.ExpressionFactory;
import org.apache.hadoop.hive.ql.wm.Trigger;
import org.junit.Test;
import com.google.common.collect.Lists;

public class TestTriggersTezSessionPoolManager extends AbstractJdbcTriggersTest {

  @Test(timeout = 60000)
  public void testTriggerSlowQueryElapsedTime() throws Exception {
    Expression expression = ExpressionFactory.fromString("ELAPSED_TIME > 20000");
    Trigger trigger = new ExecutionTrigger("slow_query", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select sleep(t1.under_col, 500), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, null, trigger + " violated");
  }

  @Test(timeout = 60000)
  public void testTriggerShortQueryElapsedTime() throws Exception {
    Expression expression = ExpressionFactory.fromString("ELAPSED_TIME > 100");
    Trigger trigger = new ExecutionTrigger("slow_query", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select sleep(t1.under_col, 500), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, null, trigger + " violated");
  }

  @Test(timeout = 60000)
  public void testTriggerSlowQueryExecutionTime() throws Exception {
    Expression expression = ExpressionFactory.fromString("EXECUTION_TIME > 1000");
    Trigger trigger = new ExecutionTrigger("slow_query", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, null, trigger + " violated");
  }

  @Test(timeout = 60000)
  public void testTriggerHighShuffleBytes() throws Exception {
    Expression expression = ExpressionFactory.fromString("SHUFFLE_BYTES > 100");
    Trigger trigger = new ExecutionTrigger("big_shuffle", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    List<String> cmds = new ArrayList<>();
    cmds.add("set hive.auto.convert.join=false");
    // to slow down the reducer so that SHUFFLE_BYTES publishing and validation can happen, adding sleep between
    // multiple reduce stages
    String query = "select count(distinct t.under_col), sleep(t.under_col, 10) from (select t1.under_col from " +
      tableName + " t1 " + "join " + tableName + " t2 on t1.under_col=t2.under_col order by sleep(t1.under_col, 0))" +
      " t group by t.under_col";
    runQueryWithTrigger(query, cmds, trigger + " violated");
  }

  @Test(timeout = 60000)
  public void testTriggerHighBytesRead() throws Exception {
    Expression expression = ExpressionFactory.fromString("HDFS_BYTES_READ > 100");
    Trigger trigger = new ExecutionTrigger("big_read", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, null, trigger + " violated");
  }

  @Test(timeout = 60000)
  public void testTriggerHighBytesWrite() throws Exception {
    Expression expression = ExpressionFactory.fromString("FILE_BYTES_WRITTEN > 100");
    Trigger trigger = new ExecutionTrigger("big_write", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, null, trigger + " violated");
  }

  @Test(timeout = 60000)
  public void testTriggerTotalTasks() throws Exception {
    Expression expression = ExpressionFactory.fromString("VERTEX_TOTAL_TASKS > 50");
    Trigger trigger = new ExecutionTrigger("highly_parallel", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, getConfigs(), trigger + " violated");
  }

  @Test(timeout = 60000)
  public void testTriggerDagTotalTasks() throws Exception {
    Expression expression = ExpressionFactory.fromString("DAG_TOTAL_TASKS > 50");
    Trigger trigger = new ExecutionTrigger("highly_parallel", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, getConfigs(), trigger + " violated");
  }

  @Test(timeout = 60000)
  public void testTriggerCustomReadOps() throws Exception {
    Expression expression = ExpressionFactory.fromString("HDFS_READ_OPS > 50");
    Trigger trigger = new ExecutionTrigger("high_read_ops", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, getConfigs(), trigger + " violated");
  }

  @Test(timeout = 120000)
  public void testTriggerCustomCreatedFiles() throws Exception {
    List<String> cmds = getConfigs();

    Expression expression = ExpressionFactory.fromString("CREATED_FILES > 5");
    Trigger trigger = new ExecutionTrigger("high_read_ops", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    String query = "create table testtab2 as select * from " + tableName;
    runQueryWithTrigger(query, cmds, trigger + " violated");

    // partitioned insert
    expression = ExpressionFactory.fromString("CREATED_FILES > 10");
    trigger = new ExecutionTrigger("high_read_ops", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    cmds.add("drop table src3");
    cmds.add("create table src3 (key int) partitioned by (value string)");
    query = "insert overwrite table src3 partition (value) select sleep(under_col, 10), value from " + tableName +
      " where under_col < 100";
    runQueryWithTrigger(query, cmds, trigger + " violated");
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
    Trigger trigger = new ExecutionTrigger("high_read_ops", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    runQueryWithTrigger(query, cmds, trigger + " violated");

    cmds = getConfigs();
    // let it create 57 partitions without any triggers
    query = "insert overwrite table src2 partition (value) select under_col, value from " + tableName +
      " where under_col < 100";
    setupTriggers(Lists.newArrayList());
    runQueryWithTrigger(query, cmds, null);

    // query will try to add 64 more partitions to already existing 57 partitions but will get cancelled for violation
    query = "insert into table src2 partition (value) select * from " + tableName + " where under_col < 200";
    expression = ExpressionFactory.fromString("CREATED_DYNAMIC_PARTITIONS > 30");
    trigger = new ExecutionTrigger("high_read_ops", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    runQueryWithTrigger(query, cmds, trigger + " violated");

    // let it create 64 more partitions (total 57 + 64 = 121) without any triggers
    query = "insert into table src2 partition (value) select * from " + tableName + " where under_col < 200";
    setupTriggers(Lists.newArrayList());
    runQueryWithTrigger(query, cmds, null);

    // re-run insert into but this time no new partitions will be created, so there will be no violation
    query = "insert into table src2 partition (value) select * from " + tableName + " where under_col < 200";
    expression = ExpressionFactory.fromString("CREATED_DYNAMIC_PARTITIONS > 10");
    trigger = new ExecutionTrigger("high_read_ops", expression, new Action(Action.Type.KILL_QUERY));
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
    Trigger trigger = new ExecutionTrigger("high_partitions", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    runQueryWithTrigger(query, cmds, trigger + " violated");
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
    Trigger trigger = new ExecutionTrigger("high_partitions", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    runQueryWithTrigger(query, cmds, trigger + " violated");
  }

  @Test(timeout = 60000)
  public void testTriggerCustomNonExistent() throws Exception {
    Expression expression = ExpressionFactory.fromString("OPEN_FILES > 50");
    Trigger trigger = new ExecutionTrigger("non_existent", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    String query =
      "select l.under_col, l.value from " + tableName + " l join " + tableName + " r on l.under_col>=r.under_col";
    runQueryWithTrigger(query, null, null);
  }

  @Test(timeout = 60000)
  public void testTriggerDagRawInputSplitsKill() throws Exception {
    // Map 1 - 55 splits
    // Map 3 - 55 splits
    Expression expression = ExpressionFactory.fromString("DAG_RAW_INPUT_SPLITS > 100");
    Trigger trigger = new ExecutionTrigger("highly_parallel", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select t1.under_col, t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, getConfigs(), "Query was cancelled");
  }

  @Test(timeout = 60000)
  public void testTriggerVertexRawInputSplitsNoKill() throws Exception {
    // Map 1 - 55 splits
    // Map 3 - 55 splits
    Expression expression = ExpressionFactory.fromString("VERTEX_RAW_INPUT_SPLITS > 100");
    Trigger trigger = new ExecutionTrigger("highly_parallel", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select t1.under_col, t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, getConfigs(), null);
  }

  @Test(timeout = 60000)
  public void testTriggerVertexRawInputSplitsKill() throws Exception {
    // Map 1 - 55 splits
    // Map 3 - 55 splits
    Expression expression = ExpressionFactory.fromString("VERTEX_RAW_INPUT_SPLITS > 50");
    Trigger trigger = new ExecutionTrigger("highly_parallel", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select t1.under_col, t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, getConfigs(), "Query was cancelled");
  }

  @Test(timeout = 60000)
  public void testTriggerDefaultRawInputSplits() throws Exception {
    // Map 1 - 55 splits
    // Map 3 - 55 splits
    Expression expression = ExpressionFactory.fromString("RAW_INPUT_SPLITS > 50");
    Trigger trigger = new ExecutionTrigger("highly_parallel", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select t1.under_col, t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, getConfigs(), "Query was cancelled");
  }

  @Test(timeout = 60000)
  public void testMultipleTriggers1() throws Exception {
    Expression shuffleExpression = ExpressionFactory.fromString("HDFS_BYTES_READ > 1000000");
    Trigger shuffleTrigger = new ExecutionTrigger("big_shuffle", shuffleExpression, new Action(Action.Type.KILL_QUERY));
    Expression execTimeExpression = ExpressionFactory.fromString("EXECUTION_TIME > 1000");
    Trigger execTimeTrigger = new ExecutionTrigger("slow_query", execTimeExpression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(shuffleTrigger, execTimeTrigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, null, execTimeTrigger + " violated");
  }

  @Test(timeout = 60000)
  public void testMultipleTriggers2() throws Exception {
    Expression shuffleExpression = ExpressionFactory.fromString("HDFS_BYTES_READ > 100");
    Trigger shuffleTrigger = new ExecutionTrigger("big_shuffle", shuffleExpression, new Action(Action.Type.KILL_QUERY));
    Expression execTimeExpression = ExpressionFactory.fromString("EXECUTION_TIME > 100000");
    Trigger execTimeTrigger = new ExecutionTrigger("slow_query", execTimeExpression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(shuffleTrigger, execTimeTrigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, null, shuffleTrigger + " violated");
  }

  @Override
  protected void setupTriggers(final List<Trigger> triggers) throws Exception {
    WMFullResourcePlan rp = new WMFullResourcePlan(
      new WMResourcePlan("rp"), null);
    for (Trigger trigger : triggers) {
      WMTrigger wmTrigger = wmTriggerFromTrigger(trigger);
      wmTrigger.setIsInUnmanaged(true);
      rp.addToTriggers(wmTrigger);
    }
    TezSessionPoolManager.getInstance().updateTriggers(rp);
  }
}