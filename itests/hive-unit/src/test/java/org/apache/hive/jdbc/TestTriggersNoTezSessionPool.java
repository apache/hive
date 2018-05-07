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

public class TestTriggersNoTezSessionPool extends AbstractJdbcTriggersTest {

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
  public void testTriggerVertexTotalTasks() throws Exception {
    Expression expression = ExpressionFactory.fromString("VERTEX_TOTAL_TASKS > 50");
    Trigger trigger = new ExecutionTrigger("highly_parallel", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, getConfigs(), trigger + " violated");
  }

  @Test(timeout = 60000)
  public void testTriggerDAGTotalTasks() throws Exception {
    Expression expression = ExpressionFactory.fromString("DAG_TOTAL_TASKS > 50");
    Trigger trigger = new ExecutionTrigger("highly_parallel", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, getConfigs(), trigger + " violated");
  }

  @Test(timeout = 60000)
  public void testTriggerTotalLaunchedTasks() throws Exception {
    Expression expression = ExpressionFactory.fromString("TOTAL_LAUNCHED_TASKS > 50");
    Trigger trigger = new ExecutionTrigger("highly_parallel", expression, new Action(Action.Type.KILL_QUERY));
    setupTriggers(Lists.newArrayList(trigger));
    String query = "select sleep(t1.under_col, 5), t1.value from " + tableName + " t1 join " + tableName +
      " t2 on t1.under_col>=t2.under_col";
    runQueryWithTrigger(query, getConfigs(), trigger + " violated");
  }

  @Override
  void setupTriggers(final List<Trigger> triggers) throws Exception {
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