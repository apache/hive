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

package org.apache.hadoop.hive.ql.parse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestColumnAccess {

  @BeforeClass
  public static void Setup() throws Exception {
    Driver driver = createDriver();
    driver.run("create table t1(id1 int, name1 string)");
    driver.run("create table t2(id2 int, id1 int, name2 string)");
    driver.run("create table t3(id1 int) partitioned by (`date` int, p0 string)");
    driver.run("create view v1 as select * from t1");
  }

  @AfterClass
  public static void Teardown() throws Exception {
    Driver driver = createDriver();
    driver.run("drop table t1");
    driver.run("drop table t2");
    driver.run("drop table t3");
    driver.run("drop view v1");
  }

  @Test
  public void testQueryTable1() throws ParseException {
    String query = "select * from t1";
    Driver driver = createDriver();
    int rc = driver.compile(query, true);
    Assert.assertEquals("Checking command success", 0, rc);
    QueryPlan plan = driver.getPlan();
    // check access columns from ColumnAccessInfo
    ColumnAccessInfo columnAccessInfo = plan.getColumnAccessInfo();
    List<String> cols = columnAccessInfo.getTableToColumnAccessMap().get("default@t1");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertTrue(cols.contains("id1"));
    Assert.assertTrue(cols.contains("name1"));

    // check access columns from readEntity
    Map<String, List<String>> tableColsMap = getColsFromReadEntity(plan.getInputs());
    cols = tableColsMap.get("default@t1");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertTrue(cols.contains("id1"));
    Assert.assertTrue(cols.contains("name1"));
  }

  @Test
  public void testJoinTable1AndTable2() throws ParseException {
    String query = "select * from t1 join t2 on (t1.id1 = t2.id1)";
    Driver driver = createDriver();
    int rc = driver.compile(query, true);
    Assert.assertEquals("Checking command success", 0, rc);
    QueryPlan plan = driver.getPlan();
    // check access columns from ColumnAccessInfo
    ColumnAccessInfo columnAccessInfo = plan.getColumnAccessInfo();
    List<String> cols = columnAccessInfo.getTableToColumnAccessMap().get("default@t1");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertTrue(cols.contains("id1"));
    Assert.assertTrue(cols.contains("name1"));
    cols = columnAccessInfo.getTableToColumnAccessMap().get("default@t2");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertTrue(cols.contains("id2"));
    Assert.assertTrue(cols.contains("id1"));
    Assert.assertTrue(cols.contains("name2"));


    // check access columns from readEntity
    Map<String, List<String>> tableColsMap = getColsFromReadEntity(plan.getInputs());
    cols = tableColsMap.get("default@t1");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertTrue(cols.contains("id1"));
    Assert.assertTrue(cols.contains("name1"));
    cols = tableColsMap.get("default@t2");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertTrue(cols.contains("id2"));
    Assert.assertTrue(cols.contains("id1"));
    Assert.assertTrue(cols.contains("name2"));
  }

  @Test
  public void testJoinView1AndTable2() throws ParseException {
    String query = "select * from v1 join t2 on (v1.id1 = t2.id1)";
    Driver driver = createDriver();
    int rc = driver.compile(query, true);
    Assert.assertEquals("Checking command success", 0, rc);
    QueryPlan plan = driver.getPlan();
    // check access columns from ColumnAccessInfo
    ColumnAccessInfo columnAccessInfo = plan.getColumnAccessInfo();
    // t1 is inside v1, we should not care about its access info.
    List<String> cols = columnAccessInfo.getTableToColumnAccessMap().get("default@t1");
    Assert.assertNull(cols);
    // v1 is top level view, we should care about its access info.
    cols = columnAccessInfo.getTableToColumnAccessMap().get("default@v1");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertTrue(cols.contains("id1"));
    Assert.assertTrue(cols.contains("name1"));
    cols = columnAccessInfo.getTableToColumnAccessMap().get("default@t2");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertTrue(cols.contains("id2"));
    Assert.assertTrue(cols.contains("id1"));
    Assert.assertTrue(cols.contains("name2"));


    // check access columns from readEntity
    Map<String, List<String>> tableColsMap = getColsFromReadEntity(plan.getInputs());
    cols = tableColsMap.get("default@t1");
    Assert.assertNull(cols);
    cols = tableColsMap.get("default@v1");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertTrue(cols.contains("id1"));
    Assert.assertTrue(cols.contains("name1"));
    cols = tableColsMap.get("default@t2");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertTrue(cols.contains("id2"));
    Assert.assertTrue(cols.contains("id1"));
    Assert.assertTrue(cols.contains("name2"));
  }

  @Test
  public void testShowPartitions() throws Exception {
    String query = "show partitions t3 where `date` > 20011113";
    Driver driver = createDriver();
    int rc = driver.compile(query, true);
    Assert.assertEquals("Checking command success", 0, rc);
    QueryPlan plan = driver.getPlan();
    ColumnAccessInfo columnAccessInfo = plan.getColumnAccessInfo();
    List<String> cols = columnAccessInfo.getTableToColumnAccessMap().get("default@t3");

    Assert.assertEquals(2, cols.size());
    Assert.assertTrue(cols.contains("date"));
    Assert.assertTrue(cols.contains("p0"));
  }

  private Map<String, List<String>> getColsFromReadEntity(Set<ReadEntity> inputs) {
    Map<String, List<String>> tableColsMap = new HashMap<String, List<String>>();
    for(ReadEntity entity: inputs) {
      switch (entity.getType()) {
        case TABLE:
          if (entity.getAccessedColumns() != null && !entity.getAccessedColumns().isEmpty()) {
            tableColsMap.put(entity.getTable().getCompleteName(), entity.getAccessedColumns());
          }
          break;
        case PARTITION:
          if (entity.getAccessedColumns() != null && !entity.getAccessedColumns().isEmpty()) {
            tableColsMap.put(entity.getPartition().getTable().getCompleteName(),
                entity.getAccessedColumns());
          }
          break;
        default:
          // no-op
      }
    }
    return tableColsMap;
  }

  private static Driver createDriver() {
    HiveConf conf = new HiveConf(Driver.class);
    conf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_COLLECT_SCANCOLS, true);
    SessionState.start(conf);
    Driver driver = new Driver(conf);
    return driver;
  }

}
