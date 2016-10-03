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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests Hive Metastore Metrics.
 *
 */
public class TestMetaStoreMetrics {


  private static HiveConf hiveConf;
  private static Driver driver;
  private static CodahaleMetrics metrics;

  @BeforeClass
  public static void before() throws Exception {
    int port = MetaStoreUtils.findFreePort();

    hiveConf = new HiveConf(TestMetaStoreMetrics.class);
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_METRICS, true);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hiveConf
        .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");

    MetricsFactory.close();
    MetricsFactory.init(hiveConf);
    metrics = (CodahaleMetrics) MetricsFactory.getInstance();

    //Increments one HMS connection
    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge(), hiveConf);

    //Increments one HMS connection (Hive.get())
    SessionState.start(new CliSessionState(hiveConf));
    driver = new Driver(hiveConf);
  }


  @Test
  public void testMethodCounts() throws Exception {
    driver.run("show databases");
    String json = metrics.dumpJson();

    //one call by init, one called here.
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.TIMER, "api_get_all_databases", 2);
  }

  @Test
  public void testMetaDataCounts() throws Exception {
    CodahaleMetrics metrics = (CodahaleMetrics) MetricsFactory.getInstance();
    String json = metrics.dumpJson();

    int initDbCount = (new Integer((MetricsTestUtils.getJsonNode(json, MetricsTestUtils.GAUGE,
        MetricsConstant.INIT_TOTAL_DATABASES)).asText())).intValue();
    int initTblCount = (new Integer((MetricsTestUtils.getJsonNode(json, MetricsTestUtils.GAUGE,
        MetricsConstant.INIT_TOTAL_TABLES)).asText())).intValue();
    int initPartCount = (new Integer((MetricsTestUtils.getJsonNode(json, MetricsTestUtils.GAUGE,
        MetricsConstant.INIT_TOTAL_PARTITIONS)).asText())).intValue();

    //1 databases created
    driver.run("create database testdb1");

    //4 tables
    driver.run("create table testtbl1 (key string)");
    driver.run("create table testtblpart (key string) partitioned by (partkey string)");
    driver.run("use testdb1");
    driver.run("create table testtbl2 (key string)");
    driver.run("create table testtblpart2 (key string) partitioned by (partkey string)");

    //6 partitions
    driver.run("alter table default.testtblpart add partition (partkey='a')");
    driver.run("alter table default.testtblpart add partition (partkey='b')");
    driver.run("alter table default.testtblpart add partition (partkey='c')");
    driver.run("alter table testdb1.testtblpart2 add partition (partkey='a')");
    driver.run("alter table testdb1.testtblpart2 add partition (partkey='b')");
    driver.run("alter table testdb1.testtblpart2 add partition (partkey='c')");


    //create and drop some additional metadata, to test drop counts.
    driver.run("create database tempdb");
    driver.run("use tempdb");

    driver.run("create table delete_by_table (key string) partitioned by (partkey string)");
    driver.run("alter table delete_by_table add partition (partkey='temp')");
    driver.run("drop table delete_by_table");

    driver.run("create table delete_by_part (key string) partitioned by (partkey string)");
    driver.run("alter table delete_by_part add partition (partkey='temp')");
    driver.run("alter table delete_by_part drop partition (partkey='temp')");

    driver.run("create table delete_by_db (key string) partitioned by (partkey string)");
    driver.run("alter table delete_by_db add partition (partkey='temp')");
    driver.run("use default");
    driver.run("drop database tempdb cascade");

    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.CREATE_TOTAL_DATABASES, 2);
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.CREATE_TOTAL_TABLES, 7);
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.CREATE_TOTAL_PARTITIONS, 9);

    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.DELETE_TOTAL_DATABASES, 1);
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.DELETE_TOTAL_TABLES, 3);
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.DELETE_TOTAL_PARTITIONS, 3);

    //to test initial metadata count metrics.
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL, ObjectStore.class.getName());
    HiveMetaStore.HMSHandler baseHandler = new HiveMetaStore.HMSHandler("test", hiveConf, false);
    baseHandler.init();
    baseHandler.updateMetrics();

    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.INIT_TOTAL_DATABASES, initDbCount + 1);
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.INIT_TOTAL_TABLES, initTblCount + 4);
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.INIT_TOTAL_PARTITIONS, initPartCount + 6);
  }


  @Test
  public void testConnections() throws Exception {

    //initial state is one connection
    String json = metrics.dumpJson();
    int initialCount = (new Integer((MetricsTestUtils.getJsonNode(json, MetricsTestUtils.COUNTER,
                                       MetricsConstant.OPEN_CONNECTIONS)).asText())).intValue();

    //create two connections
    HiveMetaStoreClient msc = new HiveMetaStoreClient(hiveConf);
    HiveMetaStoreClient msc2 = new HiveMetaStoreClient(hiveConf);

    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.OPEN_CONNECTIONS, initialCount + 2);

    //close one connection, verify still two left
    msc.close();
    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.OPEN_CONNECTIONS, initialCount + 1);

    //close one connection, verify still one left
    msc2.close();
    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.OPEN_CONNECTIONS, initialCount);
  }
}
