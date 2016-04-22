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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.common.metrics.metrics2.MetricsReporting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 * Test HMS Metrics on HBase Metastore
 */
public class TestHBaseMetastoreMetrics extends HBaseIntegrationTests {

  @BeforeClass
  public static void startup() throws Exception {
    HBaseIntegrationTests.startMiniCluster();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    HBaseIntegrationTests.shutdownMiniCluster();
  }

  @Before
  public void before() throws IOException {
    HBaseReadWrite.setConf(conf);
    conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL,
      "org.apache.hadoop.hive.metastore.hbase.HBaseStore");
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_FASTPATH, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);

    conf.setBoolVar(HiveConf.ConfVars.METASTORE_METRICS, true);
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_REPORTER, MetricsReporting.JSON_FILE.name() + "," + MetricsReporting.JMX.name());
    SessionState.start(new CliSessionState(conf));
    driver = new Driver(conf);
  }

  @Test
  public void testMetaDataCounts() throws Exception {
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

    CodahaleMetrics metrics = (CodahaleMetrics) MetricsFactory.getInstance();
    String json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.CREATE_TOTAL_DATABASES, 2);
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.CREATE_TOTAL_TABLES, 7);
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.CREATE_TOTAL_PARTITIONS, 9);

    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.DELETE_TOTAL_DATABASES, 1);
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.DELETE_TOTAL_TABLES, 3);
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, MetricsConstant.DELETE_TOTAL_PARTITIONS, 3);


    //to test initial metadata count metrics.
    conf.setVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL, ObjectStore.class.getName());
    HiveMetaStore.HMSHandler baseHandler = new HiveMetaStore.HMSHandler("test", conf, false);
    baseHandler.init();
    baseHandler.updateMetrics();

    //1 new db + default
    json = metrics.dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.INIT_TOTAL_DATABASES, 2);
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.INIT_TOTAL_TABLES, 4);
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, MetricsConstant.INIT_TOTAL_PARTITIONS, 6);
  }
}
