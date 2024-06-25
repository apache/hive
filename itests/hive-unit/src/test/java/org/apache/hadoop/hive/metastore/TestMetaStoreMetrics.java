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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests Hive Metastore Metrics.
 *
 */
public class TestMetaStoreMetrics {


  private static HiveConf hiveConf;
  private static IDriver driver;

  @BeforeClass
  public static void before() throws Exception {
    hiveConf = new HiveConfForTest(TestMetaStoreMetrics.class);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORE_THRIFT_CONNECTION_RETRIES, 3);
    hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_METRICS, true);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hiveConf
        .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");

    //Increments one HMS connection
    MetaStoreTestUtils.startMetaStoreWithRetry(hiveConf);

    //Increments one HMS connection (Hive.get())
    SessionState.start(new CliSessionState(hiveConf));
    driver = DriverFactory.newDriver(hiveConf);
  }


  @Test
  public void testMethodCounts() throws Exception {
    driver.run("show databases");

    //one call by init, one called here.
    Assert.assertEquals(2, Metrics.getRegistry().getTimers().get("api_get_databases").getCount());
  }

  @Test
  public void testMetaDataCounts() throws Exception {
    int initDbCount =
        (Integer)Metrics.getRegistry().getGauges().get(MetricsConstants.TOTAL_DATABASES).getValue();
    int initTblCount =
        (Integer)Metrics.getRegistry().getGauges().get(MetricsConstants.TOTAL_TABLES).getValue();
    int initPartCount =
        (Integer)Metrics.getRegistry().getGauges().get(MetricsConstants.TOTAL_PARTITIONS).getValue();

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

    Assert.assertEquals(2, Metrics.getRegistry().getCounters().get(MetricsConstants.CREATE_TOTAL_DATABASES).getCount());
    Assert.assertEquals(7, Metrics.getRegistry().getCounters().get(MetricsConstants.CREATE_TOTAL_TABLES).getCount());
    Assert.assertEquals(9, Metrics.getRegistry().getCounters().get(MetricsConstants.CREATE_TOTAL_PARTITIONS).getCount());

    Assert.assertEquals(1, Metrics.getRegistry().getCounters().get(MetricsConstants.DELETE_TOTAL_DATABASES).getCount());
    Assert.assertEquals(3, Metrics.getRegistry().getCounters().get(MetricsConstants.DELETE_TOTAL_TABLES).getCount());
    Assert.assertEquals(3, Metrics.getRegistry().getCounters().get(MetricsConstants.DELETE_TOTAL_PARTITIONS).getCount());

    //to test initial metadata count metrics.
    Assert.assertEquals(initDbCount + 1,
        Metrics.getRegistry().getGauges().get(MetricsConstants.TOTAL_DATABASES).getValue());
    Assert.assertEquals(initTblCount + 4,
        Metrics.getRegistry().getGauges().get(MetricsConstants.TOTAL_TABLES).getValue());
    Assert.assertEquals(initPartCount + 6,
        Metrics.getRegistry().getGauges().get(MetricsConstants.TOTAL_PARTITIONS).getValue());

  }


  @Test
  public void testConnections() throws Exception {

    Thread.sleep(2000);  // TODO Evil!  Need to figure out a way to remove this sleep.
    //initial state is one connection
    long initialCount =
        Metrics.getRegistry().getCounters().get(MetricsConstants.OPEN_CONNECTIONS).getCount();

    //create two connections
    HiveMetaStoreClient msc = new HiveMetaStoreClient(hiveConf);
    HiveMetaStoreClient msc2 = new HiveMetaStoreClient(hiveConf);

    Assert.assertEquals(initialCount + 2,
        Metrics.getRegistry().getCounters().get(MetricsConstants.OPEN_CONNECTIONS).getCount());

    //close one connection, verify still two left
    msc.close();
    Thread.sleep(2000);  // TODO Evil!  Need to figure out a way to remove this sleep.
    Assert.assertEquals(initialCount + 1,
        Metrics.getRegistry().getCounters().get(MetricsConstants.OPEN_CONNECTIONS).getCount());

    //close one connection, verify still one left
    msc2.close();
    Thread.sleep(2000);  // TODO Evil!  Need to figure out a way to remove this sleep.
    Assert.assertEquals(initialCount,
        Metrics.getRegistry().getCounters().get(MetricsConstants.OPEN_CONNECTIONS).getCount());
  }

  @Test
  public void testConnectionPoolMetrics() {
    // The connection pool's metric is in a pattern of ${poolName}.pool.${metricName}
    String secondaryPoolMetricName = "objectstore-secondary.pool.TotalConnections";
    String poolMetricName = "objectstore.pool.TotalConnections";
    Assert.assertEquals(MetastoreConf.getIntVar(hiveConf,
        MetastoreConf.ConfVars.CONNECTION_POOLING_MAX_CONNECTIONS),
        Metrics.getRegistry().getGauges().get(poolMetricName).getValue());
    Assert.assertEquals(2,
        Metrics.getRegistry().getGauges().get(secondaryPoolMetricName).getValue());
  }

}
