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

package org.apache.hadoop.hive.hbase;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestHBaseQueries {

  private static MiniZooKeeperCluster zooKeeperCluster;
  private static MiniHBaseCluster miniHBaseCluster;
  private final HiveConf baseConf;
  private IDriver driver;

  /**
   * Test class for running queries using HBase tables. Creates a mini ZK and an HBase test cluster.
   * Each test method must instantiate a driver object first, using either the baseConf or a modified version of that.
   * Once finished, each test method must take care of cleaning up any database objects they've created (tables,
   * databases, etc.), otherwise those will be visible for subsequent test methods too.
   */
  public TestHBaseQueries() throws Exception {
    baseConf = new HiveConfForTest(HBaseConfiguration.create(), TestHBaseQueries.class);
    baseConf.set(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER.varname, SQLStdHiveAuthorizerFactory.class.getName());

    // set up Zookeeper
    File tmpDir = Files.createTempDirectory(Paths.get("/tmp/"), "tmp_").toFile();
    System.setProperty("zookeeper.4lw.commands.whitelist", "stat");
    zooKeeperCluster = new MiniZooKeeperCluster();
    int zkPort = zooKeeperCluster.startup(tmpDir);
    baseConf.setInt("hbase.zookeeper.property.clientPort", zkPort);

    // set up HBase
    baseConf.setBoolean("hbase.netty.nativetransport", false);
    HBaseTestingUtility util = new HBaseTestingUtility(baseConf);
    util.setZkCluster(zooKeeperCluster);
    miniHBaseCluster = util.startMiniHBaseCluster(1, 1);

    // set up HMS backend DB
    TestTxnDbUtil.setConfValues(baseConf);
    TestTxnDbUtil.cleanDb(baseConf);
    TestTxnDbUtil.prepDb(baseConf);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (miniHBaseCluster != null) {
      miniHBaseCluster.shutdown();
    }
    if (zooKeeperCluster != null) {
      zooKeeperCluster.shutdown();
    }
  }

  @Before
  public void before() {
    SessionState.start(baseConf);
  }

  @After
  public void after() throws Exception {
    if (driver != null) {
      driver.close();
    }
    TestTxnDbUtil.cleanDb(baseConf);
  }

  @Test
  public void testRollbackDoesNotDeleteOriginTableWhenCTLTFails() throws CommandProcessorException {
    HiveConf conf = new HiveConf(baseConf);
    conf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STRICT_MANAGED_TABLES, true);
    conf.setBoolVar(HiveConf.ConfVars.CREATE_TABLES_AS_ACID, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_CREATE_TABLES_AS_INSERT_ONLY, true);
    conf.setVar(HiveConf.ConfVars.HIVE_DEFAULT_MANAGED_FILEFORMAT, "ORC");

    driver = DriverFactory.newDriver(conf);

    driver.run("CREATE EXTERNAL TABLE `old_hbase_hive_table`(\n" +
        "   `key` int COMMENT '',\n" +
        "   `value` string COMMENT '')\n" +
        " ROW FORMAT SERDE\n" +
        "   'org.apache.hadoop.hive.hbase.HBaseSerDe'\n" +
        " STORED BY\n" +
        "   'org.apache.hadoop.hive.hbase.HBaseStorageHandler'\n" +
        " WITH SERDEPROPERTIES (\n" +
        "   'hbase.columns.mapping'=':key,cf:cf')\n" +
        " TBLPROPERTIES ('hbase.table.name'='hbase_hive_table')");
    try {
      driver.run("create table new_hbase_hive_table like old_hbase_hive_table");
    } catch (Exception e) {
      // expected - above command will try to create a transactional table but will fail
    }
    // original table is still present, rollback() has not deleted it
    driver.run("select * from old_hbase_hive_table");

    driver.run("drop table old_hbase_hive_table");
  }
}
