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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hive.ql.txn.compactor.CompactorTestUtil.executeStatementOnDriverAndReturnResults;
import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.executeStatementOnDriver;

/**
 * Superclass for Test[Crud|Mm]CompactorOnTez, for setup and helper classes.
 */
public class CompactorOnTezTest {
  private static final AtomicInteger RANDOM_INT = new AtomicInteger(new Random().nextInt());
  private static final String TEST_DATA_DIR = new File(
      System.getProperty("java.io.tmpdir") + File.separator + TestCrudCompactorOnTez.class
          .getCanonicalName() + "-" + System.currentTimeMillis() + "_" + RANDOM_INT
          .getAndIncrement()).getPath().replaceAll("\\\\", "/");
  private static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";
  protected HiveConf conf;
  protected IMetaStoreClient msClient;
  protected IDriver driver;

  @Before
  // Note: we create a new conf and driver object before every test
  public void setup() throws Exception {
    File f = new File(TEST_WAREHOUSE_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if (!(new File(TEST_WAREHOUSE_DIR).mkdirs())) {
      throw new RuntimeException("Could not create " + TEST_WAREHOUSE_DIR);
    }
    HiveConf hiveConf = new HiveConf(this.getClass());
    hiveConf.setVar(HiveConf.ConfVars.PREEXECHOOKS, "");
    hiveConf.setVar(HiveConf.ConfVars.POSTEXECHOOKS, "");
    hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, TEST_WAREHOUSE_DIR);
    hiveConf.setVar(HiveConf.ConfVars.HIVEINPUTFORMAT, HiveInputFormat.class.getName());
    hiveConf.setVar(HiveConf.ConfVars.HIVEFETCHTASKCONVERSION, "none");
    TxnDbUtil.setConfValues(hiveConf);
    TxnDbUtil.cleanDb(hiveConf);
    TxnDbUtil.prepDb(hiveConf);
    conf = hiveConf;
    // Use tez as execution engine for this test class
    setupTez(conf);
    msClient = new HiveMetaStoreClient(conf);
    driver = DriverFactory.newDriver(conf);
    SessionState.start(new CliSessionState(conf));
  }

  private void setupTez(HiveConf conf) {
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    conf.setVar(HiveConf.ConfVars.HIVE_USER_INSTALL_DIR, TEST_DATA_DIR);
    conf.set("tez.am.resource.memory.mb", "128");
    conf.set("tez.am.dag.scheduler.class",
        "org.apache.tez.dag.app.dag.impl.DAGSchedulerNaturalOrderControlled");
    conf.setBoolean("tez.local.mode", true);
    conf.set("fs.defaultFS", "file:///");
    conf.setBoolean("tez.runtime.optimize.local.fetch", true);
    conf.set("tez.staging-dir", TEST_DATA_DIR);
    conf.setBoolean("tez.ignore.lib.uris", true);
    conf.set("hive.tez.container.size", "128");
    conf.setBoolean("hive.merge.tezfiles", false);
    conf.setBoolean("hive.in.tez.test", true);
  }

  @After public void tearDown() {
    if (msClient != null) {
      msClient.close();
    }
    if (driver != null) {
      driver.close();
    }
    conf = null;
  }

  protected class TestDataProvider {

    void createFullAcidTable(String tblName, boolean isPartitioned, boolean isBucketed)
        throws Exception {
      createTable(tblName, isPartitioned, isBucketed, false, "orc");
    }

    void createMmTable(String tblName, boolean isPartitioned, boolean isBucketed)
        throws Exception {
      createMmTable(tblName, isPartitioned, isBucketed, "orc");
    }

    void createMmTable(String tblName, boolean isPartitioned, boolean isBucketed, String fileFormat)
        throws Exception {
      createTable(tblName, isPartitioned, isBucketed, true, fileFormat);
    }

    private void createTable(String tblName, boolean isPartitioned, boolean isBucketed,
        boolean insertOnly, String fileFormat) throws Exception {

      executeStatementOnDriver("drop table if exists " + tblName, driver);
      StringBuilder query = new StringBuilder();
      query.append("create table ").append(tblName).append(" (a string, b int)");
      if (isPartitioned) {
        query.append(" partitioned by (ds string)");
      }
      if (isBucketed) {
        query.append(" clustered by (a) into 2 buckets");
      }
      query.append(" stored as ").append(fileFormat);
      query.append(" TBLPROPERTIES('transactional'='true',");
      if (insertOnly) {
        query.append(" 'transactional_properties'='insert_only')");
      } else {
        query.append(" 'transactional_properties'='default')");
      }
      executeStatementOnDriver(query.toString(), driver);
    }

    /**
     * 5 txns.
     */
    void insertTestDataPartitioned(String tblName) throws Exception {
      executeStatementOnDriver("insert into " + tblName
          + " values('1',2, 'today'),('1',3, 'today'),('1',4, 'yesterday'),('2',2, 'tomorrow'),"
          + "('2',3, 'yesterday'),('2',4, 'today')", driver);
      executeStatementOnDriver("insert into " + tblName
          + " values('3',2, 'tomorrow'),('3',3, 'today'),('3',4, 'yesterday'),('4',2, 'today'),"
          + "('4',3, 'tomorrow'),('4',4, 'today')", driver);
      executeStatementOnDriver("delete from " + tblName + " where b = 2", driver);
      executeStatementOnDriver("insert into " + tblName + " values('5',2, 'yesterday'),('5',3, 'yesterday'),"
          + "('5',4, 'today'),('6',2, 'today'),('6',3, 'today'),('6',4, 'today')", driver);
      executeStatementOnDriver("delete from " + tblName + " where a = '1'", driver);
    }

    /**
     * 3 txns.
     */
    protected void insertMmTestDataPartitioned(String tblName) throws Exception {
      executeStatementOnDriver("insert into " + tblName
          + " values('1',2, 'today'),('1',3, 'today'),('1',4, 'yesterday'),('2',2, 'tomorrow'),"
          + "('2',3, 'yesterday'),('2',4, 'today')", driver);
      executeStatementOnDriver("insert into " + tblName
          + " values('3',2, 'tomorrow'),('3',3, 'today'),('3',4, 'yesterday'),('4',2, 'today'),"
          + "('4',3, 'tomorrow'),('4',4, 'today')", driver);
      executeStatementOnDriver("insert into " + tblName + " values('5',2, 'yesterday'),('5',3, 'yesterday'),"
          + "('5',4, 'today'),('6',2, 'today'),('6',3, 'today'),('6',4, 'today')", driver);
    }

    /**
     * 5 txns.
     */
    protected void insertTestData(String tblName) throws Exception {
      executeStatementOnDriver("insert into " + tblName + " values('1',2),('1',3),('1',4),('2',2),('2',3),('2',4)",
          driver);
      executeStatementOnDriver("insert into " + tblName + " values('3',2),('3',3),('3',4),('4',2),('4',3),('4',4)",
          driver);
      executeStatementOnDriver("delete from " + tblName + " where b = 2", driver);
      executeStatementOnDriver("insert into " + tblName + " values('5',2),('5',3),('5',4),('6',2),('6',3),('6',4)",
          driver);
      executeStatementOnDriver("delete from " + tblName + " where a = '1'", driver);
    }

    /**
     * 3 txns.
     */
    protected void insertMmTestData(String tblName) throws Exception {
      executeStatementOnDriver("insert into " + tblName + " values('1',2),('1',3),('1',4),('2',2),('2',3),('2',4)",
          driver);
      executeStatementOnDriver("insert into " + tblName + " values('3',2),('3',3),('3',4),('4',2),('4',3),('4',4)",
          driver);
      executeStatementOnDriver("insert into " + tblName + " values('5',2),('5',3),('5',4),('6',2),('6',3),('6',4)",
          driver);
    }

    /**
     * i * 1.5 txns.
     */
    protected void insertTestData(String tblName, int iterations) throws Exception {
      for (int i = 0; i < iterations; i++) {
        executeStatementOnDriver("insert into " + tblName + " values('" + i + "'," + i + ")", driver);
      }
      for (int i = 0; i < iterations; i += 2) {
        executeStatementOnDriver("delete from " + tblName + " where b = " + i, driver);
      }
    }

    /**
     * i txns.
     */
    protected void insertMmTestData(String tblName, int iterations) throws Exception {
      for (int i = 0; i < iterations; i++) {
        executeStatementOnDriver("insert into " + tblName + " values('" + i + "'," + i + ")", driver);
      }
    }

    protected List<String> getAllData(String tblName) throws Exception {
      List<String> result = executeStatementOnDriverAndReturnResults("select * from " + tblName, driver);
      Collections.sort(result);
      return result;
    }

    protected List<String> getBucketData(String tblName, String bucketId) throws Exception {
      return executeStatementOnDriverAndReturnResults(
          "select ROW__ID, * from " + tblName + " where ROW__ID.bucketid = " + bucketId + " order by ROW__ID", driver);
    }

    protected void dropTable(String tblName) throws Exception {
      executeStatementOnDriver("drop table " + tblName, driver);
    }
  }
}
