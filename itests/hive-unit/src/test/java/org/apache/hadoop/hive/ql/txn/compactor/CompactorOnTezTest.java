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
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
  static final String CUSTOM_COMPACTION_QUEUE = "my_compaction_test_queue";

  protected HiveConf conf;
  protected IMetaStoreClient msClient;
  protected IDriver driver;
  protected boolean mmCompaction = false;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

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
    TestTxnDbUtil.setConfValues(hiveConf);
    TestTxnDbUtil.cleanDb(hiveConf);
    TestTxnDbUtil.prepDb(hiveConf);
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
    if (!mmCompaction) {
      // We need these settings to create a table which is not bucketed, but contains multiple files.
      // If these parameters are set when inserting 100 rows into the table, the rows will
      // be distributed into multiple files. This setup is used in the testMinorCompactionWithoutBuckets,
      // testMinorCompactionWithoutBucketsInsertOverwrite and testMajorCompactionWithoutBucketsInsertAndDeleteInsertOverwrite
      // tests in the TestCrudCompactorOnTez class.
      // This use case has to be tested because of HIVE-23763. The MM compaction is not affected by this issue,
      // therefore no need to set these configs for MM compaction.
      conf.set("tez.grouping.max-size", "1024");
      conf.set("tez.grouping.min-size", "1");
    }
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
      createFullAcidTable(null, tblName, isPartitioned, isBucketed, null);
    }

    void createFullAcidTable(String dbName, String tblName, boolean isPartitioned, boolean isBucketed)
        throws Exception {
      createFullAcidTable(dbName, tblName, isPartitioned, isBucketed, null);
    }

    void createFullAcidTable(String dbName, String tblName, boolean isPartitioned, boolean isBucketed,
        Map<String, String> additionalTblProperties) throws Exception {
      createTable(dbName, tblName, isPartitioned, isBucketed, false, "orc", additionalTblProperties);
    }

    void createMmTable(String tblName, boolean isPartitioned, boolean isBucketed)
        throws Exception {
      createMmTable(tblName, isPartitioned, isBucketed, "orc");
    }

    void createMmTable(String tblName, boolean isPartitioned, boolean isBucketed, String fileFormat)
        throws Exception {
      createMmTable(null, tblName, isPartitioned, isBucketed, fileFormat);
    }

    void createMmTable(String dbName, String tblName, boolean isPartitioned, boolean isBucketed, String fileFormat)
        throws Exception {
      createTable(dbName, tblName, isPartitioned, isBucketed, true, fileFormat, null);
    }

    private void createTable(String dbName, String tblName, boolean isPartitioned, boolean isBucketed,
        boolean insertOnly, String fileFormat, Map<String, String> additionalTblProperties) throws Exception {
      if (dbName != null) {
        tblName = dbName + "." + tblName;
      }
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
      if (additionalTblProperties != null) {
        for (Map.Entry<String, String> e : additionalTblProperties.entrySet()) {
          query.append("'").append(e.getKey()).append("'='").append(e.getValue()).append("', ");
        }
      }
      if (insertOnly) {
        query.append(" 'transactional_properties'='insert_only')");
      } else {
        query.append(" 'transactional_properties'='default')");
      }
      executeStatementOnDriver(query.toString(), driver);
    }

    void createDb(String dbName) throws Exception {
      executeStatementOnDriver("drop database if exists " + dbName + " cascade", driver);
      executeStatementOnDriver("create database " + dbName, driver);
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
    void insertTestData(String tblName) throws Exception {
      insertTestData(null, tblName);
    }

    /**
     * 5 txns.
     */
    void insertTestData(String dbName, String tblName) throws Exception {
      if (dbName != null) {
        tblName = dbName + "." + tblName;
      }
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
     * This method is for creating a non-bucketed table in which the data is distributed
     * into multiple splits. The initial data is 100 rows and it should be split into
     * multiple files, like bucket_000001, bucket_000002, ...
     * This is needed because the MINOR compactions had issues with tables like this. (HIVE-23763)
     * @param dbName
     * @param tblName
     * @param tempTblName
     * @param createDeletes
     * @param createInserts
     * @param insertOverwrite
     * @throws Exception
     */
    void createTableWithoutBucketWithMultipleSplits(String dbName, String tblName, String tempTblName,
        boolean createDeletes, boolean createInserts, boolean insertOverwrite) throws Exception {
      if (dbName != null) {
        tblName = dbName + "." + tblName;
        tempTblName = dbName + "." + tempTblName;
      }

      executeStatementOnDriver("drop table if exists " + tblName, driver);
      StringBuilder query = new StringBuilder();
      query.append("create table ").append(tblName).append(" (a string, b string, c string)");
      query.append(" stored as orc");
      query.append(" TBLPROPERTIES('transactional'='true',");
      query.append(" 'transactional_properties'='default')");
      executeStatementOnDriver(query.toString(), driver);

      generateInsertsWithMultipleSplits(0, 100, tblName, tempTblName + "_1", insertOverwrite);

      if (createDeletes) {
        executeStatementOnDriver("delete from " + tblName + " where a in ('41','87','53','11')", driver);
        executeStatementOnDriver("delete from " + tblName + " where a in ('42','88','81','12','86')", driver);
        executeStatementOnDriver("delete from " + tblName + " where a in ('98')", driver);
        executeStatementOnDriver("delete from " + tblName + " where a in ('40')", driver);
      }

      if (createInserts) {
        generateInsertsWithMultipleSplits(100, 250, tblName, tempTblName + "_2", false);
        generateInsertsWithMultipleSplits(300, 318, tblName, tempTblName + "_3", false);
        generateInsertsWithMultipleSplits(400, 410, tblName, tempTblName + "_4", false);
      }
    }

    private void generateInsertsWithMultipleSplits(int begin, int end, String tableName, String tempTableName,
        boolean insertOverwrite) throws Exception {
      StringBuffer sb = new StringBuffer();
      for (int i = begin; i < end; i++) {
        sb.append("('");
        sb.append(i);
        sb.append("','value");
        sb.append(i);
        sb.append("','this is some comment to increase the file size ");
        sb.append(i);
        sb.append("')");
        if (i < end - 1) {
          sb.append(",");
        }
      }
      executeStatementOnDriver("DROP TABLE IF EXISTS " + tempTableName, driver);
      executeStatementOnDriver(
          "CREATE EXTERNAL TABLE " + tempTableName + " (id string, value string, comment string) STORED AS TEXTFILE ",
          driver);
      executeStatementOnDriver("insert into " + tempTableName + " values " + sb.toString(), driver);
      if (insertOverwrite) {
        executeStatementOnDriver("insert overwrite table " + tableName + " select * from " + tempTableName, driver);
      } else {
        executeStatementOnDriver("insert into " + tableName + " select * from " + tempTableName, driver);
      }
    }

    /**
     * 5 txns.
     */
    void insertMmTestData(String tblName) throws Exception {
      insertMmTestData(null, tblName);
    }

    /**
     * 3 txns.
     */
    void insertMmTestData(String dbName, String tblName) throws Exception {
      if (dbName != null) {
        tblName = dbName + "." + tblName;
      }
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

    List<String> getAllData(String tblName) throws Exception {
      return getAllData(null, tblName, false);
    }

    List<String> getAllData(String tblName, boolean withRowId) throws Exception {
      return getAllData(null, tblName, withRowId);
    }

    List<String> getAllData(String dbName, String tblName, boolean withRowId) throws Exception {
      if (dbName != null) {
        tblName = dbName + "." + tblName;
      }
      StringBuffer query = new StringBuffer();
      query.append("select ");
      if (withRowId) {
        query.append("ROW__ID, ");
      }
      query.append("* from ");
      query.append(tblName);
      List<String> result = executeStatementOnDriverAndReturnResults(query.toString(), driver);
      Collections.sort(result);
      return result;
    }

    List<String> getDataWithInputFileNames(String dbName, String tblName) throws Exception {
      if (dbName != null) {
        tblName = dbName + "." + tblName;
      }
      StringBuffer query = new StringBuffer();
      query.append("select ");
      query.append("INPUT__FILE__NAME, a from ");
      query.append(tblName);
      query.append(" order by a");
      List<String> result = executeStatementOnDriverAndReturnResults(query.toString(), driver);
      return result;
    }

    boolean compareFileNames(List<String> expectedFileNames, List<String> actualFileNames) {
      if (expectedFileNames.size() != actualFileNames.size()) {
        return false;
      }

      Pattern p = Pattern.compile("(.*)(bucket_[0-9]+)(_[0-9]+)?");
      for (int i = 0; i < expectedFileNames.size(); i++) {
        String[] expectedParts = expectedFileNames.get(i).split("\t");
        String[] actualParts = actualFileNames.get(i).split("\t");

        if (!expectedParts[1].equals(actualParts[1])) {
          return false;
        }

        String expectedFileName = null;
        String actualFileName = null;
        Matcher m = p.matcher(expectedParts[0]);
        if (m.matches()) {
          expectedFileName = m.group(2);
        }
        m = p.matcher(actualParts[0]);
        if (m.matches()) {
          actualFileName = m.group(2);
        }

        if (expectedFileName == null || actualFileName == null || !expectedFileName.equals(actualFileName)) {
          return false;
        }
      }
      return true;
    }

    protected List<String> getBucketData(String tblName, String bucketId) throws Exception {
      return executeStatementOnDriverAndReturnResults(
          "select ROW__ID, * from " + tblName + " where ROW__ID.bucketid = " + bucketId + " order by ROW__ID, a, b", driver);
    }

    protected void dropTable(String tblName) throws Exception {
      executeStatementOnDriver("drop table " + tblName, driver);
    }
  }
}
