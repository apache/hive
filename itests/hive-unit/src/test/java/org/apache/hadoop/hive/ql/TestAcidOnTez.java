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

package org.apache.hadoop.hive.ql;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.compactor.Cleaner;
import org.apache.hadoop.hive.ql.txn.compactor.Worker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * This class resides in itests to facilitate running query using Tez engine, since the jars are
 * fully loaded here, which is not the case if it stays in ql.
 */
public class TestAcidOnTez {
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
      File.separator + TestAcidOnTez.class.getCanonicalName()
      + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");
  private static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";
  //bucket count for test tables; set it to 1 for easier debugging
  private static int BUCKET_COUNT = 2;
  @Rule
  public TestName testName = new TestName();
  private HiveConf hiveConf;
  private Driver d;
  private static enum Table {
    ACIDTBL("acidTbl"),
    ACIDTBLPART("acidTblPart"),
    NONACIDORCTBL("nonAcidOrcTbl"),
    NONACIDPART("nonAcidPart");

    private final String name;
    @Override
    public String toString() {
      return name;
    }
    Table(String name) {
      this.name = name;
    }
  }

  @Before
  public void setUp() throws Exception {
    tearDown();
    hiveConf = new HiveConf(this.getClass());
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, TEST_WAREHOUSE_DIR);
    hiveConf.setVar(HiveConf.ConfVars.HIVEMAPREDMODE, "nonstrict");
    hiveConf.setVar(HiveConf.ConfVars.HIVEINPUTFORMAT, HiveInputFormat.class.getName());
    hiveConf
        .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    TxnDbUtil.setConfValues(hiveConf);
    TxnDbUtil.prepDb();
    File f = new File(TEST_WAREHOUSE_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if (!(new File(TEST_WAREHOUSE_DIR).mkdirs())) {
      throw new RuntimeException("Could not create " + TEST_WAREHOUSE_DIR);
    }
    SessionState.start(new SessionState(hiveConf));
    d = new Driver(hiveConf);
    dropTables();
    runStatementOnDriver("create table " + Table.ACIDTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc " + getTblProperties());
    runStatementOnDriver("create table " + Table.ACIDTBLPART + "(a int, b int) partitioned by (p string) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc " + getTblProperties());
    runStatementOnDriver("create table " + Table.NONACIDORCTBL + "(a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc ");
    runStatementOnDriver("create table " + Table.NONACIDPART + "(a int, b int) partitioned by (p string) stored as orc ");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(1,2)");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(3,4)");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(5,6)");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(7,8)");
    runStatementOnDriver("insert into " + Table.ACIDTBL + "(a,b) values(9,10)");
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,2),(3,4),(5,6),(7,8),(9,10)");
  }

  /**
   * this is to test differety types of Acid tables
   */
  String getTblProperties() {
    return "TBLPROPERTIES ('transactional'='true')";
  }

  private void dropTables() throws Exception {
    for(Table t : Table.values()) {
      runStatementOnDriver("drop table if exists " + t);
    }
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (d != null) {
        dropTables();
        d.destroy();
        d.close();
        d = null;
      }
      TxnDbUtil.cleanDb();
    } finally {
      FileUtils.deleteDirectory(new File(TEST_DATA_DIR));
    }
  }

  @Test
  public void testMergeJoinOnMR() throws Exception {
    testJoin("mr", "MergeJoin");
  }

  @Test
  public void testMapJoinOnMR() throws Exception {
    testJoin("mr", "MapJoin");
  }

  @Test
  public void testMergeJoinOnTez() throws Exception {
    testJoin("tez", "MergeJoin");
  }

  @Test
  public void testMapJoinOnTez() throws Exception {
    testJoin("tez", "MapJoin");
  }

  // Ideally test like this should be a qfile test. However, the explain output from qfile is always
  // slightly different depending on where the test is run, specifically due to file size estimation
  private void testJoin(String engine, String joinType) throws Exception {
    HiveConf confForTez = new HiveConf(hiveConf); // make a clone of existing hive conf
    HiveConf confForMR = new HiveConf(hiveConf);  // make a clone of existing hive conf

    if (engine.equals("tez")) {
      setupTez(confForTez); // one-time setup to make query able to run with Tez
    }

    if (joinType.equals("MapJoin")) {
      setupMapJoin(confForTez);
      setupMapJoin(confForMR);
    }

    runQueries(engine, joinType, confForTez, confForMR);

    // Perform compaction. Join result after compaction should still be the same
    runStatementOnDriver("alter table "+ Table.ACIDTBL + " compact 'MAJOR'");
    TestTxnCommands2.runWorker(hiveConf);
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history", 1, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0 compaction state", TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(0).getState());
    TestTxnCommands2.runCleaner(hiveConf);

    runQueries(engine, joinType, confForTez, confForMR);
  }

  private void runQueries(String engine, String joinType, HiveConf confForTez, HiveConf confForMR) throws Exception {
    List<String> queries = new ArrayList<String>();
    queries.add("select count(*) from " + Table.ACIDTBL + " t1 join " + Table.ACIDTBL + " t2 on t1.a=t2.a");
    queries.add("select count(*) from " + Table.ACIDTBL + " t1 join " + Table.NONACIDORCTBL + " t2 on t1.a=t2.a");
    // more queries can be added here in the future to test acid joins

    List<String> explain; // stores Explain output
    int[][] expected = {{5}};
    List<String> rs = null;

    for (String query : queries) {
      if (engine.equals("tez")) {
        explain = runStatementOnDriver("explain " + query, confForTez);
        if (joinType.equals("MergeJoin")) {
          TestTxnCommands2.assertExplainHasString("Merge Join Operator", explain, "Didn't find " + joinType);
        } else { // MapJoin
          TestTxnCommands2.assertExplainHasString("Map Join Operator", explain, "Didn't find " + joinType);
        }
        rs = runStatementOnDriver(query, confForTez);
      } else { // mr
        explain = runStatementOnDriver("explain " + query, confForMR);
        if (joinType.equals("MergeJoin")) {
          TestTxnCommands2.assertExplainHasString("  Join Operator", explain, "Didn't find " + joinType);
        } else { // MapJoin
          TestTxnCommands2.assertExplainHasString("Map Join Operator", explain, "Didn't find " + joinType);
        }
        rs = runStatementOnDriver(query, confForMR);
      }
      Assert.assertEquals("Join result incorrect", TestTxnCommands2.stringifyValues(expected), rs);
    }
  }

  private void setupTez(HiveConf conf) {
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    conf.setVar(HiveConf.ConfVars.HIVE_USER_INSTALL_DIR, TEST_DATA_DIR);
    conf.setBoolean("tez.local.mode", true);
    conf.set("fs.defaultFS", "file:///");
    conf.setBoolean("tez.runtime.optimize.local.fetch", true);
    conf.set("tez.staging-dir", TEST_DATA_DIR);
    conf.setBoolean("tez.ignore.lib.uris", true);
  }

  private void setupMapJoin(HiveConf conf) {
    conf.setBoolVar(HiveConf.ConfVars.HIVECONVERTJOIN, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASK, true);
    conf.setLongVar(HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD, 10000);
  }

  private List<String> runStatementOnDriver(String stmt) throws Exception {
    CommandProcessorResponse cpr = d.run(stmt);
    if(cpr.getResponseCode() != 0) {
      throw new RuntimeException(stmt + " failed: " + cpr);
    }
    List<String> rs = new ArrayList<String>();
    d.getResults(rs);
    return rs;
  }

  /**
   * Run statement with customized hive conf
   */
  private List<String> runStatementOnDriver(String stmt, HiveConf conf)
      throws Exception {
    Driver driver = new Driver(conf);
    CommandProcessorResponse cpr = driver.run(stmt);
    if(cpr.getResponseCode() != 0) {
      throw new RuntimeException(stmt + " failed: " + cpr);
    }
    List<String> rs = new ArrayList<String>();
    driver.getResults(rs);
    return rs;
  }
}
