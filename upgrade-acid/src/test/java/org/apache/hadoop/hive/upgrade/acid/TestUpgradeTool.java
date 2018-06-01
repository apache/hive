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
package org.apache.hadoop.hive.upgrade.acid;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.compactor.Worker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestUpgradeTool {
  private static final Logger LOG = LoggerFactory.getLogger(TestUpgradeTool.class);
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
      File.separator + TestUpgradeTool.class.getCanonicalName() + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");

  private String getTestDataDir() {
    return TEST_DATA_DIR;
  }

  /**
   * preUpgrade: test tables that need to be compacted, waits for compaction
   * postUpgrade: generates scripts w/o asserts
   */
  @Test
  public void testUpgrade() throws Exception {
    int[][] data = {{1,2}, {3, 4}, {5, 6}};
    int[][] dataPart = {{1, 2, 10}, {3, 4, 11}, {5, 6, 12}};
    runStatementOnDriver("drop table if exists TAcid");
    runStatementOnDriver("drop table if exists TAcidPart");
    runStatementOnDriver("drop table if exists TFlat");
    runStatementOnDriver("drop table if exists TFlatText");

    runStatementOnDriver("create table TAcid (a int, b int) clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table TAcidPart (a int, b int) partitioned by (p tinyint)  clustered by (b) into 2 buckets  stored" +
        " as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("create table TFlat (a int, b int) stored as orc tblproperties('transactional'='false')");
    runStatementOnDriver("create table TFlatText (a int, b int) stored as textfile tblproperties('transactional'='false')");


    //this needs major compaction
    runStatementOnDriver("insert into TAcid" + makeValuesClause(data));
    runStatementOnDriver("update TAcid set a = 1 where b = 2");

    //this table needs to be converted to CRUD Acid
    runStatementOnDriver("insert into TFlat" + makeValuesClause(data));

    //this table needs to be converted to MM
    runStatementOnDriver("insert into TFlatText" + makeValuesClause(data));

    //p=10 needs major compaction
    runStatementOnDriver("insert into TAcidPart partition(p)" + makeValuesClause(dataPart));
    runStatementOnDriver("update TAcidPart set a = 1 where b = 2 and p = 10");

    //todo: add partitioned table that needs conversion to MM/Acid

    //todo: rename files case
    String[] args = {"-location", getTestDataDir(), "-preUpgrade", "-execute"};
    UpgradeTool.callback = new UpgradeTool.Callback() {
      @Override
      void onWaitForCompaction() throws MetaException {
        runWorker(hiveConf);
      }
    };
    UpgradeTool.pollIntervalMs = 1;
    UpgradeTool.hiveConf = hiveConf;
    UpgradeTool.main(args);
    /*
    todo: parse
    target/tmp/org.apache.hadoop.hive.upgrade.acid.TestUpgradeTool-1527286256834/compacts_1527286277624.sql
    make sure it's the only 'compacts' file and contains
    ALTER TABLE default.tacid COMPACT 'major';
ALTER TABLE default.tacidpart PARTITION(p=10Y) COMPACT 'major';
    * */

    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);

    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(2, resp.getCompactsSize());
    for(ShowCompactResponseElement e : resp.getCompacts()) {
      Assert.assertEquals(e.toString(), TxnStore.CLEANING_RESPONSE, e.getState());
    }

    String[] args2 = {"-location", getTestDataDir(), "-postUpgrade"};
    UpgradeTool.main(args2);
    /*
    * todo: parse
    * convertToAcid_1527286288784.sql make sure it has
    * ALTER TABLE default.tflat SET TBLPROPERTIES ('transactional'='true');
    * convertToMM_1527286288784.sql make sure it has
    * ALTER TABLE default.tflattext SET TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');
    * */
  }

  /**
   * includes 'execute' for postUpgrade
   * @throws Exception
   */
  @Test
  public void testPostUpgrade() throws Exception {
    int[][] dataPart = {{1, 2, 10}, {3, 4, 11}, {5, 6, 12}};
    hiveConf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "dynamic");
    runStatementOnDriver("drop table if exists TAcid");
    runStatementOnDriver("drop table if exists TAcidPart");
    runStatementOnDriver("drop table if exists TFlat");
    runStatementOnDriver("drop table if exists TFlatText");

    runStatementOnDriver("create table TAcid (a int, b int) clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='false')");
    runStatementOnDriver("create table TAcidPart (a int, b int) partitioned by (p int)  clustered by (b) into 2 buckets  stored" +
        " as orc TBLPROPERTIES ('transactional'='false')");
    //to create some partitions
    runStatementOnDriver("insert into TAcidPart partition(p)" + makeValuesClause(dataPart));


    //todo: to test these need to link against 3.x libs - maven profiles?
    //runStatementOnDriver("create table TFlat (a int, b int) stored as orc tblproperties('transactional'='false')");
    //runStatementOnDriver("create table TFlatText (a int, b int) stored as textfile tblproperties('transactional'='false')");

    Hive db = Hive.get(hiveConf);
    Table tacid = db.getTable("default", "tacid");
    Assert.assertEquals("Expected TAcid to become full acid", false, AcidUtils.isAcidTable(tacid));
    Table tacidpart = db.getTable("default", "tacidpart");
    Assert.assertEquals("Expected TAcidPart to become full acid", false,
        AcidUtils.isAcidTable(tacidpart));


    String[] args2 = {"-location", getTestDataDir(), "-postUpgrade", "-execute"};
    UpgradeTool.isTestMode = true;
    UpgradeTool.hiveConf = hiveConf;
    UpgradeTool.main(args2);

    tacid = db.getTable("default", "tacid");
    Assert.assertEquals("Expected TAcid to become full acid", true, AcidUtils.isAcidTable(tacid));
    tacidpart = db.getTable("default", "tacidpart");
    Assert.assertEquals("Expected TAcidPart to become full acid", true,
        AcidUtils.isAcidTable(tacidpart));

    /**
    todo: parse
     target/tmp/org.apache.hadoop.hive.upgrade.acid.TestUpgradeTool-1527286026461/convertToAcid_1527286063065.sql
     make sure it has:
    ALTER TABLE default.tacid SET TBLPROPERTIES ('transactional'='true');
    ALTER TABLE default.tacidpart SET TBLPROPERTIES ('transactional'='true');
     */
  }
  private static void runWorker(HiveConf hiveConf) throws MetaException {
    AtomicBoolean stop = new AtomicBoolean(true);
    Worker t = new Worker();
    t.setThreadId((int) t.getId());
    t.setHiveConf(hiveConf);
    AtomicBoolean looped = new AtomicBoolean();
    t.init(stop, looped);
    t.run();
  }

  private static String makeValuesClause(int[][] rows) {
    assert rows.length > 0;
    StringBuilder sb = new StringBuilder(" values");
    for(int[] row : rows) {
      assert row.length > 0;
      if(row.length > 1) {
        sb.append("(");
      }
      for(int value : row) {
        sb.append(value).append(",");
      }
      sb.setLength(sb.length() - 1);//remove trailing comma
      if(row.length > 1) {
        sb.append(")");
      }
      sb.append(",");
    }
    sb.setLength(sb.length() - 1);//remove trailing comma
    return sb.toString();
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
  @Before
  public void setUp() throws Exception {
    setUpInternal();
  }
  private void initHiveConf() {
    hiveConf = new HiveConf(this.getClass());
  }
  @Rule
  public TestName testName = new TestName();
  private HiveConf hiveConf;
  private Driver d;
  private void setUpInternal() throws Exception {
    initHiveConf();
    TxnDbUtil.cleanDb();//todo: api changed in 3.0
    FileUtils.deleteDirectory(new File(getTestDataDir()));

    Path workDir = new Path(System.getProperty("test.tmp.dir",
        "target" + File.separator + "test" + File.separator + "tmp"));
    hiveConf.set("mapred.local.dir", workDir + File.separator + this.getClass().getSimpleName()
        + File.separator + "mapred" + File.separator + "local");
    hiveConf.set("mapred.system.dir", workDir + File.separator + this.getClass().getSimpleName()
        + File.separator + "mapred" + File.separator + "system");
    hiveConf.set("mapreduce.jobtracker.staging.root.dir", workDir + File.separator + this.getClass().getSimpleName()
        + File.separator + "mapred" + File.separator + "staging");
    hiveConf.set("mapred.temp.dir", workDir + File.separator + this.getClass().getSimpleName()
        + File.separator + "mapred" + File.separator + "temp");
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, getWarehouseDir());
    hiveConf.setVar(HiveConf.ConfVars.HIVEINPUTFORMAT, HiveInputFormat.class.getName());
    hiveConf
        .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
            "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    hiveConf.setBoolVar(HiveConf.ConfVars.MERGE_CARDINALITY_VIOLATION_CHECK, true);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVESTATSCOLAUTOGATHER, false);
    TxnDbUtil.setConfValues(hiveConf);
    TxnDbUtil.prepDb();//todo: api changed in 3.0
    File f = new File(getWarehouseDir());
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if (!(new File(getWarehouseDir()).mkdirs())) {
      throw new RuntimeException("Could not create " + getWarehouseDir());
    }
    SessionState ss = SessionState.start(hiveConf);
    ss.applyAuthorizationPolicy();
    d = new Driver(new QueryState(hiveConf), null);
    d.setMaxRows(10000);
  }
  private String getWarehouseDir() {
    return getTestDataDir() + "/warehouse";
  }
  @After
  public void tearDown() throws Exception {
    if (d != null) {
      d.close();
      d.destroy();
      d = null;
    }
  }

}
