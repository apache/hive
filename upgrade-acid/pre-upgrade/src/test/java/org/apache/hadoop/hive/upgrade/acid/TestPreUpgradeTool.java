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

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.txn.compactor.Worker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TestPreUpgradeTool {
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
      File.separator + TestPreUpgradeTool.class.getCanonicalName() + "-" + System.currentTimeMillis()
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
    int[][] data = {{1, 2}, {3, 4}, {5, 6}};
    int[][] dataPart = {{1, 2, 10}, {3, 4, 11}, {5, 6, 12}};
    runStatementOnDriver("drop table if exists TAcid");
    runStatementOnDriver("drop table if exists TAcidPart");
    runStatementOnDriver("drop table if exists TFlat");
    runStatementOnDriver("drop table if exists TFlatText");

    try {
      runStatementOnDriver(
        "create table TAcid (a int, b int) clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
      runStatementOnDriver(
        "create table TAcidPart (a int, b int) partitioned by (p tinyint)  clustered by (b) into 2 buckets  stored" +
          " as orc TBLPROPERTIES ('transactional'='true')");
      //on 2.x these are guaranteed to not be acid
      runStatementOnDriver("create table TFlat (a int, b int) stored as orc tblproperties('transactional'='false')");
      runStatementOnDriver(
        "create table TFlatText (a int, b int) stored as textfile tblproperties('transactional'='false')");


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
      String[] args = {"-location", getTestDataDir(), "-execute"};
      PreUpgradeTool.callback = new PreUpgradeTool.Callback() {
        @Override
        void onWaitForCompaction() throws MetaException {
          runWorker(hiveConf);
        }
      };
      PreUpgradeTool.pollIntervalMs = 1;
      PreUpgradeTool.hiveConf = hiveConf;
      PreUpgradeTool.main(args);

      String[] scriptFiles = getScriptFiles();
      assertThat(scriptFiles.length, is(1));

      List<String> scriptContent = loadScriptContent(new File(getTestDataDir(), scriptFiles[0]));
      assertThat(scriptContent.size(), is(2));
      assertThat(scriptContent, hasItem(is("ALTER TABLE default.tacid COMPACT 'major';")));
      assertThat(scriptContent, hasItem(is("ALTER TABLE default.tacidpart PARTITION(p=10Y) COMPACT 'major';")));

      TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);

      ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
      Assert.assertEquals(2, resp.getCompactsSize());
      for (ShowCompactResponseElement e : resp.getCompacts()) {
        Assert.assertEquals(e.toString(), TxnStore.CLEANING_RESPONSE, e.getState());
      }

      // Check whether compaction was successful in the first run
      File secondRunDataDir = new File(getTestDataDir(), "secondRun");
      if (!secondRunDataDir.exists()) {
        if (!secondRunDataDir.mkdir()) {
          throw new IOException("Unable to create directory" + secondRunDataDir.getAbsolutePath());
        }
      }
      String[] args2 = {"-location", secondRunDataDir.getAbsolutePath()};
      PreUpgradeTool.main(args2);

      scriptFiles = secondRunDataDir.list();
      assertThat(scriptFiles, is(not(nullValue())));
      assertThat(scriptFiles.length, is(0));

    } finally {
      runStatementOnDriver("drop table if exists TAcid");
      runStatementOnDriver("drop table if exists TAcidPart");
      runStatementOnDriver("drop table if exists TFlat");
      runStatementOnDriver("drop table if exists TFlatText");
    }
  }

  private static final String INCLUDE_DATABASE_NAME ="DInclude";
  private static final String EXCLUDE_DATABASE_NAME ="DExclude";

  @Test
  public void testOnlyFilteredDatabasesAreUpgradedWhenRegexIsGiven() throws Exception {
    int[][] data = {{1, 2}, {3, 4}, {5, 6}};
    runStatementOnDriver("drop database if exists " + INCLUDE_DATABASE_NAME + " cascade");
    runStatementOnDriver("drop database if exists " + EXCLUDE_DATABASE_NAME + " cascade");

    try {
      runStatementOnDriver("create database " + INCLUDE_DATABASE_NAME);
      runStatementOnDriver("use " + INCLUDE_DATABASE_NAME);
      runStatementOnDriver("create table " + INCLUDE_TABLE_NAME + " (a int, b int) clustered by (b) " +
              "into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
      runStatementOnDriver("insert into " + INCLUDE_TABLE_NAME + makeValuesClause(data));
      runStatementOnDriver("update " + INCLUDE_TABLE_NAME + " set a = 1 where b = 2");

      runStatementOnDriver("create database " + EXCLUDE_DATABASE_NAME);
      runStatementOnDriver("use " + EXCLUDE_DATABASE_NAME);
      runStatementOnDriver("create table " + EXCLUDE_DATABASE_NAME + " (a int, b int) clustered by (b) " +
                "into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
      runStatementOnDriver("insert into " + EXCLUDE_DATABASE_NAME + makeValuesClause(data));
      runStatementOnDriver("update " + EXCLUDE_DATABASE_NAME + " set a = 1 where b = 2");

      String[] args = {"-location", getTestDataDir(), "-dbRegex", "*include*"};
      PreUpgradeTool.callback = new PreUpgradeTool.Callback() {
        @Override
        void onWaitForCompaction() throws MetaException {
          runWorker(hiveConf);
        }
      };
      PreUpgradeTool.pollIntervalMs = 1;
      PreUpgradeTool.hiveConf = hiveConf;
      PreUpgradeTool.main(args);

      String[] scriptFiles = getScriptFiles();
      assertThat(scriptFiles.length, is(1));

      List<String> scriptContent = loadScriptContent(new File(getTestDataDir(), scriptFiles[0]));
      assertThat(scriptContent.size(), is(1));
      assertThat(scriptContent.get(0), is("ALTER TABLE dinclude.tinclude COMPACT 'major';"));

    } finally {
      runStatementOnDriver("drop database if exists " + INCLUDE_DATABASE_NAME + " cascade");
      runStatementOnDriver("drop database if exists " + EXCLUDE_DATABASE_NAME + " cascade");
    }
  }

  private static final String INCLUDE_TABLE_NAME ="TInclude";
  private static final String EXCLUDE_TABLE_NAME ="TExclude";

  @Test
  public void testOnlyFilteredTablesAreUpgradedWhenRegexIsGiven() throws Exception {
    int[][] data = {{1, 2}, {3, 4}, {5, 6}};
    runStatementOnDriver("drop table if exists " + INCLUDE_TABLE_NAME);
    runStatementOnDriver("drop table if exists " + EXCLUDE_TABLE_NAME);

    try {
      runStatementOnDriver("create table " + INCLUDE_TABLE_NAME + " (a int, b int) clustered by (b) " +
              "into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");
      runStatementOnDriver("create table " + EXCLUDE_TABLE_NAME + " (a int, b int) clustered by (b) " +
              "into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true')");

      runStatementOnDriver("insert into " + INCLUDE_TABLE_NAME + makeValuesClause(data));
      runStatementOnDriver("update " + INCLUDE_TABLE_NAME + " set a = 1 where b = 2");

      runStatementOnDriver("insert into " + EXCLUDE_TABLE_NAME + makeValuesClause(data));
      runStatementOnDriver("update " + EXCLUDE_TABLE_NAME + " set a = 1 where b = 2");

      String[] args = {"-location", getTestDataDir(), "-tableRegex", "*include*"};
      PreUpgradeTool.callback = new PreUpgradeTool.Callback() {
        @Override
        void onWaitForCompaction() throws MetaException {
          runWorker(hiveConf);
        }
      };
      PreUpgradeTool.pollIntervalMs = 1;
      PreUpgradeTool.hiveConf = hiveConf;
      PreUpgradeTool.main(args);

      String[] scriptFiles = getScriptFiles();
      assertThat(scriptFiles.length, is(1));

      List<String> scriptContent = loadScriptContent(new File(getTestDataDir(), scriptFiles[0]));
      assertThat(scriptContent.size(), is(1));
      assertThat(scriptContent.get(0), allOf(
              containsString("ALTER TABLE"),
              containsString(INCLUDE_TABLE_NAME.toLowerCase()),
              containsString("COMPACT")));

    } finally {
      runStatementOnDriver("drop table if exists " + INCLUDE_TABLE_NAME);
      runStatementOnDriver("drop table if exists " + EXCLUDE_TABLE_NAME);
    }
  }

  private String[] getScriptFiles() {
    File testDataDir = new File(getTestDataDir());
    String[] scriptFiles = testDataDir.list((dir, name) -> name.startsWith("compacts_") && name.endsWith(".sql"));
    assertThat(scriptFiles, is(not(nullValue())));
    return scriptFiles;
  }

  private List<String> loadScriptContent(File file) throws IOException {
    List<String> content = org.apache.commons.io.FileUtils.readLines(file);
    content.removeIf(line -> line.startsWith("--"));
    content.removeIf(StringUtils::isBlank);
    return content;
  }

  @Test
  public void testUpgradeExternalTableNoReadPermissionForDatabase() throws Exception {
    int[][] data = {{1, 2}, {3, 4}, {5, 6}};

    runStatementOnDriver("drop database if exists test cascade");
    runStatementOnDriver("drop table if exists TExternal");

    runStatementOnDriver("create database test");
    runStatementOnDriver("create table test.TExternal (a int, b int) stored as orc tblproperties" +
      "('transactional'='false')");

    //this needs major compaction
    runStatementOnDriver("insert into test.TExternal" + makeValuesClause(data));

    String dbDir = getWarehouseDir() + "/test.db";
    File dbPath = new File(dbDir);
    try {
      Set<PosixFilePermission> perms = PosixFilePermissions.fromString("-w-------");
      Files.setPosixFilePermissions(dbPath.toPath(), perms);
      String[] args = {"-location", getTestDataDir(), "-execute"};
      PreUpgradeTool.pollIntervalMs = 1;
      PreUpgradeTool.hiveConf = hiveConf;
      Exception expected = null;
      try {
        PreUpgradeTool.main(args);
      } catch (Exception e) {
        expected = e;
      }

      Assert.assertNotNull(expected);
      Assert.assertTrue(expected instanceof HiveException);
      Assert.assertTrue(expected.getMessage().contains("Pre-upgrade tool requires " +
        "read-access to databases and tables to determine if a table has to be compacted."));
    } finally {
      Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrw----");
      Files.setPosixFilePermissions(dbPath.toPath(), perms);
    }
  }

  @Test
  public void testUpgradeExternalTableNoReadPermissionForTable() throws Exception {
    int[][] data = {{1, 2}, {3, 4}, {5, 6}};
    runStatementOnDriver("drop table if exists TExternal");

    runStatementOnDriver("create table TExternal (a int, b int) stored as orc tblproperties('transactional'='false')");

    //this needs major compaction
    runStatementOnDriver("insert into TExternal" + makeValuesClause(data));

    String tableDir = getWarehouseDir() + "/texternal";
    File tablePath = new File(tableDir);
    try {
      Set<PosixFilePermission> perms = PosixFilePermissions.fromString("-w-------");
      Files.setPosixFilePermissions(tablePath.toPath(), perms);
      String[] args = {"-location", getTestDataDir(), "-execute"};
      PreUpgradeTool.pollIntervalMs = 1;
      PreUpgradeTool.hiveConf = hiveConf;
      Exception expected = null;
      try {
        PreUpgradeTool.main(args);
      } catch (Exception e) {
        expected = e;
      }

      Assert.assertNotNull(expected);
      Assert.assertTrue(expected instanceof HiveException);
      Assert.assertTrue(expected.getMessage().contains("Pre-upgrade tool requires" +
        " read-access to databases and tables to determine if a table has to be compacted."));
    } finally {
      Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrw----");
      Files.setPosixFilePermissions(tablePath.toPath(), perms);
    }
  }

  @Test
  public void testConcurrency() throws Exception {
    int numberOfTables = 20;
    String tablePrefix = "concurrency_";

    int[][] data = {{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10},
        {11, 12}, {13, 14}, {15, 16}, {17, 18}, {19, 20}};
    for (int i = 0; i < numberOfTables; i++) {
      runStatementOnDriver("drop table if exists " + tablePrefix + i);
    }

    try {
      for (int i = 0; i < numberOfTables; i++) {
        String tableName = tablePrefix + i;
        runStatementOnDriver(
                "create table " + tableName + " (a int, b int) " +
                        "clustered by (b) " +
                        "into 10 buckets " +
                        "stored as orc TBLPROPERTIES ('transactional'='true')");
        runStatementOnDriver("insert into " + tableName + makeValuesClause(data));
      }

      String[] args = {"-location", getTestDataDir(), "-execute"};
      PreUpgradeTool.callback = new PreUpgradeTool.Callback() {
        @Override
        void onWaitForCompaction() throws MetaException {
          runWorker(hiveConf);
        }
      };
      PreUpgradeTool.pollIntervalMs = 1;
      PreUpgradeTool.hiveConf = hiveConf;
      PreUpgradeTool.main(args);

    } finally {
      for (int i = 0; i < numberOfTables; i++) {
        runStatementOnDriver("drop table if exists " + tablePrefix + i);
      }
    }
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
    hiveConf
      .setVar(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS,
        "org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener");
    hiveConf
      .setVar(HiveConf.ConfVars.HIVE_METASTORE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider");
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
