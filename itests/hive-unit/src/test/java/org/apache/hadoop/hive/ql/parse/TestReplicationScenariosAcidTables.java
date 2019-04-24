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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.CallerArguments;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.BehaviourInjection;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.Utils;

import org.junit.rules.TestName;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.apache.hadoop.hive.metastore.ReplChangeManager.SOURCE_OF_REPLICATION;

/**
 * TestReplicationScenariosAcidTables - test replication for ACID tables
 */
@Ignore("See HIVE-21647, HIVE-21648")
public class TestReplicationScenariosAcidTables {

  @Rule
  public final TestName testName = new TestName();

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationScenarios.class);
  static WarehouseInstance primary;
  private static WarehouseInstance replica, replicaNonAcid;
  static HiveConf conf;
  private String primaryDbName, replicatedDbName, primaryDbNameExtra;
  private enum OperationType {
    REPL_TEST_ACID_INSERT, REPL_TEST_ACID_INSERT_SELECT, REPL_TEST_ACID_CTAS,
    REPL_TEST_ACID_INSERT_OVERWRITE, REPL_TEST_ACID_INSERT_IMPORT, REPL_TEST_ACID_INSERT_LOADLOCAL,
    REPL_TEST_ACID_INSERT_UNION
  }
  private static List<String> dumpWithoutAcidClause = Collections.singletonList(
          "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='false'");
  private static List<String> dumpWithAcidBootstrapClause = Arrays.asList(
          "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='true'",
          "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES + "'='true'");
  private List<String> acidTableNames = new LinkedList<>();
  private List<String> nonAcidTableNames = new LinkedList<>();


  @BeforeClass
  public static void classLevelSetup() throws Exception {
    HashMap<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());

    internalBeforeClassSetup(overrides, TestReplicationScenariosAcidTables.class);
  }

  static void internalBeforeClassSetup(Map<String, String> overrides,
      Class clazz) throws Exception {

    conf = new HiveConf(clazz);
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    MiniDFSCluster miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    HashMap<String, String> acidEnableConf = new HashMap<String, String>() {{
      put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
      put("hive.support.concurrency", "true");
      put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
      put("hive.metastore.client.capability.check", "false");
      put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
      put("hive.exec.dynamic.partition.mode", "nonstrict");
      put("hive.strict.checks.bucketing", "false");
      put("hive.mapred.mode", "nonstrict");
      put("mapred.input.dir.recursive", "true");
      put("hive.metastore.disallow.incompatible.col.type.changes", "false");
      put("hive.in.repl.test", "true");
    }};

    acidEnableConf.putAll(overrides);

    primary = new WarehouseInstance(LOG, miniDFSCluster, acidEnableConf);
    replica = new WarehouseInstance(LOG, miniDFSCluster, acidEnableConf);
    HashMap<String, String> overridesForHiveConf1 = new HashMap<String, String>() {{
        put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
        put("hive.support.concurrency", "false");
        put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
        put("hive.metastore.client.capability.check", "false");
    }};
    replicaNonAcid = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConf1);
  }

  @AfterClass
  public static void classLevelTearDown() throws IOException {
    primary.close();
    replica.close();
  }

  @Before
  public void setup() throws Throwable {
    primaryDbName = testName.getMethodName() + "_" + +System.currentTimeMillis();
    replicatedDbName = "replicated_" + primaryDbName;
    primary.run("create database " + primaryDbName + " WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')");
    primaryDbNameExtra = primaryDbName+"_extra";
    primary.run("create database " + primaryDbNameExtra + " WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')");
  }

  @After
  public void tearDown() throws Throwable {
    primary.run("drop database if exists " + primaryDbName + " cascade");
    replica.run("drop database if exists " + replicatedDbName + " cascade");
    replicaNonAcid.run("drop database if exists " + replicatedDbName + " cascade");
    primary.run("drop database if exists " + primaryDbName + "_extra cascade");
  }

  private void prepareAcidData(String primaryDbName) throws Throwable {
    primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
            "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(1)")
            .run("insert into t1 values(2)")
            .run("create table t2 (place string) partitioned by (country string) clustered by(place) " +
                    "into 3 buckets stored as orc tblproperties (\"transactional\"=\"true\")")
            .run("insert into t2 partition(country='india') values ('bangalore')")
            .run("insert into t2 partition(country='us') values ('austin')")
            .run("insert into t2 partition(country='france') values ('paris')")
            .run("alter table t2 add partition(country='italy')")
            .run("create table t3 (rank int) tblproperties(\"transactional\"=\"true\", " +
                    "\"transactional_properties\"=\"insert_only\")")
            .run("insert into t3 values(11)")
            .run("insert into t3 values(22)")
            .run("create table t5 (id int) stored as orc ")
            .run("insert into t5 values(1111), (2222)")
            .run("alter table t5 set tblproperties (\"transactional\"=\"true\")")
            .run("insert into t5 values(3333)");
    acidTableNames.add("t1");
    acidTableNames.add("t2");
    acidTableNames.add("t3");
    acidTableNames.add("t5");
  }

  private void prepareNonAcidData(String primaryDbName) throws Throwable {
    primary.run("use " + primaryDbName)
            .run("create table t4 (id int)")
            .run("insert into t4 values(111), (222)");
    nonAcidTableNames.add("t4");
  }
  private WarehouseInstance.Tuple prepareDataAndDump(String primaryDbName, String fromReplId,
                                                     List<String> withClause) throws Throwable {
    prepareAcidData(primaryDbName);
    prepareNonAcidData(primaryDbName);
    return primary.run("use " + primaryDbName)
            .dump(primaryDbName, fromReplId, withClause != null ?
                    withClause : Collections.emptyList());
  }

  private void verifyNonAcidTableLoad(String replicatedDbName) throws Throwable {
    replica.run("use " + replicatedDbName)
            .run("select id from t4 order by id")
            .verifyResults(new String[] {"111", "222"});
  }

  private void verifyAcidTableLoad(String replicatedDbName) throws Throwable {
    replica.run("use " + replicatedDbName)
            .run("select id from t1 order by id")
            .verifyResults(new String[]{"1", "2"})
            .run("select country from t2 order by country")
            .verifyResults(new String[] {"france", "india", "us"})
            .run("select rank from t3 order by rank")
            .verifyResults(new String[] {"11", "22"})
            .run("select id from t5 order by id")
            .verifyResults(new String[] {"1111", "2222", "3333"});
  }

  private void verifyLoadExecution(String replicatedDbName, String lastReplId, boolean includeAcid)
          throws Throwable {
    List<String> tableNames = new LinkedList<>(nonAcidTableNames);
    if (includeAcid) {
      tableNames.addAll(acidTableNames);
    }
    replica.run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(tableNames)
            .run("repl status " + replicatedDbName)
            .verifyResult(lastReplId);
    verifyNonAcidTableLoad(replicatedDbName);
    if (includeAcid) {
      verifyAcidTableLoad(replicatedDbName);
    }
  }

  @Test
  public void testAcidTablesBootstrap() throws Throwable {
    // Bootstrap
    WarehouseInstance.Tuple bootstrapDump = prepareDataAndDump(primaryDbName, null, null);
    replica.load(replicatedDbName, bootstrapDump.dumpLocation);
    verifyLoadExecution(replicatedDbName, bootstrapDump.lastReplicationId, true);

    // First incremental, after bootstrap
    prepareIncNonAcidData(primaryDbName);
    prepareIncAcidData(primaryDbName);
    LOG.info(testName.getMethodName() + ": first incremental dump and load.");
    WarehouseInstance.Tuple incDump = primary.run("use " + primaryDbName)
            .dump(primaryDbName, bootstrapDump.lastReplicationId);
    replica.load(replicatedDbName, incDump.dumpLocation);
    verifyIncLoad(replicatedDbName, incDump.lastReplicationId);

    // Second incremental, after bootstrap
    prepareInc2NonAcidData(primaryDbName, primary.hiveConf);
    prepareInc2AcidData(primaryDbName, primary.hiveConf);
    LOG.info(testName.getMethodName() + ": second incremental dump and load.");
    WarehouseInstance.Tuple inc2Dump = primary.run("use " + primaryDbName)
            .dump(primaryDbName, incDump.lastReplicationId);
    replica.load(replicatedDbName, inc2Dump.dumpLocation);
    verifyInc2Load(replicatedDbName, inc2Dump.lastReplicationId);
  }

  @Test
  public void testAcidTablesMoveOptimizationBootStrap() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = prepareDataAndDump(primaryDbName, null, null);
    replica.load(replicatedDbName, bootstrapDump.dumpLocation,
            Collections.singletonList("'hive.repl.enable.move.optimization'='true'"));
    verifyLoadExecution(replicatedDbName, bootstrapDump.lastReplicationId, true);
  }

  private void prepareIncAcidData(String dbName) throws Throwable {
    primary.run("use " + dbName)
            .run("create table t6 (str string) stored as orc tblproperties " +
                    "(\"transactional\"=\"true\")")
            .run("insert into t6 values ('aaa'), ('bbb')")
            .run("alter table t2 add columns (placetype string)")
            .run("update t2 set placetype = 'city'");
    acidTableNames.add("t6");
  }

  private void verifyIncAcidLoad(String dbName) throws Throwable {
    replica.run("use " + dbName)
            .run("select str from t6 order by str")
            .verifyResults(new String[]{"aaa", "bbb"})
            .run("select country from t2 order by country")
            .verifyResults(new String[] {"france", "india", "us"})
            .run("select distinct placetype from t2")
            .verifyResult("city")
            .run("select id from t1 order by id")
            .verifyResults(new String[]{"1", "2"})
            .run("select rank from t3 order by rank")
            .verifyResults(new String[] {"11", "22"})
            .run("select id from t5 order by id")
            .verifyResults(new String[] {"1111", "2222", "3333"});
  }

  private void runUsingDriver(IDriver driver, String command) throws Throwable {
    CommandProcessorResponse ret = driver.run(command);
    if (ret.getException() != null) {
      throw ret.getException();
    }
  }

  private void prepareInc2AcidData(String dbName, HiveConf hiveConf) throws Throwable {
    IDriver driver = DriverFactory.newDriver(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));
    runUsingDriver(driver, "use " + dbName);
    runUsingDriver(driver, "insert into t1 values (3)");
    runUsingDriver(driver, "insert into t5 values (4444)");
  }

  private void verifyInc2AcidLoad(String dbName) throws Throwable {
    replica.run("use " + dbName)
            .run("select str from t6 order by str")
            .verifyResults(new String[]{"aaa", "bbb"})
            .run("select country from t2 order by country")
            .verifyResults(new String[] {"france", "india", "us"})
            .run("select distinct placetype from t2")
            .verifyResult("city")
            .run("select id from t1 order by id")
            .verifyResults(new String[]{"1", "2", "3"})
            .run("select rank from t3 order by rank")
            .verifyResults(new String[] {"11", "22"})
            .run("select id from t5 order by id")
            .verifyResults(new String[] {"1111", "2222", "3333", "4444"});
  }

  private void prepareIncNonAcidData(String dbName) throws Throwable {
    primary.run("use " + dbName)
            .run("insert into t4 values (333)")
            .run("create table t7 (str string)")
            .run("insert into t7 values ('aaa')");
    nonAcidTableNames.add("t7");
  }

  private void verifyIncNonAcidLoad(String dbName) throws Throwable {
    replica.run("use " + dbName)
            .run("select * from t4 order by id")
            .verifyResults(new String[] {"111", "222", "333"})
            .run("select * from t7")
            .verifyResult("aaa");
  }

  private void prepareInc2NonAcidData(String dbName, HiveConf hiveConf) throws Throwable {
    IDriver driver = DriverFactory.newDriver(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));
    runUsingDriver(driver, "use " + dbName);
    runUsingDriver(driver, "insert into t4 values (444)");
    runUsingDriver(driver, "insert into t7 values ('bbb')");
  }

  private void verifyInc2NonAcidLoad(String dbName) throws Throwable {
    replica.run("use " + dbName)
            .run("select * from t4 order by id")
            .verifyResults(new String[] {"111", "222", "333", "444"})
            .run("select * from t7")
            .verifyResults(new String[] {"aaa", "bbb"});
  }

  private void verifyIncLoad(String dbName, String lastReplId)
          throws Throwable {
    List<String> tableNames = new LinkedList<>(nonAcidTableNames);
    tableNames.addAll(acidTableNames);
    replica.run("use " + dbName)
            .run("show tables")
            .verifyResults(tableNames)
            .run("repl status " + dbName)
            .verifyResult(lastReplId);
    verifyIncNonAcidLoad(dbName);
    verifyIncAcidLoad(dbName);
  }

  private void verifyInc2Load(String dbName, String lastReplId)
          throws Throwable {
    List<String> tableNames = new LinkedList<>(nonAcidTableNames);
    tableNames.addAll(acidTableNames);
    replica.run("use " + dbName)
            .run("show tables")
            .verifyResults(tableNames)
            .run("repl status " + dbName)
            .verifyResult(lastReplId);
    verifyInc2NonAcidLoad(dbName);
    verifyInc2AcidLoad(dbName);
  }

  @Test
  public void testAcidTablesBootstrapDuringIncremental() throws Throwable {
    // Take a bootstrap dump without acid tables
    WarehouseInstance.Tuple bootstrapDump = prepareDataAndDump(primaryDbName, null,
            dumpWithoutAcidClause);
    LOG.info(testName.getMethodName() + ": loading dump without acid tables.");
    replica.load(replicatedDbName, bootstrapDump.dumpLocation);
    verifyLoadExecution(replicatedDbName, bootstrapDump.lastReplicationId, false);

    // Take a incremental dump with acid table bootstrap
    prepareIncAcidData(primaryDbName);
    prepareIncNonAcidData(primaryDbName);
    LOG.info(testName.getMethodName() + ": incremental dump and load dump with acid table bootstrap.");
    WarehouseInstance.Tuple incrementalDump = primary.run("use " + primaryDbName)
            .dump(primaryDbName, bootstrapDump.lastReplicationId, dumpWithAcidBootstrapClause);
    replica.load(replicatedDbName, incrementalDump.dumpLocation);
    verifyIncLoad(replicatedDbName, incrementalDump.lastReplicationId);
    // Ckpt should be set on bootstrapped tables.
    replica.verifyIfCkptSetForTables(replicatedDbName, acidTableNames, incrementalDump.dumpLocation);

    // Take a second normal incremental dump after Acid table boostrap
    prepareInc2AcidData(primaryDbName, primary.hiveConf);
    prepareInc2NonAcidData(primaryDbName, primary.hiveConf);
    LOG.info(testName.getMethodName()
             + ": second incremental dump and load dump after incremental with acid table " +
            "bootstrap.");
    WarehouseInstance.Tuple inc2Dump = primary.run("use " + primaryDbName)
            .dump(primaryDbName, incrementalDump.lastReplicationId);
    replica.load(replicatedDbName, inc2Dump.dumpLocation);
    verifyInc2Load(replicatedDbName, inc2Dump.lastReplicationId);
  }

  @Test
  public void testRetryAcidTablesBootstrapFromDifferentDump() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = prepareDataAndDump(primaryDbName, null,
            dumpWithoutAcidClause);
    LOG.info(testName.getMethodName() + ": loading dump without acid tables.");
    replica.load(replicatedDbName, bootstrapDump.dumpLocation);
    verifyLoadExecution(replicatedDbName, bootstrapDump.lastReplicationId, false);

    prepareIncAcidData(primaryDbName);
    prepareIncNonAcidData(primaryDbName);
    LOG.info(testName.getMethodName() + ": first incremental dump with acid table bootstrap.");
    WarehouseInstance.Tuple incDump = primary.run("use " + primaryDbName)
            .dump(primaryDbName, bootstrapDump.lastReplicationId, dumpWithAcidBootstrapClause);

    // Fail setting ckpt property for table t5 but success for earlier tables
    BehaviourInjection<CallerArguments, Boolean> callerVerifier
            = new BehaviourInjection<CallerArguments, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable CallerArguments args) {
        if (args.tblName.equalsIgnoreCase("t5") && args.dbName.equalsIgnoreCase(replicatedDbName)) {
          injectionPathCalled = true;
          LOG.warn("Verifier - DB : " + args.dbName + " TABLE : " + args.tblName);
          return false;
        }
        return true;
      }
    };

    // Fail repl load before the ckpt property is set for t4 and after it is set for t2.
    // In the retry, these half baked tables should be dropped and bootstrap should be successful.
    InjectableBehaviourObjectStore.setAlterTableModifier(callerVerifier);
    try {
      LOG.info(testName.getMethodName()
              + ": loading first incremental dump with acid table bootstrap (will fail)");
      replica.loadFailure(replicatedDbName, incDump.dumpLocation);
      callerVerifier.assertInjectionsPerformed(true, false);
    } finally {
      InjectableBehaviourObjectStore.resetAlterTableModifier();
    }

    prepareInc2AcidData(primaryDbName, primary.hiveConf);
    prepareInc2NonAcidData(primaryDbName, primary.hiveConf);
    LOG.info(testName.getMethodName() + ": second incremental dump with acid table bootstrap");
    WarehouseInstance.Tuple inc2Dump = primary.run("use " + primaryDbName)
            .dump(primaryDbName, bootstrapDump.lastReplicationId, dumpWithAcidBootstrapClause);

    // Set incorrect bootstrap dump to clean tables. Here, used the full bootstrap dump which is invalid.
    // So, REPL LOAD fails.
    List<String> loadWithClause = Collections.singletonList(
            "'" + ReplUtils.REPL_CLEAN_TABLES_FROM_BOOTSTRAP_CONFIG + "'='"
            + bootstrapDump.dumpLocation + "'");
    LOG.info(testName.getMethodName()
            + ": trying to load second incremental dump with wrong bootstrap dump "
            + " specified for cleaning ACID tables. Should fail.");
    replica.loadFailure(replicatedDbName, inc2Dump.dumpLocation, loadWithClause);

    // Set previously failed bootstrap dump to clean-up. Now, new bootstrap should overwrite the old one.
    loadWithClause = Collections.singletonList(
            "'" + ReplUtils.REPL_CLEAN_TABLES_FROM_BOOTSTRAP_CONFIG + "'='"
                    + incDump.dumpLocation + "'");

    LOG.info(testName.getMethodName()
            + ": trying to load second incremental dump with correct bootstrap dump "
            + "specified for cleaning ACID tables. Should succeed.");
    replica.load(replicatedDbName, inc2Dump.dumpLocation, loadWithClause);
    verifyInc2Load(replicatedDbName, inc2Dump.lastReplicationId);

    // Once the REPL LOAD is successful, the this config should be unset or else, the subsequent REPL LOAD
    // will also drop those tables which will cause data loss.
    loadWithClause = Collections.emptyList();

    // Verify if bootstrapping with same dump is idempotent and return same result
    LOG.info(testName.getMethodName()
            + ": trying to load second incremental dump (with acid bootstrap) again."
            + " Should succeed.");
    replica.load(replicatedDbName, inc2Dump.dumpLocation, loadWithClause);
    verifyInc2Load(replicatedDbName, inc2Dump.lastReplicationId);
  }

  @Test
  public void retryIncBootstrapAcidFromDifferentDumpWithoutCleanTablesConfig() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = prepareDataAndDump(primaryDbName, null,
            dumpWithoutAcidClause);
    replica.load(replicatedDbName, bootstrapDump.dumpLocation);

    prepareIncAcidData(primaryDbName);
    prepareIncNonAcidData(primaryDbName);
    WarehouseInstance.Tuple incDump = primary.run("use " + primaryDbName)
            .dump(primaryDbName, bootstrapDump.lastReplicationId, dumpWithAcidBootstrapClause);
    WarehouseInstance.Tuple inc2Dump = primary.run("use " + primaryDbName)
            .dump(primaryDbName, bootstrapDump.lastReplicationId, dumpWithAcidBootstrapClause);
    replica.load(replicatedDbName, incDump.dumpLocation);

    // Re-bootstrapping from different bootstrap dump without clean tables config should fail.
    replica.loadFailure(replicatedDbName, inc2Dump.dumpLocation, Collections.emptyList(),
            ErrorMsg.REPL_BOOTSTRAP_LOAD_PATH_NOT_VALID.getErrorCode());
  }

  @Test
  public void testAcidTablesMoveOptimizationIncremental() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootstrapDump.dumpLocation,
            Collections.singletonList("'hive.repl.enable.move.optimization'='true'"));
    WarehouseInstance.Tuple incrDump = prepareDataAndDump(primaryDbName,
            bootstrapDump.lastReplicationId, null);
    replica.load(replicatedDbName, incrDump.dumpLocation,
            Collections.singletonList("'hive.repl.enable.move.optimization'='true'"));
    verifyLoadExecution(replicatedDbName, incrDump.lastReplicationId, true);
  }

  private List<Long> openTxns(int numTxns, TxnStore txnHandler, HiveConf primaryConf) throws Throwable {
    OpenTxnsResponse otResp = txnHandler.openTxns(new OpenTxnRequest(numTxns, "u1", "localhost"));
    List<Long> txns = otResp.getTxn_ids();
    String txnIdRange = " txn_id >= " + txns.get(0) + " and txn_id <= " + txns.get(numTxns - 1);
    Assert.assertEquals(TxnDbUtil.queryToString(primaryConf, "select * from TXNS"),
            numTxns, TxnDbUtil.countQueryAgent(primaryConf,
                    "select count(*) from TXNS where txn_state = 'o' and " + txnIdRange));
    return txns;
  }

  private void allocateWriteIdsForTables(String primaryDbName, Map<String, Long> tables,
                                         TxnStore txnHandler,
                                         List<Long> txns, HiveConf primaryConf) throws Throwable {
    AllocateTableWriteIdsRequest rqst = new AllocateTableWriteIdsRequest();
    rqst.setDbName(primaryDbName);

    for(Map.Entry<String, Long> entry : tables.entrySet()) {
      rqst.setTableName(entry.getKey());
      rqst.setTxnIds(txns);
      txnHandler.allocateTableWriteIds(rqst);
    }
    verifyWriteIdsForTables(tables, primaryConf, primaryDbName);
  }

  private void verifyWriteIdsForTables(Map<String, Long> tables, HiveConf conf, String dbName)
          throws Throwable {
    for(Map.Entry<String, Long> entry : tables.entrySet()) {
      Assert.assertEquals(TxnDbUtil.queryToString(conf, "select * from TXN_TO_WRITE_ID"),
                          entry.getValue().longValue(),
                          TxnDbUtil.countQueryAgent(conf,
          "select count(*) from TXN_TO_WRITE_ID where t2w_database = '"
                    + dbName.toLowerCase()
                    + "' and t2w_table = '" + entry.getKey() + "'"));
    }
  }

  private void verifyAllOpenTxnsAborted(List<Long> txns, HiveConf primaryConf) throws Throwable {
    int numTxns = txns.size();
    String txnIdRange = " txn_id >= " + txns.get(0) + " and txn_id <= " + txns.get(numTxns - 1);
    Assert.assertEquals(TxnDbUtil.queryToString(primaryConf, "select * from TXNS"),
            0, TxnDbUtil.countQueryAgent(primaryConf,
                    "select count(*) from TXNS where txn_state = 'o' and " + txnIdRange));
    Assert.assertEquals(TxnDbUtil.queryToString(primaryConf, "select * from TXNS"),
            numTxns, TxnDbUtil.countQueryAgent(primaryConf,
                    "select count(*) from TXNS where txn_state = 'a' and " + txnIdRange));
  }

  private void verifyNextId(Map<String, Long> tables, String dbName, HiveConf conf) throws Throwable {
    // Verify the next write id
    for(Map.Entry<String, Long> entry : tables.entrySet()) {
      String[] nextWriteId =
              TxnDbUtil.queryToString(conf,
                      "select nwi_next from NEXT_WRITE_ID where  nwi_database = '"
                              + dbName.toLowerCase() + "' and nwi_table = '"
                              + entry.getKey() + "'").split("\n");
      Assert.assertEquals(Long.parseLong(nextWriteId[1].trim()), entry.getValue() + 1);
    }
  }

  private void verifyCompactionQueue(Map<String, Long> tables, String dbName, HiveConf conf)
          throws Throwable {
    for(Map.Entry<String, Long> entry : tables.entrySet()) {
    Assert.assertEquals(TxnDbUtil.queryToString(conf, "select * from COMPACTION_QUEUE"),
                        entry.getValue().longValue(),
                        TxnDbUtil.countQueryAgent(conf,
                    "select count(*) from COMPACTION_QUEUE where cq_database = '" + dbName
                            + "' and cq_table = '" + entry.getKey() + "'"));
    }
  }

  @Test
  public void testAcidTablesBootstrapWithOpenTxnsTimeout() throws Throwable {
    int numTxns = 5;
    HiveConf primaryConf = primary.getConf();
    TxnStore txnHandler = TxnUtils.getTxnStore(primary.getConf());
    // Open 5 txns
    List<Long> txns = openTxns(numTxns, txnHandler, primaryConf);

    // Create 2 tables, one partitioned and other not. Also, have both types of full ACID and MM tables.
    primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(1)")
            .run("create table t2 (rank int) partitioned by (name string) tblproperties(\"transactional\"=\"true\", " +
                    "\"transactional_properties\"=\"insert_only\")")
            .run("insert into t2 partition(name='Bob') values(11)")
            .run("insert into t2 partition(name='Carl') values(10)");

    // Allocate write ids for both tables t1 and t2 for all txns
    // t1=5+1(insert) and t2=5+2(insert)
    Map<String, Long> tables = new HashMap<>();
    tables.put("t1", numTxns+1L);
    tables.put("t2", numTxns+2L);
    allocateWriteIdsForTables(primaryDbName, tables, txnHandler, txns, primaryConf);

    // Bootstrap dump with open txn timeout as 1s.
    List<String> withConfigs = Arrays.asList(
            "'hive.repl.bootstrap.dump.open.txn.timeout'='1s'");
    WarehouseInstance.Tuple bootstrapDump = primary
            .run("use " + primaryDbName)
            .dump(primaryDbName, null, withConfigs);

    // After bootstrap dump, all the opened txns should be aborted. Verify it.
    verifyAllOpenTxnsAborted(txns, primaryConf);
    verifyNextId(tables, primaryDbName, primaryConf);

    // Bootstrap load which should also replicate the aborted write ids on both tables.
    HiveConf replicaConf = replica.getConf();
    replica.load(replicatedDbName, bootstrapDump.dumpLocation)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"t1", "t2"})
            .run("repl status " + replicatedDbName)
            .verifyResult(bootstrapDump.lastReplicationId)
            .run("select id from t1")
            .verifyResults(new String[]{"1"})
            .run("select rank from t2 order by rank")
            .verifyResults(new String[] {"10", "11"});

    // Verify if HWM is properly set after REPL LOAD
    verifyNextId(tables, replicatedDbName, replicaConf);

    // Verify if all the aborted write ids are replicated to the replicated DB
    for(Map.Entry<String, Long> entry : tables.entrySet()) {
      entry.setValue((long) numTxns);
    }
    verifyWriteIdsForTables(tables, replicaConf, replicatedDbName);

    // Verify if entries added in COMPACTION_QUEUE for each table/partition
    // t1-> 1 entry and t2-> 2 entries (1 per partition)
    tables.clear();
    tables.put("t1", 1L);
    tables.put("t2", 2L);
    verifyCompactionQueue(tables, replicatedDbName, replicaConf);
  }

  @Test
  public void testAcidTablesBootstrapDuringIncrementalWithOpenTxnsTimeout() throws Throwable {
    // Take a dump without ACID tables
    WarehouseInstance.Tuple bootstrapDump = prepareDataAndDump(primaryDbName, null,
                                                                dumpWithoutAcidClause);
    LOG.info(testName.getMethodName() + ": loading dump without acid tables.");
    replica.load(replicatedDbName, bootstrapDump.dumpLocation);

    // Open concurrent transactions, create data for incremental and take an incremental dump
    // with ACID table bootstrap.
    int numTxns = 5;
    HiveConf primaryConf = primary.getConf();
    TxnStore txnHandler = TxnUtils.getTxnStore(primary.getConf());
    // Open 5 txns
    List<Long> txns = openTxns(numTxns, txnHandler, primaryConf);
    prepareIncNonAcidData(primaryDbName);
    prepareIncAcidData(primaryDbName);
    // Allocate write ids for tables t1 and t2 for all txns
    // t1=5+2(insert) and t2=5+5(insert, alter add column)
    Map<String, Long> tables = new HashMap<>();
    tables.put("t1", numTxns+2L);
    tables.put("t2", numTxns+5L);
    allocateWriteIdsForTables(primaryDbName, tables, txnHandler, txns, primaryConf);

    // Bootstrap dump with open txn timeout as 1s.
    List<String> withConfigs = new LinkedList<>(dumpWithAcidBootstrapClause);
            withConfigs.add("'hive.repl.bootstrap.dump.open.txn.timeout'='1s'");
    WarehouseInstance.Tuple incDump = primary
            .run("use " + primaryDbName)
            .dump(primaryDbName, bootstrapDump.lastReplicationId, withConfigs);

    // After bootstrap dump, all the opened txns should be aborted. Verify it.
    verifyAllOpenTxnsAborted(txns, primaryConf);
    verifyNextId(tables, primaryDbName, primaryConf);

    // Incremental load with ACID bootstrap should also replicate the aborted write ids on
    // tables t1 and t2
    HiveConf replicaConf = replica.getConf();
    LOG.info(testName.getMethodName() + ": loading incremental dump with ACID bootstrap.");
    replica.load(replicatedDbName, incDump.dumpLocation);
    // During incremental dump with ACID bootstrap we do not dump ALLOC_WRITE_ID events. So the
    // two ALLOC_WRITE_ID events corresponding aborted transactions on t1 and t2 will not be
    // repliaced. Discount those.
    verifyIncLoad(replicatedDbName,
            (new Long(Long.valueOf(incDump.lastReplicationId) - 2)).toString());
    // Verify if HWM is properly set after REPL LOAD
    verifyNextId(tables, replicatedDbName, replicaConf);

    // Verify if all the aborted write ids are replicated to the replicated DB
    for(Map.Entry<String, Long> entry : tables.entrySet()) {
      entry.setValue((long) numTxns);
    }
    verifyWriteIdsForTables(tables, replicaConf, replicatedDbName);

    // Verify if entries added in COMPACTION_QUEUE for each table/partition
    // t1-> 1 entry and t2-> 2 entries (1 per partition)
    tables.clear();
    tables.put("t1", 1L);
    tables.put("t2", 4L);
    verifyCompactionQueue(tables, replicatedDbName, replicaConf);
  }

  @Test
  public void testAcidTablesBootstrapWithConcurrentWrites() throws Throwable {
    HiveConf primaryConf = primary.getConf();
    primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(1)");

    // Perform concurrent write on the acid table t1 when bootstrap dump in progress. Bootstrap
    // won't see the written data but the subsequent incremental repl should see it.
    BehaviourInjection<CallerArguments, Boolean> callerInjectedBehavior
            = new BehaviourInjection<CallerArguments, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable CallerArguments args) {
        if (injectionPathCalled) {
          nonInjectedPathCalled = true;
        } else {
          // Insert another row to t1 from another txn when bootstrap dump in progress.
          injectionPathCalled = true;
          Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
              LOG.info("Entered new thread");
              IDriver driver = DriverFactory.newDriver(primaryConf);
              SessionState.start(new CliSessionState(primaryConf));
              CommandProcessorResponse ret = driver.run("insert into " + primaryDbName + ".t1 values(2)");
              boolean success = (ret.getException() == null);
              assertTrue(success);
              LOG.info("Exit new thread success - {}", success, ret.getException());
            }
          });
          t.start();
          LOG.info("Created new thread {}", t.getName());
          try {
            t.join();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return true;
      }
    };

    InjectableBehaviourObjectStore.setCallerVerifier(callerInjectedBehavior);
    WarehouseInstance.Tuple bootstrapDump = null;
    try {
      bootstrapDump = primary.dump(primaryDbName, null);
      callerInjectedBehavior.assertInjectionsPerformed(true, true);
    } finally {
      InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour
    }

    // Bootstrap dump has taken snapshot before concurrent tread performed write. So, it won't see data "2".
    replica.load(replicatedDbName, bootstrapDump.dumpLocation)
            .run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(bootstrapDump.lastReplicationId)
            .run("select id from t1 order by id")
            .verifyResults(new String[]{"1" });

    // Incremental should include the concurrent write of data "2" from another txn.
    WarehouseInstance.Tuple incrementalDump = primary.dump(primaryDbName, bootstrapDump.lastReplicationId);
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
            .run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId)
            .run("select id from t1 order by id")
            .verifyResults(new String[]{"1", "2" });
  }

  @Test
  public void testBootstrapAcidTablesDuringIncrementalWithConcurrentWrites() throws Throwable {
    // Dump and load bootstrap without ACID tables.
    WarehouseInstance.Tuple bootstrapDump = prepareDataAndDump(primaryDbName, null,
                                                                dumpWithoutAcidClause);
    LOG.info(testName.getMethodName() + ": loading dump without acid tables.");
    replica.load(replicatedDbName, bootstrapDump.dumpLocation);

    // Create incremental data for incremental load with bootstrap of ACID
    prepareIncNonAcidData(primaryDbName);
    prepareIncAcidData(primaryDbName);
    // Perform concurrent writes. Bootstrap won't see the written data but the subsequent
    // incremental repl should see it. We can not inject callerVerifier since an incremental dump
    // would not cause an ALTER DATABASE event. Instead we piggy back on
    // getCurrentNotificationEventId() which is anyway required for a bootstrap.
    BehaviourInjection<CurrentNotificationEventId, CurrentNotificationEventId> callerInjectedBehavior
            = new BehaviourInjection<CurrentNotificationEventId, CurrentNotificationEventId>() {
      @Nullable
      @Override
      public CurrentNotificationEventId apply(@Nullable CurrentNotificationEventId input) {
        if (injectionPathCalled) {
          nonInjectedPathCalled = true;
        } else {
          // Do some writes through concurrent thread
          injectionPathCalled = true;
          Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
              LOG.info("Entered new thread");
              try {
                prepareInc2NonAcidData(primaryDbName, primary.hiveConf);
                prepareInc2AcidData(primaryDbName, primary.hiveConf);
              } catch (Throwable t) {
                Assert.assertNull(t);
              }
              LOG.info("Exit new thread success");
            }
          });
          t.start();
          LOG.info("Created new thread {}", t.getName());
          try {
            t.join();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return input;
      }
    };

    InjectableBehaviourObjectStore.setGetCurrentNotificationEventIdBehaviour(callerInjectedBehavior);
    WarehouseInstance.Tuple incDump = null;
    try {
      incDump = primary.dump(primaryDbName, bootstrapDump.lastReplicationId, dumpWithAcidBootstrapClause);
      callerInjectedBehavior.assertInjectionsPerformed(true, true);
    } finally {
      // reset the behaviour
      InjectableBehaviourObjectStore.resetGetCurrentNotificationEventIdBehaviour();
    }

    // While bootstrapping ACID tables it has taken snapshot before concurrent thread performed
    // write. So concurrent writes won't be dumped.
    LOG.info(testName.getMethodName() +
            ": loading incremental dump containing bootstrapped ACID tables.");
    replica.load(replicatedDbName, incDump.dumpLocation);
    verifyIncLoad(replicatedDbName, incDump.lastReplicationId);

    // Next Incremental should include the concurrent writes
    LOG.info(testName.getMethodName() +
            ": dumping second normal incremental dump from event id = " + incDump.lastReplicationId);
    WarehouseInstance.Tuple inc2Dump = primary.dump(primaryDbName, incDump.lastReplicationId);
    LOG.info(testName.getMethodName() +
            ": loading second normal incremental dump from event id = " + incDump.lastReplicationId);
    replica.load(replicatedDbName, inc2Dump.dumpLocation);
    verifyInc2Load(replicatedDbName, inc2Dump.lastReplicationId);
  }

  @Test
  public void testAcidTablesBootstrapWithConcurrentDropTable() throws Throwable {
    HiveConf primaryConf = primary.getConf();
    primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(1)");

    // Perform concurrent write + drop on the acid table t1 when bootstrap dump in progress. Bootstrap
    // won't dump the table but the subsequent incremental repl with new table with same name should be seen.
    BehaviourInjection<CallerArguments, Boolean> callerInjectedBehavior
            = new BehaviourInjection<CallerArguments, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable CallerArguments args) {
        if (injectionPathCalled) {
          nonInjectedPathCalled = true;
        } else {
          // Insert another row to t1 and drop the table from another txn when bootstrap dump in progress.
          injectionPathCalled = true;
          Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
              LOG.info("Entered new thread");
              IDriver driver = DriverFactory.newDriver(primaryConf);
              SessionState.start(new CliSessionState(primaryConf));
              CommandProcessorResponse ret = driver.run("insert into " + primaryDbName + ".t1 values(2)");
              boolean success = (ret.getException() == null);
              assertTrue(success);
              ret = driver.run("drop table " + primaryDbName + ".t1");
              success = (ret.getException() == null);
              assertTrue(success);
              LOG.info("Exit new thread success - {}", success, ret.getException());
            }
          });
          t.start();
          LOG.info("Created new thread {}", t.getName());
          try {
            t.join();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return true;
      }
    };

    InjectableBehaviourObjectStore.setCallerVerifier(callerInjectedBehavior);
    WarehouseInstance.Tuple bootstrapDump = null;
    try {
      bootstrapDump = primary.dump(primaryDbName, null);
      callerInjectedBehavior.assertInjectionsPerformed(true, true);
    } finally {
      InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour
    }

    // Bootstrap dump has taken latest list of tables and hence won't see table t1 as it is dropped.
    replica.load(replicatedDbName, bootstrapDump.dumpLocation)
            .run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(bootstrapDump.lastReplicationId)
            .run("show tables")
            .verifyResult(null);

    // Create another ACID table with same name and insert a row. It should be properly replicated.
    WarehouseInstance.Tuple incrementalDump = primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(100)")
            .dump(primaryDbName, bootstrapDump.lastReplicationId);

    replica.load(replicatedDbName, incrementalDump.dumpLocation)
            .run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId)
            .run("select id from t1 order by id")
            .verifyResult("100");
  }

  @Test
  public void testOpenTxnEvent() throws Throwable {
    String tableName = testName.getMethodName();
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    primary.run("use " + primaryDbName)
           .run("CREATE TABLE " + tableName +
            " (key int, value int) PARTITIONED BY (load_date date) " +
            "CLUSTERED BY(key) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')")
            .run("SHOW TABLES LIKE '" + tableName + "'")
            .verifyResult(tableName)
            .run("INSERT INTO " + tableName + " partition (load_date='2016-03-01') VALUES (1, 1)")
            .run("select key from " + tableName)
            .verifyResult("1");

    WarehouseInstance.Tuple incrementalDump =
            primary.dump(primaryDbName, bootStrapDump.lastReplicationId);

    long lastReplId = Long.parseLong(bootStrapDump.lastReplicationId);
    primary.testEventCounts(primaryDbName, lastReplId, null, null, 22);

    // Test load
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId);

    // Test the idempotent behavior of Open and Commit Txn
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId);
  }

  @Test
  public void testAbortTxnEvent() throws Throwable {
    String tableName = testName.getMethodName();
    String tableNameFail = testName.getMethodName() + "Fail";
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    // this should fail
    primary.run("use " + primaryDbName)
            .runFailure("CREATE TABLE " + tableNameFail +
            " (key int, value int) PARTITIONED BY (load_date date) " +
            "CLUSTERED BY(key) ('transactional'='true')")
            .run("SHOW TABLES LIKE '" + tableNameFail + "'")
            .verifyFailure(new String[]{tableNameFail});

    WarehouseInstance.Tuple incrementalDump =
            primary.dump(primaryDbName, bootStrapDump.lastReplicationId);
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId);

    // Test the idempotent behavior of Abort Txn
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId);
  }

  @Test
  public void testTxnEventNonAcid() throws Throwable {
    String tableName = testName.getMethodName();
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replicaNonAcid.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    primary.run("use " + primaryDbName)
            .run("CREATE TABLE " + tableName +
            " (key int, value int) PARTITIONED BY (load_date date) " +
            "CLUSTERED BY(key) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')")
            .run("SHOW TABLES LIKE '" + tableName + "'")
            .verifyResult(tableName)
            .run("INSERT INTO " + tableName +
                    " partition (load_date='2016-03-01') VALUES (1, 1)")
            .run("select key from " + tableName)
            .verifyResult("1");

    WarehouseInstance.Tuple incrementalDump =
            primary.dump(primaryDbName, bootStrapDump.lastReplicationId);
    replicaNonAcid.runFailure("REPL LOAD " + replicatedDbName + " FROM '" + incrementalDump.dumpLocation + "'")
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);
  }

  @Test
  public void testAcidBootstrapReplLoadRetryAfterFailure() throws Throwable {
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(1)")
            .run("create table t2 (rank int) partitioned by (name string) tblproperties(\"transactional\"=\"true\", " +
                    "\"transactional_properties\"=\"insert_only\")")
            .run("insert into t2 partition(name='bob') values(11)")
            .run("insert into t2 partition(name='carl') values(10)")
            .dump(primaryDbName, null);

    WarehouseInstance.Tuple tuple2 = primary
            .run("use " + primaryDbName)
            .dump(primaryDbName, null);

    // Inject a behavior where REPL LOAD failed when try to load table "t2", it fails.
    BehaviourInjection<CallerArguments, Boolean> callerVerifier
            = new BehaviourInjection<CallerArguments, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable CallerArguments args) {
        injectionPathCalled = true;
        if (!args.dbName.equalsIgnoreCase(replicatedDbName)) {
          LOG.warn("Verifier - DB: " + String.valueOf(args.dbName));
          return false;
        }
        if (args.tblName != null) {
          LOG.warn("Verifier - Table: " + String.valueOf(args.tblName));
          return args.tblName.equals("t1");
        }
        return true;
      }
    };
    InjectableBehaviourObjectStore.setCallerVerifier(callerVerifier);

    List<String> withConfigs = Arrays.asList("'hive.repl.approx.max.load.tasks'='1'");
    replica.loadFailure(replicatedDbName, tuple.dumpLocation, withConfigs);
    callerVerifier.assertInjectionsPerformed(true, false);
    InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour

    replica.run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult("null")
            .run("show tables like t2")
            .verifyResults(new String[] { });

    // Retry with different dump should fail.
    replica.loadFailure(replicatedDbName, tuple2.dumpLocation);

    // Verify if no create table on t1. Only table t2 should  be created in retry.
    callerVerifier = new BehaviourInjection<CallerArguments, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable CallerArguments args) {
        injectionPathCalled = true;
        if (!args.dbName.equalsIgnoreCase(replicatedDbName)) {
          LOG.warn("Verifier - DB: " + String.valueOf(args.dbName));
          return false;
        }
        return true;
      }
    };
    InjectableBehaviourObjectStore.setCallerVerifier(callerVerifier);

    // Retry with same dump with which it was already loaded should resume the bootstrap load.
    // This time, it completes by adding just constraints for table t4.
    replica.load(replicatedDbName, tuple.dumpLocation);
    callerVerifier.assertInjectionsPerformed(true, false);
    InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour

    replica.run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("show tables")
            .verifyResults(new String[] { "t1", "t2" })
            .run("select id from t1")
            .verifyResults(Arrays.asList("1"))
            .run("select name from t2 order by name")
            .verifyResults(Arrays.asList("bob", "carl"));
  }

  @Test
  public void testDumpAcidTableWithPartitionDirMissing() throws Throwable {
    String dbName = testName.getMethodName();
    primary.run("CREATE DATABASE " + dbName + " WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')")
    .run("CREATE TABLE " + dbName + ".normal (a int) PARTITIONED BY (part int)" +
            " STORED AS ORC TBLPROPERTIES ('transactional'='true')")
    .run("INSERT INTO " + dbName + ".normal partition (part= 124) values (1)");

    Path path = new Path(primary.warehouseRoot, dbName.toLowerCase()+".db");
    path = new Path(path, "normal");
    path = new Path(path, "part=124");
    FileSystem fs = path.getFileSystem(conf);
    fs.delete(path);

    CommandProcessorResponse ret = primary.runCommand("REPL DUMP " + dbName +
            " with ('hive.repl.dump.include.acid.tables' = 'true')");
    Assert.assertEquals(ret.getResponseCode(), ErrorMsg.FILE_NOT_FOUND.getErrorCode());

    primary.run("DROP TABLE " + dbName + ".normal");
    primary.run("drop database " + dbName);
  }

  @Test
  public void testDumpAcidTableWithTableDirMissing() throws Throwable {
    String dbName = testName.getMethodName();
    primary.run("CREATE DATABASE " + dbName + " WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')")
            .run("CREATE TABLE " + dbName + ".normal (a int) " +
                    " STORED AS ORC TBLPROPERTIES ('transactional'='true')")
            .run("INSERT INTO " + dbName + ".normal values (1)");

    Path path = new Path(primary.warehouseRoot, dbName.toLowerCase()+".db");
    path = new Path(path, "normal");
    FileSystem fs = path.getFileSystem(conf);
    fs.delete(path);

    CommandProcessorResponse ret = primary.runCommand("REPL DUMP " + dbName +
            " with ('hive.repl.dump.include.acid.tables' = 'true')");
    Assert.assertEquals(ret.getResponseCode(), ErrorMsg.FILE_NOT_FOUND.getErrorCode());

    primary.run("DROP TABLE " + dbName + ".normal");
    primary.run("drop database " + dbName);
  }

  @Test
  public void testMultiDBTxn() throws Throwable {
    String tableName = testName.getMethodName();
    String dbName1 = tableName + "_db1";
    String dbName2 = tableName + "_db2";
    String[] resultArray = new String[]{"1", "2", "3", "4", "5"};
    String tableProperty = "'transactional'='true'";
    String txnStrStart = "START TRANSACTION";
    String txnStrCommit = "COMMIT";

    WarehouseInstance.Tuple incrementalDump;
    primary.run("alter database default set dbproperties ('repl.source.for' = '1, 2, 3')");
    WarehouseInstance.Tuple bootStrapDump = primary.dump("`*`", null);

    primary.run("use " + primaryDbName)
          .run("create database " + dbName1 + " WITH DBPROPERTIES ( '" + SOURCE_OF_REPLICATION + "' = '1,2,3')")
          .run("create database " + dbName2 + " WITH DBPROPERTIES ( '" + SOURCE_OF_REPLICATION + "' = '1,2,3')")
          .run("CREATE TABLE " + dbName1 + "." + tableName + " (key int, value int) PARTITIONED BY (load_date date) " +
                  "CLUSTERED BY(key) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ( " + tableProperty + ")")
          .run("use " + dbName1)
          .run("SHOW TABLES LIKE '" + tableName + "'")
          .verifyResult(tableName)
          .run("CREATE TABLE " + dbName2 + "." + tableName + " (key int, value int) PARTITIONED BY (load_date date) " +
                  "CLUSTERED BY(key) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ( " + tableProperty + ")")
          .run("use " + dbName2)
          .run("SHOW TABLES LIKE '" + tableName + "'")
          .verifyResult(tableName)
          .run(txnStrStart)
          .run("INSERT INTO " + dbName2 + "." + tableName + " partition (load_date='2016-03-02') VALUES (5, 5)")
          .run("INSERT INTO " + dbName1 + "." + tableName + " partition (load_date='2016-03-01') VALUES (1, 1)")
          .run("INSERT INTO " + dbName1 + "." + tableName + " partition (load_date='2016-03-01') VALUES (2, 2)")
          .run("INSERT INTO " + dbName2 + "." + tableName + " partition (load_date='2016-03-01') VALUES (2, 2)")
          .run("INSERT INTO " + dbName2 + "." + tableName + " partition (load_date='2016-03-02') VALUES (3, 3)")
          .run("INSERT INTO " + dbName1 + "." + tableName + " partition (load_date='2016-03-02') VALUES (3, 3)")
          .run("INSERT INTO " + dbName1 + "." + tableName + " partition (load_date='2016-03-03') VALUES (4, 4)")
          .run("INSERT INTO " + dbName1 + "." + tableName + " partition (load_date='2016-03-02') VALUES (5, 5)")
          .run("INSERT INTO " + dbName2 + "." + tableName + " partition (load_date='2016-03-01') VALUES (1, 1)")
          .run("INSERT INTO " + dbName2 + "." + tableName + " partition (load_date='2016-03-03') VALUES (4, 4)")
          .run("select key from " + dbName2 + "." + tableName + " order by key")
          .verifyResults(resultArray)
          .run("select key from " + dbName1 + "." + tableName + " order by key")
          .verifyResults(resultArray)
          .run(txnStrCommit);

    incrementalDump = primary.dump("`*`", bootStrapDump.lastReplicationId);

    // Due to the limitation that we can only have one instance of Persistence Manager Factory in a JVM
    // we are not able to create multiple embedded derby instances for two different MetaStore instances.
    primary.run("drop database " + primaryDbName + " cascade");
    primary.run("drop database " + dbName1 + " cascade");
    primary.run("drop database " + dbName2 + " cascade");
    //End of additional steps

    replica.loadWithoutExplain("", bootStrapDump.dumpLocation)
            .run("REPL STATUS default")
            .verifyResult(bootStrapDump.lastReplicationId);

    replica.loadWithoutExplain("", incrementalDump.dumpLocation)
          .run("REPL STATUS " + dbName1)
          .run("select key from " + dbName1 + "." + tableName + " order by key")
          .verifyResults(resultArray)
          .run("select key from " + dbName2 + "." + tableName + " order by key")
          .verifyResults(resultArray);

    replica.run("drop database " + primaryDbName + " cascade");
    replica.run("drop database " + dbName1 + " cascade");
    replica.run("drop database " + dbName2 + " cascade");
  }
}
