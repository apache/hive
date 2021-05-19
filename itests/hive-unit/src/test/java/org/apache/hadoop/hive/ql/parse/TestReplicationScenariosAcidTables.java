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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.CallerArguments;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.BehaviourInjection;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.exec.repl.ReplAck;
import org.apache.hadoop.hive.ql.exec.repl.ReplDumpWork;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.Utils;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.BeforeClass;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


import static org.apache.hadoop.hive.metastore.ReplChangeManager.SOURCE_OF_REPLICATION;
import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.DUMP_ACKNOWLEDGEMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
/**
 * TestReplicationScenariosAcidTables - test replication for ACID tables.
 */
public class TestReplicationScenariosAcidTables extends BaseReplicationScenariosAcidTables {

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());

    internalBeforeClassSetup(overrides, TestReplicationScenariosAcidTables.class);
  }

  static void internalBeforeClassSetup(Map<String, String> overrides,
      Class clazz) throws Exception {

    conf = new HiveConf(clazz);
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("metastore.warehouse.tenant.colocation", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    MiniDFSCluster miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    Map<String, String> acidEnableConf = new HashMap<String, String>() {{
        put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
        put("hive.support.concurrency", "true");
        put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        put("hive.metastore.client.capability.check", "false");
        put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
        put("hive.strict.checks.bucketing", "false");
        put("hive.mapred.mode", "nonstrict");
        put("mapred.input.dir.recursive", "true");
        put("hive.metastore.disallow.incompatible.col.type.changes", "false");
        put("metastore.warehouse.tenant.colocation", "true");
        put("hive.in.repl.test", "true");
        put(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname, "false");
        put(HiveConf.ConfVars.REPL_RETAIN_CUSTOM_LOCATIONS_FOR_DB_ON_TARGET.varname, "false");
      }};

    acidEnableConf.putAll(overrides);

    primary = new WarehouseInstance(LOG, miniDFSCluster, acidEnableConf);
    acidEnableConf.put(MetastoreConf.ConfVars.REPLDIR.getHiveName(), primary.repldDir);
    replica = new WarehouseInstance(LOG, miniDFSCluster, acidEnableConf);
    Map<String, String> overridesForHiveConf1 = new HashMap<String, String>() {{
          put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
          put("hive.support.concurrency", "false");
          put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
          put("hive.metastore.client.capability.check", "false");
      }};
    overridesForHiveConf1.put(MetastoreConf.ConfVars.REPLDIR.getHiveName(), primary.repldDir);
    replicaNonAcid = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConf1);
  }

  @Before
  public void setup() throws Throwable {
    super.setup();
  }

  @After
  public void tearDown() throws Throwable {
    primary.run("drop database if exists " + primaryDbName + " cascade");
    replica.run("drop database if exists " + replicatedDbName + " cascade");
    replicaNonAcid.run("drop database if exists " + replicatedDbName + " cascade");
    primary.run("drop database if exists " + primaryDbName + "_extra cascade");
  }

  @Test
  public void testReplOperationsNotCapturedInNotificationLog() throws Throwable {
    //Perform empty bootstrap dump and load
    primary.dump(primaryDbName);
    replica.run("REPL LOAD " + primaryDbName + " INTO " + replicatedDbName);
    //Perform empty incremental dump and load so that all db level properties are altered.
    primary.dump(primaryDbName);
    replica.run("REPL LOAD " + primaryDbName + " INTO " + replicatedDbName);

    long lastEventId = primary.getCurrentNotificationEventId().getEventId();
    WarehouseInstance.Tuple incDump = primary.dump(primaryDbName);
    assert primary.getNoOfEventsDumped(incDump.dumpLocation, conf) == 0;
    long currentEventId = primary.getCurrentNotificationEventId().getEventId();
    assert lastEventId == currentEventId;
    lastEventId = replica.getCurrentNotificationEventId().getEventId();
    replica.run("REPL LOAD " + primaryDbName + " INTO " + replicatedDbName);
    currentEventId = replica.getCurrentNotificationEventId().getEventId();
    //This iteration of repl load will have only one event i.e ALTER_DATABASE to update repl.last.id for the target db.
    assert currentEventId == lastEventId + 1;

    primary.run("ALTER DATABASE " + primaryDbName +
            " SET DBPROPERTIES('" + ReplConst.TARGET_OF_REPLICATION + "'='true')");
    lastEventId = primary.getCurrentNotificationEventId().getEventId();
    primary.dumpFailure(primaryDbName);
    currentEventId = primary.getCurrentNotificationEventId().getEventId();
    assert lastEventId == currentEventId;

    primary.run("ALTER DATABASE " + primaryDbName +
            " SET DBPROPERTIES('" + ReplConst.TARGET_OF_REPLICATION + "'='')");
    primary.dump(primaryDbName);
    replica.run("DROP DATABASE " + replicatedDbName);
    lastEventId = replica.getCurrentNotificationEventId().getEventId();
    replica.loadFailure(replicatedDbName, primaryDbName);
    currentEventId = replica.getCurrentNotificationEventId().getEventId();
    assert lastEventId == currentEventId;
  }

  @Test
  public void testAcidTablesBootstrap() throws Throwable {
    // Bootstrap
    WarehouseInstance.Tuple bootstrapDump = prepareDataAndDump(primaryDbName, null);
    replica.load(replicatedDbName, primaryDbName);
    verifyLoadExecution(replicatedDbName, bootstrapDump.lastReplicationId, true);

    // First incremental, after bootstrap
    prepareIncNonAcidData(primaryDbName);
    prepareIncAcidData(primaryDbName);
    LOG.info(testName.getMethodName() + ": first incremental dump and load.");
    WarehouseInstance.Tuple incDump = primary.run("use " + primaryDbName)
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);
    verifyIncLoad(replicatedDbName, incDump.lastReplicationId);

    // Second incremental, after bootstrap
    prepareInc2NonAcidData(primaryDbName, primary.hiveConf);
    prepareInc2AcidData(primaryDbName, primary.hiveConf);
    LOG.info(testName.getMethodName() + ": second incremental dump and load.");
    WarehouseInstance.Tuple inc2Dump = primary.run("use " + primaryDbName)
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);
    verifyInc2Load(replicatedDbName, inc2Dump.lastReplicationId);
  }

  @Test
  public void testNotificationFromLoadMetadataAck() throws Throwable {
    long previousLoadNotificationID = 0;
    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
            .verifyResults(new String[] {});
    long currentLoadNotificationID = fetchNotificationIDFromDump(new Path(bootstrapDump.dumpLocation));
    long currentNotificationID = replica.getCurrentNotificationEventId().getEventId();
    assertTrue(currentLoadNotificationID > previousLoadNotificationID);
    assertTrue(currentNotificationID == currentLoadNotificationID);
    previousLoadNotificationID = currentLoadNotificationID;
    WarehouseInstance.Tuple incrementalDump1 = primary.run("insert into t1 values (1)")
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
            .verifyResults(new String[] {});
    currentLoadNotificationID = fetchNotificationIDFromDump(new Path(incrementalDump1.dumpLocation));
    currentNotificationID = replica.getCurrentNotificationEventId().getEventId();
    assertTrue(currentLoadNotificationID > previousLoadNotificationID);
    assertTrue(currentNotificationID == currentLoadNotificationID);
  }

  private long fetchNotificationIDFromDump(Path dumpLocation) throws Exception {
    Path hiveDumpDir = new Path(dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR.toString());
    Path loadMetadataFilePath = new Path(hiveDumpDir, ReplAck.LOAD_METADATA.toString());
    FileSystem fs = dumpLocation.getFileSystem(conf);
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(loadMetadataFilePath)));
    String line = reader.readLine();
    assertTrue(line != null && reader.readLine() == null);
    if (reader != null) {
      reader.close();
    }
    return Long.parseLong(line);
  }

  @Test
  /**
   * Testcase for getting immutable dataset dump, and its corresponding repl load.
   */
  public void testAcidTablesBootstrapWithMetadataAlone() throws Throwable {
    List<String> withClauseOptions = new LinkedList<>();
    withClauseOptions.add("'hive.repl.dump.skip.immutable.data.copy'='true'");

    WarehouseInstance.Tuple tuple = prepareDataAndDump(primaryDbName, withClauseOptions);
    replica.load(replicatedDbName, primaryDbName, withClauseOptions);
    verifyAcidTableLoadWithoutData(replicatedDbName);

    Path hiveDumpDir = new Path(tuple.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path loadAck = new Path(hiveDumpDir, ReplAck.LOAD_ACKNOWLEDGEMENT.toString());
    FileSystem fs = FileSystem.get(hiveDumpDir.toUri(), primary.hiveConf);
    assertFalse("For immutable dataset, load ack should not be there", fs.exists(loadAck));
  }

  private void verifyAcidTableLoadWithoutData(String replicatedDbName) throws Throwable {
    replica.run("use " + replicatedDbName)
        // no data should be there.
        .run("select id from t1 order by id")
        .verifyResults(new String[] {})
        // all 4 partitions should be there
        .run("show partitions t2")
        .verifyResults(new String[] {"country=france", "country=india", "country=italy", "country=us"})
        // no data should be there.
        .run("select place from t2")
        .verifyResults(new String[] {});
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
    tables.put("t1", numTxns + 1L);
    tables.put("t2", numTxns + 2L);
    List<Long> lockIds = allocateWriteIdsForTablesAndAquireLocks(primaryDbName, tables, txnHandler, txns, primaryConf);

    // Bootstrap dump with open txn timeout as 1s.
    List<String> withConfigs = Arrays.asList(
      "'"+ HiveConf.ConfVars.REPL_BOOTSTRAP_DUMP_OPEN_TXN_TIMEOUT+"'='1s'");
    WarehouseInstance.Tuple bootstrapDump = primary
            .run("use " + primaryDbName)
            .dump(primaryDbName, withConfigs);

    // After bootstrap dump, all the opened txns should be aborted. Verify it.
    verifyAllOpenTxnsAborted(txns, primaryConf);
    //Release the locks
    releaseLocks(txnHandler, lockIds);
    verifyNextId(tables, primaryDbName, primaryConf);

    // Bootstrap load which should also replicate the aborted write ids on both tables.
    HiveConf replicaConf = replica.getConf();
    replica.load(replicatedDbName, primaryDbName)
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
  public void testAcidTablesCreateTableIncremental() throws Throwable {
    // Create 2 tables, one partitioned and other not.
    primary.run("use " + primaryDbName)
      .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
        "tblproperties (\"transactional\"=\"true\")")
      .run("insert into t1 values(1)")
      .run("create table t2 (rank int) partitioned by (name string) tblproperties(\"transactional\"=\"true\", " +
        "\"transactional_properties\"=\"insert_only\")")
      .run("insert into t2 partition(name='Bob') values(11)")
      .run("insert into t2 partition(name='Carl') values(10)");

    WarehouseInstance.Tuple bootstrapDump = primary
      .run("use " + primaryDbName)
      .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
      .run("use " + replicatedDbName)
      .run("show tables")
      .verifyResults(new String[] {"t1", "t2"})
      .run("repl status " + replicatedDbName)
      .verifyResult(bootstrapDump.lastReplicationId)
      .run("select id from t1")
      .verifyResults(new String[]{"1"})
      .run("select rank from t2 order by rank")
      .verifyResults(new String[] {"10", "11"});

    WarehouseInstance.Tuple incrDump = primary.run("use "+ primaryDbName)
      .run("create table t3 (id int)")
      .run("insert into t3 values (99)")
      .run("create table t4 (standard int) partitioned by (name string) stored as orc " +
        "tblproperties (\"transactional\"=\"true\")")
      .run("insert into t4 partition(name='Tom') values(11)")
      .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
      .run("use " + replicatedDbName)
      .run("show tables")
      .verifyResults(new String[] {"t1", "t2", "t3", "t4"})
      .run("repl status " + replicatedDbName)
      .verifyResult(incrDump.lastReplicationId)
      .run("select id from t1")
      .verifyResults(new String[]{"1"})
      .run("select rank from t2 order by rank")
      .verifyResults(new String[] {"10", "11"})
      .run("select id from t3")
      .verifyResults(new String[]{"99"})
      .run("select standard from t4 order by standard")
      .verifyResults(new String[] {"11"});
  }


  @Test
  public void testAcidTablesBootstrapWithOpenTxnsDiffDb() throws Throwable {
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

    // Allocate write ids for both tables of secondary db for all txns
    // t1=5 and t2=5
    Map<String, Long> tablesInSecDb = new HashMap<>();
    tablesInSecDb.put("t1", (long) numTxns);
    tablesInSecDb.put("t2", (long) numTxns);
    List<Long> lockIds = allocateWriteIdsForTablesAndAquireLocks(primaryDbName + "_extra",
      tablesInSecDb, txnHandler, txns, primaryConf);

    // Bootstrap dump with open txn timeout as 300s.
    //Since transactions belong to different db it won't wait.
    List<String> withConfigs = Arrays.asList(
      "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_DUMP_OPEN_TXN_TIMEOUT + "'='300s'");
    long timeStarted = System.currentTimeMillis();
    WarehouseInstance.Tuple bootstrapDump = null;
    try {
      bootstrapDump = primary
        .run("use " + primaryDbName)
        .dump(primaryDbName, withConfigs);
    } finally {
      //Dump shouldn't wait for 300s. It should check in the 30 secs itself that those txns belong to different db
      Assert.assertTrue(System.currentTimeMillis() - timeStarted < 300000);
    }

    // After bootstrap dump, all the opened txns should not be aborted as itr belongs to a diff db. Verify it.
    verifyAllOpenTxnsNotAborted(txns, primaryConf);
    Map<String, Long> tablesInPrimary = new HashMap<>();
    tablesInPrimary.put("t1", 1L);
    tablesInPrimary.put("t2", 2L);
    verifyNextId(tablesInPrimary, primaryDbName, primaryConf);

    // Bootstrap load which should not replicate the write ids on both tables as they are on different db.
    HiveConf replicaConf = replica.getConf();
    replica.load(replicatedDbName, primaryDbName)
      .run("use " + replicatedDbName)
      .run("show tables")
      .verifyResults(new String[]{"t1", "t2"})
      .run("repl status " + replicatedDbName)
      .verifyResult(bootstrapDump.lastReplicationId)
      .run("select id from t1")
      .verifyResults(new String[]{"1"})
      .run("select rank from t2 order by rank")
      .verifyResults(new String[]{"10", "11"});

    // Verify if HWM is properly set after REPL LOAD
    verifyNextId(tablesInPrimary, replicatedDbName, replicaConf);

    // Verify if none of the write ids are not replicated to the replicated DB as they belong to diff db
    for (Map.Entry<String, Long> entry : tablesInPrimary.entrySet()) {
      entry.setValue((long) 0);
    }
    verifyWriteIdsForTables(tablesInPrimary, replicaConf, replicatedDbName);
    //Abort the txns
    txnHandler.abortTxns(new AbortTxnsRequest(txns));
    verifyAllOpenTxnsAborted(txns, primaryConf);
    //Release the locks
    releaseLocks(txnHandler, lockIds);
  }

  @Test
  public void testAcidTablesBootstrapWithOpenTxnsWaitingForLock() throws Throwable {
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

    // Bootstrap dump with open txn timeout as 80s. Dump should fail as there will be open txns and
    // lock is not acquired by them and we have set abort to false
    List<String> withConfigs = Arrays.asList(
      "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_DUMP_OPEN_TXN_TIMEOUT + "'='80s'",
      "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_DUMP_ABORT_WRITE_TXN_AFTER_TIMEOUT + "'='false'");
    try {
      primary
        .run("use " + primaryDbName)
        .dump(primaryDbName, withConfigs);
      fail();
    } catch (Exception e) {
      Assert.assertEquals(IllegalStateException.class, e.getClass());
      Assert.assertEquals("REPL DUMP cannot proceed. Force abort all the open txns is disabled. "
        + "Enable hive.repl.bootstrap.dump.abort.write.txn.after.timeout to proceed.", e.getMessage());
    }

    withConfigs = Arrays.asList(
      "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_DUMP_OPEN_TXN_TIMEOUT + "'='1s'",
      "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_DUMP_ABORT_WRITE_TXN_AFTER_TIMEOUT + "'='true'");
    // Acquire locks
    // Allocate write ids for both tables of secondary db for all txns
    // t1=5 and t2=5
    Map<String, Long> tablesInSecDb = new HashMap<>();
    tablesInSecDb.put("t1", (long) numTxns);
    tablesInSecDb.put("t2", (long) numTxns);
    List<Long> lockIds = allocateWriteIdsForTablesAndAquireLocks(primaryDbName + "_extra",
      tablesInSecDb, txnHandler, txns, primaryConf);

    WarehouseInstance.Tuple bootstrapDump  = primary
      .run("use " + primaryDbName)
      .dump(primaryDbName, withConfigs);
    // After bootstrap dump, all the opened txns should not be aborted as itr belongs to a diff db. Verify it.
    verifyAllOpenTxnsNotAborted(txns, primaryConf);
    Map<String, Long> tablesInPrimary = new HashMap<>();
    tablesInPrimary.put("t1", 1L);
    tablesInPrimary.put("t2", 2L);
    verifyNextId(tablesInPrimary, primaryDbName, primaryConf);

    // Bootstrap load which should not replicate the write ids on both tables as they are on different db.
    HiveConf replicaConf = replica.getConf();
    replica.load(replicatedDbName, primaryDbName)
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
    verifyNextId(tablesInPrimary, replicatedDbName, replicaConf);

    // Verify if none of the write ids are not replicated to the replicated DB as they belong to diff db
    for(Map.Entry<String, Long> entry : tablesInPrimary.entrySet()) {
      entry.setValue((long) 0);
    }
    verifyWriteIdsForTables(tablesInPrimary, replicaConf, replicatedDbName);
    //Abort the txns
    txnHandler.abortTxns(new AbortTxnsRequest(txns));
    verifyAllOpenTxnsAborted(txns, primaryConf);
    //Release the locks
    releaseLocks(txnHandler, lockIds);
  }

  @Test
  public void testAcidTablesBootstrapWithOpenTxnsPrimaryAndSecondaryDb() throws Throwable {
    int numTxns = 5;
    HiveConf primaryConf = primary.getConf();
    TxnStore txnHandler = TxnUtils.getTxnStore(primary.getConf());
    // Open 5 txns for secondary db
    List<Long> txns = openTxns(numTxns, txnHandler, primaryConf);
    // Open 5 txns for primary db
    List<Long> txnsSameDb = openTxns(numTxns, txnHandler, primaryConf);

    // Create 2 tables, one partitioned and other not. Also, have both types of full ACID and MM tables.
    primary.run("use " + primaryDbName)
      .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
        "tblproperties (\"transactional\"=\"true\")")
      .run("insert into t1 values(1)")
      .run("create table t2 (rank int) partitioned by (name string) tblproperties(\"transactional\"=\"true\", " +
        "\"transactional_properties\"=\"insert_only\")")
      .run("insert into t2 partition(name='Bob') values(11)")
      .run("insert into t2 partition(name='Carl') values(10)");

    // Allocate write ids for both tables of secondary db for all txns
    // t1=5 and t2=5
    Map<String, Long> tablesInSecDb = new HashMap<>();
    tablesInSecDb.put("t1", (long) numTxns);
    tablesInSecDb.put("t2", (long) numTxns);
    List<Long> lockIds = allocateWriteIdsForTablesAndAquireLocks(primaryDbName + "_extra",
      tablesInSecDb, txnHandler, txns, primaryConf);
    // Allocate write ids for both tables of primary db for all txns
    // t1=5+1L and t2=5+2L inserts
    Map<String, Long> tablesInPrimDb = new HashMap<>();
    tablesInPrimDb.put("t1", (long) numTxns + 1L);
    tablesInPrimDb.put("t2", (long) numTxns + 2L);
    lockIds.addAll(allocateWriteIdsForTablesAndAquireLocks(primaryDbName,
      tablesInPrimDb, txnHandler, txnsSameDb, primaryConf));

    // Bootstrap dump with open txn timeout as 1s.
    List<String> withConfigs = Arrays.asList(
      "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_DUMP_OPEN_TXN_TIMEOUT + "'='1s'");
    WarehouseInstance.Tuple bootstrapDump = primary
      .run("use " + primaryDbName)
      .dump(primaryDbName, withConfigs);

    // After bootstrap dump, all the opened txns should not be aborted as it belongs to a diff db. Verify it.
    verifyAllOpenTxnsNotAborted(txns, primaryConf);
    // After bootstrap dump, all the opened txns should be aborted as it belongs to db under replication. Verify it.
    verifyAllOpenTxnsAborted(txnsSameDb, primaryConf);
    verifyNextId(tablesInPrimDb, primaryDbName, primaryConf);

    // Bootstrap load which should replicate the write ids on both tables as they are on same db and
    // not on different db.
    HiveConf replicaConf = replica.getConf();
    replica.load(replicatedDbName, primaryDbName)
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
    verifyNextId(tablesInPrimDb, replicatedDbName, replicaConf);

    // Verify if only the write ids belonging to primary db are replicated to the replicated DB.
    for(Map.Entry<String, Long> entry : tablesInPrimDb.entrySet()) {
      entry.setValue((long) numTxns);
    }
    verifyWriteIdsForTables(tablesInPrimDb, replicaConf, replicatedDbName);
    //Abort the txns for secondary db
    txnHandler.abortTxns(new AbortTxnsRequest(txns));
    verifyAllOpenTxnsAborted(txns, primaryConf);
    //Release the locks
    releaseLocks(txnHandler, lockIds);
  }

  @Test
  public void testAcidTablesBootstrapWithOpenTxnsAbortDisabled() throws Throwable {
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
    tables.put("t1", numTxns + 1L);
    tables.put("t2", numTxns + 2L);
    List<Long> lockIds = allocateWriteIdsForTablesAndAquireLocks(primaryDbName, tables, txnHandler, txns, primaryConf);

    // Bootstrap dump with open txn timeout as 1s.
    List<String> withConfigs = Arrays.asList(
      "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_DUMP_OPEN_TXN_TIMEOUT + "'='1s'",
      "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_DUMP_ABORT_WRITE_TXN_AFTER_TIMEOUT + "'='false'");
    try {
      WarehouseInstance.Tuple bootstrapDump = primary
        .run("use " + primaryDbName)
        .dump(primaryDbName, withConfigs);
    } catch (Exception e) {
      Assert.assertEquals("REPL DUMP cannot proceed. Force abort all the open txns is disabled. Enable " +
        "hive.repl.bootstrap.dump.abort.write.txn.after.timeout to proceed.", e.getMessage());
    }

    // After bootstrap dump, all the opened txns should not be aborted as it belongs to diff db. Verify it.
    verifyAllOpenTxnsNotAborted(txns, primaryConf);
    //Abort the txns
    txnHandler.abortTxns(new AbortTxnsRequest(txns));
    verifyAllOpenTxnsAborted(txns, primaryConf);
    //Release the locks
    releaseLocks(txnHandler, lockIds);
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
              try {
                driver.run("insert into " + primaryDbName + ".t1 values(2)");
              } catch (CommandProcessorException e) {
                throw new RuntimeException(e);
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
        return true;
      }
    };

    InjectableBehaviourObjectStore.setCallerVerifier(callerInjectedBehavior);
    WarehouseInstance.Tuple bootstrapDump = null;
    try {
      bootstrapDump = primary.dump(primaryDbName);
      callerInjectedBehavior.assertInjectionsPerformed(true, true);
    } finally {
      InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour
    }

    // Bootstrap dump has taken snapshot before concurrent tread performed write. So, it won't see data "2".
    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(bootstrapDump.lastReplicationId)
            .run("select id from t1 order by id")
            .verifyResults(new String[]{"1" });

    // Incremental should include the concurrent write of data "2" from another txn.
    WarehouseInstance.Tuple incrementalDump = primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId)
            .run("select id from t1 order by id")
            .verifyResults(new String[]{"1", "2" });
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
              try {
                driver.run("insert into " + primaryDbName + ".t1 values(2)");
                driver.run("drop table " + primaryDbName + ".t1");
              } catch (CommandProcessorException e) {
                throw new RuntimeException(e);
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
        return true;
      }
    };

    InjectableBehaviourObjectStore.setCallerVerifier(callerInjectedBehavior);
    WarehouseInstance.Tuple bootstrapDump = null;
    try {
      bootstrapDump = primary.dump(primaryDbName);
      callerInjectedBehavior.assertInjectionsPerformed(true, true);
    } finally {
      InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour
    }

    // Bootstrap dump has taken latest list of tables and hence won't see table t1 as it is dropped.
    replica.load(replicatedDbName, primaryDbName)
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
            .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId)
            .run("select id from t1 order by id")
            .verifyResult("100");
  }

  @Test
  public void testOpenTxnEvent() throws Throwable {
    String tableName = testName.getMethodName();
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
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
            primary.dump(primaryDbName);

    long lastReplId = Long.parseLong(bootStrapDump.lastReplicationId);
    primary.testEventCounts(primaryDbName, lastReplId, null, null, 16);

    // Test load
    replica.load(replicatedDbName, primaryDbName)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId);

    // Test the idempotent behavior of Open and Commit Txn
    replica.load(replicatedDbName, primaryDbName)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId);
  }

  @Test
  public void testAbortTxnEvent() throws Throwable {
    String tableNameFail = testName.getMethodName() + "Fail";
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
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
            primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId);

    // Test the idempotent behavior of Abort Txn
    replica.load(replicatedDbName, primaryDbName)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId);
  }

  @Test
  public void testTxnEventNonAcid() throws Throwable {
    String tableName = testName.getMethodName();
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName);
    replicaNonAcid.load(replicatedDbName, primaryDbName)
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
            primary.dump(primaryDbName);
    replicaNonAcid.runFailure("REPL LOAD " + primaryDbName + " INTO " + replicatedDbName + "'")
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
            .dump(primaryDbName);

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
    replica.loadFailure(replicatedDbName, primaryDbName, withConfigs);
    callerVerifier.assertInjectionsPerformed(true, false);
    InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour

    replica.run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult("null")
            .run("show tables like t2")
            .verifyResults(new String[] { });

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
    replica.load(replicatedDbName, primaryDbName);
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
            .verifyResults(Arrays.asList("bob", "carl"))
            .verifyReplTargetProperty(replicatedDbName);
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

    try {
      primary.runCommand("REPL DUMP " + dbName + " with ('hive.repl.dump.include.acid.tables' = 'true')");
      assert false;
    } catch (CommandProcessorException e) {
      Assert.assertEquals(e.getResponseCode(), ErrorMsg.FILE_NOT_FOUND.getErrorCode());
    }

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

    try {
      primary.runCommand("REPL DUMP " + dbName + " with ('hive.repl.dump.include.acid.tables' = 'true')");
      assert false;
    } catch (CommandProcessorException e) {
      Assert.assertEquals(e.getResponseCode(), ErrorMsg.FILE_NOT_FOUND.getErrorCode());
    }

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

    primary.run("alter database default set dbproperties ('repl.source.for' = '1, 2, 3')");

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

    primary.dump("`*`");

    // Due to the limitation that we can only have one instance of Persistence Manager Factory in a JVM
    // we are not able to create multiple embedded derby instances for two different MetaStore instances.
    primary.run("drop database " + primaryDbName + " cascade");
    primary.run("drop database " + dbName1 + " cascade");
    primary.run("drop database " + dbName2 + " cascade");
    //End of additional steps
    try {
      replica.loadWithoutExplain("", "`*`");
    } catch (SemanticException e) {
      assertEquals("REPL LOAD * is not supported", e.getMessage());
    }
  }

  @Test
  public void testIncrementalDumpCheckpointing() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
            .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {})
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[] {});


    //Case 1: When the last dump finished all the events and
    //only  _finished_dump file at the hiveDumpRoot was about to be written when it failed.
    ReplDumpWork.testDeletePreviousDumpMetaPath(true);

    WarehouseInstance.Tuple incrementalDump1 = primary.run("use " + primaryDbName)
            .run("insert into t1 values (1)")
            .run("insert into t2 values (2)")
            .dump(primaryDbName);

    Path hiveDumpDir = new Path(incrementalDump1.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path ackFile = new Path(hiveDumpDir, ReplAck.DUMP_ACKNOWLEDGEMENT.toString());
    Path ackLastEventID = new Path(hiveDumpDir, ReplAck.EVENTS_DUMP.toString());
    FileSystem fs = FileSystem.get(hiveDumpDir.toUri(), primary.hiveConf);
    assertTrue(fs.exists(ackFile));
    assertTrue(fs.exists(ackLastEventID));

    fs.delete(ackFile, false);

    long firstIncEventID = Long.parseLong(bootstrapDump.lastReplicationId) + 1;
    long lastIncEventID = Long.parseLong(incrementalDump1.lastReplicationId);
    assertTrue(lastIncEventID > (firstIncEventID + 1));
    Map<Path, Long> pathModTimeMap = new HashMap<>();
    for (long eventId=firstIncEventID; eventId<=lastIncEventID; eventId++) {
      Path eventRoot = new Path(hiveDumpDir, String.valueOf(eventId));
      if (fs.exists(eventRoot)) {
        for (FileStatus fileStatus: fs.listStatus(eventRoot)) {
          pathModTimeMap.put(fileStatus.getPath(), fileStatus.getModificationTime());
        }
      }
    }

    ReplDumpWork.testDeletePreviousDumpMetaPath(false);
    WarehouseInstance.Tuple incrementalDump2 = primary.run("use " + primaryDbName)
            .dump(primaryDbName);
    assertEquals(incrementalDump1.dumpLocation, incrementalDump2.dumpLocation);
    assertTrue(fs.exists(ackFile));
    //check events were not rewritten.
    for(Map.Entry<Path, Long> entry :pathModTimeMap.entrySet()) {
      assertEquals((long)entry.getValue(),
              fs.getFileStatus(new Path(hiveDumpDir, entry.getKey())).getModificationTime());
    }

    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1"})
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[] {"2"});


    //Case 2: When the last dump was half way through
    ReplDumpWork.testDeletePreviousDumpMetaPath(true);

    WarehouseInstance.Tuple incrementalDump3 = primary.run("use " + primaryDbName)
            .run("insert into t1 values (3)")
            .run("insert into t2 values (4)")
            .dump(primaryDbName);

    hiveDumpDir = new Path(incrementalDump3.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    ackFile = new Path(hiveDumpDir, ReplAck.DUMP_ACKNOWLEDGEMENT.toString());
    ackLastEventID = new Path(hiveDumpDir, ReplAck.EVENTS_DUMP.toString());
    fs = FileSystem.get(hiveDumpDir.toUri(), primary.hiveConf);
    assertTrue(fs.exists(ackFile));
    assertTrue(fs.exists(ackLastEventID));

    fs.delete(ackFile, false);
    //delete last three events and test if it recovers.
    long lastEventID = Long.parseLong(incrementalDump3.lastReplicationId);
    Path lastEvtRoot = new Path(hiveDumpDir + File.separator + String.valueOf(lastEventID));
    Path secondLastEvtRoot = new Path(hiveDumpDir + File.separator + String.valueOf(lastEventID - 1));
    Path thirdLastEvtRoot = new Path(hiveDumpDir + File.separator + String.valueOf(lastEventID - 2));
    assertTrue(fs.exists(lastEvtRoot));
    assertTrue(fs.exists(secondLastEvtRoot));
    assertTrue(fs.exists(thirdLastEvtRoot));

    pathModTimeMap = new HashMap<>();
    for (long idx = Long.parseLong(incrementalDump2.lastReplicationId)+1; idx < (lastEventID - 2); idx++) {
      Path eventRoot  = new Path(hiveDumpDir, String.valueOf(idx));
      if (fs.exists(eventRoot)) {
        for (FileStatus fileStatus: fs.listStatus(eventRoot)) {
          pathModTimeMap.put(fileStatus.getPath(), fileStatus.getModificationTime());
        }
      }
    }
    long lastEvtModTimeOld = fs.getFileStatus(lastEvtRoot).getModificationTime();
    long secondLastEvtModTimeOld = fs.getFileStatus(secondLastEvtRoot).getModificationTime();
    long thirdLastEvtModTimeOld = fs.getFileStatus(thirdLastEvtRoot).getModificationTime();

    fs.delete(lastEvtRoot, true);
    fs.delete(secondLastEvtRoot, true);
    fs.delete(thirdLastEvtRoot, true);
    org.apache.hadoop.hive.ql.parse.repl.dump.Utils.writeOutput(String.valueOf(lastEventID - 3), ackLastEventID,
            primary.hiveConf);
    ReplDumpWork.testDeletePreviousDumpMetaPath(false);

    WarehouseInstance.Tuple incrementalDump4 = primary.run("use " + primaryDbName)
            .dump(primaryDbName);

    assertEquals(incrementalDump3.dumpLocation, incrementalDump4.dumpLocation);

    verifyPathExist(fs, ackFile);
    verifyPathExist(fs, ackLastEventID);
    verifyPathExist(fs, lastEvtRoot);
    verifyPathExist(fs, secondLastEvtRoot);
    verifyPathExist(fs, thirdLastEvtRoot);
    assertTrue(fs.getFileStatus(lastEvtRoot).getModificationTime() > lastEvtModTimeOld);
    assertTrue(fs.getFileStatus(secondLastEvtRoot).getModificationTime() > secondLastEvtModTimeOld);
    assertTrue(fs.getFileStatus(thirdLastEvtRoot).getModificationTime() > thirdLastEvtModTimeOld);

    //Check other event dump files have not been modified.
    for (Map.Entry<Path, Long> entry:pathModTimeMap.entrySet()) {
      assertEquals((long)entry.getValue(), fs.getFileStatus(entry.getKey()).getModificationTime());
    }

    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1", "3"})
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[] {"2", "4"});
  }

  @Test
  public void testIncrementalResumeDump() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {});

    ReplDumpWork.testDeletePreviousDumpMetaPath(true);

    WarehouseInstance.Tuple incrementalDump1 = primary.run("use " + primaryDbName)
            .run("insert into t1 values (1)")
            .dump(primaryDbName);

    Path hiveDumpDir = new Path(incrementalDump1.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path ackFile = new Path(hiveDumpDir, ReplAck.DUMP_ACKNOWLEDGEMENT.toString());
    Path ackLastEventID = new Path(hiveDumpDir, ReplAck.EVENTS_DUMP.toString());
    Path dumpMetaData = new Path(hiveDumpDir, "_dumpmetadata");

    FileSystem fs = FileSystem.get(hiveDumpDir.toUri(), primary.hiveConf);
    assertTrue(fs.exists(ackFile));
    assertTrue(fs.exists(ackLastEventID));
    assertTrue(fs.exists(dumpMetaData));

    fs.delete(ackLastEventID, false);
    fs.delete(ackFile, false);
    //delete only last event root dir
    Path lastEventRoot = new Path(hiveDumpDir, String.valueOf(incrementalDump1.lastReplicationId));
    assertTrue(fs.exists(lastEventRoot));
    fs.delete(lastEventRoot, true);

    // It should create a fresh dump dir as _events_dump doesn't exist.
    WarehouseInstance.Tuple incrementalDump2 = primary.run("use " + primaryDbName)
            .dump(primaryDbName);
    assertTrue(incrementalDump1.dumpLocation != incrementalDump2.dumpLocation);
    assertTrue(incrementalDump1.lastReplicationId != incrementalDump2.lastReplicationId);
    assertTrue(fs.getFileStatus(new Path(incrementalDump2.dumpLocation)).getModificationTime()
            > fs.getFileStatus(new Path(incrementalDump1.dumpLocation)).getModificationTime());

    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1"});
  }

  @Test
  public void testIncrementalResumeDumpFromInvalidEventDumpFile() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {});

    ReplDumpWork.testDeletePreviousDumpMetaPath(true);

    WarehouseInstance.Tuple incrementalDump1 = primary.run("use " + primaryDbName)
            .run("insert into t1 values (1)")
            .dump(primaryDbName);

    Path hiveDumpDir = new Path(incrementalDump1.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path ackFile = new Path(hiveDumpDir, ReplAck.DUMP_ACKNOWLEDGEMENT.toString());
    Path ackLastEventID = new Path(hiveDumpDir, ReplAck.EVENTS_DUMP.toString());
    Path dumpMetaData = new Path(hiveDumpDir, "_dumpmetadata");

    FileSystem fs = FileSystem.get(hiveDumpDir.toUri(), primary.hiveConf);
    assertTrue(fs.exists(ackFile));
    assertTrue(fs.exists(ackLastEventID));
    assertTrue(fs.exists(dumpMetaData));

    fs.delete(ackFile, false);
    fs.delete(ackLastEventID, false);
    //Case 1: File exists and it has some invalid content
    FSDataOutputStream os = fs.create(ackLastEventID);
    os.write("InvalidContent".getBytes());
    os.close();
    assertEquals("InvalidContent".length(), fs.getFileStatus(ackLastEventID).getLen());
    //delete only last event root dir
    Path lastEventRoot = new Path(hiveDumpDir, String.valueOf(incrementalDump1.lastReplicationId));
    assertTrue(fs.exists(lastEventRoot));
    fs.delete(lastEventRoot, true);

    // It should create a fresh dump dir as _events_dump has some invalid content.
    WarehouseInstance.Tuple incrementalDump2 = primary.run("use " + primaryDbName)
            .dump(primaryDbName);
    assertTrue(incrementalDump1.dumpLocation != incrementalDump2.dumpLocation);
    assertTrue(fs.getFileStatus(new Path(incrementalDump2.dumpLocation)).getModificationTime()
            > fs.getFileStatus(new Path(incrementalDump1.dumpLocation)).getModificationTime());

    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1"});

    ReplDumpWork.testDeletePreviousDumpMetaPath(true);
    //Case 2: File exists and it has no content
    WarehouseInstance.Tuple incrementalDump3 = primary.run("use " + primaryDbName)
            .run("insert into t1 values (2)")
            .dump(primaryDbName);
    hiveDumpDir = new Path(incrementalDump3.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    ackFile = new Path(hiveDumpDir, ReplAck.DUMP_ACKNOWLEDGEMENT.toString());
    ackLastEventID = new Path(hiveDumpDir, ReplAck.EVENTS_DUMP.toString());
    dumpMetaData = new Path(hiveDumpDir, "_dumpmetadata");

    assertTrue(fs.exists(ackFile));
    assertTrue(fs.exists(ackLastEventID));
    assertTrue(fs.exists(dumpMetaData));

    fs.delete(ackFile, false);
    fs.delete(ackLastEventID, false);
    os = fs.create(ackLastEventID);
    os.write("".getBytes());
    os.close();
    assertEquals(0, fs.getFileStatus(ackLastEventID).getLen());
    //delete only last event root dir
    lastEventRoot = new Path(hiveDumpDir, String.valueOf(incrementalDump3.lastReplicationId));
    assertTrue(fs.exists(lastEventRoot));
    fs.delete(lastEventRoot, true);

    // It should create a fresh dump dir as _events_dump is empty.
    WarehouseInstance.Tuple incrementalDump4 = primary.run("use " + primaryDbName)
            .dump(primaryDbName);
    assertTrue(incrementalDump3.dumpLocation != incrementalDump4.dumpLocation);
    assertTrue(fs.getFileStatus(new Path(incrementalDump4.dumpLocation)).getModificationTime()
            > fs.getFileStatus(new Path(incrementalDump3.dumpLocation)).getModificationTime());

    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1", "2"});
  }

  @Test
  public void testCheckpointingOnFirstEventDump() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {});

    // Testing a scenario where first event was getting dumped and it failed.
    ReplDumpWork.testDeletePreviousDumpMetaPath(true);

    WarehouseInstance.Tuple incrementalDump1 = primary.run("use " + primaryDbName)
            .run("insert into t1 values (1)")
            .dump(primaryDbName);

    Path hiveDumpDir = new Path(incrementalDump1.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path ackFile = new Path(hiveDumpDir, ReplAck.DUMP_ACKNOWLEDGEMENT.toString());
    Path ackLastEventID = new Path(hiveDumpDir, ReplAck.EVENTS_DUMP.toString());
    Path dumpMetaData = new Path(hiveDumpDir, "_dumpmetadata");

    FileSystem fs = FileSystem.get(hiveDumpDir.toUri(), primary.hiveConf);
    assertTrue(fs.exists(ackFile));
    assertTrue(fs.exists(dumpMetaData));

    fs.delete(ackFile, false);
    fs.delete(ackLastEventID, false);
    fs.delete(dumpMetaData, false);
    //delete all the event folder except first one.
    long firstIncEventID = Long.parseLong(bootstrapDump.lastReplicationId) + 1;
    long lastIncEventID = Long.parseLong(incrementalDump1.lastReplicationId);
    assertTrue(lastIncEventID > (firstIncEventID + 1));

    for (long eventId=firstIncEventID + 1; eventId<=lastIncEventID; eventId++) {
      Path eventRoot = new Path(hiveDumpDir, String.valueOf(eventId));
      if (fs.exists(eventRoot)) {
        fs.delete(eventRoot, true);
      }
    }

    Path firstIncEventRoot =  new Path(hiveDumpDir, String.valueOf(firstIncEventID));
    long firstIncEventModTimeOld = fs.getFileStatus(firstIncEventRoot).getModificationTime();
    ReplDumpWork.testDeletePreviousDumpMetaPath(false);

    WarehouseInstance.Tuple incrementalDump2 = primary.run("use " + primaryDbName)
            .dump(primaryDbName);

    assertTrue(incrementalDump1.dumpLocation != incrementalDump2.dumpLocation);
    hiveDumpDir = new Path(incrementalDump2.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    ackFile = new Path(hiveDumpDir, ReplAck.DUMP_ACKNOWLEDGEMENT.toString());
    firstIncEventRoot =  new Path(hiveDumpDir, String.valueOf(firstIncEventID));
    assertTrue(fs.exists(ackFile));
    long firstIncEventModTimeNew =  fs.getFileStatus(firstIncEventRoot).getModificationTime();
    assertTrue(firstIncEventModTimeNew > firstIncEventModTimeOld);

    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1"});
  }

  @Test
  public void testCheckpointingIncrWithTableDrop() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .run("insert into t1 values (1)")
            .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
            .run("insert into t2 values (2)")
            .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1"})
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[] {"2"});


    ReplDumpWork.testDeletePreviousDumpMetaPath(true);

    WarehouseInstance.Tuple incrementalDump1 = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t3(a string) STORED AS TEXTFILE")
            .run("insert into t3 values (3)")
            .run("insert into t1 values (4)")
            .run("DROP TABLE t2")
            .dump(primaryDbName);

    Path hiveDumpDir = new Path(incrementalDump1.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path ackFile = new Path(hiveDumpDir, ReplAck.DUMP_ACKNOWLEDGEMENT.toString());
    Path ackLastEventID = new Path(hiveDumpDir, ReplAck.EVENTS_DUMP.toString());
    Path dumpMetaData = new Path(hiveDumpDir, "_dumpmetadata");

    FileSystem fs = FileSystem.get(hiveDumpDir.toUri(), primary.hiveConf);
    assertTrue(fs.exists(ackFile));
    assertTrue(fs.exists(dumpMetaData));

    fs.delete(ackFile, false);
    fs.delete(dumpMetaData, false);
    //delete last five events
    long fifthLastIncEventID = Long.parseLong(incrementalDump1.lastReplicationId) - 4;
    long lastIncEventID = Long.parseLong(incrementalDump1.lastReplicationId);
    assertTrue(lastIncEventID > fifthLastIncEventID);

    for (long eventId=fifthLastIncEventID + 1; eventId<=lastIncEventID; eventId++) {
      Path eventRoot = new Path(hiveDumpDir, String.valueOf(eventId));
      if (fs.exists(eventRoot)) {
        fs.delete(eventRoot, true);
      }
    }

    org.apache.hadoop.hive.ql.parse.repl.dump.Utils.writeOutput(String.valueOf(fifthLastIncEventID), ackLastEventID,
            primary.hiveConf);

    ReplDumpWork.testDeletePreviousDumpMetaPath(false);

    WarehouseInstance.Tuple incrementalDump2 = primary.run("use " + primaryDbName)
            .dump(primaryDbName);

    assertEquals(incrementalDump1.dumpLocation, incrementalDump2.dumpLocation);
    ackFile = new Path(hiveDumpDir, ReplAck.DUMP_ACKNOWLEDGEMENT.toString());
    assertTrue(fs.exists(ackFile));
    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1", "4"})
            .run("select * from " + replicatedDbName + ".t3")
            .verifyResults(new String[] {"3"})
            .runFailure("select * from " + replicatedDbName + ".t2");
  }

  @Test
  public void testCheckPointingDataDumpFailureBootstrapDuringIncremental() throws Throwable {
    List<String> dumpClause = Arrays.asList(
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
            "'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='false'",
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
            "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
                    + UserGroupInformation.getCurrentUser().getUserName() + "'");

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("create table t1(a int) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .dump(primaryDbName, dumpClause);
    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1", "2"});

    dumpClause = Arrays.asList(
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
            "'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='false'",
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
            "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
                    + UserGroupInformation.getCurrentUser().getUserName() + "'",
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES + "'='true'");

    ReplDumpWork.testDeletePreviousDumpMetaPath(true);
    WarehouseInstance.Tuple incrementalDump1 = primary.run("use " + primaryDbName)
            .run("create table t2(a int) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("insert into t2 values (3)")
            .run("insert into t2 values (4)")
            .run("insert into t2 values (5)")
            .dump(primaryDbName, dumpClause);

    Path hiveDumpDir = new Path(incrementalDump1.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path ackFile = new Path(hiveDumpDir, ReplAck.DUMP_ACKNOWLEDGEMENT.toString());
    Path ackLastEventID = new Path(hiveDumpDir, ReplAck.EVENTS_DUMP.toString());
    Path bootstrapDir = new Path(hiveDumpDir, ReplUtils.INC_BOOTSTRAP_ROOT_DIR_NAME);
    Path metaDir = new Path(bootstrapDir, EximUtil.METADATA_PATH_NAME);

    Path t2dataDir = new Path(bootstrapDir, EximUtil.DATA_PATH_NAME + File.separator
            + primaryDbName + File.separator + "t2");
    FileSystem fs = FileSystem.get(hiveDumpDir.toUri(), primary.hiveConf);

    verifyPathExist(fs, ackFile);
    verifyPathExist(fs, ackLastEventID);

    long oldMetadirModTime = fs.getFileStatus(metaDir).getModificationTime();
    long oldT2DatadirModTime = fs.getFileStatus(t2dataDir).getModificationTime();

    fs.delete(ackFile, false);

    //Do another dump and test the rewrite happended for meta and no write for data folder
    ReplDumpWork.testDeletePreviousDumpMetaPath(false);
    WarehouseInstance.Tuple incrementalDump2 = primary.run("use " + primaryDbName)
            .dump(primaryDbName, dumpClause);
    assertEquals(incrementalDump1.dumpLocation, incrementalDump2.dumpLocation);
    assertTrue(fs.exists(ackFile));
    verifyPathExist(fs, ackFile);
    verifyPathExist(fs, ackLastEventID);

    long newMetadirModTime = fs.getFileStatus(metaDir).getModificationTime();
    long newT2DatadirModTime = fs.getFileStatus(t2dataDir).getModificationTime();

    assertTrue(newMetadirModTime > oldMetadirModTime);
    assertEquals(oldT2DatadirModTime, newT2DatadirModTime);

    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[] {"3", "4", "5"});
  }

  @Test
  public void testCheckPointingBootstrapDuringIncrementalRegularCopy() throws Throwable {

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("create table t1(a int) stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1", "2"});

    List<String> dumpClause = Arrays.asList(
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES + "'='true'");

    ReplDumpWork.testDeletePreviousDumpMetaPath(true);
    WarehouseInstance.Tuple incrementalDump1 = primary.run("use " + primaryDbName)
            .run("create table t2(a int) stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("insert into t2 values (3)")
            .run("insert into t2 values (4)")
            .run("insert into t2 values (5)")
            .dump(primaryDbName, dumpClause);

    Path hiveDumpDir = new Path(incrementalDump1.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path ackFile = new Path(hiveDumpDir, ReplAck.DUMP_ACKNOWLEDGEMENT.toString());
    Path ackLastEventID = new Path(hiveDumpDir, ReplAck.EVENTS_DUMP.toString());
    FileSystem fs = FileSystem.get(hiveDumpDir.toUri(), primary.hiveConf);
    verifyPathExist(fs, ackFile);
    verifyPathExist(fs, ackLastEventID);

    Path bootstrapDir = new Path(hiveDumpDir, ReplUtils.INC_BOOTSTRAP_ROOT_DIR_NAME);
    Path metaDir = new Path(bootstrapDir, EximUtil.METADATA_PATH_NAME);
    Path metaDBPath = new Path(metaDir, primaryDbName);
    Path metaTablePath = new Path(metaDBPath, "t2");
    Path metadata = new Path(metaTablePath, EximUtil.METADATA_NAME);
    Path dataDir = new Path(bootstrapDir, EximUtil.DATA_PATH_NAME);
    Path dataDBPath = new Path(dataDir, primaryDbName);
    Path dataTablePath = new Path(dataDBPath, "t2");
    Path deltaDir = fs.listStatus(dataTablePath)[0].getPath();
    Path dataFilePath = fs.listStatus(deltaDir)[0].getPath();

    Map<Path, Long> path2modTimeMapForT2 = new HashMap<>();
    path2modTimeMapForT2.put(metaDir, fs.getFileStatus(metaDir).getModificationTime());
    path2modTimeMapForT2.put(metaDBPath, fs.getFileStatus(metaDBPath).getModificationTime());
    path2modTimeMapForT2.put(metaTablePath, fs.getFileStatus(metaTablePath).getModificationTime());
    path2modTimeMapForT2.put(metadata, fs.getFileStatus(metadata).getModificationTime());
    path2modTimeMapForT2.put(dataTablePath, fs.getFileStatus(dataTablePath).getModificationTime());
    path2modTimeMapForT2.put(deltaDir, fs.getFileStatus(deltaDir).getModificationTime());
    path2modTimeMapForT2.put(dataFilePath, fs.getFileStatus(dataFilePath).getModificationTime());

    fs.delete(ackFile, false);

    //Do another dump and test the rewrite happened for both meta and data for table t2.
    ReplDumpWork.testDeletePreviousDumpMetaPath(false);
    WarehouseInstance.Tuple incrementalDump2 = primary.run("use " + primaryDbName)
            .dump(primaryDbName, dumpClause);
    assertEquals(incrementalDump1.dumpLocation, incrementalDump2.dumpLocation);
    verifyPathExist(fs, ackFile);
    verifyPathExist(fs, ackLastEventID);
    for (Map.Entry<Path, Long> entry:path2modTimeMapForT2.entrySet()) {
      assertTrue(entry.getValue() < fs.getFileStatus(entry.getKey()).getModificationTime());
    }
    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1", "2"})
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[] {"3", "4", "5"});
  }

  @Test
  public void testHdfsMaxDirItemsLimitDuringIncremental() throws Throwable {

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("create table t1(a int) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("insert into t1 values (1)")
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1"});

    List<String> dumpClause = Arrays.asList("'" + ReplUtils.DFS_MAX_DIR_ITEMS_CONFIG + "'='"
            + (ReplUtils.RESERVED_DIR_ITEMS_COUNT + 5) +"'",
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES + "'='true'");

    WarehouseInstance.Tuple incrementalDump1 = primary.run("use " + primaryDbName)
            .run("insert into t1 values (2)")
            .run("insert into t1 values (3)")
            .run("insert into t1 values (4)")
            .run("insert into t1 values (5)")
            .run("insert into t1 values (6)")
            .run("insert into t1 values (7)")
            .run("insert into t1 values (8)")
            .run("insert into t1 values (9)")
            .run("insert into t1 values (10)")
            .run("create table t2(a int) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("insert into t2 values (100)")
            .dump(primaryDbName, dumpClause);

    int eventCount = primary.getNoOfEventsDumped(incrementalDump1.dumpLocation, conf);
    assertEquals(eventCount, 5);

    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1"})
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[] {"100"});

    dumpClause = Arrays.asList("'" + ReplUtils.DFS_MAX_DIR_ITEMS_CONFIG + "'='1000'");

    WarehouseInstance.Tuple incrementalDump2 = primary.run("use " + primaryDbName)
            .dump(primaryDbName, dumpClause);

    eventCount = primary.getNoOfEventsDumped(incrementalDump2.dumpLocation, conf);
    assertTrue(eventCount > 5 && eventCount < 1000);

    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"});
  }

  @Test
  public void testCheckPointingDataDumpFailure() throws Throwable {
    //To force distcp copy
    List<String> dumpClause = Arrays.asList(
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
            "'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='false'",
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
            "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
                    + UserGroupInformation.getCurrentUser().getUserName() + "'");

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("insert into t1 values (3)")
            .run("insert into t2 values (11)")
            .run("insert into t2 values (21)")
            .dump(primaryDbName);

    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    Path metadataPath = new Path(dumpPath, EximUtil.METADATA_PATH_NAME);
    long modifiedTimeMetadata = fs.getFileStatus(metadataPath).getModificationTime();
    Path dataPath = new Path(dumpPath, EximUtil.DATA_PATH_NAME);
    Path dbDataPath = new Path(dataPath, primaryDbName.toLowerCase());
    Path tablet1Path = new Path(dbDataPath, "t1");
    Path tablet2Path = new Path(dbDataPath, "t2");
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    //Delete dump ack and t2 data, metadata should be rewritten, data should be same for t1 but rewritten for t2
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), true);
    assertFalse(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    FileStatus[] statuses = fs.listStatus(tablet2Path);
    //Delete t2 data
    fs.delete(statuses[0].getPath(), true);
    long modifiedTimeTable1 = fs.getFileStatus(tablet1Path).getModificationTime();
    long modifiedTimeTable1CopyFile = fs.listStatus(tablet1Path)[0].getModificationTime();
    long modifiedTimeTable2 = fs.getFileStatus(tablet2Path).getModificationTime();
    //Do another dump. It should only dump table t2. Modification time of table t1 should be same while t2 is greater
    WarehouseInstance.Tuple nextDump = primary.dump(primaryDbName, dumpClause);
    assertEquals(nextDump.dumpLocation, bootstrapDump.dumpLocation);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertEquals(modifiedTimeTable1, fs.getFileStatus(tablet1Path).getModificationTime());
    assertEquals(modifiedTimeTable1CopyFile, fs.listStatus(tablet1Path)[0].getModificationTime());
    assertTrue(modifiedTimeTable2 < fs.getFileStatus(tablet2Path).getModificationTime());
    assertTrue(modifiedTimeMetadata < fs.getFileStatus(metadataPath).getModificationTime());
    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1", "2", "3"})
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[]{"11", "21"});
  }

  @Test
  public void testCheckPointingDataDumpFailureRegularCopy() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("insert into t1 values (3)")
            .run("insert into t2 values (11)")
            .run("insert into t2 values (21)")
            .dump(primaryDbName);
    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    Path metadataPath = new Path(dumpPath, EximUtil.METADATA_PATH_NAME);
    long modifiedTimeMetadata = fs.getFileStatus(metadataPath).getModificationTime();
    Path dataPath = new Path(dumpPath, EximUtil.DATA_PATH_NAME);
    Path dbPath = new Path(dataPath, primaryDbName.toLowerCase());
    Path tablet1Path = new Path(dbPath, "t1");
    Path tablet2Path = new Path(dbPath, "t2");
    //Delete dump ack and t2 data, metadata should be rewritten, data should be same for t1 but rewritten for t2
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), true);
    assertFalse(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    FileStatus[] statuses = fs.listStatus(tablet2Path);
    //Delete t2 data
    fs.delete(statuses[0].getPath(), true);
    long modifiedTimeTable1 = fs.getFileStatus(tablet1Path).getModificationTime();
    long modifiedTimeTable1CopyFile = fs.listStatus(tablet1Path)[0].getModificationTime();
    long modifiedTimeTable2 = fs.getFileStatus(tablet2Path).getModificationTime();
    //Do another dump. It should only dump table t2. Modification time of table t1 should be same while t2 is greater
    WarehouseInstance.Tuple nextDump = primary.dump(primaryDbName);
    assertEquals(nextDump.dumpLocation, bootstrapDump.dumpLocation);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    //File is copied again as we are using regular copy
    assertTrue(modifiedTimeTable1 < fs.getFileStatus(tablet1Path).getModificationTime());
    assertTrue(modifiedTimeTable1CopyFile < fs.listStatus(tablet1Path)[0].getModificationTime());
    assertTrue(modifiedTimeTable2 < fs.getFileStatus(tablet2Path).getModificationTime());
    assertTrue(modifiedTimeMetadata < fs.getFileStatus(metadataPath).getModificationTime());
    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1", "2", "3"})
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[]{"11", "21"});
  }

  @Test
  public void testCheckPointingDataDumpFailureORCTableRegularCopy() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a int) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("CREATE TABLE t2(a string) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("CREATE TABLE t3(a string) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("insert into t1 values (3)")
            .run("insert into t2 values (11)")
            .run("insert into t2 values (21)")
            .dump(primaryDbName);
    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    Path metadataPath = new Path(dumpPath, EximUtil.METADATA_PATH_NAME);
    long modifiedTimeMetadata = fs.getFileStatus(metadataPath).getModificationTime();
    Path dataPath = new Path(dumpPath, EximUtil.DATA_PATH_NAME);
    Path dbPath = new Path(dataPath, primaryDbName.toLowerCase());
    Path tablet1Path = new Path(dbPath, "t1");
    Path tablet2Path = new Path(dbPath, "t2");
    //Delete dump ack and t2 data, metadata should be rewritten,
    // data will also be rewritten for t1 and t2 as regular copy
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), true);
    assertFalse(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    FileStatus[] statuses = fs.listStatus(tablet2Path);
    //Delete t2 data
    fs.delete(statuses[0].getPath(), true);
    long modifiedTimeTable1 = fs.getFileStatus(tablet1Path).getModificationTime();
    long modifiedTimeTable1CopyFile = fs.listStatus(tablet1Path)[0].getModificationTime();
    long modifiedTimeTable2 = fs.getFileStatus(tablet2Path).getModificationTime();
    WarehouseInstance.Tuple nextDump = primary.dump(primaryDbName);
    assertEquals(nextDump.dumpLocation, bootstrapDump.dumpLocation);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    //File is copied again as we are using regular copy
    assertTrue(modifiedTimeTable1 < fs.getFileStatus(tablet1Path).getModificationTime());
    assertTrue(modifiedTimeTable1CopyFile < fs.listStatus(tablet1Path)[0].getModificationTime());
    assertTrue(modifiedTimeTable2 < fs.getFileStatus(tablet2Path).getModificationTime());
    assertTrue(modifiedTimeMetadata < fs.getFileStatus(metadataPath).getModificationTime());
    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("show tables ")
            .verifyResults(new String[]{"t1", "t2", "t3"})
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1", "2", "3"})
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[]{"11", "21"});
  }

  @Test
  public void testCheckPointingDataDumpFailureORCTableDistcpCopy() throws Throwable {
    //Distcp copy
    List<String> dumpClause = Arrays.asList(
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
            "'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='false'",
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
            "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
                    + UserGroupInformation.getCurrentUser().getUserName() + "'");
    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a int) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("CREATE TABLE t2(a string) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("insert into t1 values (3)")
            .run("insert into t2 values (11)")
            .run("insert into t2 values (21)")
            .dump(primaryDbName, dumpClause);
    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    Path metadataPath = new Path(dumpPath, EximUtil.METADATA_PATH_NAME);
    long modifiedTimeMetadata = fs.getFileStatus(metadataPath).getModificationTime();
    Path dataPath = new Path(dumpPath, EximUtil.DATA_PATH_NAME);
    Path dbPath = new Path(dataPath, primaryDbName.toLowerCase());
    Path tablet1Path = new Path(dbPath, "t1");
    Path tablet2Path = new Path(dbPath, "t2");
    //Delete dump ack and t2 data, metadata should be rewritten, data should be same for t1 but rewritten for t2
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), true);
    assertFalse(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    FileStatus[] statuses = fs.listStatus(tablet2Path);
    //Delete t2 data
    fs.delete(statuses[0].getPath(), true);
    long modifiedTimeTable1 = fs.getFileStatus(tablet1Path).getModificationTime();
    long modifiedTimeTable1CopyFile = fs.listStatus(tablet1Path)[0].getModificationTime();
    long modifiedTimeTable2 = fs.getFileStatus(tablet2Path).getModificationTime();
    //Do another dump. It should only dump table t2. Modification time of table t1 should be same while t2 is greater
    WarehouseInstance.Tuple nextDump = primary.dump(primaryDbName, dumpClause);
    assertEquals(nextDump.dumpLocation, bootstrapDump.dumpLocation);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertEquals(modifiedTimeTable1, fs.getFileStatus(tablet1Path).getModificationTime());
    assertEquals(modifiedTimeTable1CopyFile, fs.listStatus(tablet1Path)[0].getModificationTime());
    assertTrue(modifiedTimeTable2 < fs.getFileStatus(tablet2Path).getModificationTime());
    assertTrue(modifiedTimeMetadata < fs.getFileStatus(metadataPath).getModificationTime());
    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1", "2", "3"})
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[]{"11", "21"});
  }

  @Test
  public void testCheckPointingWithSourceTableDataInserted() throws Throwable {
    //To force distcp copy
    List<String> dumpClause = Arrays.asList(
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
            "'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='false'",
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
            "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
                    + UserGroupInformation.getCurrentUser().getUserName() + "'");

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("insert into t1 values (3)")
            .run("insert into t2 values (11)")
            .run("insert into t2 values (21)")
            .dump(primaryDbName);

    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    Path dataPath = new Path(dumpPath, EximUtil.DATA_PATH_NAME);
    Path dbPath = new Path(dataPath, primaryDbName.toLowerCase());
    Path tablet1Path = new Path(dbPath, "t1");
    Path tablet2Path = new Path(dbPath, "t2");
    long modifiedTimeTable2 = fs.getFileStatus(tablet2Path).getModificationTime();
    //Delete table 2 data
    FileStatus[] statuses = fs.listStatus(tablet2Path);
    //Delete t2 data
    fs.delete(statuses[0].getPath(), true);
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), true);
    long modifiedTimeTable1CopyFile = fs.listStatus(tablet1Path)[0].getModificationTime();

    //Do another dump. It should only dump table t2. Also insert new data in existing tables.
    // New data should be there in target
    primary.run("use " + primaryDbName)
            .run("insert into t2 values (13)")
            .run("insert into t2 values (24)")
            .run("insert into t1 values (4)")
            .dump(primaryDbName, dumpClause);

    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select * from t1")
            .verifyResults(new String[]{"1", "2", "3", "4"})
            .run("select * from t2")
            .verifyResults(new String[]{"11", "21", "13", "24"});
    assertEquals(modifiedTimeTable1CopyFile, fs.listStatus(tablet1Path)[0].getModificationTime());
    assertTrue(modifiedTimeTable2 < fs.getFileStatus(tablet2Path).getModificationTime());
  }

  @Test
  public void testCheckPointingWithNewTablesAdded() throws Throwable {
    //To force distcp copy
    List<String> dumpClause = Arrays.asList(
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
            "'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='false'",
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
            "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
                    + UserGroupInformation.getCurrentUser().getUserName() + "'");

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("insert into t1 values (3)")
            .run("insert into t2 values (11)")
            .run("insert into t2 values (21)")
            .dump(primaryDbName);

    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    Path dataPath = new Path(dumpPath, EximUtil.DATA_PATH_NAME);
    Path dbPath = new Path(dataPath, primaryDbName.toLowerCase());
    Path tablet1Path = new Path(dbPath, "t1");
    Path tablet2Path = new Path(dbPath, "t2");
    long modifiedTimeTable1 = fs.getFileStatus(tablet1Path).getModificationTime();
    long modifiedTimeTable2 = fs.getFileStatus(tablet2Path).getModificationTime();
    //Delete table 2 data
    FileStatus[] statuses = fs.listStatus(tablet2Path);
    fs.delete(statuses[0].getPath(), true);
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), true);
    long modifiedTimeTable1CopyFile = fs.listStatus(tablet1Path)[0].getModificationTime();

    // Do another dump. It should only dump table t2 and next table.
    // Also insert new tables. New tables will be there in target
    primary.run("use " + primaryDbName)
            .run("insert into t2 values (13)")
            .run("insert into t2 values (24)")
            .run("create table t3(a string) STORED AS TEXTFILE")
            .run("insert into t3 values (1)")
            .run("insert into t3 values (2)")
            .run("insert into t3 values (3)")
            .dump(primaryDbName, dumpClause);

    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select * from t1")
            .verifyResults(new String[]{"1", "2", "3"})
            .run("select * from t2")
            .verifyResults(new String[]{"11", "21", "13", "24"})
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2", "t3"})
            .run("select * from t3")
            .verifyResults(new String[]{"1", "2", "3"});
    assertEquals(modifiedTimeTable1, fs.getFileStatus(tablet1Path).getModificationTime());
    assertEquals(modifiedTimeTable1CopyFile, fs.listStatus(tablet1Path)[0].getModificationTime());
    assertTrue(modifiedTimeTable2 < fs.getFileStatus(tablet2Path).getModificationTime());
  }

  @Test
  public void testManagedTableLazyCopy() throws Throwable {
    List<String> withClause = Arrays.asList(
            "'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='true'");

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .run("CREATE TABLE t2(a string) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("insert into t2 values (11)")
            .run("insert into t2 values (12)")
            .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("use " + replicatedDbName)
            .run("select * from t1")
            .verifyResults(new String[]{"1", "2"})
            .run("select * from t2")
            .verifyResults(new String[]{"11", "12"})
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2"});

    primary.run("use " + primaryDbName)
            .run("insert into t1 values (3)")
            .run("insert into t2 values (13)")
            .run("create table t3(a string) STORED AS TEXTFILE")
            .run("CREATE TABLE t4(a string) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("insert into t3 values (21)")
            .run("insert into t4 values (31)")
            .run("insert into t4 values (32)")
            .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("use " + replicatedDbName)
            .run("select * from t1")
            .verifyResults(new String[]{"1", "2", "3"})
            .run("select * from t2")
            .verifyResults(new String[]{"11", "12", "13"})
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2", "t3", "t4"})
            .run("select * from t3")
            .verifyResults(new String[]{"21"})
            .run("select * from t4")
            .verifyResults(new String[]{"31","32"});
  }

  @Test
  public void testCheckPointingWithSourceTableDeleted() throws Throwable {
    //To force distcp copy
    List<String> dumpClause = Arrays.asList(
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
            "'" + HiveConf.ConfVars.HIVE_IN_TEST.varname + "'='false'",
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
            "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
                    + UserGroupInformation.getCurrentUser().getUserName() + "'");

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("insert into t1 values (3)")
            .run("insert into t2 values (11)")
            .run("insert into t2 values (21)")
            .dump(primaryDbName);

    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));


    //Delete dump ack and t2 data, Also drop table. New data will be there in target
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), true);
    assertFalse(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    Path dataPath = new Path(dumpPath, EximUtil.DATA_PATH_NAME);
    Path dbPath = new Path(dataPath, primaryDbName.toLowerCase());
    Path tablet2Path = new Path(dbPath, "t2");
    FileStatus[] statuses = fs.listStatus(tablet2Path);
    //Delete t2 data.
    fs.delete(statuses[0].getPath(), true);
    //Drop table t1. Target shouldn't have t1 table as metadata dump is rewritten
    primary.run("use " + primaryDbName)
            .run("drop table t1")
            .dump(primaryDbName, dumpClause);

    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t2"})
            .run("select * from t2")
            .verifyResults(new String[]{"11", "21"});
  }

  @Test
  public void testCheckPointingMetadataDumpFailure() throws Throwable {
    //To force distcp copy
    List<String> dumpClause = Arrays.asList(
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
            "'" + HiveConf.ConfVars.HIVE_IN_TEST.varname + "'='false'",
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
            "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
                    + UserGroupInformation.getCurrentUser().getUserName() + "'");

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("insert into t1 values (3)")
            .run("insert into t2 values (11)")
            .run("insert into t2 values (21)")
            .dump(primaryDbName);
    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));

    //Delete dump ack and metadata ack, everything should be rewritten in a new dump dir
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), true);
    fs.delete(new Path(dumpPath, "_dumpmetadata"), true);
    assertFalse(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    //Insert new data
    primary.run("insert into "+ primaryDbName +".t1 values (12)");
    primary.run("insert into "+ primaryDbName +".t1 values (13)");
    //Do another dump. It should dump everything in a new dump dir
    // checkpointing will not be used
    WarehouseInstance.Tuple nextDump = primary.dump(primaryDbName, dumpClause);
    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select * from t2")
            .verifyResults(new String[]{"11", "21"})
            .run("select * from t1")
            .verifyResults(new String[]{"1", "2", "3", "12", "13"});
    assertNotEquals(nextDump.dumpLocation, bootstrapDump.dumpLocation);
    dumpPath = new Path(nextDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
  }

  @Test
  public void testReplTargetOfReplication() throws Throwable {
    // Bootstrap
    WarehouseInstance.Tuple bootstrapDump = prepareDataAndDump(primaryDbName, null);
    replica.load(replicatedDbName, primaryDbName).verifyReplTargetProperty(replicatedDbName);
    verifyLoadExecution(replicatedDbName, bootstrapDump.lastReplicationId, true);

    //Try to do a dump on replicated db. It should fail
    replica.run("alter database " + replicatedDbName + " set dbproperties ('repl.source.for'='1')");
    try {
      replica.dump(replicatedDbName);
    } catch (Exception e) {
      Assert.assertEquals("Cannot dump database as it is a Target of replication.", e.getMessage());
      Assert.assertEquals(ErrorMsg.REPL_DATABASE_IS_TARGET_OF_REPLICATION.getErrorCode(),
        ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode());
    }
    replica.run("alter database " + replicatedDbName + " set dbproperties ('repl.source.for'='')");

    //Try to dump a different db on replica. That should succeed
    replica.run("create database " + replicatedDbName + "_extra with dbproperties ('repl.source.for' = '1, 2, 3')")
      .dump(replicatedDbName + "_extra");
    replica.run("drop database if exists " + replicatedDbName + "_extra cascade");
  }

  private void verifyPathExist(FileSystem fs, Path filePath) throws IOException {
    assertTrue("Path not found:" + filePath, fs.exists(filePath));
  }

  @Test
  public void testMaterializedViewOnAcidTableReplication() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string)" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("create materialized view mat_view as select * from t1")
            .run("show tables")
            .verifyResults(new String[]{"t1", "mat_view"})
            .run("select * from mat_view")
            .verifyResults(new String[]{"1", "2"})
            .dump(primaryDbName);

    //confirm materialized-view not replicated
    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select * from t1")
            .verifyResults(new String[]{"1", "2"})
            .run("show tables")
            .verifyResults(new String[]{"t1"});

    //test alter materialized-view rebuild
    primary.run("use " + primaryDbName)
            .run("insert into t1 values (3)")
            .run("alter materialized view mat_view rebuild")
            .run("select * from mat_view")
            .verifyResults(new String[]{"1", "2", "3"})
            .dump(primaryDbName);

    //confirm materialized-view not replicated
    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select * from t1")
            .verifyResults(new String[]{"1", "2", "3"})
            .run("show tables")
            .verifyResults(new String[]{"t1"});

    //test alter materialized-view disable rewrite
    primary.run("use " + primaryDbName)
            .run("alter materialized view mat_view disable rewrite")
            .run("select * from mat_view")
            .verifyResults(new String[]{"1", "2", "3"})
            .dump(primaryDbName);

    //confirm materialized-view not replicated
    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1"});
  }

  @Test
  public void testORCTableRegularCopyWithCopyOnTarget() throws Throwable {
    ArrayList<String> withClause = new ArrayList<>();
    withClause.add("'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='true'");
    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a int) stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("CREATE TABLE t2(a string) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("CREATE TABLE t3(a string) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("CREATE TABLE tpart1(a int) partitioned by (name string)" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("CREATE TABLE tpart2(a int) partitioned by (name string) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("CREATE TABLE text1(a string) STORED AS TEXTFILE")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (11)")
            .run("insert into t2 values (2)")
            .run("insert into t2 values (22)")
            .run("insert into t3 values (33)")
            .run("insert into tpart1 partition(name='Tom') values(100)")
            .run("insert into tpart1 partition(name='Jerry') values(101)")
            .run("insert into tpart2 partition(name='Bob') values(200)")
            .run("insert into tpart2 partition(name='Carl') values(201)")
            .run("insert into text1 values ('ricky')")
            .dump(primaryDbName, withClause);

    replica.run("DROP TABLE t3");

    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2", "t3", "tpart1", "tpart2", "text1"})
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1", "11"})
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[]{"2", "22"})
            .run("select a from " + replicatedDbName + ".tpart1")
            .verifyResults(new String[]{"100", "101"})
            .run("show partitions " + replicatedDbName + ".tpart1")
            .verifyResults(new String[]{"name=Tom", "name=Jerry"})
            .run("select a from " + replicatedDbName + ".tpart2")
            .verifyResults(new String[]{"200", "201"})
            .run("show partitions " + replicatedDbName + ".tpart2")
            .verifyResults(new String[]{"name=Bob", "name=Carl"})
            .run("select a from " + replicatedDbName + ".text1")
            .verifyResults(new String[]{"ricky"});

    WarehouseInstance.Tuple incrementalDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t4(a int) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("CREATE TABLE tpart3(a int) partitioned by (name string)" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("CREATE TABLE tpart4(a int) partitioned by (name string) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("insert into t1 values (111)")
            .run("insert into t2 values (222)")
            .run("insert into t4 values (4)")
            .run("insert into tpart1 partition(name='Tom') values(102)")
            .run("insert into tpart1 partition(name='Jerry') values(103)")
            .run("insert into tpart2 partition(name='Bob') values(202)")
            .run("insert into tpart2 partition(name='Carl') values(203)")
            .run("insert into tpart3 partition(name='Tom3') values(300)")
            .run("insert into tpart3 partition(name='Jerry3') values(301)")
            .run("insert into tpart4 partition(name='Bob4') values(400)")
            .run("insert into tpart4 partition(name='Carl4') values(401)")
            .run("insert into text1 values ('martin')")
            .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("use " + replicatedDbName)
            .run("show tables ")
            .verifyResults(new String[]{"t1", "t2", "t4", "tpart1", "tpart2", "tpart3", "tpart4", "text1"})
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1", "11", "111"})
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[]{"2", "22", "222"})
            .run("select * from " + replicatedDbName + ".t4")
            .verifyResults(new String[]{"4"})
            .run("select a from " + replicatedDbName + ".tpart1")
            .verifyResults(new String[]{"100", "101", "102", "103"})
            .run("show partitions " + replicatedDbName + ".tpart1")
            .verifyResults(new String[]{"name=Tom", "name=Jerry"})
            .run("select a from " + replicatedDbName + ".tpart2")
            .verifyResults(new String[]{"200", "201", "202", "203"})
            .run("show partitions " + replicatedDbName + ".tpart2")
            .verifyResults(new String[]{"name=Bob", "name=Carl"})
            .run("select a from " + replicatedDbName + ".tpart3")
            .verifyResults(new String[]{"300", "301"})
            .run("show partitions " + replicatedDbName + ".tpart3")
            .verifyResults(new String[]{"name=Tom3", "name=Jerry3"})
            .run("select a from " + replicatedDbName + ".tpart4")
            .verifyResults(new String[]{"400", "401"})
            .run("show partitions " + replicatedDbName + ".tpart4")
            .verifyResults(new String[]{"name=Bob4", "name=Carl4"})
            .run("select a from " + replicatedDbName + ".text1")
            .verifyResults(new String[]{"ricky", "martin"});

    incrementalDump = primary.run("use " + primaryDbName)
            .run("insert into t4 values (44)")
            .run("insert into t1 values (1111)")
            .run("DROP TABLE t1")
            .run("insert into t2 values (2222)")
            .run("insert into tpart1 partition(name='Tom') values(104)")
            .run("insert into tpart1 partition(name='Tom_del') values(1000)")
            .run("insert into tpart1 partition(name='Harry') values(10001)")
            .run("insert into tpart1 partition(name='Jerry') values(105)")
            .run("ALTER TABLE tpart1 DROP PARTITION(name='Tom')")
            .run("DROP TABLE tpart2")
            .dump(primaryDbName, withClause);

    replica.run("DROP TABLE t4")
            .run("ALTER TABLE tpart1 DROP PARTITION(name='Tom_del')");

    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("use " + replicatedDbName)
            .run("show tables ")
            .verifyResults(new String[]{"t2", "t4", "tpart1", "tpart3", "tpart4", "text1"})
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[]{"2", "22", "222", "2222"})
            .run("select a from " + replicatedDbName + ".tpart1")
            .verifyResults(new String[]{"101", "103", "105", "1000", "10001"})
            .run("show partitions " + replicatedDbName + ".tpart1")
            .verifyResults(new String[]{"name=Harry", "name=Jerry", "name=Tom_del"});
  }

  @Test
  public void testORCTableDistcpCopyWithCopyOnTarget() throws Throwable {
    //Distcp copy
    List<String> withClause = Arrays.asList(
      "'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='true'",
      "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
      "'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='false'",
      "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
      "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
        + UserGroupInformation.getCurrentUser().getUserName() + "'");
    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
      .run("CREATE TABLE t1(a int) stored as orc TBLPROPERTIES ('transactional'='true')")
      .run("CREATE TABLE t2(a string) clustered by (a) into 2 buckets" +
        " stored as orc TBLPROPERTIES ('transactional'='true')")
      .run("CREATE TABLE t3(a string) clustered by (a) into 2 buckets" +
        " stored as orc TBLPROPERTIES ('transactional'='true')")
      .run("CREATE TABLE tpart1(a int) partitioned by (name string)" +
        " stored as orc TBLPROPERTIES ('transactional'='true')")
      .run("CREATE TABLE tpart2(a int) partitioned by (name string) clustered by (a) into 2 buckets" +
        " stored as orc TBLPROPERTIES ('transactional'='true')")
      .run("CREATE TABLE text1(a string) STORED AS TEXTFILE")
      .run("insert into t1 values (1)")
      .run("insert into t1 values (11)")
      .run("insert into t2 values (2)")
      .run("insert into t2 values (22)")
      .run("insert into t3 values (33)")
      .run("insert into tpart1 partition(name='Tom') values(100)")
      .run("insert into tpart1 partition(name='Jerry') values(101)")
      .run("insert into tpart2 partition(name='Bob') values(200)")
      .run("insert into tpart2 partition(name='Carl') values(201)")
      .run("insert into text1 values ('ricky')")
      .dump(primaryDbName, withClause);

    replica.run("DROP TABLE t3");

    replica.load(replicatedDbName, primaryDbName, withClause)
      .run("use " + replicatedDbName)
      .run("show tables")
      .verifyResults(new String[]{"t1", "t2", "t3", "tpart1", "tpart2", "text1"})
      .run("select * from " + replicatedDbName + ".t1")
      .verifyResults(new String[]{"1", "11"})
      .run("select * from " + replicatedDbName + ".t2")
      .verifyResults(new String[]{"2", "22"})
      .run("select a from " + replicatedDbName + ".tpart1")
      .verifyResults(new String[]{"100", "101"})
      .run("show partitions " + replicatedDbName + ".tpart1")
      .verifyResults(new String[]{"name=Tom", "name=Jerry"})
      .run("select a from " + replicatedDbName + ".tpart2")
      .verifyResults(new String[]{"200", "201"})
      .run("show partitions " + replicatedDbName + ".tpart2")
      .verifyResults(new String[]{"name=Bob", "name=Carl"})
      .run("select a from " + replicatedDbName + ".text1")
      .verifyResults(new String[]{"ricky"});

    WarehouseInstance.Tuple incrementalDump = primary.run("use " + primaryDbName)
      .run("CREATE TABLE t4(a int) clustered by (a) into 2 buckets" +
        " stored as orc TBLPROPERTIES ('transactional'='true')")
      .run("CREATE TABLE tpart3(a int) partitioned by (name string)" +
        " stored as orc TBLPROPERTIES ('transactional'='true')")
      .run("CREATE TABLE tpart4(a int) partitioned by (name string) clustered by (a) into 2 buckets" +
        " stored as orc TBLPROPERTIES ('transactional'='true')")
      .run("insert into t1 values (111)")
      .run("insert into t2 values (222)")
      .run("insert into t4 values (4)")
      .run("insert into tpart1 partition(name='Tom') values(102)")
      .run("insert into tpart1 partition(name='Jerry') values(103)")
      .run("insert into tpart2 partition(name='Bob') values(202)")
      .run("insert into tpart2 partition(name='Carl') values(203)")
      .run("insert into tpart3 partition(name='Tom3') values(300)")
      .run("insert into tpart3 partition(name='Jerry3') values(301)")
      .run("insert into tpart4 partition(name='Bob4') values(400)")
      .run("insert into tpart4 partition(name='Carl4') values(401)")
      .run("insert into text1 values ('martin')")
      .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
      .run("use " + replicatedDbName)
      .run("show tables ")
      .verifyResults(new String[]{"t1", "t2", "t4", "tpart1", "tpart2", "tpart3", "tpart4", "text1"})
      .run("select * from " + replicatedDbName + ".t1")
      .verifyResults(new String[]{"1", "11", "111"})
      .run("select * from " + replicatedDbName + ".t2")
      .verifyResults(new String[]{"2", "22", "222"})
      .run("select * from " + replicatedDbName + ".t4")
      .verifyResults(new String[]{"4"})
      .run("select a from " + replicatedDbName + ".tpart1")
      .verifyResults(new String[]{"100", "101", "102", "103"})
      .run("show partitions " + replicatedDbName + ".tpart1")
      .verifyResults(new String[]{"name=Tom", "name=Jerry"})
      .run("select a from " + replicatedDbName + ".tpart2")
      .verifyResults(new String[]{"200", "201", "202", "203"})
      .run("show partitions " + replicatedDbName + ".tpart2")
      .verifyResults(new String[]{"name=Bob", "name=Carl"})
      .run("select a from " + replicatedDbName + ".tpart3")
      .verifyResults(new String[]{"300", "301"})
      .run("show partitions " + replicatedDbName + ".tpart3")
      .verifyResults(new String[]{"name=Tom3", "name=Jerry3"})
      .run("select a from " + replicatedDbName + ".tpart4")
      .verifyResults(new String[]{"400", "401"})
      .run("show partitions " + replicatedDbName + ".tpart4")
      .verifyResults(new String[]{"name=Bob4", "name=Carl4"})
      .run("select a from " + replicatedDbName + ".text1")
      .verifyResults(new String[]{"ricky", "martin"});

    incrementalDump = primary.run("use " + primaryDbName)
      .run("insert into t4 values (44)")
      .run("insert into t1 values (1111)")
      .run("DROP TABLE t1")
      .run("insert into t2 values (2222)")
      .run("insert into tpart1 partition(name='Tom') values(104)")
      .run("insert into tpart1 partition(name='Tom_del') values(1000)")
      .run("insert into tpart1 partition(name='Harry') values(10001)")
      .run("insert into tpart1 partition(name='Jerry') values(105)")
      .run("ALTER TABLE tpart1 DROP PARTITION(name='Tom')")
      .run("DROP TABLE tpart2")
      .dump(primaryDbName, withClause);

    replica.run("DROP TABLE t4")
      .run("ALTER TABLE tpart1 DROP PARTITION(name='Tom_del')");

    replica.load(replicatedDbName, primaryDbName, withClause)
      .run("use " + replicatedDbName)
      .run("show tables ")
      .verifyResults(new String[]{"t2", "t4", "tpart1", "tpart3", "tpart4", "text1"})
      .run("select * from " + replicatedDbName + ".t2")
      .verifyResults(new String[]{"2", "22", "222", "2222"})
      .run("select a from " + replicatedDbName + ".tpart1")
      .verifyResults(new String[]{"101", "103", "105", "1000", "10001"})
      .run("show partitions " + replicatedDbName + ".tpart1")
      .verifyResults(new String[]{"name=Harry", "name=Jerry", "name=Tom_del"});
  }

  @Test
  public void testTableWithPartitionsInBatch() throws Throwable {

    List<String> withClause = new ArrayList<>();
    withClause.add("'" + HiveConf.ConfVars.REPL_LOAD_PARTITIONS_WITH_DATA_COPY_BATCH_SIZE.varname + "'='" + 1 + "'");

    primary.run("use " + primaryDbName)
      .run("create table t2 (place string) partitioned by (country string)")
      .run("insert into t2 partition(country='india') values ('bangalore')")
      .run("insert into t2 partition(country='france') values ('paris')")
      .run("insert into t2 partition(country='australia') values ('sydney')")
      .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
      .run("use " + replicatedDbName)
      .run("show tables like 't2'")
      .verifyResults(new String[] { "t2" })
      .run("select distinct(country) from t2")
      .verifyResults(new String[] { "india", "france", "australia" })
      .run("select place from t2")
      .verifyResults(new String[] { "bangalore", "paris", "sydney" })
      .verifyReplTargetProperty(replicatedDbName);
  }
}
