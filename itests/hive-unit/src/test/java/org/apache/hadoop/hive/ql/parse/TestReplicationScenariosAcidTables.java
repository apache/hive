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
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.CompactionInfoStruct;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.CallerArguments;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.BehaviourInjection;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.exec.repl.ReplAck;
import org.apache.hadoop.hive.ql.exec.repl.ReplDumpWork;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.dump.EventsDumpMetadata;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.hive.ql.parse.repl.load.EventDumpDirComparator;
import org.apache.hadoop.hive.ql.parse.repl.load.FailoverMetaData;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.parse.repl.metric.MetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Metric;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.ReplicationMetric;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Stage;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Status;
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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;


import static org.apache.hadoop.hive.common.repl.ReplConst.REPL_RESUME_STARTED_AFTER_FAILOVER;
import static org.apache.hadoop.hive.common.repl.ReplConst.REPL_TARGET_DATABASE_PROPERTY;
import static org.apache.hadoop.hive.common.repl.ReplConst.REPL_TARGET_TABLE_PROPERTY;
import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;
import static org.apache.hadoop.hive.common.repl.ReplConst.REPL_ENABLE_BACKGROUND_THREAD;
import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.runCleaner;
import static org.apache.hadoop.hive.ql.TxnCommandsBaseForTests.runWorker;
import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.DUMP_ACKNOWLEDGEMENT;
import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.LOAD_ACKNOWLEDGEMENT;

import static org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector.isMetricsEnabledForTests;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
        new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).build();
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
        put("hive.txn.readonly.enabled", "true");
        //HIVE-25267
        put(MetastoreConf.ConfVars.TXN_OPENTXN_TIMEOUT.getVarname(), "2000");
        put(HiveConf.ConfVars.REPL_BATCH_INCREMENTAL_EVENTS.varname, "false");
        put(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname, "false");
        put(HiveConf.ConfVars.REPL_RETAIN_CUSTOM_LOCATIONS_FOR_DB_ON_TARGET.varname, "false");
      }};

    acidEnableConf.putAll(overrides);

    setReplicaExternalBase(miniDFSCluster.getFileSystem(), acidEnableConf);
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
  public void testReplAlterDbEventsNotCapturedInNotificationLog() throws Throwable {
    String srcDbName = "srcDb";
    String replicaDb = "tgtDb";
    try {
      //Perform empty bootstrap dump and load
      primary.run("CREATE DATABASE " + srcDbName);
      long lastEventId = primary.getCurrentNotificationEventId().getEventId();
      //Assert that repl.source.for is not captured in NotificationLog
      WarehouseInstance.Tuple dumpData = primary.dump(srcDbName);
      long latestEventId = primary.getCurrentNotificationEventId().getEventId();
      assertEquals(lastEventId, latestEventId);

      replica.run("REPL LOAD " + srcDbName + " INTO " + replicaDb);
      latestEventId = replica.getCurrentNotificationEventId().getEventId();
      //Assert that repl.target.id, hive.repl.ckpt.key and hive.repl.first.inc.pending is not captured in notificationLog.
      assertEquals(latestEventId, lastEventId + 1); //This load will generate only 1 event i.e. CREATE_DATABASE

      WarehouseInstance.Tuple incDump = primary.run("use " + srcDbName)
              .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                      "tblproperties (\"transactional\"=\"true\")")
              .run("insert into t1 values(1)")
              .dump(srcDbName);

      //Assert that repl.last.id is not captured in notification log.
      long noOfEventsInDumpDir = primary.getNoOfEventsDumped(incDump.dumpLocation, conf);
      lastEventId = primary.getCurrentNotificationEventId().getEventId();
      replica.run("REPL LOAD " + srcDbName + " INTO " + replicaDb);

      latestEventId = replica.getCurrentNotificationEventId().getEventId();

      //Validate that there is no addition event generated in notificationLog table apart from replayed ones.
      assertEquals(latestEventId, lastEventId + noOfEventsInDumpDir);

      long targetDbReplId = Long.parseLong(replica.getDatabase(replicaDb)
              .getParameters().get(ReplConst.REPL_TARGET_TABLE_PROPERTY));
      //Validate that repl.last.id db property has been updated successfully.
      assertEquals(targetDbReplId, lastEventId);
    } finally {
      primary.run("DROP DATABASE IF EXISTS " + srcDbName + " CASCADE");
      replica.run("DROP DATABASE IF EXISTS " + replicaDb + " CASCADE");
    }
  }

  @Test
  public void testTargetDbReplIncompatibleWithNoPropSet() throws Throwable {
    testTargetDbReplIncompatible(false);
  }

  @Test
  public void testTargetDbReplIncompatibleWithPropSet() throws Throwable {
    testTargetDbReplIncompatible(true);
  }

  private void testTargetDbReplIncompatible(boolean setReplIncompProp) throws Throwable {
    HiveConf primaryConf = primary.getConf();
    TxnStore txnHandler = TxnUtils.getTxnStore(primary.getConf());

    primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);

    if (setReplIncompProp) {
      replica.run("ALTER DATABASE " + replicatedDbName +
              " SET DBPROPERTIES('" + ReplConst.REPL_INCOMPATIBLE + "'='false')");
      assert "false".equals(replica.getDatabase(replicatedDbName).getParameters().get(ReplConst.REPL_INCOMPATIBLE));
    }

    assertFalse(MetaStoreUtils.isDbReplIncompatible(replica.getDatabase(replicatedDbName)));

    Long sourceTxnId = openTxns(1, txnHandler, primaryConf).get(0);
    txnHandler.abortTxn(new AbortTxnRequest(sourceTxnId));

    try {
      sourceTxnId = openTxns(1, txnHandler, primaryConf).get(0);

      primary.dump(primaryDbName);
      replica.load(replicatedDbName, primaryDbName);
      assertFalse(MetaStoreUtils.isDbReplIncompatible(replica.getDatabase(replicatedDbName)));

      Long targetTxnId = txnHandler.getTargetTxnId(HiveUtils.getReplPolicy(replicatedDbName), sourceTxnId);
      txnHandler.abortTxn(new AbortTxnRequest(targetTxnId));
      assertTrue(MetaStoreUtils.isDbReplIncompatible(replica.getDatabase(replicatedDbName)));

      WarehouseInstance.Tuple dumpData = primary.dump(primaryDbName);

      assertFalse(ReplUtils.failedWithNonRecoverableError(new Path(dumpData.dumpLocation), conf));
      replica.loadFailure(replicatedDbName, primaryDbName, null, ErrorMsg.REPL_INCOMPATIBLE_EXCEPTION.getErrorCode());
      assertTrue(ReplUtils.failedWithNonRecoverableError(new Path(dumpData.dumpLocation), conf));

      primary.dumpFailure(primaryDbName);
      assertTrue(ReplUtils.failedWithNonRecoverableError(new Path(dumpData.dumpLocation), conf));
    } finally {
      txnHandler.abortTxn(new AbortTxnRequest(sourceTxnId));
    }
  }

  @Test
  public void testCompleteFailoverWithReverseBootstrap() throws Throwable {
    HiveConf primaryConf = primary.getConf();
    TxnStore txnHandler = TxnUtils.getTxnStore(primary.getConf());
    List<String> failoverConfigs = Arrays.asList("'" + HiveConf.ConfVars.HIVE_REPL_FAILOVER_START + "'='true'");
    WarehouseInstance.Tuple dumpData = primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("create table t2 (rank int) partitioned by (name string) tblproperties(\"transactional\"=\"true\", " +
                    "\"transactional_properties\"=\"insert_only\")")
            .dump(primaryDbName, failoverConfigs);

    //This dump is not failover ready as target db can be used for replication only after first incremental load.
    FileSystem fs = new Path(dumpData.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(primary.getDatabase(primaryDbName)));

    replica.load(replicatedDbName, primaryDbName, failoverConfigs)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2"})
            .run("repl status " + replicatedDbName)
            .verifyResult(dumpData.lastReplicationId);

    Database db = replica.getDatabase(replicatedDbName);
    assertTrue(MetaStoreUtils.isTargetOfReplication(db));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(db));

    primary.run("use " + primaryDbName)
            .run("insert into t1 values(1)")
            .run("insert into t2 partition(name='Bob') values(11)")
            .run("insert into t2 partition(name='Carl') values(10)");

    /**Open transactions can be of two types:
     Case 1) Txns that have not acquired HIVE LOCKS or they belong to different db: These txns would be captured in
     _failovermetadata file inside dump directory.
     Case 2) Txns that have acquired HIVE LOCKS and belong to db under replication: These txns would be aborted by hive
     as part of dump operation.
     */
    // Open 3 txns for Database which is not under replication
    int numTxnsForSecDb = 3;
    List<Long> txnsForSecDb = openTxns(numTxnsForSecDb, txnHandler, primaryConf);

    // Allocate write ids for both tables of secondary db for 3 txns
    // t1=5 and t2=5
    Map<String, Long> tablesInSecDb = new HashMap<>();
    tablesInSecDb.put("t1", (long) numTxnsForSecDb);
    tablesInSecDb.put("t2", (long) numTxnsForSecDb);
    List<Long> lockIdsForSecDb = allocateWriteIdsForTablesAndAcquireLocks(primaryDbName + "_extra",
            tablesInSecDb, txnHandler, txnsForSecDb, primaryConf);

    //Open 2 txns for Primary Db
    int numTxnsForPrimaryDb = 2;
    List<Long> txnsForPrimaryDb = openTxns(numTxnsForPrimaryDb, txnHandler, primaryConf);

    // Allocate write ids for both tables of primary db for 2 txns
    // t1=5 and t2=5
    Map<String, Long> tablesInPrimaryDb = new HashMap<>();
    tablesInPrimaryDb.put("t1", (long) numTxnsForPrimaryDb + 1);
    tablesInPrimaryDb.put("t2", (long) numTxnsForPrimaryDb + 2);
    List<Long> lockIdsForPrimaryDb = allocateWriteIdsForTablesAndAcquireLocks(primaryDbName,
            tablesInPrimaryDb, txnHandler, txnsForPrimaryDb, primaryConf);

    //Open 1 txn with no hive locks acquired
    List<Long> txnsWithNoLocks = openTxns(1, txnHandler, primaryConf);

    dumpData = primary.dump(primaryDbName, failoverConfigs);

    dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertTrue(fs.exists(new Path(dumpPath, FailoverMetaData.FAILOVER_METADATA)));
    assertTrue(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(primary.getDatabase(primaryDbName),
            MetaStoreUtils.FailoverEndpoint.SOURCE));
    FailoverMetaData failoverMD = new FailoverMetaData(dumpPath, conf);

    List<Long> openTxns = failoverMD.getOpenTxns();
    List<Long> txnsAborted = failoverMD.getAbortedTxns();
    assertTrue(txnsAborted.size() == 2);
    assertTrue(txnsAborted.containsAll(txnsForPrimaryDb));
    assertTrue(openTxns.size() == 4);
    assertTrue(openTxns.containsAll(txnsForSecDb));
    assertTrue(openTxns.containsAll(txnsWithNoLocks));
    assertTrue(failoverMD.getTxnsWithoutLock().equals(txnsWithNoLocks));


    //TxnsForPrimaryDb and txnsWithNoLocks would have been aborted by dump operation.
    verifyAllOpenTxnsAborted(txnsForPrimaryDb, primaryConf);
    verifyAllOpenTxnsNotAborted(txnsForSecDb, primaryConf);
    verifyAllOpenTxnsNotAborted(txnsWithNoLocks, primaryConf);
    //Abort the txns
    txnHandler.abortTxns(new AbortTxnsRequest(txnsForSecDb));
    txnHandler.abortTxns(new AbortTxnsRequest(txnsWithNoLocks));
    verifyAllOpenTxnsAborted(txnsForSecDb, primaryConf);
    verifyAllOpenTxnsAborted(txnsWithNoLocks, primaryConf);
    releaseLocks(txnHandler, lockIdsForSecDb);

    replica.load(replicatedDbName, primaryDbName, failoverConfigs)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2"})
            .run("repl status " + replicatedDbName)
            .verifyResult(dumpData.lastReplicationId)
            .run("select id from t1")
            .verifyResults(new String[]{"1"})
            .run("select rank from t2 order by rank")
            .verifyResults(new String[]{"10", "11"});

    db = replica.getDatabase(replicatedDbName);
    assertTrue(MetaStoreUtils.isTargetOfReplication(db));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(db, MetaStoreUtils.FailoverEndpoint.TARGET));
    assertTrue(fs.exists(new Path(dumpPath, ReplAck.LOAD_ACKNOWLEDGEMENT.toString())));

    Path dbRootDir = new Path(dumpData.dumpLocation).getParent();
    long prevDumpDirModifTime = getLatestDumpDirModifTime(dbRootDir);
    primary.run("REPL DUMP " + primaryDbName + " with ('" + HiveConf.ConfVars.HIVE_REPL_FAILOVER_START + "' = 'true')");
    Assert.assertEquals(dumpData.dumpLocation, ReplUtils.getLatestDumpPath(dbRootDir, conf).toString());
    Assert.assertEquals(prevDumpDirModifTime, getLatestDumpDirModifTime(dbRootDir));
    dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, ReplAck.LOAD_ACKNOWLEDGEMENT.toString())));

    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(primary.getDatabase(primaryDbName),
            MetaStoreUtils.FailoverEndpoint.SOURCE));

    assertFalse(ReplChangeManager.isSourceOfReplication(replica.getDatabase(replicatedDbName)));

    WarehouseInstance.Tuple reverseDumpData = replica.run("create table t3 (id int)")
                                                      .run("insert into t2 partition(name='Bob') values(20)")
                                                      .run("insert into t3 values (2)")
                                                      .dump(replicatedDbName);

    assertNotEquals(reverseDumpData.dumpLocation, dumpData.dumpLocation);
    assertTrue(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    dumpPath = new Path(reverseDumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertTrue(new DumpMetaData(dumpPath, conf).getDumpType() == DumpType.PRE_OPTIMIZED_BOOTSTRAP);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    db = replica.getDatabase(replicatedDbName);
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(db, MetaStoreUtils.FailoverEndpoint.TARGET));
    assertTrue(MetaStoreUtils.isTargetOfReplication(db));
    //do a second reverse dump.
    primary.load(primaryDbName, replicatedDbName);
    reverseDumpData = replica.dump(replicatedDbName);
    dumpPath = new Path(reverseDumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);

    primary.load(primaryDbName, replicatedDbName)
            .run("use " + primaryDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2", "t3"})
            .run("repl status " + primaryDbName)
            .verifyResult(reverseDumpData.lastReplicationId)
            .run("select id from t1")
            .verifyResults(new String[]{"1"})
            .run("select rank from t2 order by rank")
            .verifyResults(new String[]{"10", "11", "20"})
            .run("select id from t3")
            .verifyResults(new String[]{"2"});

    Database primaryDb = primary.getDatabase(primaryDbName);
    assertFalse(primaryDb == null);
    assertFalse(ReplUtils.isFirstIncPending(primaryDb.getParameters()));
    assertTrue(MetaStoreUtils.isTargetOfReplication(primaryDb));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(primaryDb));
    assertTrue(fs.exists(new Path(dumpPath, LOAD_ACKNOWLEDGEMENT.toString())));
    assertFalse(ReplChangeManager.isSourceOfReplication(primaryDb));
    assertTrue(ReplChangeManager.isSourceOfReplication(replica.getDatabase(replicatedDbName)));

    reverseDumpData = replica.run("insert into t3 values (3)")
            .run("insert into t2 partition(name='Bob') values(30)")
            .dump(replicatedDbName);
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(replica.getDatabase(replicatedDbName)));

    primary.load(primaryDbName, replicatedDbName)
            .run("select rank from t2 order by rank")
            .verifyResults(new String[]{"10", "11", "20", "30"})
            .run("select id from t3")
            .verifyResults(new String[]{"2", "3"})
            .run("repl status " + primaryDbName)
            .verifyResult(reverseDumpData.lastReplicationId);
    assertFalse(ReplUtils.isFirstIncPending(primary.getDatabase(primaryDbName).getParameters()));
  }

  @Test
  public void testFailoverRollback() throws Throwable {
    List<String> failoverConfigs = Arrays.asList("'" + HiveConf.ConfVars.HIVE_REPL_FAILOVER_START + "'='true'");
    WarehouseInstance.Tuple dumpData = primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("create table t2 (rank int) partitioned by (name string) tblproperties(\"transactional\"=\"true\", " +
                    "\"transactional_properties\"=\"insert_only\")")
            .dump(primaryDbName, failoverConfigs);

    FileSystem fs = new Path(dumpData.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(primary.getDatabase(primaryDbName)));

    replica.load(replicatedDbName, primaryDbName, failoverConfigs)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2"})
            .run("repl status " + replicatedDbName)
            .verifyResult(dumpData.lastReplicationId);

    Database db = replica.getDatabase(replicatedDbName);
    assertTrue(MetaStoreUtils.isTargetOfReplication(db));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(db));

    dumpData = primary.run("use " + primaryDbName)
            .run("insert into t1 values(1)")
            .run("insert into t2 partition(name='Bob') values(11)")
            .run("insert into t2 partition(name='Carl') values(10)")
            .dump(primaryDbName, failoverConfigs);

    primary.run("insert into t2 partition(name='Marie') values(40)");

    dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertTrue(fs.exists(new Path(dumpPath, FailoverMetaData.FAILOVER_METADATA)));
    assertTrue(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(primary.getDatabase(primaryDbName),
            MetaStoreUtils.FailoverEndpoint.SOURCE));

    replica.load(replicatedDbName, primaryDbName, failoverConfigs)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2"})
            .run("repl status " + replicatedDbName)
            .verifyResult(dumpData.lastReplicationId)
            .run("select id from t1")
            .verifyResults(new String[]{"1"})
            .run("select rank from t2 order by rank")
            .verifyResults(new String[]{"10", "11"})
            .run("show partitions t2")
            .verifyResults(new String[]{"name=Bob", "name=Carl"});

    assertTrue(fs.exists(new Path(dumpPath, LOAD_ACKNOWLEDGEMENT.toString())));
    db = replica.getDatabase(replicatedDbName);
    assertTrue(MetaStoreUtils.isTargetOfReplication(db));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(db, MetaStoreUtils.FailoverEndpoint.TARGET));

    dumpData = primary.run("create table t3(id int)")
            .run("insert into t3 values (3)")
            .dump(primaryDbName);
    dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertEquals(new DumpMetaData(dumpPath, conf).getDumpType(), DumpType.INCREMENTAL);
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertFalse(fs.exists(new Path(dumpPath, FailoverMetaData.FAILOVER_METADATA)));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(primary.getDatabase(primaryDbName)));
    replica.load(replicatedDbName, primaryDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2", "t3"})
            .run("select id from t3")
            .verifyResults(new String[]{"3"})
            .run("select rank from t2 order by rank")
            .verifyResults(new String[]{"10", "11", "40"})
            .run("show partitions t2")
            .verifyResults(new String[]{"name=Bob", "name=Carl", "name=Marie"})
            .run("repl status " + replicatedDbName)
            .verifyResult(dumpData.lastReplicationId);

    db = replica.getDatabase(replicatedDbName);
    assertTrue(MetaStoreUtils.isTargetOfReplication(db));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(db));
  }

  @Test
  public void testFailoverFailureBeforeReverseReplication() throws Throwable {
    List<String> failoverConfigs = Arrays.asList("'" + HiveConf.ConfVars.HIVE_REPL_FAILOVER_START + "'='true'",
            "'" + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR + "'='true'",
            "'" + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR_COUNT + "'='1'");
    List<String> retainPrevDumpDir = Arrays.asList("'" + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR + "'='true'",
            "'" + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR_COUNT + "'='1'",
            "'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET + "'='true'"
    );
    WarehouseInstance.Tuple dumpData = primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("create table t2 (rank int) partitioned by (name string) tblproperties(\"transactional\"=\"true\", " +
                    "\"transactional_properties\"=\"insert_only\")")
            .dump(primaryDbName, failoverConfigs);

    //This dump is not failover ready as target db can be used for replication only after first incremental load.
    FileSystem fs = new Path(dumpData.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertFalse(fs.exists(new Path(dumpPath, FailoverMetaData.FAILOVER_METADATA)));
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(primary.getDatabase(primaryDbName)));

    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2"})
            .run("repl status " + replicatedDbName)
            .verifyResult(dumpData.lastReplicationId);

    assertTrue(MetaStoreUtils.isTargetOfReplication(replica.getDatabase(replicatedDbName)));
    dumpData = primary.run("use " + primaryDbName)
            .run("insert into t1 values(1)")
            .run("insert into t2 partition(name='Bob') values(11)")
            .dump(primaryDbName, failoverConfigs);

    dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path dumpAckFile = new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString());
    Path failoverMdFile = new Path(dumpPath, FailoverMetaData.FAILOVER_METADATA);
    assertTrue(fs.exists(dumpAckFile));
    assertTrue(fs.exists(failoverMdFile));
    assertTrue(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(primary.getDatabase(primaryDbName),
            MetaStoreUtils.FailoverEndpoint.SOURCE));
    FailoverMetaData previousFmd = new FailoverMetaData(dumpPath, conf);
    Long failoverEventId = previousFmd.getFailoverEventId();
    assertTrue(failoverEventId >= Long.parseLong(dumpData.lastReplicationId));
    Long failoverMdModifTime = fs.getFileStatus(failoverMdFile).getModificationTime();

    fs.delete(dumpAckFile, false);

    dumpData = primary.run("use " + primaryDbName)
            .run("insert into t2 partition(name='Carl') values(10)")
            .run("create table t3 (id int)")
            .run("insert into t3 values (2)")
            .dump(primaryDbName, failoverConfigs);

    dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    failoverMdFile = new Path(dumpPath, FailoverMetaData.FAILOVER_METADATA);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertTrue(fs.exists(failoverMdFile));
    Assert.assertEquals(failoverMdModifTime, (Long)fs.getFileStatus(failoverMdFile).getModificationTime());
    assertTrue(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(primary.getDatabase(primaryDbName),
            MetaStoreUtils.FailoverEndpoint.SOURCE));
    assertTrue(failoverEventId >= Long.parseLong(dumpData.lastReplicationId));
    FailoverMetaData currentFmd = new FailoverMetaData(dumpPath, conf);
    assertTrue(currentFmd.equals(previousFmd));

    replica.load(replicatedDbName, primaryDbName, failoverConfigs)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2"})
            .run("repl status " + replicatedDbName)
            .verifyResult(dumpData.lastReplicationId)
            .run("select id from t1")
            .verifyResults(new String[]{"1"})
            .run("select rank from t2 order by rank")
            .verifyResults(new String[]{"11"});

    Database db = replica.getDatabase(replicatedDbName);
    assertTrue(MetaStoreUtils.isTargetOfReplication(db));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(db, MetaStoreUtils.FailoverEndpoint.TARGET));
    assertTrue(fs.exists(new Path(dumpPath, ReplAck.LOAD_ACKNOWLEDGEMENT.toString())));

    WarehouseInstance.Tuple reverseDumpData = replica.run("use " + replicatedDbName)
            .run("insert into t2 partition(name='Carl') values(12)")
            .run("create table t3 (id int)")
            .run("insert into t3 values (10)")
            .dump(replicatedDbName, retainPrevDumpDir);
    assertNotEquals(reverseDumpData.dumpLocation, dumpData.dumpLocation);
    assertTrue(fs.exists(dumpPath));
    assertTrue(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    dumpPath = new Path(reverseDumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    dumpAckFile = new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString());
    assertTrue(fs.exists(dumpAckFile));
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertTrue(new DumpMetaData(dumpPath, conf).getDumpType() == DumpType.PRE_OPTIMIZED_BOOTSTRAP);
    db = replica.getDatabase(replicatedDbName);
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(db, MetaStoreUtils.FailoverEndpoint.TARGET));
    assertTrue(MetaStoreUtils.isTargetOfReplication(db));
    primary.load(primaryDbName, replicatedDbName, retainPrevDumpDir);
    //do a second reverse dump.
    reverseDumpData = replica.dump(replicatedDbName, retainPrevDumpDir);
    dumpPath = new Path(reverseDumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);

    primary.load(primaryDbName, replicatedDbName, retainPrevDumpDir)
            .run("use " + primaryDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2", "t3"})
            .run("repl status " + primaryDbName)
            .verifyResult(reverseDumpData.lastReplicationId)
            .run("select id from t1")
            .verifyResults(new String[]{"1"})
            .run("select rank from t2 order by rank")
            .verifyResults(new String[]{"11", "12"})
            .run("select id from t3")
            .verifyResults(new String[]{"10"});

    assertTrue(fs.exists(new Path(dumpPath, LOAD_ACKNOWLEDGEMENT.toString())));

    reverseDumpData = replica.run("use " + replicatedDbName)
            .run("insert into t3 values (15)")
            .dump(replicatedDbName);
    dumpPath = new Path(reverseDumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(replica.getDatabase(replicatedDbName)));

    primary.load(primaryDbName, replicatedDbName)
            .run("use " + primaryDbName)
            .run("select id from t3")
            .verifyResults(new String[]{"10", "15"});;
    assertTrue(fs.exists(new Path(dumpPath, LOAD_ACKNOWLEDGEMENT.toString())));
  }

  @Test
  public void testFailoverFailureInReverseReplication() throws Throwable {
    List<String> failoverConfigs = Arrays.asList("'" + HiveConf.ConfVars.HIVE_REPL_FAILOVER_START + "'='true'",
            "'" + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR + "'='true'",
            "'" + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR_COUNT + "'='1'");
    List<String> retainPrevDumpDir = Arrays.asList("'" + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR + "'='true'",
            "'" + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR_COUNT + "'='1'",
            "'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET + "'='true'"
    );
    WarehouseInstance.Tuple dumpData = primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("create table t2 (rank int) partitioned by (name string) tblproperties(\"transactional\"=\"true\", " +
                    "\"transactional_properties\"=\"insert_only\")")
            .dump(primaryDbName, failoverConfigs);

    //This dump is not failover ready as target db can be used for replication only after first incremental load.
    FileSystem fs = new Path(dumpData.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertFalse(fs.exists(new Path(dumpPath, FailoverMetaData.FAILOVER_METADATA)));
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(primary.getDatabase(primaryDbName)));

    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2"})
            .run("repl status " + replicatedDbName)
            .verifyResult(dumpData.lastReplicationId);

    assertTrue(MetaStoreUtils.isTargetOfReplication(replica.getDatabase(replicatedDbName)));
    dumpData = primary.run("use " + primaryDbName)
            .run("insert into t1 values(1)")
            .run("insert into t2 partition(name='Bob') values(11)")
            .dump(primaryDbName, failoverConfigs);

    dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertTrue(fs.exists(new Path(dumpPath, FailoverMetaData.FAILOVER_METADATA)));
    assertTrue(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(primary.getDatabase(primaryDbName),
            MetaStoreUtils.FailoverEndpoint.SOURCE));

    replica.load(replicatedDbName, primaryDbName, failoverConfigs)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2"})
            .run("repl status " + replicatedDbName)
            .verifyResult(dumpData.lastReplicationId)
            .run("select id from t1")
            .verifyResults(new String[]{"1"})
            .run("select rank from t2 order by rank")
            .verifyResults(new String[]{"11"});

    Database db = replica.getDatabase(replicatedDbName);
    assertTrue(MetaStoreUtils.isTargetOfReplication(db));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(db, MetaStoreUtils.FailoverEndpoint.TARGET));
    assertTrue(fs.exists(new Path(dumpPath, ReplAck.LOAD_ACKNOWLEDGEMENT.toString())));

    WarehouseInstance.Tuple reverseDumpData = replica.run("use " + replicatedDbName)
            .run("insert into t2 partition(name='Bob') values(20)")
            .run("create table t3 (id int)")
            .run("insert into t3 values (10)")
            .dump(replicatedDbName, retainPrevDumpDir);
    assertNotEquals(reverseDumpData.dumpLocation, dumpData.dumpLocation);
    assertTrue(fs.exists(dumpPath));
    assertTrue(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    dumpPath = new Path(reverseDumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path dumpAckFile = new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString());
    assertTrue(fs.exists(dumpAckFile));
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertTrue(new DumpMetaData(dumpPath, conf).getDumpType() == DumpType.PRE_OPTIMIZED_BOOTSTRAP);
    db = replica.getDatabase(replicatedDbName);
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(db, MetaStoreUtils.FailoverEndpoint.TARGET));
    assertTrue(MetaStoreUtils.isTargetOfReplication(db));
    fs.delete(dumpAckFile, false);
    assertFalse(fs.exists(dumpAckFile));
    WarehouseInstance.Tuple preFailoverDumpData = dumpData;
    dumpData = replica.dump(replicatedDbName, retainPrevDumpDir);
    dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    dumpAckFile = new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString());
    assertNotEquals(dumpData.dumpLocation, preFailoverDumpData.dumpLocation);
    assertTrue(fs.exists(new Path(preFailoverDumpData.dumpLocation)));
    assertNotEquals(reverseDumpData.dumpLocation, dumpData.dumpLocation);
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertTrue(new DumpMetaData(dumpPath, conf).getDumpType() == DumpType.PRE_OPTIMIZED_BOOTSTRAP);
    assertTrue(fs.exists(dumpAckFile));
    db = replica.getDatabase(replicatedDbName);
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(db, MetaStoreUtils.FailoverEndpoint.TARGET));
    assertTrue(MetaStoreUtils.isTargetOfReplication(db));
    primary.load(primaryDbName, replicatedDbName);
    dumpData = replica.dump(replicatedDbName, retainPrevDumpDir);

    primary.load(primaryDbName, replicatedDbName, retainPrevDumpDir)
            .run("use " + primaryDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2", "t3"})
            .run("repl status " + primaryDbName)
            .verifyResult(dumpData.lastReplicationId)
            .run("select id from t1")
            .verifyResults(new String[]{"1"})
            .run("select rank from t2 order by rank")
            .verifyResults(new String[]{"11", "20"})
            .run("select id from t3")
            .verifyResults(new String[]{"10"});

    assertTrue(fs.exists(new Path(dumpPath, LOAD_ACKNOWLEDGEMENT.toString())));

    reverseDumpData = replica.run("use " + replicatedDbName)
            .run("insert into t3 values (3)")
            .run("insert into t2 partition(name='Bob') values(30)")
            .dump(replicatedDbName, retainPrevDumpDir);
    dumpPath = new Path(reverseDumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    dumpAckFile = new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString());
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(replica.getDatabase(replicatedDbName),
            MetaStoreUtils.FailoverEndpoint.TARGET));
    fs.delete(dumpAckFile);
    replica.run("ALTER DATABASE " + replicatedDbName + " SET DBPROPERTIES('" + ReplConst.REPL_FAILOVER_ENDPOINT + "'='"
            + MetaStoreUtils.FailoverEndpoint.TARGET + "')");
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(replica.getDatabase(replicatedDbName),
            MetaStoreUtils.FailoverEndpoint.TARGET));
    assertFalse(fs.exists(dumpAckFile));
    dumpData = replica.dump(replicatedDbName, retainPrevDumpDir);
    assertEquals(reverseDumpData.dumpLocation, dumpData.dumpLocation);
    fs.exists(dumpAckFile);
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(replica.getDatabase(replicatedDbName),
            MetaStoreUtils.FailoverEndpoint.TARGET));

    primary.load(primaryDbName, replicatedDbName, retainPrevDumpDir)
            .run("use " + primaryDbName)
            .run("select rank from t2 order by rank")
            .verifyResults(new String[]{"11", "20", "30"})
            .run("select id from t3")
            .verifyResults(new String[]{"10", "3"});;
    assertTrue(fs.exists(new Path(dumpPath, LOAD_ACKNOWLEDGEMENT.toString())));
  }

  @Test
  public void testFailoverFailedAndRollback() throws Throwable {
    List<String> failoverConfigs = Arrays.asList("'" + HiveConf.ConfVars.HIVE_REPL_FAILOVER_START + "'='true'",
            "'" + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR + "'='true'",
            "'" + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR_COUNT + "'='1'");
    WarehouseInstance.Tuple dumpData = primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("create table t2 (rank int) partitioned by (name string) tblproperties(\"transactional\"=\"true\", " +
                    "\"transactional_properties\"=\"insert_only\")")
            .dump(primaryDbName, failoverConfigs);

    //This dump is not failover ready as target db can be used for replication only after first incremental load.
    FileSystem fs = new Path(dumpData.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertFalse(fs.exists(new Path(dumpPath, FailoverMetaData.FAILOVER_METADATA)));
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(primary.getDatabase(primaryDbName)));

    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2"})
            .run("repl status " + replicatedDbName)
            .verifyResult(dumpData.lastReplicationId);

    assertTrue(MetaStoreUtils.isTargetOfReplication(replica.getDatabase(replicatedDbName)));
    dumpData = primary.run("use " + primaryDbName)
            .run("insert into t1 values(1)")
            .run("insert into t2 partition(name='Bob') values(11)")
            .dump(primaryDbName, failoverConfigs);

    dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path dumpAckFile = new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString());
    assertTrue(fs.exists(dumpAckFile));
    assertTrue(fs.exists(new Path(dumpPath, FailoverMetaData.FAILOVER_METADATA)));
    assertTrue(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(primary.getDatabase(primaryDbName),
            MetaStoreUtils.FailoverEndpoint.SOURCE));

    fs.delete(dumpAckFile, false);

    dumpData = primary.run("use " + primaryDbName)
            .run("insert into t2 partition(name='Carl') values(10)")
            .run("create table t3 (id int)")
            .run("insert into t3 values (2)")
            .dump(primaryDbName);

    dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, FailoverMetaData.FAILOVER_METADATA)));
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(primary.getDatabase(primaryDbName)));

    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2", "t3"})
            .run("repl status " + replicatedDbName)
            .verifyResult(dumpData.lastReplicationId)
            .run("select id from t1")
            .verifyResults(new String[]{"1"})
            .run("select rank from t2 order by rank")
            .verifyResults(new String[]{"11", "10"})
            .run("select id from t3")
            .verifyResults(new String[]{"2"});
    assertTrue(fs.exists(new Path(dumpPath, LOAD_ACKNOWLEDGEMENT.toString())));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(replica.getDatabase(replicatedDbName)));
  }

  @Test
  public void testEnablementOfReplBackgroundThreadDuringFailover() throws Throwable{
    List<String> failoverConfigs = Arrays.asList("'" + HiveConf.ConfVars.HIVE_REPL_FAILOVER_START + "'='true'");

    WarehouseInstance.Tuple dumpData = primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("create table t2 (rank int) partitioned by (name string) tblproperties(\"transactional\"=\"true\", " +
                    "\"transactional_properties\"=\"insert_only\")")
            .dump(primaryDbName, failoverConfigs);

    FileSystem fs = new Path(dumpData.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(primary.getDatabase(primaryDbName)));

    replica.load(replicatedDbName, primaryDbName, failoverConfigs)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2"})
            .run("repl status " + replicatedDbName)
            .verifyResult(dumpData.lastReplicationId);

    Database db = replica.getDatabase(replicatedDbName);
    assertTrue(MetaStoreUtils.isTargetOfReplication(db));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(db));

    dumpData = primary.run("use " + primaryDbName)
            .run("insert into t1 values(1)")
            .run("insert into t2 partition(name='Bob') values(11)")
            .run("insert into t2 partition(name='Carl') values(10)")
            .dump(primaryDbName, failoverConfigs);

    dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path failoverReadyMarker = new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString());
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertTrue(fs.exists(new Path(dumpPath, FailoverMetaData.FAILOVER_METADATA)));
    assertTrue(fs.exists(failoverReadyMarker));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(primary.getDatabase(primaryDbName),
            MetaStoreUtils.FailoverEndpoint.SOURCE));

    replica.load(replicatedDbName, primaryDbName, failoverConfigs)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2"})
            .run("repl status " + replicatedDbName)
            .verifyResult(dumpData.lastReplicationId)
            .run("select id from t1")
            .verifyResults(new String[]{"1"})
            .run("select rank from t2 order by rank")
            .verifyResults(new String[]{"10", "11"});

    assertTrue(fs.exists(new Path(dumpPath, LOAD_ACKNOWLEDGEMENT.toString())));
    db = replica.getDatabase(replicatedDbName);
    assertTrue(MetaStoreUtils.isTargetOfReplication(db));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(db, MetaStoreUtils.FailoverEndpoint.TARGET));
    assert "true".equals(db.getParameters().get(REPL_ENABLE_BACKGROUND_THREAD));
  }

  @Test
  public void testFailoverFailureDuringRollback() throws Throwable {
    List<String> failoverConfigs = Arrays.asList("'" + HiveConf.ConfVars.HIVE_REPL_FAILOVER_START + "'='true'",
            "'" + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR + "'='true'",
            "'" + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR_COUNT + "'='1'");
    List<String> retainPrevDumpDir = Arrays.asList("'" + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR + "'='true'",
            "'" + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR_COUNT + "'='1'");
    WarehouseInstance.Tuple dumpData = primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("create table t2 (rank int) partitioned by (name string) tblproperties(\"transactional\"=\"true\", " +
                    "\"transactional_properties\"=\"insert_only\")")
            .dump(primaryDbName, failoverConfigs);

    FileSystem fs = new Path(dumpData.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(primary.getDatabase(primaryDbName)));

    replica.load(replicatedDbName, primaryDbName, failoverConfigs)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2"})
            .run("repl status " + replicatedDbName)
            .verifyResult(dumpData.lastReplicationId);

    Database db = replica.getDatabase(replicatedDbName);
    assertTrue(MetaStoreUtils.isTargetOfReplication(db));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(db));

    dumpData = primary.run("use " + primaryDbName)
            .run("insert into t1 values(1)")
            .run("insert into t2 partition(name='Bob') values(11)")
            .run("insert into t2 partition(name='Carl') values(10)")
            .dump(primaryDbName, failoverConfigs);

    dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path failoverReadyMarker = new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString());
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertTrue(fs.exists(new Path(dumpPath, FailoverMetaData.FAILOVER_METADATA)));
    assertTrue(fs.exists(failoverReadyMarker));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(primary.getDatabase(primaryDbName),
            MetaStoreUtils.FailoverEndpoint.SOURCE));

    replica.load(replicatedDbName, primaryDbName, failoverConfigs)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2"})
            .run("repl status " + replicatedDbName)
            .verifyResult(dumpData.lastReplicationId)
            .run("select id from t1")
            .verifyResults(new String[]{"1"})
            .run("select rank from t2 order by rank")
            .verifyResults(new String[]{"10", "11"});

    assertTrue(fs.exists(new Path(dumpPath, LOAD_ACKNOWLEDGEMENT.toString())));
    db = replica.getDatabase(replicatedDbName);
    assertTrue(MetaStoreUtils.isTargetOfReplication(db));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(db, MetaStoreUtils.FailoverEndpoint.TARGET));

    dumpData = primary.run("insert into t1 values (5)")
            .dump(primaryDbName, retainPrevDumpDir);
    dumpPath = new Path(dumpData.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(primary.getDatabase(primaryDbName)));
    Path dumpAck = new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString());
    assertTrue(fs.exists(dumpAck));
    fs.delete(dumpAck, false);
    fs.create(failoverReadyMarker);
    assertTrue(fs.exists(failoverReadyMarker));
    primary.run("ALTER DATABASE " + primaryDbName + " SET DBPROPERTIES('" + ReplConst.REPL_FAILOVER_ENDPOINT + "'='"
            + MetaStoreUtils.FailoverEndpoint.SOURCE + "')");
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(primary.getDatabase(primaryDbName),
            MetaStoreUtils.FailoverEndpoint.SOURCE));

    WarehouseInstance.Tuple newDumpData = primary.dump(primaryDbName, retainPrevDumpDir);
    assertEquals(newDumpData.dumpLocation, dumpData.dumpLocation);
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(primary.getDatabase(primaryDbName)));
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertFalse(fs.exists(new Path(dumpPath, FailoverMetaData.FAILOVER_METADATA)));
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));

    replica.load(replicatedDbName, primaryDbName)
            .run("select id from t1")
            .verifyResults(new String[]{"1", "5"});
    assertTrue(fs.exists(new Path(dumpPath, LOAD_ACKNOWLEDGEMENT.toString())));
    db = replica.getDatabase(replicatedDbName);
    assertTrue(MetaStoreUtils.isTargetOfReplication(db));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(db));
  }

  private long getLatestDumpDirModifTime(Path dumpRoot) throws Exception {
    FileSystem fs = dumpRoot.getFileSystem(conf);
    long latestModifTime = -1;
    if (fs.exists(dumpRoot)) {
      for (FileStatus status : fs.listStatus(dumpRoot)) {
        if (status.getModificationTime() > latestModifTime) {
          latestModifTime = status.getModificationTime();
        }
      }
    }
    return latestModifTime;
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
    assert currentEventId == lastEventId;

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
  public void testReadOperationsNotCapturedInNotificationLog() throws Throwable {
    //Perform empty bootstrap dump and load
    String dbName = testName.getMethodName();
    String replDbName = "replicated_" + testName.getMethodName();
    try {
      primary.run("CREATE DATABASE " + dbName + " WITH DBPROPERTIES ( '" +
              SOURCE_OF_REPLICATION + "' = '1,2,3')");
      primary.hiveConf.set("hive.txn.readonly.enabled", "true");
      primary.run("CREATE TABLE " + dbName + ".t1 (id int)");
      primary.run("CREATE table " + dbName
              + ".source (q1 int , a1 int) stored as orc tblproperties (\"transactional\"=\"true\")");
      primary.run("CREATE table " + dbName
              + ".target (b int, p int) stored as orc tblproperties (\"transactional\"=\"true\")");
      primary.run("INSERT into " + dbName + ".source values(1,5)");
      primary.run("INSERT into " + dbName + ".target values(10,1)");
      primary.dump(dbName);
      replica.run("REPL LOAD " + dbName + " INTO " + replDbName);
      //Perform empty incremental dump and load so that all db level properties are altered.
      primary.dump(dbName);
      replica.run("REPL LOAD " + dbName + " INTO " + replDbName);
      primary.run("INSERT INTO " + dbName + ".t1 VALUES(1)");
      long lastEventId = primary.getCurrentNotificationEventId().getEventId();
      primary.run("USE " + dbName);
      primary.run("DESCRIBE DATABASE " + dbName);
      primary.run("DESCRIBE "+ dbName + ".t1");
      primary.run("SELECT * FROM " + dbName + ".t1");
      primary.run("SHOW TABLES " + dbName);
      primary.run("SHOW TABLE EXTENDED LIKE 't1'");
      primary.run("SHOW TBLPROPERTIES t1");
      primary.run("EXPLAIN SELECT * from " + dbName + ".t1");
      primary.run("SHOW LOCKS");
      primary.run("EXPLAIN SHOW LOCKS");
      primary.run("EXPLAIN LOCKS UPDATE target SET b = 1 WHERE p IN (SELECT t.q1 FROM source t WHERE t.a1=5)");
      long currentEventId = primary.getCurrentNotificationEventId().getEventId();
      Assert.assertEquals(lastEventId, currentEventId);
    } finally {
      primary.run("DROP DATABASE " + dbName + " CASCADE");
      replica.run("DROP DATABASE " + replDbName + " CASCADE");
    }
  }

  @Test
  public void testXAttrsPreserved() throws Throwable {
    String nonTxnTable = "nonTxnTable";
    String unptnedTable = "unptnedTable";
    String ptnedTable = "ptnedTable";
    String clusteredTable = "clusteredTable";
    String clusteredAndPtnedTable = "clusteredAndPtnedTable";
    primary.run("use " + primaryDbName)
            .run("create table " + nonTxnTable + " (id int)")
            .run("create table " + unptnedTable + " (id int) stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("create table " + ptnedTable + " (id int) partitioned by (name string) stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("create table " + clusteredTable + " (id int) clustered by (id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("create table " + clusteredAndPtnedTable + " (id int) partitioned by (name string) clustered by(id)" +
                    "into 3 buckets stored as orc tblproperties (\"transactional\"=\"true\")")
            .run("insert into " + nonTxnTable + " values (2)").run("INSERT into " + unptnedTable + " values(1)")
            .run("INSERT into " + ptnedTable + " values(2, 'temp')").run("INSERT into " + clusteredTable + " values(1)")
            .run("INSERT into " + clusteredAndPtnedTable + " values(1, 'temp')");
    for (String table : primary.getAllTables(primaryDbName)) {
      org.apache.hadoop.hive.ql.metadata.Table tb = Hive.get(primary.hiveConf).getTable(primaryDbName, table);
      if (tb.isPartitioned()) {
        List<Partition> partitions = primary.getAllPartitions(primaryDbName, table);
        for (Partition partition: partitions) {
          Path partitionLoc = new Path(partition.getSd().getLocation());
          FileSystem fs = partitionLoc.getFileSystem(conf);
          setXAttrsRecursive(fs, partitionLoc, true);
        }
      } else {
        Path tablePath = tb.getDataLocation();
        FileSystem fs = tablePath.getFileSystem(conf);
        setXAttrsRecursive(fs, tablePath, true);
      }
    }
    primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);
    Path srcDbPath = new Path(primary.getDatabase(primaryDbName).getLocationUri());
    Path replicaDbPath = new Path(primary.getDatabase(replicatedDbName).getLocationUri());
    verifyXAttrsPreserved(srcDbPath.getFileSystem(conf), srcDbPath, replicaDbPath);
  }

  private void setXAttrsRecursive(FileSystem fs, Path path, boolean isParent) throws Exception {
    if (fs.getFileStatus(path).isDirectory()) {
      RemoteIterator<FileStatus> content = fs.listStatusIterator(path);
      while(content.hasNext()) {
        setXAttrsRecursive(fs, content.next().getPath(), false);
      }
    }
    if (!isParent) {
      fs.setXAttr(path, "user.random", "value".getBytes(StandardCharsets.UTF_8));
    }
  }

  private void verifyXAttrsPreserved(FileSystem fs, Path src, Path dst) throws Exception {
    FileStatus srcStatus = fs.getFileStatus(src);
    FileStatus dstStatus = fs.getFileStatus(dst);
    if (srcStatus.isDirectory()) {
      assertTrue(dstStatus.isDirectory());
      for(FileStatus srcContent: fs.listStatus(src)) {
        Path dstContent = new Path(dst, srcContent.getPath().getName());
        assertTrue(fs.exists(dstContent));
        verifyXAttrsPreserved(fs, srcContent.getPath(), dstContent);
      }
    } else {
      assertFalse(dstStatus.isDirectory());
    }
    Map<String, byte[]> values = fs.getXAttrs(dst);
    for(Map.Entry<String, byte[]> value : fs.getXAttrs(src).entrySet()) {
      assertEquals(new String(value.getValue()), new String(values.get(value.getKey())));
    }
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
    List<Long> lockIds = allocateWriteIdsForTablesAndAcquireLocks(primaryDbName, tables, txnHandler, txns, primaryConf);

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
    List<Long> lockIds = allocateWriteIdsForTablesAndAcquireLocks(primaryDbName + "_extra",
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
    List<Long> lockIds = allocateWriteIdsForTablesAndAcquireLocks(primaryDbName + "_extra",
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
    List<Long> lockIds = allocateWriteIdsForTablesAndAcquireLocks(primaryDbName + "_extra",
      tablesInSecDb, txnHandler, txns, primaryConf);
    // Allocate write ids for both tables of primary db for all txns
    // t1=5+1L and t2=5+2L inserts
    Map<String, Long> tablesInPrimDb = new HashMap<>();
    tablesInPrimDb.put("t1", (long) numTxns + 1L);
    tablesInPrimDb.put("t2", (long) numTxns + 2L);
    lockIds.addAll(allocateWriteIdsForTablesAndAcquireLocks(primaryDbName,
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
    List<Long> lockIds = allocateWriteIdsForTablesAndAcquireLocks(primaryDbName, tables, txnHandler, txns, primaryConf);

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
    primary.testEventCounts(primaryDbName, lastReplId, null, null, 11);

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
      fail();
    } catch (HiveException e) {
      assertEquals("REPL LOAD Target database name shouldn't be null", e.getMessage());
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
    EventsDumpMetadata eventsDumpMetadata = EventsDumpMetadata.deserialize(ackLastEventID, conf);
    eventsDumpMetadata.setLastReplId(lastEventID - 3);
    eventsDumpMetadata.setEventsDumpedCount(eventsDumpMetadata.getEventsDumpedCount() - 3);
    org.apache.hadoop.hive.ql.parse.repl.dump.Utils.writeOutput(eventsDumpMetadata.serialize(), ackLastEventID,
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
    long firstIncEventID = -1;
    long lastIncEventID = Long.parseLong(incrementalDump1.lastReplicationId);
    assertTrue(lastIncEventID > (firstIncEventID + 1));

    for (long eventId=Long.parseLong(bootstrapDump.lastReplicationId) + 1; eventId<=lastIncEventID; eventId++) {
      Path eventRoot = new Path(hiveDumpDir, String.valueOf(eventId));
      if (fs.exists(eventRoot)) {
        if (firstIncEventID == -1){
          firstIncEventID = eventId;
        } else {
          fs.delete(eventRoot, true);
        }
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
    int deletedEventsCount = 0;
    for (long eventId=fifthLastIncEventID + 1; eventId<=lastIncEventID; eventId++) {
      Path eventRoot = new Path(hiveDumpDir, String.valueOf(eventId));
      if (fs.exists(eventRoot)) {
        deletedEventsCount++;
        fs.delete(eventRoot, true);
      }
    }
    EventsDumpMetadata eventsDumpMetadata = EventsDumpMetadata.deserialize(ackLastEventID, conf);
    eventsDumpMetadata.setLastReplId(fifthLastIncEventID);
    eventsDumpMetadata.setEventsDumpedCount(eventsDumpMetadata.getEventsDumpedCount() - deletedEventsCount);
    org.apache.hadoop.hive.ql.parse.repl.dump.Utils.writeOutput(eventsDumpMetadata.serialize(), ackLastEventID,
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
    isMetricsEnabledForTests(false);
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
    isMetricsEnabledForTests(false);
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
            + (ReplUtils.RESERVED_DIR_ITEMS_COUNT + 5) +"'");

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
            .dump(primaryDbName, dumpClause);

    int eventCount = primary.getNoOfEventsDumped(incrementalDump1.dumpLocation, conf);
    assertEquals(eventCount, 5);

    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1"});

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


  @Test
  public void testTxnTblReplWithSameNameAsDroppedNonTxnTbl() throws Throwable {
    List<String> withClauseOptions = new LinkedList<>();
    withClauseOptions.add("'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname
        + "'='" + UserGroupInformation.getCurrentUser().getUserName() + "'");

    String tbl = "t1";
    primary
        .run("use " + primaryDbName)
        .run("create table " + tbl + " (id int)")
        .run("insert into table " + tbl + " values (1)")
        .dump(primaryDbName, withClauseOptions);

    replica
        .load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("select id from " + tbl)
        .verifyResults(new String[] {"1"});

    assertFalse(AcidUtils.isTransactionalTable(replica.getTable(replicatedDbName, tbl)));

    primary
        .run("use " + primaryDbName)
        .run("drop table " + tbl)
        .run("create table " + tbl + " (id int) clustered by(id) into 3 buckets stored as orc " +
            "tblproperties (\"transactional\"=\"true\")")
        .run("insert into table " + tbl + " values (2)")
        .dump(primaryDbName, withClauseOptions);

    replica
        .load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("select id from " + tbl)
        .verifyResults(new String[] {"2"});

    assertTrue(AcidUtils.isTransactionalTable(replica.getTable(replicatedDbName, tbl)));
  }

  @Test
  public void testTxnTblReplWithSameNameAsDroppedExtTbl() throws Throwable {
    List<String> withClauseOptions = new LinkedList<>();
    withClauseOptions.add("'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname
        + "'='" + UserGroupInformation.getCurrentUser().getUserName() + "'");

    String tbl = "t1";
    primary
        .run("use " + primaryDbName)
        .run("create external table " + tbl + " (id int)")
        .run("insert into table " + tbl + " values (1)")
        .dump(primaryDbName, withClauseOptions);

    replica
        .load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("select id from " + tbl)
        .verifyResults(new String[] {"1"});

    assertFalse(AcidUtils.isTransactionalTable(replica.getTable(replicatedDbName, tbl)));
    assertTrue(replica.getTable(replicatedDbName, tbl).getTableType().equals(TableType.EXTERNAL_TABLE.toString()));

    primary
        .run("use " + primaryDbName)
        .run("drop table " + tbl)
        .run("create table " + tbl + " (id int) clustered by(id) into 3 buckets stored as orc " +
            "tblproperties (\"transactional\"=\"true\")")
        .run("insert into table " + tbl + " values (2)")
        .dump(primaryDbName, withClauseOptions);

    replica
        .load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("select id from " + tbl)
        .verifyResults(new String[] {"2"});

    assertTrue(AcidUtils.isTransactionalTable(replica.getTable(replicatedDbName, tbl)));
    assertFalse(replica.getTable(replicatedDbName, tbl).getTableType().equals(TableType.EXTERNAL_TABLE.toString()));
  }

  @Test
  public void testReplTargetLastIdNotUpdatedInCaseOfResume() throws Throwable {
    Map<String, String> params = new HashMap<>();
    params.put(REPL_RESUME_STARTED_AFTER_FAILOVER, "true");
    params.put(REPL_TARGET_TABLE_PROPERTY, "19");
    params.put(REPL_TARGET_DATABASE_PROPERTY, "15");

    // create database and set the parameters
    String dbName = "db1";
    Database db1 = primary.run("create database " + dbName)
                          .getDatabase(dbName);
    db1.setParameters(params);

    // let's change 'repl.last.id', now this should not update 'repl.target.last.id' as it finds
    // 'repl.resume.started' flag
    primary.run("alter database " + dbName + " set dbproperties('repl.last.id'='21')");
    Map<String, String> updatedParams = db1.getParameters();


    assertEquals(params.get(REPL_TARGET_DATABASE_PROPERTY),
      updatedParams.get(REPL_TARGET_DATABASE_PROPERTY));
    assertEquals("15", updatedParams.get(REPL_TARGET_DATABASE_PROPERTY));
  }
  @Test
  public void testBatchingOfIncrementalEvents() throws Throwable {
    final int REPL_MAX_LOAD_TASKS = 5;
    List<String> incrementalBatchConfigs = Arrays.asList(
            String.format("'%s'='%s'", HiveConf.ConfVars.REPL_BATCH_INCREMENTAL_EVENTS, "true"),
            String.format("'%s'='%d'", HiveConf.ConfVars.REPL_APPROX_MAX_LOAD_TASKS, REPL_MAX_LOAD_TASKS)
    );

    //bootstrap run, config should have no effect
    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("create table t1 (id int)")
            .run("insert into table t1 values (1)")
            .dump(primaryDbName, incrementalBatchConfigs);

    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);

    replica.load(replicatedDbName, primaryDbName, incrementalBatchConfigs)
            .run("use " + replicatedDbName)
            .run("select * from t1")
            .verifyResults(new String[]{"1"});

    //incremental run
    WarehouseInstance.Tuple incrementalDump = primary.run("use " + primaryDbName)
            .run("insert into t1 values(2)")
            .run("insert into t1 values(3)")
            .run("insert into t1 values(4)")
            .run("insert into t1 values(5)")
            .dump(primaryDbName, incrementalBatchConfigs);

    Path dumpPath = new Path(incrementalDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path ackLastEventID = new Path(dumpPath, ReplAck.EVENTS_DUMP.toString());
    EventsDumpMetadata eventsDumpMetadata = EventsDumpMetadata.deserialize(ackLastEventID, conf);
    assertTrue(eventsDumpMetadata.isEventsBatched());

    int eventsCountInAckFile = eventsDumpMetadata.getEventsDumpedCount(), expectedEventsCount = 0;
    String eventsBatchDirPrefix = ReplUtils.INC_EVENTS_BATCH.replaceAll("%d", "");

    List<FileStatus> batchFiles = Arrays.stream(fs.listStatus(dumpPath))
            .filter(fileStatus -> fileStatus.getPath().getName()
                    .startsWith(eventsBatchDirPrefix)).collect(Collectors.toList());


    for (FileStatus fileStatus : batchFiles) {
      int eventsPerBatch = fs.listStatus(fileStatus.getPath()).length;
      assertTrue(eventsPerBatch <= REPL_MAX_LOAD_TASKS);
      expectedEventsCount += eventsPerBatch;
    }
    assertEquals(eventsCountInAckFile, expectedEventsCount);

    // Repl Load should be agnostic of batch size and REPL_BATCH_INCREMENTAL_EVENTS config.
    // hence not passing incrementalBatchConfigs here.
    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select * from t1")
            .verifyResults(new String[]{"1", "2", "3", "4", "5"});

    assertTrue(fs.exists(new Path(dumpPath, LOAD_ACKNOWLEDGEMENT.toString())));

    ReplDumpWork.testDeletePreviousDumpMetaPath(true);
    //second round of incremental dump.
    incrementalDump = primary.run("use " + primaryDbName)
            .run("insert into t1 values(6)")
            .run("insert into t1 values(7)")
            .run("insert into t1 values(8)")
            .run("insert into t1 values(9)")
            .dump(primaryDbName, incrementalBatchConfigs);

    // simulate a failure in repl dump when batching was enabled.
    dumpPath = new Path(incrementalDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    batchFiles = Arrays.stream(fs.listStatus(dumpPath))
            .filter(fileStatus -> fileStatus.getPath().getName()
                    .startsWith(eventsBatchDirPrefix))
            .sorted(new EventDumpDirComparator()).collect(Collectors.toList());


    FileStatus lastBatch = batchFiles.get(batchFiles.size() - 1);
    Path lastBatchPath = lastBatch.getPath();

    FileStatus[] eventsOfLastBatch = fs.listStatus(lastBatchPath);
    Arrays.sort(eventsOfLastBatch, new EventDumpDirComparator());
    Map<FileStatus, Long> modificationTimes = batchFiles.stream().filter(file -> !Objects.equals(lastBatch, file))
            .collect(Collectors.toMap(Function.identity(), FileStatus::getModificationTime));

    long lastReplId = Long.parseLong(eventsOfLastBatch[0].getPath().getName()) - 1;
    ackLastEventID = new Path(dumpPath, ReplAck.EVENTS_DUMP.toString());
    eventsDumpMetadata = EventsDumpMetadata.deserialize(ackLastEventID, conf);
    eventsDumpMetadata.setLastReplId(lastReplId);
    eventsDumpMetadata.setEventsDumpedCount(eventsDumpMetadata.getEventsDumpedCount() - (eventsOfLastBatch.length - 1));
    org.apache.hadoop.hive.ql.parse.repl.dump.Utils.writeOutput(eventsDumpMetadata.serialize(),
            ackLastEventID,
            primary.hiveConf);
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), false);
    //delete all events of last batch except one.
    for (int idx = 1; idx < eventsOfLastBatch.length; idx++)
      fs.delete(eventsOfLastBatch[idx].getPath(), true);

    // when we try to resume a failed dump which was batched without setting REPL_BATCH_INCREMENTAL_EVENTS = true
    // in the next run dump should fail.
    primary.dumpFailure(primaryDbName);
    if (ReplUtils.failedWithNonRecoverableError(dumpPath, conf)) {
      fs.delete(new Path(dumpPath, ReplAck.NON_RECOVERABLE_MARKER.toString()), false);
    }

    WarehouseInstance.Tuple dumpAfterFailure = primary.dump(primaryDbName, incrementalBatchConfigs);
    //ensure dump did recover and dump location of new dump is same as the previous one.
    assertEquals(dumpAfterFailure.dumpLocation, incrementalDump.dumpLocation);

    List<FileStatus> filesAfterFailedDump = Arrays.stream(fs.listStatus(dumpPath))
            .filter(fileStatus -> fileStatus.getPath().getName()
                    .startsWith(eventsBatchDirPrefix))
            .sorted(new EventDumpDirComparator()).collect(Collectors.toList());

    //ensure all event files are dumped again.
    assertEquals(batchFiles, filesAfterFailedDump);

    //ensure last batch had events dumped and was indeed modified.
    assertNotEquals(lastBatch.getModificationTime(),
            filesAfterFailedDump.get(filesAfterFailedDump.size() - 1).getModificationTime());

    assertArrayEquals(fs.listStatus(lastBatchPath),
            fs.listStatus(filesAfterFailedDump.get(filesAfterFailedDump.size() - 1).getPath()));

    //ensure remaining batches were not modified.
    assertTrue(filesAfterFailedDump.stream()
            .filter(file -> !Objects.equals(file, lastBatch))
            .allMatch(file -> file.getModificationTime() == modificationTimes.get(file)));
    //ensure successful repl load.
    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select * from t1")
            .verifyResults(new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9"});

    ReplDumpWork.testDeletePreviousDumpMetaPath(false);
  }
  @Test
  public void testEventsDumpedCountWithFilteringOfOpenTransactions() throws Throwable {
    final int REPL_MAX_LOAD_TASKS = 5;
    List<String> incrementalBatchConfigs = Arrays.asList(
            String.format("'%s'='%s'", HiveConf.ConfVars.REPL_BATCH_INCREMENTAL_EVENTS, "true"),
            String.format("'%s'='%d'", HiveConf.ConfVars.REPL_APPROX_MAX_LOAD_TASKS, REPL_MAX_LOAD_TASKS),
            String.format("'%s'='%s'", HiveConf.ConfVars.REPL_FILTER_TRANSACTIONS, "true")
    );

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("create table t1 (id int) stored as orc tblproperties (\"transactional\"=\"true\")")
            .run("insert into table t1 values (1)")
            .dump(primaryDbName, incrementalBatchConfigs);

    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);

    replica.load(replicatedDbName, primaryDbName, incrementalBatchConfigs)
            .run("use " + replicatedDbName)
            .run("select * from t1")
            .verifyResults(new String[]{"1"});

    isMetricsEnabledForTests(true);
    MetricCollector collector = MetricCollector.getInstance();
    //incremental run
    WarehouseInstance.Tuple incrementalDump = primary.run("use " + primaryDbName)
            .run("insert into t1 values(2)")
            .run("insert into t1 values(3)")
            .run("select * from t1")  // will open a read only transaction which should be filtered.
            .run("insert into t1 values(4)")
            .run("insert into t1 values(5)")
            .dump(primaryDbName, incrementalBatchConfigs);

    ReplicationMetric metric = collector.getMetrics().getLast();
    Stage stage = metric.getProgress().getStageByName("REPL_DUMP");
    Metric eventMetric = stage.getMetricByName(ReplUtils.MetricName.EVENTS.name());
    long eventCountFromMetrics = eventMetric.getTotalCount();

    Path dumpPath = new Path(incrementalDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path ackLastEventID = new Path(dumpPath, ReplAck.EVENTS_DUMP.toString());
    EventsDumpMetadata eventsDumpMetadata = EventsDumpMetadata.deserialize(ackLastEventID, conf);

    int eventsCountInAckFile = eventsDumpMetadata.getEventsDumpedCount(), eventCountFromStagingDir = 0;

    String eventsBatchDirPrefix = ReplUtils.INC_EVENTS_BATCH.replaceAll("%d", "");
    List<FileStatus> batchFiles = Arrays.stream(fs.listStatus(dumpPath))
            .filter(fileStatus -> fileStatus.getPath().getName()
                    .startsWith(eventsBatchDirPrefix)).collect(Collectors.toList());

    for (FileStatus fileStatus : batchFiles) {
      eventCountFromStagingDir += fs.listStatus(fileStatus.getPath()).length;
    }
    // open transactions were filtered.
    assertTrue(eventCountFromStagingDir < eventCountFromMetrics);
    // ensure event count is captured appropriately in EventsDumpMetadata.
    assertEquals(eventsCountInAckFile, eventCountFromStagingDir);
  }

  @Test
  public void testResumeWorkFlow() throws Throwable {
    isMetricsEnabledForTests(true);

    MetricCollector.getInstance().getMetrics().clear();

    // Do bootstrap
    primary.run("use " + primaryDbName)
      .run("create table tb1(id int)")
      .run("insert into tb1 values(10)")
      .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);

    // incremental
    primary.run("use " + primaryDbName)
      .run("insert into tb1 values(20)")
      .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);

    // suppose this is the point of failover
    List<String> failoverConfigs = Arrays.asList("'" + HiveConf.ConfVars.HIVE_REPL_FAILOVER_START + "'='true'");
    primary.dump(primaryDbName, failoverConfigs);
    replica.load(replicatedDbName, primaryDbName, failoverConfigs);

    // let's modify replica/target after failover
    replica.run("use " + replicatedDbName)
      .run("insert into tb1 values(30),(40)")
      .run("create table tb2(id int)")
      .run("insert into tb2 values(10),(20)");

    // orchestrator will do the swapping and setting correct db params
    Map<String, String> dbParams = replica.getDatabase(replicatedDbName).getParameters();
    String lastId = dbParams.get("repl.last.id");
    String targetLastId = dbParams.get("repl.target.last.id");

    primary.run("alter database " + primaryDbName
      + " set dbproperties('repl.resume.started'='true', 'repl.source.for'='', 'repl.target" +
      ".for'='true', 'repl.last.id'='" + targetLastId + "' ,'repl.target.last.id'='" + lastId +
      "')");

    replica.run("alter database " + replicatedDbName
      + " set dbproperties('repl.target.for'='', 'repl.source.for'='p1','repl.resume" +
      ".started'='true')");

    // initiate RESET (1st cycle of optimised bootstrap)
    primary.dump(primaryDbName);

    MetricCollector collector = MetricCollector.getInstance();
    ReplicationMetric metric = collector.getMetrics().getLast();
    assertEquals(Status.RESUME_READY, metric.getProgress().getStatus());

    replica.load(replicatedDbName, primaryDbName);
    metric = collector.getMetrics().getLast();
    assertEquals(Status.RESUME_READY, metric.getProgress().getStatus());

    // this will be completion cycle for RESET
    primary.dump(primaryDbName);
    metric = collector.getMetrics().getLast();
    assertEquals(metric.getProgress()
                       .getStages()
                       .get(0)
                       .getMetrics()
                       .stream()
                       .filter(m -> Objects.equals(m.getName(), "TABLES"))
                       .findFirst()
                       .get()
                       .getTotalCount(), 1);

    replica.load(replicatedDbName, primaryDbName);
    metric = collector.getMetrics().getLast();
    assertEquals(metric.getProgress()
                       .getStages()
                       .get(0)
                       .getMetrics()
                       .stream()
                       .filter(m -> Objects.equals(m.getName(), "TABLES"))
                       .findFirst()
                       .get()
                       .getTotalCount(), 1);

    // AFTER RESET : 1. New table got dropped
    //               2. Changes made on existing table got discarded.
    replica.run("use "+ replicatedDbName)
           .run("select id from tb1")
           .verifyResults(new String[]{"10", "20"})
           .run("show tables in " + replicatedDbName)
           .verifyResults(new String[]{"tb1"});

    // verify that the db params got reset after RESUME
    Map<String, String> srcParams = primary.getDatabase(primaryDbName).getParameters();
    assertNotNull(srcParams.get("repl.source.for"));
    assertNull(srcParams.get("repl.target.for"));
    assertNull(srcParams.get("repl.resume.started"));
    assertNull(srcParams.get("repl.target.last.id"));
    assertNull(srcParams.get("repl.last.id"));

    Map<String, String> targetParams = replica.getDatabase(replicatedDbName).getParameters();
    assertTrue(Boolean.parseBoolean(targetParams.get("repl.target.for")));
    assertNull(targetParams.get("repl.source.for"));
    assertNull(targetParams.get("repl.resume.started"));
    assertNotNull(targetParams.get("repl.target.last.id"));
    assertNotNull(targetParams.get("repl.last.id"));
  }

  @Test
  public void testSizeOfDatabaseReplicationViaDistCp() throws Throwable {
    testSizeOfDatabaseReplication(true);
  }

  @Test
  public void testSizeOfDatabaseReplicationViaRegularCopy() throws Throwable {
    testSizeOfDatabaseReplication(false);

  }
  private void testSizeOfDatabaseReplication(boolean useDistcp) throws Throwable {
    isMetricsEnabledForTests(true);
    HiveConf primaryConf = primary.getConf();
    HiveConf replicaConf = replica.getConf();

    MetastoreConf.setTimeVar(primaryConf, MetastoreConf.ConfVars.REPL_METRICS_UPDATE_FREQUENCY, 10,
        TimeUnit.MINUTES);
    MetastoreConf.setTimeVar(replicaConf, MetastoreConf.ConfVars.REPL_METRICS_UPDATE_FREQUENCY, 10,
        TimeUnit.MINUTES);

    List<String> withClause = new ArrayList<>();
    withClause.add("'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='true'");
    if (useDistcp) {
      List<String> clauseForDistcp = Arrays.asList(
          "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
          "'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='false'",
          "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
          "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
              + UserGroupInformation.getCurrentUser().getUserName() + "'");
      withClause.addAll(clauseForDistcp);
    }

    List<String> managedTblList = new ArrayList<String>();

    primary.run("use " + primaryDbName);

    int pt = 2;
    for (int i = 0; i < 2; i++) {
      primary.run("CREATE TABLE ptnmgned" + i + " (a int) partitioned by (b int)");
      managedTblList.add("ptnmgned" + i);

      for (int j = 0; j < pt; j++) {
        primary.run("ALTER TABLE ptnmgned" + i + " ADD PARTITION(b=" + j + ")");
        // insert some rows in each partitions of table
        for (int k = 0; k < pt; k++) {
          primary.run("INSERT INTO TABLE ptnmgned" + i + " PARTITION(b=" + j + ") VALUES (" + k + ")");
        }
      }
    }
    // create 2 un-partitioned table
    for (int i = 0; i < 2; i++) {
      primary.run("CREATE TABLE unptnmgned" + i + " (a int)");
      managedTblList.add("unptnmgned" + i);
      //insert some rows in each tables
      for (int j = 0; j < 2; j++) {
        primary.run("INSERT INTO TABLE unptnmgned" + i + " VALUES (" + j + ")");
      }
    }
    // start bootstrap dump
    WarehouseInstance.Tuple tuple = primary.dump(primaryDbName, withClause);

    List<String> newTableList = new ArrayList<String>();
    newTableList.addAll(managedTblList);

    // start bootstrap load
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables")
        .verifyResults(newTableList);

    MetricCollector collector = MetricCollector.getInstance();
    ReplicationMetric metric = collector.getMetrics().getLast();
    double dataReplicationSizeInKB = metric.getMetadata().getReplicatedDBSizeInKB();
    assertNotEquals(0.0, dataReplicationSizeInKB);

    primary.run("use " + primaryDbName);
    // create 2 un-partitioned table for incremental dump
    for (int i = 0; i < 2; i++) {
      primary.run("CREATE TABLE incrunptnmgned" + i + "(a int)");
      managedTblList.add("incrunptnmgned" + i);
      // insert some rows in each tables
      for (int j = 0; j < 2; j++) {
        primary.run("INSERT INTO TABLE incrunptnmgned" + i + " VALUES (" + j + ")");
      }
    }

    for (int i = 0; i < 2; i++) {
      //insert some rows in each tables
      for (int j = 2; j < 4; j++) {
        primary.run("INSERT INTO TABLE unptnmgned" + i + " VALUES (" + j + ")");
      }
    }

    newTableList.clear();
    newTableList.addAll(managedTblList);

    //start incremental dump
    WarehouseInstance.Tuple newTuple = primary.dump(primaryDbName, withClause);

    //start incremental load
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables")
        .verifyResults(newTableList);

    metric = collector.getMetrics().getLast();
    dataReplicationSizeInKB = metric.getMetadata().getReplicatedDBSizeInKB();
    assertNotEquals(0.0, dataReplicationSizeInKB);

    MetastoreConf.setTimeVar(primaryConf, MetastoreConf.ConfVars.REPL_METRICS_UPDATE_FREQUENCY, 1,
        TimeUnit.MINUTES);
    MetastoreConf.setTimeVar(replicaConf, MetastoreConf.ConfVars.REPL_METRICS_UPDATE_FREQUENCY, 1,
        TimeUnit.MINUTES);
    isMetricsEnabledForTests(false);
  }

  private void runCompaction(String dbName, String tblName, String partName, CompactionType compactionType)
          throws Throwable {
    HiveConf hiveConf = new HiveConf(primary.getConf());
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    markPreviousCompactionsAsComplete(txnHandler, hiveConf);
    CompactionRequest rqst = new CompactionRequest(dbName, tblName, compactionType);
    rqst.setPartitionname(partName);
    txnHandler.compact(rqst);
    hiveConf.setBoolVar(HiveConf.ConfVars.COMPACTOR_CRUD_QUERY_BASED, false);
    runWorker(hiveConf);
    runCleaner(hiveConf);
  }

  private void markPreviousCompactionsAsComplete(TxnStore txnHandler, HiveConf conf) throws Throwable {
    Connection conn = TestTxnDbUtil.getConnection(conf);
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("select CQ_ID from COMPACTION_QUEUE");
    List<Long> openCompactionIds = new ArrayList<>();
    while (rs.next()) {
      openCompactionIds.add(rs.getLong(1));
    }
    openCompactionIds.forEach(id->{
      CompactionInfoStruct compactionInfoStruct = new CompactionInfoStruct();
      compactionInfoStruct.setId(id);
      CompactionInfo compactionInfo = CompactionInfo.compactionStructToInfo(compactionInfoStruct);
      try {
        txnHandler.markCompacted(compactionInfo);
      } catch (MetaException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private FileStatus[] getDirsInTableLoc(WarehouseInstance wh, String db, String table) throws Throwable {
    Path tblLoc = new Path(wh.getTable(db, table).getSd().getLocation());
    FileSystem fs = tblLoc.getFileSystem(wh.getConf());
    return fs.listStatus(tblLoc, EximUtil.getDirectoryFilter(fs));
  }

  private FileStatus[] getDirsInPartitionLoc(WarehouseInstance wh, Partition partition)
          throws Throwable {
    Path tblLoc = new Path(partition.getSd().getLocation());
    FileSystem fs = tblLoc.getFileSystem(wh.getConf());
    return fs.listStatus(tblLoc, EximUtil.getDirectoryFilter(fs));
  }

  private long getMinorCompactedTxnId(FileStatus[] fileStatuses) {
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.getPath().getName().startsWith(AcidUtils.DELTA_PREFIX)) {
        AcidUtils.ParsedDeltaLight delta = AcidUtils.ParsedDelta.parse(fileStatus.getPath());
        if (delta.getVisibilityTxnId() != 0) {
          return delta.getVisibilityTxnId();
        }
      }
    }
    return -1;
  }

  private long getMajorCompactedWriteId(FileStatus[] fileStatuses, boolean replica) {
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.getPath().getName().startsWith(AcidUtils.BASE_PREFIX)) {
        AcidUtils.ParsedBaseLight pbl = AcidUtils.ParsedBase.parseBase(fileStatus.getPath());
        long writeId = pbl.getWriteId();
        if (replica) {
          assertEquals(0, pbl.getVisibilityTxnId());
        }
        return writeId;
      }
    }
    return -1;
  }

  @Test
  public void testAcidTablesBootstrapWithMajorCompaction() throws Throwable {
    String tableName = testName.getMethodName();
    String tableNamepart = testName.getMethodName() + "_part";
    primary.run("use " + primaryDbName)
            .run("create table " + tableName + " (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into " + tableName + " values(1)")
            .run("insert into " + tableName + " values(2)")
            .run("create table " + tableNamepart + " (id int) partitioned by (part int) clustered by(id) " +
                    "into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\") ")
            .run("insert into " + tableNamepart + " values(1, 2)")
            .run("insert into " + tableNamepart + " values(2, 2)");
    runCompaction(primaryDbName, tableName, null, CompactionType.MAJOR);

    List<Partition> partList = primary.getAllPartitions(primaryDbName, tableNamepart);
    for (Partition part : partList) {
      Table tbl = primary.getTable(primaryDbName, tableNamepart);
      String partName = Warehouse.makePartName(tbl.getPartitionKeys(), part.getValues());
      runCompaction(primaryDbName, tableNamepart, partName, CompactionType.MAJOR);
    }

    List<String> withClause = Arrays.asList(
            "'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='true'");
    WarehouseInstance.Tuple dump = primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause);
    replica.run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{tableName, tableNamepart})
            .run("repl status " + replicatedDbName)
            .verifyResult(dump.lastReplicationId)
            .run("select id from " + tableName + " order by id")
            .verifyResults(new String[]{"1", "2"})
            .run("select id from " + tableNamepart + " order by id")
            .verifyResults(new String[]{"1", "2"});

    FileStatus[] fileStatuses = getDirsInTableLoc(primary, primaryDbName, tableName);
    long writeId = getMajorCompactedWriteId(fileStatuses, false);
    assertTrue(writeId != -1);

    fileStatuses = getDirsInTableLoc(replica, replicatedDbName, tableName);
    // replica write id should be same as source write id.
    assertEquals(writeId, getMajorCompactedWriteId(fileStatuses, true));

    // check for partitioned table.
    for (Partition part : partList) {
      fileStatuses = getDirsInPartitionLoc(primary, part);
      writeId = getMajorCompactedWriteId(fileStatuses, false);
      assertTrue(writeId != -1);
      Partition partReplica = replica.getPartition(replicatedDbName, tableNamepart, part.getValues());
      fileStatuses = getDirsInPartitionLoc(replica, partReplica);
      assertEquals(writeId, getMajorCompactedWriteId(fileStatuses, true));
    }
  }

  @Test
  public void testAcidTablesBootstrapWithMinorCompaction() throws Throwable {
    String tableName = testName.getMethodName();
    String tableNamepart = testName.getMethodName() + "_part";
    primary.run("use " + primaryDbName)
            .run("create table " + tableName + " (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into " + tableName + " values(1)")
            .run("insert into " + tableName + " values(2)")
            .run("create table " + tableNamepart + " (id int) partitioned by (part int) clustered by(id) " +
                    "into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\") ")
            .run("insert into " + tableNamepart + " values(1, 2)")
            .run("insert into " + tableNamepart + " values(2, 2)");

    runCompaction(primaryDbName, tableName, null, CompactionType.MINOR);

    List<Partition> partList = primary.getAllPartitions(primaryDbName, tableNamepart);
    for (Partition part : partList) {
      Table tbl = primary.getTable(primaryDbName, tableNamepart);
      String partName = Warehouse.makePartName(tbl.getPartitionKeys(), part.getValues());
      runCompaction(primaryDbName, tableNamepart, partName, CompactionType.MINOR);
    }
    List<String> withClause = Arrays.asList(
            "'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='true'");

    WarehouseInstance.Tuple dump = primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause);
    replica.run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{tableName, tableNamepart})
            .run("repl status " + replicatedDbName)
            .verifyResult(dump.lastReplicationId)
            .run("select id from " + tableName + " order by id")
            .verifyResults(new String[]{"1", "2"})
            .run("select id from " + tableNamepart + " order by id")
            .verifyResults(new String[]{"1", "2"});

    FileStatus[] fileStatuses = getDirsInTableLoc(primary, primaryDbName, tableName);
    assertTrue(-1 != getMinorCompactedTxnId(fileStatuses));

    fileStatuses = getDirsInTableLoc(replica, replicatedDbName, tableName);
    Assert.assertEquals(-1, getMinorCompactedTxnId(fileStatuses));

    // check for partitioned table.
    for (Partition part : partList) {
      fileStatuses = getDirsInPartitionLoc(primary, part);
      assertTrue(-1 != getMinorCompactedTxnId(fileStatuses));
      Partition partReplica = replica.getPartition(replicatedDbName, tableNamepart, part.getValues());
      fileStatuses = getDirsInPartitionLoc(replica, partReplica);
      assertTrue(-1 == getMinorCompactedTxnId(fileStatuses));
    }
  }

}
