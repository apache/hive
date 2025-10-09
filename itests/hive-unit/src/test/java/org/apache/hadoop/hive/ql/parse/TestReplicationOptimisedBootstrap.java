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

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.repl.ReplAck;
import org.apache.hadoop.hive.ql.parse.repl.load.FailoverMetaData;
import org.apache.hadoop.hive.ql.parse.repl.metric.MetricCollector;
import org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.ReplicationMetric;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Stage;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Metric;
import static org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector.isMetricsEnabledForTests;

import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.QUOTA_DONT_SET;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.QUOTA_RESET;
import static org.apache.hadoop.hive.common.repl.ReplConst.REPL_ENABLE_BACKGROUND_THREAD;
import static org.apache.hadoop.hive.common.repl.ReplConst.REPL_TARGET_DB_PROPERTY;
import static org.apache.hadoop.hive.common.repl.ReplConst.REPL_FAILOVER_ENDPOINT;
import static org.apache.hadoop.hive.common.repl.ReplConst.TARGET_OF_REPLICATION;
import static org.apache.hadoop.hive.metastore.ReplChangeManager.SOURCE_OF_REPLICATION;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.EVENT_ACK_FILE;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.TABLE_DIFF_COMPLETE_DIRECTORY;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.TABLE_DIFF_INPROGRESS_DIRECTORY;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.getPathsFromTableFile;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.getTablesFromTableDiffFile;

import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.INC_BOOTSTRAP_ROOT_DIR_NAME;
import static org.apache.hadoop.hive.ql.parse.ReplicationSpec.KEY.CURR_STATE_ID_SOURCE;
import static org.apache.hadoop.hive.ql.parse.ReplicationSpec.KEY.CURR_STATE_ID_TARGET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestReplicationOptimisedBootstrap extends BaseReplicationScenariosAcidTables {

  String extraPrimaryDb;
  HiveConf primaryConf;
  TxnStore txnHandler;
  List<Long> tearDownTxns = new ArrayList<>();
  List<Long> tearDownLockIds = new ArrayList<>();

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    HashMap<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());
    overrides.put(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY.varname, "false");
    overrides.put(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname, "true");
    overrides.put(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname, UserGroupInformation.getCurrentUser().getUserName());
    overrides.put(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname, "true");
    overrides.put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
    overrides.put("hive.in.repl.test", "true");
    internalBeforeClassSetup(overrides, TestReplicationOptimisedBootstrap.class);
  }

  @Before
  public void setup() throws Throwable {
    super.setup();
    extraPrimaryDb = "extra_" + primaryDbName;
    primaryConf = primary.getConf();
    txnHandler = TxnUtils.getTxnStore(primary.getConf());
  }

  @After
  public void tearDown() throws Throwable {
    if (!tearDownTxns.isEmpty()) {
      //Abort the left out transactions which might not be completed due to some test failures.
      txnHandler.abortTxns(new AbortTxnsRequest(tearDownTxns));
    }
    //Release the unreleased locks acquired during tests. Although, we specifically release the locks when not required.
    //But there may be case when test failed and locks are left in dangling state.
    releaseLocks(txnHandler, tearDownLockIds);
    primary.run("drop database if exists " + extraPrimaryDb + " cascade");
    super.tearDown();
  }

  @Test
  public void testBuildTableDiffGeneration() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + primary.repldDir + "'");
    // Create two external & two managed tables and do a bootstrap dump & load.
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create external table t1 (id int)")
        .run("insert into table t1 values (1)")
        .run("insert into table t1 values (2),(3),(4)")
        .run("create external table t2 (place string) partitioned by (country string)")
        .run("insert into table t2 partition(country='india') values ('chennai')")
        .run("insert into table t2 partition(country='us') values ('new york')")
        .run("create table t1_managed (id int) clustered by(id) into 3 buckets stored as orc " +
                "tblproperties (\"transactional\"=\"true\")")
        .run("insert into table t1_managed values (10)")
        .run("insert into table t1_managed values (20),(31),(42)")
        .run("create table t2_managed (place string) partitioned by (country string)")
        .run("insert into table t2_managed partition(country='india') values ('bangalore')")
        .run("insert into table t2_managed partition(country='us') values ('austin')")
        .dump(primaryDbName, withClause);

    // Do the bootstrap load and check all the external & managed tables are present.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("use " + replicatedDbName)
        .run("show tables")
        .verifyResults(new String[]{"t1", "t2", "t1_managed", "t2_managed"})
        .verifyReplTargetProperty(replicatedDbName);

    // Do an incremental dump & load, Add one table which we can drop & an empty table as well.
    tuple = primary.run("use " + primaryDbName)
        .run("create table t5_managed (id int)")
        .run("insert into table t5_managed values (110)")
        .run("insert into table t5_managed values (110)")
        .run("create table t6_managed (id int)")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables")
        .verifyResults(new String[]{"t1", "t2", "t1_managed", "t2_managed", "t5_managed", "t6_managed"})
        .verifyReplTargetProperty(replicatedDbName);

    // Do some modifications on other database with similar table names &  some modifications on original source
    // cluster.
    primary.run("create database " + extraPrimaryDb)
        .run("use " + extraPrimaryDb)
        .run("create external table t1 (id int)")
        .run("create table t1_managed (id int)")
        .run("use " + primaryDbName)
        .run("create external table t4 (id int)")
        .run("insert into table t4 values (100)")
        .run("insert into table t4 values (201)")
        .run("create table t4_managed (id int) clustered by(id) into 3 buckets stored as orc " +
                "tblproperties (\"transactional\"=\"true\")")
        .run("insert into table t4_managed values (110)")
        .run("insert into table t4_managed values (220)")
        .run("insert into table t2 partition(country='france') values ('lyon')")
        .run("insert into table t2_managed partition(country='france') values ('nice')")
        .run("alter table t6_managed add columns (name string)")
        .run("drop table t5_managed");

    // Prepare for reverse replication.
    DistributedFileSystem replicaFs = replica.miniDFSCluster.getFileSystem();
    Path newReplDir = new Path(replica.repldDir + "1");
    replicaFs.mkdirs(newReplDir);
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + newReplDir + "'");

    // Do a reverse dump
    tuple = replica.dump(replicatedDbName, withClause);

    // Check the event ack file got created.
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));

    // Check in case the dump isn't consumed, and we attempt a dump again, that gets skipped and the dump directory
    // doesn't change, without any errors.

    Path dumpPath = new Path(tuple.dumpLocation);
    ContentSummary beforeContentSummary = replicaFs.getContentSummary(dumpPath.getParent());
    WarehouseInstance.Tuple emptyTuple = replica.dump(replicatedDbName, withClause);
    assertTrue(emptyTuple.dumpLocation.isEmpty());
    assertTrue(emptyTuple.lastReplicationId.isEmpty());
    ContentSummary afterContentSummary = replicaFs.getContentSummary(dumpPath.getParent());
    assertEquals(beforeContentSummary.getFileAndDirectoryCount(), afterContentSummary.getFileAndDirectoryCount());

    // Check the event ack file stays intact, despite having a skipped dump.
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString(),
        replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));

    // Do a load, this should create a table_diff_complete directory
    primary.load(primaryDbName,replicatedDbName, withClause);

    // Check the table diff directory exist.
    assertTrue(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY)));

    // Check the table diff has all the modified table, including the dropped and empty ones
    HashSet<String> tableDiffEntries = getTablesFromTableDiffFile(dumpPath, conf);
    assertTrue("Table Diff Contains " + tableDiffEntries, tableDiffEntries
        .containsAll(Arrays.asList("t4", "t2", "t4_managed", "t2_managed", "t5_managed", "t6_managed")));

    // Do a load again and see, nothing changes as this load isn't consumed.
    beforeContentSummary = replicaFs.getContentSummary(dumpPath.getParent());
    primary.load(primaryDbName, replicatedDbName, withClause);
    assertTrue("Table Diff Contains " + tableDiffEntries, tableDiffEntries
        .containsAll(Arrays.asList("t4", "t2", "t4_managed", "t2_managed", "t5_managed", "t6_managed")));
    afterContentSummary = replicaFs.getContentSummary(dumpPath.getParent());
    assertEquals(beforeContentSummary.getFileAndDirectoryCount(), afterContentSummary.getFileAndDirectoryCount());

    // Check there are entries in the table files.
    assertFalse(getPathsFromTableFile("t4", dumpPath, conf).isEmpty());
    assertFalse(getPathsFromTableFile("t2", dumpPath, conf).isEmpty());
    assertFalse(getPathsFromTableFile("t4_managed", dumpPath, conf).isEmpty());
    assertFalse(getPathsFromTableFile("t2_managed", dumpPath, conf).isEmpty());

    // Check the dropped and empty tables.
    assertTrue(getPathsFromTableFile("t5_managed", dumpPath, conf).isEmpty());
    assertTrue(getPathsFromTableFile("t6_managed", dumpPath, conf).size() == 1);
  }

  @Test
  public void testEmptyDiffForControlFailover() throws Throwable {

    // In case of control failover both A & B will be in sync, so the table diff should be created empty, without any
    // error.
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + primary.repldDir + "'");

    // Do a bootstrap cycle(A->B)
    primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause);

    // Add some tables & do a incremental dump.
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create external table t1 (id int)")
        .run("insert into table t1 values (100),(200)")
        .run("insert into table t1 values (12),(35),(46)")
        .run("create  table t1_managed (id int)")
        .run("insert into table t1_managed values (120)")
        .run("insert into table t1_managed values (10),(321),(423)")
        .dump(primaryDbName, withClause);

    // Do an incremental load and see all the tables are there.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId).run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("show tables like 't1_managed'")
        .verifyResult("t1_managed");

    // Trigger reverse cycle. Do dump on target cluster.
    DistributedFileSystem replicaFs = replica.miniDFSCluster.getFileSystem();
    Path newReplDir = new Path(replica.repldDir + "rev");
    replicaFs.mkdirs(newReplDir);
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + newReplDir + "'");

    // Do a reverse dump
    tuple = replica.dump(replicatedDbName, withClause);

    // Even though no diff, the event ack file should be created.
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));

    // Do a reverse load.
    primary.load(primaryDbName, replicatedDbName, withClause);

    // Check the table diff directory still gets created.
    assertTrue(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY)));

    // Check the table diff is empty, since we are in sync, so no tables got modified.
    HashSet<String> tableDiffEntries = getTablesFromTableDiffFile(new Path(tuple.dumpLocation), conf);
    assertEquals("Table diff is not empty, contains :" + tableDiffEntries, 0, tableDiffEntries.size());
  }

  @Test
  public void testFirstIncrementalMandatory() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + primary.repldDir + "'");
    // Create one external and one managed tables and do a bootstrap dump.
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create external table t1 (id int)")
        .run("insert into table t1 values (1)")
        .run("insert into table t1 values (2),(3),(4)")
        .run("create  table t1_managed (id int)")
        .run("insert into table t1_managed values (10)")
        .run("insert into table t1_managed values (20),(31),(42)")
        .dump(primaryDbName, withClause);

    // Do a bootstrap load and check both managed and external tables are loaded.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId).run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("show tables like 't1_managed'")
        .verifyResult("t1_managed")
        .verifyReplTargetProperty(replicatedDbName);

    // Trigger reverse dump just after the bootstrap cycle.
    DistributedFileSystem replicaFs = replica.miniDFSCluster.getFileSystem();
    Path newReplDir = new Path(replica.repldDir + "1");
    replicaFs.mkdirs(newReplDir);
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + newReplDir + "'");

    // Do a dump on cluster B, it should throw an exception, since the first incremental isn't done yet.
    try {
      replica.dump(replicatedDbName, withClause);
    } catch (HiveException he) {
      assertTrue(he.getMessage()
          .contains("Replication dump not allowed for replicated database with first incremental dump pending : " + replicatedDbName));
    }

    // Do a incremental cycle and check we don't get this exception.
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + primary.repldDir + "'");
    primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause);

    // Retrigger reverse dump, this time it should be successful and event ack should get created.
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + newReplDir + "'");

    tuple = replica.dump(replicatedDbName, withClause);

    // Check event ack file should get created.
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));
  }

  @Test
  public void testFailureCasesInTableDiffGeneration() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + primary.repldDir + "'");

    // Do a bootstrap cycle(A->B)
    primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause);

    // Add some table & do an incremental dump.
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create external table t1 (id int)")
        .run("insert into table t1 values (1)")
        .run("create  table t1_managed (id string)")
        .run("insert into table t1_managed values ('A')")
        .dump(primaryDbName, withClause);

    // Do an incremental load and check the tables are there.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId).run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("show tables like 't1_managed'")
        .verifyResult("t1_managed")
        .verifyReplTargetProperty(replicatedDbName);

    // Do some modifications on the source cluster, so we have some entries in the table diff.
    primary.run("use " + primaryDbName)
        .run("create table t2_managed (id string)")
        .run("insert into table t1_managed values ('S')")
        .run("insert into table t2_managed values ('A'),('B'),('C')");

    // Do some modifications in another database to have unrelated events as well after the last load, which should
    // get filtered.

    primary.run("create database " + extraPrimaryDb)
        .run("use " + extraPrimaryDb)
        .run("create external table t1 (id int)")
        .run("insert into table t1 values (15),(1),(96)")
        .run("create  table t1_managed (id string)")
        .run("insert into table t1_managed values ('SA'),('PS')");

    // Prepare for reverse replication.
    DistributedFileSystem replicaFs = replica.miniDFSCluster.getFileSystem();
    Path newReplDir = new Path(replica.repldDir + "reverse");
    replicaFs.mkdirs(newReplDir);
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + newReplDir + "'");

    // Trigger dump on target cluster.

    replicaFs.setQuota(newReplDir, 1, 10000);
    try {
      tuple = replica.dump(replicatedDbName, withClause);
      fail("Should have failed due to quota violation");
    } catch (Exception e) {
      // Ignore it is expected due to Quota violation.
    }
    // Check the event_ack file doesn't exist.
    assertFalse("event ack file exists despite quota violation", replicaFs.listFiles(newReplDir, true).hasNext());

    // Set the quota to a value that makes sure event ack file gets created and then fails
    replicaFs.setQuota(newReplDir, replicaFs.getQuotaUsage(newReplDir).getFileAndDirectoryCount() + 3, QUOTA_RESET);
    try {
      tuple = replica.dump(replicatedDbName, withClause);
      fail("Should have failed due to quota violation");
    } catch (Exception e) {
      // Ignore it is expected due to Quota violation.
    }

    // Check the event ack file got created despite exception and failure.
    assertEquals("event_ack", replicaFs.listFiles(newReplDir, true).next().getPath().getName());

    // Remove quota for a successful dump
    replicaFs.setQuota(newReplDir, QUOTA_RESET, QUOTA_RESET);

    // Retry Dump
    tuple = replica.dump(replicatedDbName, withClause);

    // Check event ack file is there.
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));

    // Set quota again to restrict creation of table diff in middle during load.
    replicaFs.setQuota(newReplDir, replicaFs.getQuotaUsage(newReplDir).getFileAndDirectoryCount() + 2, QUOTA_RESET);

    try {
      primary.load(primaryDbName, replicatedDbName, withClause);
    } catch (Exception e) {
      // Ignore, expected due to quota violation.
    }

    // Check table diff in progress directory gets created.
    assertTrue(new Path(tuple.dumpLocation, TABLE_DIFF_INPROGRESS_DIRECTORY).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, TABLE_DIFF_INPROGRESS_DIRECTORY)));

    // Check table diff complete directory doesn't gets created.
    assertFalse(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY).toString() + " exist",
        replicaFs.exists(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY)));

    // Set Quota to a value so that table diff complete gets created and we fail post that.
    replicaFs.setQuota(newReplDir, replicaFs.getQuotaUsage(newReplDir).getFileAndDirectoryCount() + 1, QUOTA_RESET);
    try {
      primary.load(primaryDbName, replicatedDbName, withClause);
      fail("Expected failure due to quota violation");
    } catch (Exception e) {
      // Ignore, expected due to quota violation.
    }

    // Check table diff complete directory gets created.
    assertTrue(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY)));

    // Remove the quota and see everything recovers.
    replicaFs.setQuota(newReplDir, QUOTA_RESET, QUOTA_RESET);
    primary.load(primaryDbName, replicatedDbName, withClause);

    // Check table diff in complete directory gets created.
    assertTrue(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY)));

    // Check table diff in progress directory isn't there now.
    assertFalse(new Path(tuple.dumpLocation, TABLE_DIFF_INPROGRESS_DIRECTORY).toString() + " exist",
        replicaFs.exists(new Path(tuple.dumpLocation, TABLE_DIFF_INPROGRESS_DIRECTORY)));

    // Check the entries in table diff are correct.
    HashSet<String> tableDiffEntries = getTablesFromTableDiffFile(new Path(tuple.dumpLocation), conf);
    assertTrue("Table Diff Contains " + tableDiffEntries,
        tableDiffEntries.containsAll(Arrays.asList("t1_managed", "t2_managed")));
  }

  @Test
  public void testReverseReplicationFailureWhenSourceDbIsDropped() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + primary.repldDir + "'");

    // Do a bootstrap cycle.
    primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause);

    // Create 1 managed table and do a dump & load.
    WarehouseInstance.Tuple tuple =
      primary.run("use " + primaryDbName)
        .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
          "tblproperties (\"transactional\"=\"true\")")
        .run("insert into table t1 values (1)")
        .dump(primaryDbName, withClause);

    // Do the load and check tables is present.
    replica.load(replicatedDbName, primaryDbName, withClause)
          .run("repl status " + replicatedDbName)
          .verifyResult(tuple.lastReplicationId)
          .run("use " + replicatedDbName)
          .run("show tables like 't1'")
          .verifyResult("t1")
          .verifyReplTargetProperty(replicatedDbName);

    // suppose source database got dropped before initiating reverse replication( B -> A )
    primary.run("alter database " + primaryDbName + " set dbproperties('repl.source.for'='')")
           .run("drop database "+ primaryDbName +" cascade");

    // Do some modifications on the target cluster. (t1)
    replica.run("use " + replicatedDbName)
           .run("insert into table t1 values (101)")
           .run("insert into table t1 values (210),(321)");

    // Prepare for reverse replication.
    DistributedFileSystem replicaFs = replica.miniDFSCluster.getFileSystem();
    Path newReplDir = new Path(replica.repldDir + "1");
    replicaFs.mkdirs(newReplDir);
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + newReplDir + "'");

    // Do a reverse dump, this should create event_ack file
    tuple = replica.dump(replicatedDbName, withClause);

    // Check the event ack file got created.
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
      replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));

    // this load should throw exception
    List<String> finalWithClause = withClause;
    assertThrows("Should fail with db doesn't exist exception", SemanticException.class, () -> {
      primary.load(primaryDbName, replicatedDbName, finalWithClause);
    });
  }

  @Test
  public void testReverseBootstrap() throws Throwable {
    List<String> withClause = setUpFirstIterForOptimisedBootstrap();

    // Open 3 txns for Database which is not under replication
    int numTxnsForSecDb = 3;
    List<Long> txnsForSecDb = openTxns(numTxnsForSecDb, txnHandler, primaryConf);
    tearDownTxns.addAll(txnsForSecDb);

    Map<String, Long> tablesInSecDb = new HashMap<>();
    tablesInSecDb.put("t1", (long) numTxnsForSecDb + 4);
    tablesInSecDb.put("t2", (long) numTxnsForSecDb + 4);
    List<Long> lockIdsForSecDb = allocateWriteIdsForTablesAndAcquireLocks(primaryDbName + "_extra",
            tablesInSecDb, txnHandler, txnsForSecDb, primaryConf);
    tearDownLockIds.addAll(lockIdsForSecDb);

    //Open 2 txns for Primary Db
    int numTxnsForPrimaryDb = 2;
    List<Long> txnsForSourceDb = openTxns(numTxnsForPrimaryDb, txnHandler, primaryConf);
    tearDownTxns.addAll(txnsForSourceDb);

    // Allocate write ids for both tables of source database.
    Map<String, Long> tablesInSourceDb = new HashMap<>();
    tablesInSourceDb.put("t1", (long) numTxnsForPrimaryDb + 6);
    tablesInSourceDb.put("t2", (long) numTxnsForPrimaryDb);
    List<Long> lockIdsForSourceDb = allocateWriteIdsForTablesAndAcquireLocks(replicatedDbName, tablesInSourceDb, txnHandler,
            txnsForSourceDb, replica.getConf());
    tearDownLockIds.addAll(lockIdsForSourceDb);

    //Open 1 txn with no hive locks acquired
    List<Long> txnsWithNoLocks = openTxns(1, txnHandler, primaryConf);
    tearDownTxns.addAll(txnsWithNoLocks);

    // Do a reverse second dump, this should do a bootstrap dump for the tables in the table_diff and incremental for
    // rest.
    List<Long> allReplCreatedTxnsOnSource = getReplCreatedTxns();
    tearDownTxns.addAll(allReplCreatedTxnsOnSource);

    assertTrue("value1".equals(primary.getDatabase(primaryDbName).getParameters().get("key1")));
    WarehouseInstance.Tuple tuple = replica.dump(replicatedDbName, withClause);

    verifyAllOpenTxnsAborted(allReplCreatedTxnsOnSource, primaryConf);

    //Verify that openTxns for sourceDb were aborted before proceeding with bootstrap dump.
    verifyAllOpenTxnsAborted(txnsForSourceDb, primaryConf);
    verifyAllOpenTxnsNotAborted(txnsForSecDb, primaryConf);
    verifyAllOpenTxnsNotAborted(txnsWithNoLocks, primaryConf);
    txnHandler.abortTxns(new AbortTxnsRequest(txnsForSecDb));
    txnHandler.abortTxns(new AbortTxnsRequest(txnsForSecDb));
    txnHandler.abortTxns(new AbortTxnsRequest(txnsWithNoLocks));
    releaseLocks(txnHandler, lockIdsForSecDb);
    releaseLocks(txnHandler, lockIdsForSecDb);

    String hiveDumpDir = tuple.dumpLocation + File.separator + ReplUtils.REPL_HIVE_BASE_DIR;
    // _bootstrap directory should be created as bootstrap enabled on external tables.
    Path dumpPath1 = new Path(hiveDumpDir, INC_BOOTSTRAP_ROOT_DIR_NAME +"/" + EximUtil.METADATA_PATH_NAME +"/" + replicatedDbName);
    FileStatus[] listStatus = dumpPath1.getFileSystem(conf).listStatus(dumpPath1);
    ArrayList<String> tablesBootstrapped = new ArrayList<String>();
    for (FileStatus file : listStatus) {
      tablesBootstrapped.add(file.getPath().getName());
    }

    assertTrue(tablesBootstrapped.containsAll(Arrays.asList("t1", "t2", "t3")));

    // Get source or replica database properties and verify replication metrics properties
    Map<String, String> sourceParams = replica.getDatabase(replicatedDbName).getParameters();
    verifyReplicationMetricsStatistics(sourceParams, 1, 1, ReplConst.FailoverType.UNPLANNED.toString());

    // Do a reverse load, this should do a bootstrap load for the tables in table_diff and incremental for the rest.
    primary.load(primaryDbName, replicatedDbName, withClause);

    assertFalse("value1".equals(primary.getDatabase(primaryDbName).getParameters().get("key1")));

    primary.run("use " + primaryDbName)
        .run("select id from t1")
        .verifyResults(new String[] { "1", "2", "3", "4", "101", "210", "321" })
        .run("select place from t2 where country = 'india'")
        .verifyResults(new String[] { "delhi", "chennai" })
        .run("select place from t2 where country = 'france'")
        .verifyFailure(new String[] { "lyon" })
        .run("select id from t3")
        .verifyResults(new String[] { "10", "20", "31", "42", "11" })
        .run("select place from t4 where country = 'india'")
        .verifyResults(new String[] { "bangalore", "lucknow" })
        .run("select place from t5 where country = 'china'")
        .verifyResults(new String[] { "beejing" });

    // Get target or primary database properties and verify replication metrics properties
    Map<String, String> targetParams = primary.getDatabase(primaryDbName).getParameters();
    verifyReplicationMetricsStatistics(targetParams, 1, 1, ReplConst.FailoverType.UNPLANNED.toString());

    // Check the properties on the new target database.
    assertTrue(targetParams.containsKey(TARGET_OF_REPLICATION));
    assertFalse(targetParams.containsKey(SOURCE_OF_REPLICATION));

    // Check the properties on the new source database.
    assertFalse(sourceParams.containsKey(TARGET_OF_REPLICATION));
    assertFalse(sourceParams.containsKey(CURR_STATE_ID_TARGET.toString()));
    assertFalse(sourceParams.containsKey(CURR_STATE_ID_SOURCE.toString()));
    assertFalse(sourceParams.containsKey(REPL_TARGET_DB_PROPERTY));
    assertTrue(sourceParams.containsKey(SOURCE_OF_REPLICATION));
    assertFalse(sourceParams.containsKey(ReplConst.REPL_ENABLE_BACKGROUND_THREAD));

    // Proceed with normal incremental flow, post optimised bootstrap is over.
    replica.run("use " + replicatedDbName)
        .run("insert into table t1 values (98)")
        .run("insert into table t2 partition(country='england') values ('london')")
        .run("insert into table t2 partition(country='india') values ('jaipur')")
        .run("insert into table t3 values (15),(16)")
        .run("drop table t4")
        .run("insert into table t5 partition(country='china') values ('chengdu')")
        .dump(replicatedDbName, withClause);

    // Do load and check if the data gets loaded.
    primary.load(primaryDbName, replicatedDbName, withClause)
        .run("select id from t1")
        .verifyResults(new String[] { "1", "2", "3", "4", "101", "210", "321", "98" })
        .run("select place from t2 where country = 'england'")
        .verifyResults(new String[] { "london" })
        .run("select place from t2 where country = 'india'")
        .verifyResults(new String[] { "delhi", "chennai", "jaipur" })
        .run("select id from t3")
        .verifyResults(new String[] { "10", "20", "31", "42", "11", "15", "16" })
        .run("show tables like 't4'")
        .verifyFailure(new String[]{"t4"})
        .run("select place from t5 where country = 'china'")
        .verifyResults(new String[] { "beejing", "chengdu" });
  }

  @Test
  public void testReverseBootstrapWithFailedIncremental() throws Throwable {
    List<String> withClause = setUpFirstIterForOptimisedBootstrap();
    WarehouseInstance.Tuple tuple;

    // Do a reverse second dump, this should do a bootstrap dump for the tables in the table_diff and incremental for
    // rest.
    Path replDir = new Path(replica.repldDir + "1");
    DistributedFileSystem dfs = (DistributedFileSystem) replDir.getFileSystem(replica.getConf());
    QuotaUsage quotaUsage = dfs.getQuotaUsage(replDir);
    dfs.setQuota(replDir, quotaUsage.getFileAndDirectoryCount() + 4, QUOTA_DONT_SET);

    try {
      tuple = replica.dump(replicatedDbName, withClause);
      fail("Expected the dump to fail due to quota violation in middle");
    } catch (Exception e) {
      // expected
    }

    // Remove the quota & retry the dump, this time it should be successful
    dfs.setQuota(replDir, QUOTA_RESET, QUOTA_RESET);

    tuple = replica.dump(replicatedDbName, withClause);

    String hiveDumpDir = tuple.dumpLocation + File.separator + ReplUtils.REPL_HIVE_BASE_DIR;
    // _bootstrap directory should be created as bootstrap enabled on external tables.
    Path dumpPath1 = new Path(hiveDumpDir, INC_BOOTSTRAP_ROOT_DIR_NAME +"/metadata/" + replicatedDbName);
    FileStatus[] listStatus = dumpPath1.getFileSystem(conf).listStatus(dumpPath1);
    ArrayList<String> tablesBootstrapped = new ArrayList<String>();
    for (FileStatus file : listStatus) {
      tablesBootstrapped.add(file.getPath().getName());
    }

    assertTrue(tablesBootstrapped.containsAll(Arrays.asList("t1", "t2", "t3")));

    // Do a reverse load, this should do a bootstrap load for the tables in table_diff and incremental for the rest.

    dfs.setQuota(replDir, quotaUsage.getFileAndDirectoryCount() + 1, QUOTA_DONT_SET);

    try {
      primary.load(primaryDbName, replicatedDbName, withClause);
      fail("Expected incremental load to fail due to quota violation");
    } catch (Exception e) {
      // expected
    }

    // Reset the quota & retry load
    dfs.setQuota(replDir, QUOTA_RESET, QUOTA_RESET);

    primary.load(primaryDbName, replicatedDbName, withClause);

    // Check the data is correct
    primary.run("use " + primaryDbName)
        .run("select id from t1")
        .verifyResults(new String[] { "1", "2", "3", "4", "101", "210", "321" })
        .run("select place from t2 where country = 'india'")
        .verifyResults(new String[] { "delhi", "chennai" })
        .run("select place from t2 where country = 'france'")
        .verifyFailure(new String[] { "lyon" })
        .run("select id from t3")
        .verifyResults(new String[] { "10", "20", "31", "42", "11" })
        .run("select place from t4 where country = 'india'")
        .verifyResults(new String[] { "bangalore", "lucknow" })
        .run("select place from t5 where country = 'china'")
        .verifyResults(new String[] { "beejing" });

    // Check for correct db Properties set.

    Map<String, String> targetParams = primary.getDatabase(primaryDbName).getParameters();
    Map<String, String> sourceParams = replica.getDatabase(replicatedDbName).getParameters();

    // Check the properties on the new target database.
    assertTrue(targetParams.containsKey(TARGET_OF_REPLICATION));
    assertTrue(targetParams.containsKey(CURR_STATE_ID_TARGET.toString()));
    assertTrue(targetParams.containsKey(CURR_STATE_ID_SOURCE.toString()));
    assertFalse(targetParams.containsKey(SOURCE_OF_REPLICATION));

    // Check the properties on the new source database.
    assertFalse(sourceParams.containsKey(TARGET_OF_REPLICATION));
    assertFalse(sourceParams.containsKey(CURR_STATE_ID_TARGET.toString()));
    assertFalse(sourceParams.containsKey(CURR_STATE_ID_SOURCE.toString()));
    assertFalse(sourceParams.containsKey(REPL_TARGET_DB_PROPERTY));
    assertTrue(sourceParams.containsKey(SOURCE_OF_REPLICATION));

    // Proceed with normal incremental flow, post optimised bootstrap is over.
    replica.run("use " + replicatedDbName)
        .run("insert into table t1 values (98)")
        .run("insert into table t2 partition(country='england') values ('london')")
        .run("insert into table t2 partition(country='india') values ('jaipur')")
        .run("insert into table t3 values (15),(16)")
        .run("drop table t4")
        .run("insert into table t5 partition(country='china') values ('chengdu')")
        .dump(replicatedDbName, withClause);

    // Do load and check if the data gets loaded.
    primary.load(primaryDbName, replicatedDbName, withClause)
        .run("select id from t1")
        .verifyResults(new String[] { "1", "2", "3", "4", "101", "210", "321", "98" })
        .run("select place from t2 where country = 'england'")
        .verifyResults(new String[] { "london" })
        .run("select place from t2 where country = 'india'")
        .verifyResults(new String[] { "delhi", "chennai", "jaipur" })
        .run("select id from t3")
        .verifyResults(new String[] { "10", "20", "31", "42", "11", "15", "16" })
        .run("show tables like 't4'")
        .verifyFailure(new String[]{"t4"})
        .run("select place from t5 where country = 'china'")
        .verifyResults(new String[] { "beejing", "chengdu" });
  }

  @Test
  public void testOverwriteDuringBootstrap() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + primary.repldDir + "'");

    // Do a bootstrap cycle.
    primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause);

    // Create some partitioned and non partitioned tables and do a dump & load.
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                "tblproperties (\"transactional\"=\"true\")")
        .run("insert into table t1 values (1)")
        .run("insert into table t1 values (2),(3),(4)")
        .run("create table t2 (id int)")
        .run("insert into table t2 values (15),(16)")
        .run("create table t3 (place string) partitioned by (country string)")
        .run("insert into table t3 partition(country='india') values ('chennai')")
        .run("insert into table t3 partition(country='us') values ('new york')")
        .run("create table t4 (place string) partitioned by (country string)")
        .run("insert into table t4 partition(country='china') values ('beejing')")
        .run("insert into table t4 partition(country='nepal') values ('kathmandu')")
        .dump(primaryDbName, withClause);

    // Do the load and check all the external & managed tables are present.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("use " + replicatedDbName)
        .run("show tables")
        .verifyResults(new String[]{"t1", "t2", "t3", "t4"})
        .verifyReplTargetProperty(replicatedDbName);

    // Prepare for reverse bootstrap.
    // Do some modifications on original source cluster. The diff becomes(tnew_managed, t1, t2, t3)
    // Create one new table: It should get dropped. (tnew_managed)
    // Create some new partition: The new partition should get dropped. (t2: france)
    // Modify a table, the data should get overwritten. (t1)
    // Modify a partition, the data should be overwritten. (t3: india value delhi)
    // Drop a table, the table should be recreated(t2)
    primary.run("use " + primaryDbName)
        .run("create table tnew_managed (id int)")
        .run("insert into table t1 values (25)")
        .run("insert into table tnew_managed values (110)")
        .run("insert into table t3 partition(country='france') values ('lyon')")
        .run("insert into table t3 partition(country='india') values ('delhi')")
        .run("drop table t2");

    // Do some modifications on the target cluster. (t1, t2, t3: bootstrap & t2, t4, t5: incremental)
    replica.run("use " + replicatedDbName)
        .run("insert into table t1 values (101)")
        .run("insert into table t1 values (121),(211)")
        .run("insert into table t3 partition(country='india') values ('lucknow')")
        .run("insert into table t2 values (11)")
        .run("insert into table t4 partition(country='india') values ('kanpur')")
        .run("create table t5 (place string) partitioned by (country string)")
        .run("insert into table t5 partition(country='china') values ('beejing')")
        .run("insert into table t4 partition(country='china') values ('Shanghai')");

    // Prepare for reverse replication.
    DistributedFileSystem replicaFs = replica.miniDFSCluster.getFileSystem();
    Path newReplDir = new Path(replica.repldDir + "1");
    replicaFs.mkdirs(newReplDir);
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + newReplDir + "'");

    // Do a reverse dump
    tuple = replica.dump(replicatedDbName, withClause);

    // Check the event ack file got created.
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));

    Path dumpPath = new Path(tuple.dumpLocation);

    // Do a load, this should create a table_diff_complete directory
    primary.load(primaryDbName, replicatedDbName, withClause);

    // Check the table diff directory exist.
    assertTrue(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY)));

    // Check the table diff has all the modified table, including the dropped and empty ones
    HashSet<String> tableDiffEntries = getTablesFromTableDiffFile(dumpPath, conf);
    assertTrue("Table Diff Contains " + tableDiffEntries, tableDiffEntries
        .containsAll(Arrays.asList("tnew_managed", "t1", "t2", "t3")));

    // Do a reverse second dump, this should do a bootstrap dump for the tables in the table_diff and incremental for
    // rest.
    tuple = replica.dump(replicatedDbName, withClause);

    String hiveDumpDir = tuple.dumpLocation + File.separator + ReplUtils.REPL_HIVE_BASE_DIR;
    // _bootstrap directory should be created as bootstrap enabled on external tables.
    Path dumpPath1 = new Path(hiveDumpDir, INC_BOOTSTRAP_ROOT_DIR_NAME +"/metadata/" + replicatedDbName);
    FileStatus[] listStatus =
        dumpPath1.getFileSystem(conf).listStatus(dumpPath1);
    ArrayList<String> tablesBootstrapped = new ArrayList<String>();
    for (FileStatus file : listStatus) {
      tablesBootstrapped.add(file.getPath().getName());
    }

    assertTrue(tablesBootstrapped.containsAll(Arrays.asList("t1", "t2", "t3")));

    // Do a reverse load, this should do a bootstrap load for the tables in table_diff and incremental for the rest.
    primary.load(primaryDbName, replicatedDbName, withClause);

    primary.run("use " + primaryDbName)
        .run("select id from t1")
        .verifyResults(new String[] { "1", "2", "3", "4", "101", "121", "211" })
        .run("select id from t2")
        .verifyResults(new String[] { "15", "16", "11" })
        .run("select place from t3 where country = 'india'")
        .verifyResults(new String[] {"chennai", "lucknow" })
        .run("select place from t3 where country = 'us'")
        .verifyResults(new String[] {"new york" })
        .run("select place from t3 where country = 'france'")
        .verifyFailure(new String[] { "lyon" })
        .run("select place from t4 where country = 'china'")
        .verifyResults(new String[] { "beejing", "Shanghai" })
        .run("select place from t4 where country = 'india'")
        .verifyResults(new String[] { "kanpur" })
        .run("select place from t5 where country = 'china'")
        .verifyResults(new String[] { "beejing" })
        .run("show tables like 'tnew_managed'")
        .verifyFailure(new String[]{"tnew_managed"});
  }

  @Test
  public void testTblMetricRegisterDuringSecondCycleOfOptimizedBootstrap() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(false);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + primary.repldDir + "'");
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .run("create table t1_managed (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into table t1_managed values (10)")
            .run("insert into table t1_managed values (20),(31),(42)")
            .dump(primaryDbName, withClause);

    // Do the bootstrap load and check all the external & managed tables are present.
    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("repl status " + replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1_managed"})
            .verifyReplTargetProperty(replicatedDbName);

    // Do an incremental dump & load, Add one table which we can drop & an empty table as well.
    tuple = primary.run("use " + primaryDbName)
            .run("create table t2_managed (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into table t2_managed values (10)")
            .run("insert into table t2_managed values (20),(31),(42)")
            .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1_managed", "t2_managed"})
            .verifyReplTargetProperty(replicatedDbName);

    primary.run("use " + primaryDbName)
            .run("insert into table t1_managed values (30)")
            .run("insert into table t1_managed values (50),(51),(52)");

    // Prepare for reverse replication.
    DistributedFileSystem replicaFs = replica.miniDFSCluster.getFileSystem();
    Path newReplDir = new Path(replica.repldDir + "1");
    replicaFs.mkdirs(newReplDir);
    withClause = ReplicationTestUtils.includeExternalTableClause(false);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + newReplDir + "'");


    // Do a reverse dump
    tuple = replica.dump(replicatedDbName, withClause);

    // Check the event ack file got created.
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
            replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));


    // Do a load, this should create a table_diff_complete directory
    primary.load(primaryDbName,replicatedDbName, withClause);

    // Check the table diff directory exist.
    assertTrue(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY).toString() + " doesn't exist",
            replicaFs.exists(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY)));

    Path dumpPath = new Path(tuple.dumpLocation);
    // Check the table diff has all the modified table, including the dropped and empty ones
    HashSet<String> tableDiffEntries = getTablesFromTableDiffFile(dumpPath, conf);
    assertTrue("Table Diff Contains " + tableDiffEntries, tableDiffEntries
            .containsAll(Arrays.asList("t1_managed")));

    isMetricsEnabledForTests(true);
    replica.dump(replicatedDbName, withClause);
    MetricCollector collector = MetricCollector.getInstance();
    ReplicationMetric metric = collector.getMetrics().getLast();
    Stage stage = metric.getProgress().getStageByName("REPL_DUMP");
    Metric tableMetric = stage.getMetricByName(ReplUtils.MetricName.TABLES.name());
    assertEquals(tableMetric.getTotalCount(), tableDiffEntries.size());
  }

  @Test
  public void testTblMetricRegisterDuringSecondLoadCycleOfOptimizedBootstrap() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(false);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + primary.repldDir + "'");
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .run("create table t1_managed (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into table t1_managed values (10)")
            .run("insert into table t1_managed values (20),(31),(42)")
            .dump(primaryDbName, withClause);

    // Do the bootstrap load and check all the external & managed tables are present.
    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("repl status " + replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1_managed"})
            .verifyReplTargetProperty(replicatedDbName);

    // Do an incremental dump & load, Add one table which we can drop & an empty table as well.
    tuple = primary.run("use " + primaryDbName)
            .run("create table t2_managed (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into table t2_managed values (10)")
            .run("insert into table t2_managed values (20),(31),(42)")
            .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t1_managed", "t2_managed"})
            .verifyReplTargetProperty(replicatedDbName);

    primary.run("use " + primaryDbName)
            .run("insert into table t1_managed values (30)")
            .run("insert into table t1_managed values (50),(51),(52)");

    // Prepare for reverse replication.
    DistributedFileSystem replicaFs = replica.miniDFSCluster.getFileSystem();
    Path newReplDir = new Path(replica.repldDir + "1");
    replicaFs.mkdirs(newReplDir);
    withClause = ReplicationTestUtils.includeExternalTableClause(false);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + newReplDir + "'");


    // Do a reverse dump
    tuple = replica.dump(replicatedDbName, withClause);

    // Check the event ack file got created.
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
            replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));


    // Do a load, this should create a table_diff_complete directory
    primary.load(primaryDbName,replicatedDbName, withClause);

    // Check the table diff directory exist.
    assertTrue(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY).toString() + " doesn't exist",
            replicaFs.exists(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY)));

    Path dumpPath = new Path(tuple.dumpLocation);
    // Check the table diff has all the modified table, including the dropped and empty ones
    HashSet<String> tableDiffEntries = getTablesFromTableDiffFile(dumpPath, conf);
    assertTrue("Table Diff Contains " + tableDiffEntries, tableDiffEntries
            .containsAll(Arrays.asList("t1_managed")));

    isMetricsEnabledForTests(true);
    replica.dump(replicatedDbName, withClause);

    //do a load on primary and verify insert queries are discarded
    primary.load(primaryDbName,replicatedDbName, withClause)
            .run("select id from t1_managed")
            .verifyResults(new String[] { "10", "20", "31", "42" });
    MetricCollector collector = MetricCollector.getInstance();
    ReplicationMetric metric = collector.getMetrics().getLast();
    Stage stage = metric.getProgress().getStageByName("REPL_LOAD");
    Metric tableMetric = stage.getMetricByName(ReplUtils.MetricName.TABLES.name());
    assertEquals(tableMetric.getTotalCount(), tableDiffEntries.size());
  }

  @NotNull
  private List<String> setUpFirstIterForOptimisedBootstrap() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + primary.repldDir + "'");

    // Do a bootstrap cycle.
    primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause);

    // Open 3 txns for Database which is not under replication
    int numTxnsForSecDb = 3;
    List<Long> txnsForSecDb = openTxns(numTxnsForSecDb, txnHandler, primaryConf);
    tearDownTxns.addAll(txnsForSecDb);

    Map<String, Long> tablesInSecDb = new HashMap<>();
    tablesInSecDb.put("t1", (long) numTxnsForSecDb);
    tablesInSecDb.put("t2", (long) numTxnsForSecDb);
    List<Long> lockIdsForSecDb = allocateWriteIdsForTablesAndAcquireLocks(primaryDbName + "_extra",
            tablesInSecDb, txnHandler, txnsForSecDb, primaryConf);
    tearDownLockIds.addAll(lockIdsForSecDb);

    //Open 2 txns for Primary Db
    int numTxnsForPrimaryDb = 2;
    List<Long> txnsForSourceDb = openTxns(numTxnsForPrimaryDb, txnHandler, primaryConf);
    tearDownTxns.addAll(txnsForSourceDb);

    // Allocate write ids for both tables of source database.
    Map<String, Long> tablesInSourceDb = new HashMap<>();
    tablesInSourceDb.put("t1", (long) numTxnsForPrimaryDb);
    tablesInSourceDb.put("t5", (long) numTxnsForPrimaryDb);
    List<Long> lockIdsForSourceDb = allocateWriteIdsForTablesAndAcquireLocks(primaryDbName, tablesInSourceDb, txnHandler,
            txnsForSourceDb, primary.getConf());
    tearDownLockIds.addAll(lockIdsForSourceDb);

    //Open 1 txn with no hive locks acquired
    List<Long> txnsWithNoLocks = openTxns(1, txnHandler, primaryConf);
    tearDownTxns.addAll(txnsWithNoLocks);

    // Create 4 managed tables and do a dump & load.
    WarehouseInstance.Tuple tuple =
        primary.run("use " + primaryDbName)
                .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                        "tblproperties (\"transactional\"=\"true\")")
                .run("insert into table t1 values (1)")
            .run("insert into table t1 values (2),(3),(4)")
            .run("create table t2 (place string) partitioned by (country string)")
            .run("insert into table t2 partition(country='india') values ('chennai')")
            .run("insert into table t2 partition(country='us') values ('new york')").run("create table t3 (id int)")
            .run("insert into table t3 values (10)").run("insert into table t3 values (20),(31),(42)")
            .run("create table t4 (place string) partitioned by (country string)")
            .run("insert into table t4 partition(country='india') values ('bangalore')")
            .run("insert into table t4 partition(country='us') values ('austin')").dump(primaryDbName, withClause);

    // Do the load and check all the external & managed tables are present.
    replica.load(replicatedDbName, primaryDbName, withClause).run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId).run("use " + replicatedDbName).run("show tables like 't1'")
        .verifyResult("t1").run("show tables like 't2'").verifyResult("t2").run("show tables like 't3'")
        .verifyResult("t3").run("show tables like 't4'").verifyResult("t4").verifyReplTargetProperty(replicatedDbName);

    String forwardReplPolicy = HiveUtils.getReplPolicy(replicatedDbName);
    List<Long> targetReplCreatedTxnIds = new ArrayList<>();
    for (Long txn: txnsForSecDb) {
      targetReplCreatedTxnIds.add(txnHandler.getTargetTxnId(forwardReplPolicy, txn));
    }
    for (Long txn: txnsForSourceDb) {
      targetReplCreatedTxnIds.add(txnHandler.getTargetTxnId(forwardReplPolicy, txn));
    }
    for (Long txn: txnsWithNoLocks) {
      targetReplCreatedTxnIds.add(txnHandler.getTargetTxnId(forwardReplPolicy, txn));
    }

    verifyAllOpenTxnsNotAborted(targetReplCreatedTxnIds, primaryConf);

    //Open New transactions on original source cluster post it went down.

    // Open 1 txn for secondary Database
    List<Long> newTxnsForSecDb = openTxns(1, txnHandler, primaryConf);
    tearDownTxns.addAll(newTxnsForSecDb);

    Map<String, Long> newTablesForSecDb = new HashMap<>();
    newTablesForSecDb.put("t1", (long) numTxnsForSecDb + 1);
    newTablesForSecDb.put("t2", (long) numTxnsForSecDb + 1);
    List<Long> newLockIdsForSecDb = allocateWriteIdsForTablesAndAcquireLocks(primaryDbName + "_extra",
            newTablesForSecDb, txnHandler, newTxnsForSecDb, primaryConf);
    tearDownLockIds.addAll(newLockIdsForSecDb);

    //Open 1 txn for Primary Db
    List<Long> newTxnsForSourceDb = openTxns(1, txnHandler, primaryConf);
    tearDownTxns.addAll(newTxnsForSourceDb);

    // Allocate write ids for both tables of source database.
    Map<String, Long> newTablesInSourceDb = new HashMap<>();
    newTablesInSourceDb.put("t1", (long) 5);
    newTablesInSourceDb.put("t5", (long) 3);
    List<Long> newLockIdsForSourceDb = allocateWriteIdsForTablesAndAcquireLocks(primaryDbName, newTablesInSourceDb, txnHandler,
            newTxnsForSourceDb, primary.getConf());
    tearDownLockIds.addAll(newLockIdsForSourceDb);

    //Open 1 txn with no hive locks acquired
    List<Long> newTxnsWithNoLock = openTxns(1, txnHandler, primaryConf);
    tearDownTxns.addAll(newTxnsWithNoLock);

    // Do some modifications on original source cluster. The diff becomes(tnew_managed, t1, t2, t3)
    primary.run("use " + primaryDbName).run("create table tnew_managed (id int) clustered by(id) into 3 buckets " +
                    "stored as orc tblproperties (\"transactional\"=\"true\")")
        .run("insert into table t1 values (25)").run("insert into table tnew_managed values (110)")
        .run("insert into table t2 partition(country='france') values ('lyon')").run("drop table t3")
        .run("alter database "+ primaryDbName + " set DBPROPERTIES ('key1'='value1')");

    assertTrue("value1".equals(primary.getDatabase(primaryDbName).getParameters().get("key1")));

    // Do some modifications on the target cluster. (t1, t2, t3: bootstrap & t4, t5: incremental)
    replica.run("use " + replicatedDbName).run("insert into table t1 values (101)")
        .run("insert into table t1 values (210),(321)")
        .run("insert into table t2 partition(country='india') values ('delhi')").run("insert into table t3 values (11)")
        .run("insert into table t4 partition(country='india') values ('lucknow')")
        .run("create table t5 (place string) partitioned by (country string)")
        .run("insert into table t5 partition(country='china') values ('beejing')")
        .run("alter database "+ replicatedDbName + " set DBPROPERTIES ('" +
                ReplConst.REPL_ENABLE_BACKGROUND_THREAD + "'='true')");

    assertTrue (MetaStoreUtils.isBackgroundThreadsEnabledForRepl(replica.getDatabase(replicatedDbName)));

    // Prepare for reverse replication.
    DistributedFileSystem replicaFs = replica.miniDFSCluster.getFileSystem();
    Path newReplDir = new Path(replica.repldDir + "1");
    replicaFs.mkdirs(newReplDir);
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + newReplDir + "'");

    // Do a reverse dump
    tuple = replica.dump(replicatedDbName, withClause);

    // Check the event ack file got created.
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));

    Path dumpPath = new Path(tuple.dumpLocation);

    // Do a load, this should create a table_diff_complete directory
    primary.load(primaryDbName, replicatedDbName, withClause);

    verifyAllOpenTxnsAborted(txnsForSourceDb, primaryConf);
    verifyAllOpenTxnsNotAborted(txnsForSecDb, primaryConf);
    verifyAllOpenTxnsNotAborted(txnsWithNoLocks, primaryConf);
    verifyAllOpenTxnsAborted(newTxnsForSourceDb, primaryConf);
    verifyAllOpenTxnsNotAborted(newTxnsForSecDb, primaryConf);
    verifyAllOpenTxnsNotAborted(newTxnsWithNoLock, primaryConf);

    txnHandler.abortTxns(new AbortTxnsRequest(txnsForSecDb));
    releaseLocks(txnHandler, lockIdsForSecDb);
    txnHandler.abortTxns(new AbortTxnsRequest(txnsWithNoLocks));
    txnHandler.abortTxns(new AbortTxnsRequest(newTxnsForSecDb));
    releaseLocks(txnHandler, newLockIdsForSecDb);
    txnHandler.abortTxns(new AbortTxnsRequest(newTxnsWithNoLock));

    // Check the table diff directory exist.
    assertTrue(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY)));

    assertTrue(new Path(tuple.dumpLocation, OptimisedBootstrapUtils.ABORT_TXNS_FILE).toString() + " doesn't exist",
            replicaFs.exists(new Path(tuple.dumpLocation, OptimisedBootstrapUtils.ABORT_TXNS_FILE)));

    List<Long> txnsInAbortTxnFile = OptimisedBootstrapUtils.
            getTxnIdFromAbortTxnsFile(new Path(tuple.dumpLocation), primaryConf);
    assertTrue (txnsInAbortTxnFile.containsAll(txnsForSourceDb));
    assertTrue (txnsInAbortTxnFile.containsAll(txnsForSecDb));
    assertTrue (txnsInAbortTxnFile.containsAll(txnsWithNoLocks));
    assertEquals (txnsInAbortTxnFile.size(), txnsForSecDb.size() + txnsForSourceDb.size() + txnsWithNoLocks.size());

    // Check the table diff has all the modified table, including the dropped and empty ones
    HashSet<String> tableDiffEntries = getTablesFromTableDiffFile(dumpPath, conf);
    assertTrue("Table Diff Contains " + tableDiffEntries,
        tableDiffEntries.containsAll(Arrays.asList("tnew_managed", "t1", "t2", "t3")));
    return withClause;
  }

  List<Long> getReplCreatedTxns() throws MetaException {
    List<TxnType> txnListExcludingReplCreated = new ArrayList<>();
    for (TxnType type : TxnType.values()) {
      // exclude REPL_CREATED txn
      if (type != TxnType.REPL_CREATED) {
        txnListExcludingReplCreated.add(type);
      }
    }
    return txnHandler.getOpenTxns(txnListExcludingReplCreated).getOpen_txns();
  }

  @Test
  public void testDbParametersAfterOptimizedBootstrap() throws Throwable {
    List<String> withClause = Arrays.asList(
            String.format("'%s'='%s'", HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname, "false"),
            String.format("'%s'='%s'", HiveConf.ConfVars.HIVE_REPL_FAILOVER_START.varname, "true")
    );

    // bootstrap
    primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into table t1 values (1),(2)")
            .dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause);

    // incremental
    primary.run("use " + primaryDbName)
            .run("insert into table t1 values (3)")
            .dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause);

    // make some changes on primary
    primary.run("use " + primaryDbName)
            .run("create table t2(name string) stored as orc tblproperties(\"transactional\"=\"true\")")
            .run("insert into t2 values('a')");

    withClause = Arrays.asList(
            String.format("'%s'='%s'", HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname, "false")
    );
    // 1st cycle of optimized bootstrap
    replica.dump(replicatedDbName, withClause);
    primary.load(primaryDbName, replicatedDbName, withClause);

    String[] dbParams = new String[]{
            TARGET_OF_REPLICATION,
            CURR_STATE_ID_SOURCE.toString(),
            CURR_STATE_ID_TARGET.toString(),
            REPL_TARGET_DB_PROPERTY,
            REPL_ENABLE_BACKGROUND_THREAD
    };
    //verify if all db parameters are set
    for (String paramKey : dbParams) {
      assertTrue(replica.getDatabase(replicatedDbName).getParameters().containsKey(paramKey));
    }

    // 2nd cycle of optimized bootstrap
    replica.dump(replicatedDbName, withClause);
    primary.load(primaryDbName, replicatedDbName, withClause);

    for (String paramKey : dbParams) {
      assertFalse(replica.getDatabase(replicatedDbName).getParameters().containsKey(paramKey));
    }
    // ensure optimized bootstrap was successful.
    primary.run(String.format("select * from %s.t1", primaryDbName))
            .verifyResults(new String[]{"1", "2", "3"})
            .run("show tables in "+primaryDbName)
            .verifyResults(new String[]{"t1"});
  }
  @Test
  public void testReverseFailoverBeforeOptimizedBootstrap() throws Throwable {
    // Do bootstrap dump and load
    primary.run("use " + primaryDbName)
            .run("create  table t1 (id string)")
            .run("insert into table t1 values ('A')")
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);

    // Do incremental dump and load
    primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);
    //initiate a controlled failover from primary to replica.
    List<String> failoverConfigs = Arrays.asList("'" + HiveConf.ConfVars.HIVE_REPL_FAILOVER_START + "'='true'");
    primary.dump(primaryDbName, failoverConfigs);
    replica.load(replicatedDbName, primaryDbName, failoverConfigs);

    //modify primary after failover.
    primary.run("use " + primaryDbName)
            .run("insert into t1 values('B')");
    //initiate a controlled failover from replica to primary before the first cycle of optimized bootstrap is run.
    WarehouseInstance.Tuple reverseDump = replica.run("use " + replicatedDbName)
            .run("create table t2 (col int)")
            .run("insert into t2 values(1),(2)")
            .dump(replicatedDbName, failoverConfigs);

    // the first reverse dump should NOT be failover ready.
    FileSystem fs = new Path(reverseDump.dumpLocation).getFileSystem(conf);
    assertTrue(fs.exists(new Path(reverseDump.dumpLocation, EVENT_ACK_FILE)));
    Path dumpPath = new Path(reverseDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertFalse(fs.exists(new Path(dumpPath, FailoverMetaData.FAILOVER_METADATA)));
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    // ensure load was successful.
    primary.load(primaryDbName, replicatedDbName, failoverConfigs);
    assertTrue(fs.exists(new Path(reverseDump.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY)));
    assertTrue(fs.exists(new Path(dumpPath, ReplAck.LOAD_ACKNOWLEDGEMENT.toString())));
    HashSet<String> tableDiffEntries = getTablesFromTableDiffFile(new Path(reverseDump.dumpLocation), conf);
    assertTrue(!tableDiffEntries.isEmpty()); // we have modified a table t1 at source

    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(primary.getDatabase(primaryDbName),
            MetaStoreUtils.FailoverEndpoint.SOURCE));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(replica.getDatabase(replicatedDbName),
            MetaStoreUtils.FailoverEndpoint.TARGET));

    //do a second dump, this dump should NOT be failover ready as some tables need to be bootstrapped (here it is t1).
    reverseDump = replica.dump(replicatedDbName, failoverConfigs);
    assertTrue(fs.exists(new Path(reverseDump.dumpLocation, OptimisedBootstrapUtils.BOOTSTRAP_TABLES_LIST)));
    dumpPath = new Path(reverseDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));

    primary.load(primaryDbName, replicatedDbName, failoverConfigs);
    //ensure optimized bootstrap was successful
    primary.run(String.format("select * from %s.t1", primaryDbName))
            .verifyResults(new String[]{"A"})
            .run(String.format("select * from %s.t2", primaryDbName))
            .verifyResults(new String[]{"1", "2"});

    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(primary.getDatabase(primaryDbName),
            MetaStoreUtils.FailoverEndpoint.SOURCE));
    assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(replica.getDatabase(replicatedDbName),
            MetaStoreUtils.FailoverEndpoint.TARGET));

    // Get source and target database properties after optimised bootstrap
    Map<String, String> sourceParams = replica.getDatabase(replicatedDbName).getParameters();
    Map<String, String> targetParams = primary.getDatabase(primaryDbName).getParameters();

    // verify db failback metrics are set properly for source db after optimised bootstrap
    verifyReplicationMetricsStatistics(sourceParams, 1, 2, ReplConst.FailoverType.PLANNED.toString());

    // verify db failback metrics are set properly for target db after optimised bootstrap
    verifyReplicationMetricsStatistics(targetParams, 1, 1, ReplConst.FailoverType.PLANNED.toString());

    //do a third dump, this should be failover ready.
    reverseDump = replica.dump(replicatedDbName, failoverConfigs);
    dumpPath = new Path(reverseDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));
    assertTrue(fs.exists(new Path(dumpPath, FailoverMetaData.FAILOVER_METADATA)));

    primary.load(primaryDbName, replicatedDbName, failoverConfigs);
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(primary.getDatabase(primaryDbName),
            MetaStoreUtils.FailoverEndpoint.TARGET));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(replica.getDatabase(replicatedDbName),
            MetaStoreUtils.FailoverEndpoint.SOURCE));

    sourceParams = replica.getDatabase(replicatedDbName).getParameters();
    targetParams = primary.getDatabase(primaryDbName).getParameters();

    // verify db failback metrics are set properly for source db after optimised bootstrap
    verifyReplicationMetricsStatistics(sourceParams, 1, 3, ReplConst.FailoverType.PLANNED.toString());

    // verify db failback metrics are set properly for target db after optimised bootstrap
    verifyReplicationMetricsStatistics(targetParams, 1, 2, ReplConst.FailoverType.PLANNED.toString());

    //initiate a failover from primary to replica.
    WarehouseInstance.Tuple forwardDump = primary.dump(primaryDbName, failoverConfigs);
    assertTrue(fs.exists(new Path(forwardDump.dumpLocation, EVENT_ACK_FILE)));
    dumpPath = new Path(forwardDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertFalse(fs.exists(new Path(dumpPath, FailoverMetaData.FAILOVER_METADATA)));
    assertFalse(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));

    replica.load(replicatedDbName, primaryDbName, failoverConfigs);
    assertTrue(fs.exists(new Path(forwardDump.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY)));
    tableDiffEntries = getTablesFromTableDiffFile(new Path(forwardDump.dumpLocation), conf);
    assertTrue(tableDiffEntries.isEmpty()); // nothing was modified
    // here second dump will be failover ready, since no tables need to be bootstrapped.
    forwardDump = primary.dump(primaryDbName, failoverConfigs);
    assertTrue(fs.exists(new Path(forwardDump.dumpLocation, OptimisedBootstrapUtils.BOOTSTRAP_TABLES_LIST)));
    dumpPath = new Path(forwardDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, ReplAck.FAILOVER_READY_MARKER.toString())));

    replica.load(replicatedDbName, primaryDbName, failoverConfigs);
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(primary.getDatabase(primaryDbName),
            MetaStoreUtils.FailoverEndpoint.SOURCE));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(replica.getDatabase(replicatedDbName),
            MetaStoreUtils.FailoverEndpoint.TARGET));

    sourceParams = replica.getDatabase(replicatedDbName).getParameters();
    targetParams = primary.getDatabase(primaryDbName).getParameters();

    // verify db failback metrics are set properly for source db after optimised bootstrap
    verifyReplicationMetricsStatistics(sourceParams, 2, 4, ReplConst.FailoverType.PLANNED.toString());

    // verify db failback metrics are set properly for target db after optimised bootstrap
    verifyReplicationMetricsStatistics(targetParams, 2, 3, ReplConst.FailoverType.PLANNED.toString());
  }
  @Test
  public void testOptimizedBootstrapWithControlledFailover() throws Throwable {
    primary.run("use " + primaryDbName)
            .run("create  table t1 (id string)")
            .run("insert into table t1 values ('A')")
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);

    primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);
    //initiate a controlled failover from primary to replica.
    List<String> failoverConfigs = Arrays.asList("'" + HiveConf.ConfVars.HIVE_REPL_FAILOVER_START + "'='true'");
    primary.dump(primaryDbName, failoverConfigs);
    replica.load(replicatedDbName, primaryDbName, failoverConfigs);

    primary.run("use " + primaryDbName)
            .run("create  table t3 (id int)")
            .run("insert into t3 values(1),(2),(3)")
            .run("insert into t1 values('B')"); //modify primary after failover.

    // initiate first cycle of optimized bootstrap
    WarehouseInstance.Tuple reverseDump = replica.run("use " + replicatedDbName)
            .run("create table t2 (col int)")
            .run("insert into t2 values(1),(2)")
            .dump(replicatedDbName);

    FileSystem fs = new Path(reverseDump.dumpLocation).getFileSystem(conf);
    assertTrue(fs.exists(new Path(reverseDump.dumpLocation, EVENT_ACK_FILE)));

    primary.load(primaryDbName, replicatedDbName);

    assertEquals(MetaStoreUtils.FailoverEndpoint.SOURCE.toString(),
            primary.getDatabase(primaryDbName).getParameters().get(REPL_FAILOVER_ENDPOINT));

    assertEquals(MetaStoreUtils.FailoverEndpoint.TARGET.toString(),
            replica.getDatabase(replicatedDbName).getParameters().get(REPL_FAILOVER_ENDPOINT));

    assertTrue(fs.exists(new Path(reverseDump.dumpLocation, TABLE_DIFF_COMPLETE_DIRECTORY)));
    HashSet<String> tableDiffEntries = getTablesFromTableDiffFile(new Path(reverseDump.dumpLocation), conf);
    assertTrue(!tableDiffEntries.isEmpty());

    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(primary.getDatabase(primaryDbName),
            MetaStoreUtils.FailoverEndpoint.SOURCE));
    assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(replica.getDatabase(replicatedDbName),
            MetaStoreUtils.FailoverEndpoint.TARGET));

    // second cycle of optimized bootstrap
    reverseDump = replica.dump(replicatedDbName);
    assertTrue(fs.exists(new Path(reverseDump.dumpLocation, OptimisedBootstrapUtils.BOOTSTRAP_TABLES_LIST)));

    primary.load(primaryDbName, replicatedDbName);
    //ensure optimized bootstrap was successful
    primary.run(String.format("select * from %s.t1", primaryDbName))
            .verifyResults(new String[]{"A"})
            .run(String.format("select * from %s.t2", primaryDbName))
            .verifyResults(new String[]{"1", "2"})
            .run("show tables in " + primaryDbName)
            .verifyResults(new String[]{"t1", "t2"});

    assertFalse(primary.getDatabase(primaryDbName).getParameters().containsKey(REPL_FAILOVER_ENDPOINT));
    assertFalse(replica.getDatabase(replicatedDbName).getParameters().containsKey(REPL_FAILOVER_ENDPOINT));
  }

  private void verifyReplicationMetricsStatistics(Map<String, String> dbParams, int expectedFailbackCount, int expectedFailoverCount, String expectedFailoverType) {
    // verify failover metrics
    assertTrue(dbParams.containsKey(ReplConst.REPL_METRICS_LAST_FAILOVER_TYPE));
    String failoverType = dbParams.get(ReplConst.REPL_METRICS_LAST_FAILOVER_TYPE);
    assertEquals(failoverType, expectedFailoverType);

    assertTrue(dbParams.containsKey(ReplConst.REPL_METRICS_FAILOVER_COUNT));
    String failoverCount = dbParams.get(ReplConst.REPL_METRICS_FAILOVER_COUNT);
    assertEquals(NumberUtils.toInt(failoverCount, 0), expectedFailoverCount);

    // verify failback metrics
    assertTrue(dbParams.containsKey(ReplConst.REPL_METRICS_LAST_FAILBACK_STARTTIME));
    String failbackStartTime = dbParams.get(ReplConst.REPL_METRICS_LAST_FAILBACK_STARTTIME);
    assertNotEquals(NumberUtils.toLong(failbackStartTime, 0), 0);

    assertTrue(dbParams.containsKey(ReplConst.REPL_METRICS_FAILBACK_COUNT));
    String failbackCount = dbParams.get(ReplConst.REPL_METRICS_FAILBACK_COUNT);
    assertEquals(NumberUtils.toInt(failbackCount, 0), expectedFailbackCount);

    assertTrue(dbParams.containsKey(ReplConst.REPL_METRICS_LAST_FAILBACK_ENDTIME));
    String failbackEndTime = dbParams.get(ReplConst.REPL_METRICS_LAST_FAILBACK_ENDTIME);
    assertNotEquals(NumberUtils.toLong(failbackEndTime, 0), 0);
  }

}
