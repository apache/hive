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

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.event.filters.DatabaseAndTableFilter;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.QUOTA_RESET;
import static org.apache.hadoop.hive.common.repl.ReplConst.REPL_TARGET_DB_PROPERTY;
import static org.apache.hadoop.hive.common.repl.ReplConst.TARGET_OF_REPLICATION;
import static org.apache.hadoop.hive.metastore.ReplChangeManager.SOURCE_OF_REPLICATION;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.EVENT_ACK_FILE;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.TABLE_DIFF_COMPLETE_DIRECTORY;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.TABLE_DIFF_INPROGRESS_DIRECTORY;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.getEventIdFromFile;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.getPathsFromTableFile;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.getTablesFromTableDiffFile;

import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.INC_BOOTSTRAP_ROOT_DIR_NAME;
import static org.apache.hadoop.hive.ql.parse.ReplicationSpec.KEY.CURR_STATE_ID_SOURCE;
import static org.apache.hadoop.hive.ql.parse.ReplicationSpec.KEY.CURR_STATE_ID_TARGET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestReplicationOptimisedBootstrap extends BaseReplicationAcrossInstances {

  String extraPrimaryDb;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    HashMap<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());
    overrides.put(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY.varname, "false");
    overrides.put(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname, "true");
    overrides.put(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname, UserGroupInformation.getCurrentUser().getUserName());
    overrides.put(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname, "true");

    internalBeforeClassSetupExclusiveReplica(overrides, overrides, TestReplicationOptimisedBootstrap.class);
  }

  @Before
  public void setup() throws Throwable {
    super.setup();
    extraPrimaryDb = "extra_" + primaryDbName;
  }

  @After
  public void tearDown() throws Throwable {
    primary.run("drop database if exists " + extraPrimaryDb + " cascade");
    super.tearDown();
  }

  @Test
  public void testBuildTableDiffGeneration() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + primary.repldDir + "'");
    // Create two external & two managed tables and do a bootstrap dump & load.
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create external table t1 (id int)")
        .run("insert into table t1 values (1)")
        .run("insert into table t1 values (2),(3),(4)")
        .run("create external table t2 (place string) partitioned by (country string)")
        .run("insert into table t2 partition(country='india') values ('chennai')")
        .run("insert into table t2 partition(country='us') values ('new york')")
        .run("create table t1_managed (id int)")
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
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("show tables like 't2'")
        .verifyResult("t2")
        .run("show tables like 't1_managed'")
        .verifyResult("t1_managed")
        .run("show tables like 't2_managed'")
        .verifyResult("t2_managed")
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
        .run("show tables like 't5_managed'")
        .verifyResult("t5_managed")
        .run("show tables like 't6_managed'")
        .verifyResult("t6_managed")
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
        .run("create table t4_managed (id int)")
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
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + newReplDir + "'");

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
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + primary.repldDir + "'");

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
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + newReplDir + "'");

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
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + primary.repldDir + "'");
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
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + newReplDir + "'");

    // Do a dump on cluster B, it should throw an exception, since the first incremental isn't done yet.
    try {
      replica.dump(replicatedDbName, withClause);
    } catch (HiveException he) {
      assertTrue(he.getMessage()
          .contains("Replication dump not allowed for replicated database with first incremental dump pending : " + replicatedDbName));
    }

    // Do a incremental cycle and check we don't get this exception.
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + primary.repldDir + "'");
    primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause);

    // Retrigger reverse dump, this time it should be successful and event ack should get created.
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + newReplDir + "'");

    tuple = replica.dump(replicatedDbName, withClause);

    // Check event ack file should get created.
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));
  }

  @Test
  public void testFailureCasesInTableDiffGeneration() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + primary.repldDir + "'");

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
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + newReplDir + "'");

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
  public void testTargetEventIdGenerationAfterFirstIncremental() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + primary.repldDir + "'");

    // Do a bootstrap cycle(A->B)
    primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause);

    // Add some table & do an incremental dump.
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create external table table1 (id int)")
        .run("insert into table table1 values (100)")
        .run("create  table table1_managed (name string)")
        .run("insert into table table1_managed values ('ABC')")
        .dump(primaryDbName, withClause);

    // Do an incremental load
    replica.load(replicatedDbName, primaryDbName, withClause);

    // Get the latest notification from the notification log for the target database, just after replication.
    CurrentNotificationEventId notificationIdAfterRepl = replica.getCurrentNotificationEventId();

    // Check the tables are there post incremental load.
    replica.run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("use " + replicatedDbName)
        .run("select id from table1")
        .verifyResult("100")
        .run("select name from table1_managed")
        .verifyResult("ABC")
        .verifyReplTargetProperty(replicatedDbName);

    // Do some modifications on the source cluster, so we have some entries in the table diff.
    primary.run("use " + primaryDbName)
        .run("create table table2_managed (id string)")
        .run("insert into table table1_managed values ('SDC')")
        .run("insert into table table2_managed values ('A'),('B'),('C')");


    // Do some modifications in another database to have unrelated events as well after the last load, which should
    // get filtered.

    primary.run("create database " + extraPrimaryDb)
        .run("use " + extraPrimaryDb)
        .run("create external table t1 (id int)")
        .run("insert into table t1 values (15),(1),(96)")
        .run("create  table t1_managed (id string)")
        .run("insert into table t1_managed values ('SA'),('PS')");

    // Do some modifications on the target database.
    replica.run("use " + replicatedDbName)
        .run("alter database "+ replicatedDbName + " set DBPROPERTIES ('key1'='value1')")
        .run("alter database "+ replicatedDbName + " set DBPROPERTIES ('key2'='value2')");

    // Validate the current replication id on original target has changed now.
    assertNotEquals(replica.getCurrentNotificationEventId().getEventId(), notificationIdAfterRepl.getEventId());

    // Prepare for reverse replication.
    DistributedFileSystem replicaFs = replica.miniDFSCluster.getFileSystem();
    Path newReplDir = new Path(replica.repldDir + "reverse1");
    replicaFs.mkdirs(newReplDir);
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + newReplDir + "'");

    tuple = replica.dump(replicatedDbName);

    // Check event ack file should get created.
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));

    // Get the target event id.
    NotificationEventResponse nl = new HiveMetaStoreClient(replica.hiveConf)
        .getNextNotification(Long.parseLong(getEventIdFromFile(new Path(tuple.dumpLocation), conf)[1]), -1,
            new DatabaseAndTableFilter(replicatedDbName, null));

    // There should be 4 events, one for alter db, second to remove first incremental pending and then two custom
    // alter operations.
    assertEquals(4, nl.getEvents().size());
  }

  @Test
  public void testTargetEventIdGeneration() throws Throwable {
    // Do a a cycle of bootstrap dump & load.
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + primary.repldDir + "'");

    // Do a bootstrap cycle(A->B)
    primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause);

    // Add some table & do the first incremental dump.
    primary.run("use " + primaryDbName)
        .run("create external table tablei1 (id int)")
        .run("create external table tablei2 (id int)")
        .run("create table tablem1 (id int)")
        .run("create table tablem2 (id int)")
        .run("insert into table tablei1 values(1),(2),(3),(4)")
        .run("insert into table tablei2 values(10),(20),(30),(40)")
        .run("insert into table tablem1 values(5),(10),(15),(20)")
        .run("insert into table tablem2 values(6),(12),(18),(24)")
        .dump(primaryDbName, withClause);

    // Do the incremental load, and check everything is intact.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use "+ replicatedDbName)
        .run("select id from tablei1")
        .verifyResults(new String[]{"1","2","3","4"})
        .run("select id from tablei2")
        .verifyResults(new String[]{"10","20","30","40"})
        .run("select id from tablem1")
        .verifyResults(new String[]{"5","10","15","20"})
        .run("select id from tablem2")
        .verifyResults(new String[]{"6","12","18","24"});

    // Do some modifications & call for the second cycle of incremental dump & load.
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create external table table1 (id int)")
        .run("insert into table table1 values (25),(35),(82)")
        .run("create  table table1_managed (name string)")
        .run("insert into table table1_managed values ('CAD'),('DAS'),('MSA')")
        .run("insert into table tablei1 values(15),(62),(25),(62)")
        .run("insert into table tablei2 values(10),(22),(11),(22)")
        .run("insert into table tablem1 values(5),(10),(15),(20)")
        .run("alter table table1 set TBLPROPERTIES('comment'='abc')")
        .dump(primaryDbName, withClause);

    // Do an incremental load
    replica.load(replicatedDbName, primaryDbName, withClause);

    // Get the latest notification from the notification log for the target database, just after replication.
    CurrentNotificationEventId notificationIdAfterRepl = replica.getCurrentNotificationEventId();

    // Check the tables are there post incremental load.
    replica.run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("use " + replicatedDbName)
        .run("select id from table1")
        .verifyResults(new String[]{"25", "35", "82"})
        .run("select name from table1_managed")
        .verifyResults(new String[]{"CAD", "DAS", "MSA"})
        .verifyReplTargetProperty(replicatedDbName);

    // Do some modifications on the source cluster, so we have some entries in the table diff.
    primary.run("use " + primaryDbName)
        .run("create table table2_managed (id string)")
        .run("insert into table table1_managed values ('AAA'),('BBB')")
        .run("insert into table table2_managed values ('A1'),('B1'),('C2')");


    // Do some modifications in another database to have unrelated events as well after the last load, which should
    // get filtered.

    primary.run("create database " + extraPrimaryDb)
        .run("use " + extraPrimaryDb)
        .run("create external table table1 (id int)")
        .run("insert into table table1 values (15),(1),(96)")
        .run("create  table table1_managed (id string)")
        .run("insert into table table1_managed values ('SAA'),('PSA')");

    // Do some modifications on the target database.
    replica.run("use " + replicatedDbName)
        .run("alter database "+ replicatedDbName + " set DBPROPERTIES ('repl1'='value1')")
        .run("alter database "+ replicatedDbName + " set DBPROPERTIES ('repl2'='value2')");

    // Validate the current replication id on original target has changed now.
    assertNotEquals(replica.getCurrentNotificationEventId().getEventId(), notificationIdAfterRepl.getEventId());

    // Prepare for reverse replication.
    DistributedFileSystem replicaFs = replica.miniDFSCluster.getFileSystem();
    Path newReplDir = new Path(replica.repldDir + "reverse01");
    replicaFs.mkdirs(newReplDir);
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + newReplDir + "'");

    tuple = replica.dump(replicatedDbName, withClause);

    // Check event ack file should get created.
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));

    // Get the target event id.
    NotificationEventResponse nl = new HiveMetaStoreClient(replica.hiveConf)
        .getNextNotification(Long.parseLong(getEventIdFromFile(new Path(tuple.dumpLocation), conf)[1]), 10,
            new DatabaseAndTableFilter(replicatedDbName, null));

    assertEquals(1, nl.getEvents().size());
  }

  @Test
  public void testTargetEventIdWithNotificationsExpired() throws Throwable {
    // Do a a cycle of bootstrap dump & load.
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + primary.repldDir + "'");

    // Do a bootstrap cycle(A->B)
    primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause);

    // Add some table & do the first incremental dump.
    primary.run("use " + primaryDbName)
        .run("create external table tablei1 (id int)")
        .run("create table tablem1 (id int)")
        .run("insert into table tablei1 values(1),(2),(3),(4)")
        .run("insert into table tablem1 values(5),(10),(15),(20)")
        .dump(primaryDbName, withClause);

    // Do the incremental load, and check everything is intact.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use "+ replicatedDbName)
        .run("select id from tablei1")
        .verifyResults(new String[]{"1","2","3","4"})
        .run("select id from tablem1")
        .verifyResults(new String[]{"5","10","15","20"});

    // Explicitly make the notification logs.
    // Get the latest notification from the notification log for the target database, just after replication.
    CurrentNotificationEventId notificationIdAfterRepl = replica.getCurrentNotificationEventId();
    // Inject a behaviour where some events missing from notification_log table.
    // This ensures the incremental dump doesn't get all events for replication.
    InjectableBehaviourObjectStore.BehaviourInjection<NotificationEventResponse, NotificationEventResponse>
        eventIdSkipper =
        new InjectableBehaviourObjectStore.BehaviourInjection<NotificationEventResponse, NotificationEventResponse>() {

      @Nullable
      @Override
      public NotificationEventResponse apply(@Nullable NotificationEventResponse eventIdList) {
        if (null != eventIdList) {
          List<NotificationEvent> eventIds = eventIdList.getEvents();
          List<NotificationEvent> outEventIds = new ArrayList<>();
          for (NotificationEvent event : eventIds) {
            // Skip the last db event.
            if (event.getDbName().equalsIgnoreCase(replicatedDbName)) {
              injectionPathCalled = true;
              continue;
            }
            outEventIds.add(event);
          }

          // Return the new list
          return new NotificationEventResponse(outEventIds);
        } else {
          return null;
        }
      }
    };

    try {
      InjectableBehaviourObjectStore.setGetNextNotificationBehaviour(eventIdSkipper);

      // Prepare for reverse replication.
      DistributedFileSystem replicaFs = replica.miniDFSCluster.getFileSystem();
      Path newReplDir = new Path(replica.repldDir + "reverse01");
      replicaFs.mkdirs(newReplDir);
      withClause = ReplicationTestUtils.includeExternalTableClause(true);
      withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + newReplDir + "'");

      try {
        replica.dump(replicatedDbName, withClause);
        fail("Expected the dump to fail since the notification event is missing.");
      } catch (Exception e) {
        // Expected due to missing notification log entry.
      }

      // Check if there is a non-recoverable error or not.
      Path nonRecoverablePath =
          TestReplicationScenarios.getNonRecoverablePath(newReplDir, replicatedDbName, replica.hiveConf);
      assertTrue(replicaFs.exists(nonRecoverablePath));
    } finally {
      InjectableBehaviourObjectStore.resetGetNextNotificationBehaviour();  // reset the behaviour
    }
  }


  @Test
  public void testReverseBootstrap() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + primary.repldDir + "'");

    // Do a bootstrap cycle.
    primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause);

    // Create 4 managed tables and do a dump & load.
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create table t1 (id int)")
        .run("insert into table t1 values (1)")
        .run("insert into table t1 values (2),(3),(4)")
        .run("create table t2 (place string) partitioned by (country string)")
        .run("insert into table t2 partition(country='india') values ('chennai')")
        .run("insert into table t2 partition(country='us') values ('new york')")
        .run("create table t3 (id int)")
        .run("insert into table t3 values (10)")
        .run("insert into table t3 values (20),(31),(42)")
        .run("create table t4 (place string) partitioned by (country string)")
        .run("insert into table t4 partition(country='india') values ('bangalore')")
        .run("insert into table t4 partition(country='us') values ('austin')")
        .dump(primaryDbName, withClause);

    // Do the load and check all the external & managed tables are present.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("show tables like 't2'")
        .verifyResult("t2")
        .run("show tables like 't3'")
        .verifyResult("t3")
        .run("show tables like 't4'")
        .verifyResult("t4")
        .verifyReplTargetProperty(replicatedDbName);


    // Do some modifications on original source cluster. The diff becomes(tnew_managed, t1, t2, t3)
    primary.run("use " + primaryDbName)
        .run("create table tnew_managed (id int)")
        .run("insert into table t1 values (25)")
        .run("insert into table tnew_managed values (110)")
        .run("insert into table t2 partition(country='france') values ('lyon')")
        .run("drop table t3");

    // Do some modifications on the target cluster. (t1, t2, t3: bootstrap & t4, t5: incremental)
    replica.run("use " + replicatedDbName)
        .run("insert into table t1 values (101)")
        .run("insert into table t1 values (210),(321)")
        .run("insert into table t2 partition(country='india') values ('delhi')")
        .run("insert into table t3 values (11)")
        .run("insert into table t4 partition(country='india') values ('lucknow')")
        .run("create table t5 (place string) partitioned by (country string)")
        .run("insert into table t5 partition(country='china') values ('beejing')");

    // Prepare for reverse replication.
    DistributedFileSystem replicaFs = replica.miniDFSCluster.getFileSystem();
    Path newReplDir = new Path(replica.repldDir + "1");
    replicaFs.mkdirs(newReplDir);
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + newReplDir + "'");

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
}
