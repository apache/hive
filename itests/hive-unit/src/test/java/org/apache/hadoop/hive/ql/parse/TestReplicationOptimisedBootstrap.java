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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.QUOTA_RESET;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.EVENT_ACK_FILE;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.TABLE_DIFF_COMPLETE_DIRECTORY;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.TABLE_DIFF_INPROGRESS_DIRECTORY;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.getPathsFromTableFile;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.getTablesFromTableDiffFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
}
