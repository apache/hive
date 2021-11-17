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

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.EVENT_ACK_FILE;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.TABLE_DIFF_COMPLETE_FILE;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.getEventIdFromFile;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.getPathsFromTableFile;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.getTablesFromTableDiffFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
        .run("create  table t1_managed (id int)")
        .run("insert into table t1_managed values (10)")
        .run("insert into table t1_managed values (20),(31),(42)")
        .run("create external table t2_managed (place string) partitioned by (country string)")
        .run("insert into table t2_managed partition(country='india') values ('bangalore')")
        .run("insert into table t2_managed partition(country='us') values ('austin')")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId).run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("show tables like 't2'")
        .verifyResult("t2")
        .verifyReplTargetProperty(replicatedDbName);

    // Trigger reverse dump just after the bootstrap cycle.
    DistributedFileSystem replicaFs = replica.miniDFSCluster.getFileSystem();
    Path newReplDir = new Path(replica.repldDir + "1");
    replicaFs.mkdirs(newReplDir);
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + newReplDir + "'");

    // Do a dump on cluster B, it should create just EVENT_ACK file.
    tuple = replica.dump(replicatedDbName, withClause);
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));

    // Get evenID from file
    String eventID = getEventIdFromFile(new Path(tuple.dumpLocation), conf);
    assertNotNull(eventID);

    // Do a load, this should create a table_diff_complete file
    primary.load(primaryDbName, replicatedDbName, withClause);

    // Table diff file should still get created.
    assertTrue(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_FILE).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_FILE)));

    // Check the table diff directory has no entries, since we are in sync.
    assertEquals(0, replicaFs.listStatus(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_FILE)).length);

    // Do some changes on the database and see if we get entries in the table diff file.
    primary.run("use " + primaryDbName)
        .run("create external table t3 (id int)")
        .run("insert into table t3 values (10)")
        .run("insert into table t3 values (20)")
        .run("create external table t3_managed (id int)")
        .run("insert into table t3_managed values (10)")
        .run("insert into table t3_managed values (20)")
        .run("insert into table t1 values (12),(32),(44)")
        .run("insert into table t1_managed values (220),(231),(432)")
        .run("insert into table t2 partition(country='france') values ('paris')")
        .run("insert into table t2_managed partition(country='france') values ('paris')");

    // Delete the dump dir to trigger the cycle again.
    replicaFs.delete(newReplDir, true);

    // Do a dump, it will create a eventID file
    tuple = replica.dump(replicatedDbName, withClause);
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));

    // Check the event id is still the same post having changes on the database as well, since the event id is only
    // dependent on replication.
    String newEventID = getEventIdFromFile(new Path(tuple.dumpLocation), conf);
    assertEquals("Earlier eventID " + eventID + " is not same as new eventID " + newEventID, eventID, newEventID);

    // Do a load, this should create a table_diff_complete file
    primary.load(primaryDbName, replicatedDbName, withClause);

    // Table diff file should get created.
    assertTrue(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_FILE).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_FILE)));

    HashSet<String> tableDiffEntries = getTablesFromTableDiffFile(new Path(tuple.dumpLocation), conf);
    assertTrue("Table Diff Contains " + tableDiffEntries,
        tableDiffEntries.containsAll(Arrays.asList("t1","t2", "t3", "t1_managed","t2_managed", "t3_managed")));

    // Clean up this and check if the failover is triggered after incremental.
    replicaFs.delete(newReplDir, true);
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + primary.repldDir + "'");

    // Do an incremental dump & load
    tuple = primary.dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 't3_managed'")
        .verifyResult("t3_managed")
        .run("show tables like 't3'")
        .verifyResult("t3")
        .verifyReplTargetProperty(replicatedDbName);

    // Do some modifications on original source cluster.
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
        .run("insert into table t2_managed partition(country='france') values ('nice')");

    // Do a dump on cluster B, it should create just EVENT_ACK file.
    replicaFs.mkdirs(newReplDir);
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + newReplDir + "'");

    // Do a reverse dump
    tuple = replica.dump(replicatedDbName, withClause);
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));

    // Check in case the dump isn't consumed, the dump directory doesn't change, the dump gets skipped only without
    // error.
    ContentSummary beforeContentSummary = replicaFs.getContentSummary(new Path(tuple.dumpLocation).getParent());
    WarehouseInstance.Tuple emptyTuple = replica.dump(replicatedDbName, withClause);
    assertTrue(emptyTuple.dumpLocation.isEmpty());
    assertTrue(emptyTuple.lastReplicationId.isEmpty());
    ContentSummary afterContentSummary = replicaFs.getContentSummary(new Path(tuple.dumpLocation).getParent());
    assertEquals(beforeContentSummary.getFileAndDirectoryCount(), afterContentSummary.getFileAndDirectoryCount());
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString(),
        replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));

    // Do a load, this should create a table_diff_complete file
    primary.load(primaryDbName,replicatedDbName, withClause);

    assertTrue(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_FILE).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_FILE)));

    tableDiffEntries = getTablesFromTableDiffFile(new Path(tuple.dumpLocation), conf);
    assertTrue("Table Diff Contains " + tableDiffEntries,
        tableDiffEntries.containsAll(Arrays.asList("t4", "t2", "t4_managed", "t2_managed")));

    // Do a load again and see, nothing changes as this load isn't consumed.
    beforeContentSummary = replicaFs.getContentSummary(new Path(tuple.dumpLocation).getParent());
    primary.load(primaryDbName, replicatedDbName, withClause);
    assertTrue("Table Diff Contains " + tableDiffEntries,
        tableDiffEntries.containsAll(Arrays.asList("t4", "t2", "t4_managed", "t2_managed")));
    afterContentSummary = replicaFs.getContentSummary(new Path(tuple.dumpLocation).getParent());
    assertEquals(beforeContentSummary.getFileAndDirectoryCount(), afterContentSummary.getFileAndDirectoryCount());

    // Check there are entries in the table files.
    assertFalse(getPathsFromTableFile("t4", new Path(tuple.dumpLocation), conf).isEmpty());
    assertFalse(getPathsFromTableFile("t2", new Path(tuple.dumpLocation), conf).isEmpty());
    assertFalse(getPathsFromTableFile("t4_managed", new Path(tuple.dumpLocation), conf).isEmpty());
    assertFalse(getPathsFromTableFile("t2_managed", new Path(tuple.dumpLocation), conf).isEmpty());

    // Delete the reverse dump directory and do the normal A->B to make A & B in sync.
    replica.miniDFSCluster.getFileSystem().delete(newReplDir, true);
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + primary.repldDir + "'");
    primary.dump(primaryDbName, withClause);

    replica.load(replicatedDbName,primaryDbName, withClause)
        .run("show tables like 't4_managed'")
        .verifyResult("t4_managed")
        .run("show tables like 't4'")
        .verifyResult("t4");

    // Now trigger a reverse dump & load, to see table_diff file should be empty.
    replica.miniDFSCluster.getFileSystem().mkdirs(newReplDir);
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + newReplDir + "'");

    // Do a reverse dump
    tuple = replica.dump(replicatedDbName, withClause);
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));

    // Do a load, this should create a table_diff_complete file
    primary.load(primaryDbName, replicatedDbName, withClause);

    // Table diff file should still get created.
    assertTrue(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_FILE).toString() + " doesn't exist",
        replicaFs.exists(new Path(tuple.dumpLocation, TABLE_DIFF_COMPLETE_FILE)));

    // Check the table diff file has no entries, since we are in sync.
    tableDiffEntries = getTablesFromTableDiffFile(new Path(tuple.dumpLocation), conf);
    assertTrue(tableDiffEntries.isEmpty());
  }
}