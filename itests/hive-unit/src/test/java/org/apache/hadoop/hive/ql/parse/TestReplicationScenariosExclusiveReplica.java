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
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.event.filters.DatabaseAndTableFilter;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.EVENT_ACK_FILE;
import static org.apache.hadoop.hive.ql.exec.repl.OptimisedBootstrapUtils.getEventIdFromFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test replication scenarios with staging on replica.
 */
public class TestReplicationScenariosExclusiveReplica extends BaseReplicationAcrossInstances {

  String extraPrimaryDb;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());
    overrides.put(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY.varname, "false");
    overrides.put(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname, "true");
    overrides.put(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname,
        UserGroupInformation.getCurrentUser().getUserName());
    internalBeforeClassSetupExclusiveReplica(overrides, overrides, TestReplicationScenarios.class);
  }

  @Before
  public void setup() throws Throwable {
    super.setup();
    extraPrimaryDb = "extra_" + primaryDbName;
  }

  @After
  public void tearDown() throws Throwable {
    super.tearDown();
  }

  @Test
  public void testTargetEventIdGenerationAfterFirstIncrementalInOptFailover() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + primary.repldDir + "'");

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
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + newReplDir + "'");

    tuple = replica.dump(replicatedDbName);

    // Check event ack file should get created.
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
            replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));

    // Get the target event id.
    NotificationEventResponse nl = new HiveMetaStoreClient(replica.hiveConf)
            .getNextNotification(Long.parseLong(getEventIdFromFile(new Path(tuple.dumpLocation), conf)[1]), -1,
                    new DatabaseAndTableFilter(replicatedDbName, null));

    // There should be 2 events, two custom alter operations.
    assertEquals(2, nl.getEvents().size());
  }

  @Test
  public void testTargetEventIdGenerationInOptmisedFailover() throws Throwable {
    // Do a a cycle of bootstrap dump & load.
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + primary.repldDir + "'");

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
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + newReplDir + "'");

    tuple = replica.dump(replicatedDbName, withClause);

    // Check event ack file should get created.
    assertTrue(new Path(tuple.dumpLocation, EVENT_ACK_FILE).toString() + " doesn't exist",
            replicaFs.exists(new Path(tuple.dumpLocation, EVENT_ACK_FILE)));

    // Get the target event id.
    NotificationEventResponse nl = new HiveMetaStoreClient(replica.hiveConf)
            .getNextNotification(Long.parseLong(getEventIdFromFile(new Path(tuple.dumpLocation), conf)[1]), 10,
                    new DatabaseAndTableFilter(replicatedDbName, null));

    assertEquals(0, nl.getEventsSize());
  }

  @Test
  public void testTargetEventIdWithNotificationsExpiredInOptimisedFailover() throws Throwable {
    // Do a a cycle of bootstrap dump & load.
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + primary.repldDir + "'");

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
      withClause.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + newReplDir + "'");

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
  public void testDistCpCopyWithRemoteStagingAndCopyTaskOnTarget() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(replica.repldDir, true);
    withClauseOptions.add("'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='" + false + "'");
    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create external table t1 (id int)")
        .run("insert into table t1 values (100)")
        .run("create table t2 (id int)")
        .run("insert into table t2 values (200)")
        .dump(primaryDbName, withClauseOptions);

    // verify that the external table list is written correctly for bootstrap
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1"), tuple.dumpLocation, replica);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("select id from t1")
        .verifyResult("100")
        .run("show tables like 't2'")
        .verifyResult("t2")
        .run("select id from t2")
        .verifyResult("200");

    tuple = primary.run("use " + primaryDbName)
        .run("create external table t3 (id int)")
        .run("insert into table t3 values (300)")
        .run("create table t4 (id int)")
        .run("insert into table t4 values (400)")
        .dump(primaryDbName, withClauseOptions);

    // verify that the external table list is written correctly for incremental
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1", "t3"), tuple.dumpLocation, replica);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("show tables like 't2'")
        .verifyResult("t2")
        .run("show tables like 't3'")
        .verifyResult("t3")
        .run("show tables like 't4'")
        .verifyResult("t4")
        .run("select id from t1")
        .verifyResult("100")
        .run("select id from t2")
        .verifyResult("200")
        .run("select id from t3")
        .verifyResult("300")
        .run("select id from t4")
        .verifyResult("400");
  }

  @Test
  public void testTableLevelReplicationWithRemoteStaging() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(replica.repldDir, true);
    withClauseOptions.add("'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='" + false + "'");
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (100)")
            .run("create table t2 (id int)")
            .run("insert into table t2 values (200)")
            .dump(primaryDbName +".'t[0-9]+'", withClauseOptions);

    // verify that the external table list is written correctly for bootstrap
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1"), tuple.dumpLocation, replica);

    //verify table list
    verifyTableListForPolicy(replica.miniDFSCluster.getFileSystem(),
            tuple.dumpLocation, new String[]{"t1", "t2"});

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResult("100")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResult("200");

    tuple = primary.run("use " + primaryDbName)
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (300)")
            .run("create table t4 (id int)")
            .run("insert into table t4 values (400)")
            .run("create table t5 (id int) partitioned by (p int)")
            .run("insert into t5 partition(p=1) values(10)")
            .run("insert into t5 partition(p=2) values(20)")
            .dump(primaryDbName + ".'t[0-9]+'", withClauseOptions);

    // verify that the external table list is written correctly for incremental
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1", "t3"), tuple.dumpLocation, replica);

    //verify table list
    verifyTableListForPolicy(replica.miniDFSCluster.getFileSystem(),
            tuple.dumpLocation, new String[]{"t1", "t2", "t3", "t4", "t5"});

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("show tables like 't4'")
            .verifyResult("t4")
            .run("show tables like 't5'")
            .verifyResult("t5")
            .run("select id from t1")
            .verifyResult("100")
            .run("select id from t2")
            .verifyResult("200")
            .run("select id from t3")
            .verifyResult("300")
            .run("select id from t4")
            .verifyResult("400")
            .run("select id from t5")
            .verifyResults(new String[]{"10", "20"});
  }

  @Test
  public void testDistCpCopyWithLocalStagingAndCopyTaskOnTarget() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(primary.repldDir, true);
    withClauseOptions.add("'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='" + false + "'");
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (500)")
            .run("create table t2 (id int)")
            .run("insert into table t2 values (600)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table list is written correctly for bootstrap
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1"), tuple.dumpLocation, primary);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResult("500")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResult("600");

    tuple = primary.run("use " + primaryDbName)
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (700)")
            .run("create table t4 (id int)")
            .run("insert into table t4 values (800)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table list is written correctly for incremental
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1", "t3"), tuple.dumpLocation, primary);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("show tables like 't4'")
            .verifyResult("t4")
            .run("select id from t1")
            .verifyResult("500")
            .run("select id from t2")
            .verifyResult("600")
            .run("select id from t3")
            .verifyResult("700")
            .run("select id from t4")
            .verifyResult("800");
  }

  @Test
  public void testDistCpCopyWithRemoteStagingAndCopyTaskOnSource() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(replica.repldDir, true);
    withClauseOptions.add("'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='" + false + "'");
    withClauseOptions.add("'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='" + false + "'");
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (100)")
            .run("create table t2 (id int)")
            .run("insert into table t2 values (200)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table list is written correctly for bootstrap
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1"), tuple.dumpLocation, replica);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResult("100")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResult("200");

    tuple = primary.run("use " + primaryDbName)
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (300)")
            .run("create table t4 (id int)")
            .run("insert into table t4 values (400)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table list is written correctly for incremental
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1", "t3"), tuple.dumpLocation, replica);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("show tables like 't4'")
            .verifyResult("t4")
            .run("select id from t1")
            .verifyResult("100")
            .run("select id from t2")
            .verifyResult("200")
            .run("select id from t3")
            .verifyResult("300")
            .run("select id from t4")
            .verifyResult("400");
  }

  @Test
  public void testDistCpCopyWithLocalStagingAndCopyTaskOnSource() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(primary.repldDir, true);
    withClauseOptions.add("'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='" + false + "'");
    withClauseOptions.add("'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='" + false + "'");
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (500)")
            .run("create table t2 (id int)")
            .run("insert into table t2 values (600)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table list is written correctly for bootstrap
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1"), tuple.dumpLocation, primary);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResult("500")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResult("600");

    tuple = primary.run("use " + primaryDbName)
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (700)")
            .run("create table t4 (id int)")
            .run("insert into table t4 values (800)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table list is written correctly for incremental
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1", "t3"), tuple.dumpLocation, primary);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("show tables like 't4'")
            .verifyResult("t4")
            .run("select id from t1")
            .verifyResult("500")
            .run("select id from t2")
            .verifyResult("600")
            .run("select id from t3")
            .verifyResult("700")
            .run("select id from t4")
            .verifyResult("800");
  }

  @Test
  public void testRegularCopyRemoteStagingAndCopyTaskOnSource() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(replica.repldDir, false);
    withClauseOptions.add("'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='" + false + "'");
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (500)")
            .run("create table t2 (id int)")
            .run("insert into table t2 values (600)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table list is written correctly for bootstrap
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1"), tuple.dumpLocation, replica);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResult("500")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResult("600");

    tuple = primary.run("use " + primaryDbName)
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (700)")
            .run("create table t4 (id int)")
            .run("insert into table t4 values (800)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table list is written correctly for incremental
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1", "t3"), tuple.dumpLocation, replica);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("show tables like 't4'")
            .verifyResult("t4")
            .run("select id from t1")
            .verifyResult("500")
            .run("select id from t2")
            .verifyResult("600")
            .run("select id from t3")
            .verifyResult("700")
            .run("select id from t4")
            .verifyResult("800");
  }

  @Test
  public void testRegularCopyWithLocalStagingAndCopyTaskOnTarget() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(primary.repldDir, false);
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (500)")
            .run("create table t2 (id int)")
            .run("insert into table t2 values (600)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table list is written correctly for bootstrap
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1"), tuple.dumpLocation, primary);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResult("500")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResult("600");

    tuple = primary.run("use " + primaryDbName)
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (700)")
            .run("create table t4 (id int)")
            .run("insert into table t4 values (800)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table list is written correctly for incremental
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1", "t3"), tuple.dumpLocation, primary);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("show tables like 't4'")
            .verifyResult("t4")
            .run("select id from t1")
            .verifyResult("500")
            .run("select id from t2")
            .verifyResult("600")
            .run("select id from t3")
            .verifyResult("700")
            .run("select id from t4")
            .verifyResult("800");
  }

  @Test
  public void testReplicationWithSnapshotsWithSourceStaging() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(primary.repldDir, false);
    withClauseOptions.add("'" + HiveConf.ConfVars.REPL_SNAPSHOT_DIFF_FOR_EXTERNAL_TABLE_COPY.varname + "'='" + true + "'");
    withClauseOptions.add("'" + HiveConf.ConfVars.REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK.varname + "'='" + true + "'");
    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create external table t1 (id int)")
        .run("insert into table t1 values (500)")
        .run("create table t2 (id int)")
        .run("insert into table t2 values (600)")
        .dump(primaryDbName, withClauseOptions);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("select id from t1")
        .verifyResult("500")
        .run("show tables like 't2'")
        .verifyResult("t2")
        .run("select id from t2")
        .verifyResult("600");

    tuple = primary.run("use " + primaryDbName)
        .run("create external table t3 (id int)")
        .run("insert into table t3 values (700)")
        .run("create table t4 (id int)")
        .run("insert into table t4 values (800)")
        .dump(primaryDbName, withClauseOptions);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("show tables like 't2'")
        .verifyResult("t2")
        .run("show tables like 't3'")
        .verifyResult("t3")
        .run("show tables like 't4'")
        .verifyResult("t4")
        .run("select id from t1")
        .verifyResult("500")
        .run("select id from t2")
        .verifyResult("600")
        .run("select id from t3")
        .verifyResult("700")
        .run("select id from t4")
        .verifyResult("800");
  }

  @Test
  public void externalTableReplicationDropDatabase() throws Throwable {
    String primaryDb = "primarydb1";
    String replicaDb = "repldb1";
    String tableName = "t1";
    List<String> withClauseOptions = getStagingLocationConfig(primary.repldDir, false);
    WarehouseInstance.Tuple tuple = primary
            .run("create database " + primaryDb)
            .run("alter database "+ primaryDb + " set dbproperties('repl.source.for'='1,2,3')")
            .run("use " + primaryDb)
            .run("create external table " +  tableName + " (id int)")
            .run("insert into table " + tableName + " values (500)")
            .dump(primaryDb, withClauseOptions);

    replica.load(replicaDb, primaryDb, withClauseOptions)
            .run("use " + replicaDb)
            .run("show tables like '" + tableName + "'")
            .verifyResult(tableName)
            .run("select id from " + tableName)
            .verifyResult("500");

    Path dbDataLocPrimary = new Path(primary.externalTableWarehouseRoot, primaryDb + ".db");
    Path extTableBase = new Path(replica.getConf().get(HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname));
    Path dbDataLocReplica = new Path(extTableBase + dbDataLocPrimary.toUri().getPath());
    verifyTableDataExists(primary, dbDataLocPrimary, tableName, true);
    verifyTableDataExists(replica, dbDataLocReplica, tableName, true);

    primary.run("show databases like '" + primaryDb + "'")
            .verifyResult(primaryDb);
    replica.run("show databases like '" + replicaDb + "'")
            .verifyResult(replicaDb);
    primary.run("drop database " + primaryDb + " cascade");
    replica.run("drop database " + replicaDb + " cascade");
    primary.run("show databases like '" + primaryDb + "'")
            .verifyResult(null);
    replica.run("show databases like '" + replicaDb + "'")
            .verifyResult(null);

    verifyTableDataExists(primary, dbDataLocPrimary, tableName, false);
    verifyTableDataExists(replica, dbDataLocReplica, tableName, true);
  }

  @Test
  public void testCustomWarehouseLocations() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(primary.repldDir, false);
    String dbWhManagedLoc = new Path(primary.warehouseRoot.getParent(), "customManagedLoc").toUri().getPath();
    String dbWhExternalLoc = new Path(primary.externalTableWarehouseRoot.getParent(),
            "customExternalLoc").toUri().getPath();
    String srcDb = "srcDb";
    WarehouseInstance.Tuple tuple = primary
            .run("create database " + srcDb + " LOCATION '" + dbWhExternalLoc + "' MANAGEDLOCATION '" + dbWhManagedLoc
                    + "' WITH DBPROPERTIES ( '" + SOURCE_OF_REPLICATION + "' = '1,2,3')")
            .run("use " + srcDb)
            .run("create table t1 (id int)")
            .run("insert into table t1 values (500)")
            .run("create external table t2 (id int)")
            .run("insert into table t2 values (1000)")
            .run("create table tp1 (id int) partitioned by (p int)")
            .run("insert into tp1 partition(p=1) values(10)")
            .run("insert into tp1 partition(p=2) values(20)")
            .dump(srcDb, withClauseOptions);

    replica.load(replicatedDbName, srcDb, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResult("500")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResult("1000")
            .run("show tables like 'tp1'")
            .verifyResult("tp1")
            .run("select id from tp1")
            .verifyResults(new String[]{"10", "20"});
    List<String> listOfTables = new ArrayList<>();
    listOfTables.addAll(Arrays.asList("t1", "t2", "tp1"));
    verifyCustomDBLocations(srcDb, listOfTables, dbWhManagedLoc, dbWhExternalLoc, true);
    primary.run("use " + srcDb)
            .run("insert into table t1 values (1000)")
            .run("insert into table t2 values (2000)")
            .run("insert into tp1 partition(p=1) values(30)")
            .run("insert into tp1 partition(p=2) values(40)")
            .dump(srcDb, withClauseOptions);
    replica.load(replicatedDbName, srcDb, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResults(new String[]{"500", "1000"})
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResults(new String[]{"1000", "2000"})
            .run("show tables like 'tp1'")
            .verifyResult("tp1")
            .run("select id from tp1")
            .verifyResults(new String[]{"10", "20", "30", "40"});
    primary.run("use " + srcDb)
            .run("insert into table t1 values (2000)")
            .run("insert into table t2 values (3000)")
            .run("create table t3 (id int)")
            .run("insert into table t3 values (3000)")
            .run("create external table t4 (id int)")
            .run("insert into table t4 values (4000)")
            .run("insert into tp1 partition(p=1) values(50)")
            .run("insert into tp1 partition(p=2) values(60)")
            .run("create table tp2 (id int) partitioned by (p int)")
            .run("insert into tp2 partition(p=1) values(100)")
            .run("insert into tp2 partition(p=2) values(200)")
            .dump(srcDb, withClauseOptions);
    replica.load(replicatedDbName, srcDb, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResults(new String[]{"500", "1000", "2000"})
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResults(new String[]{"1000", "2000", "3000"})
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("select id from t3")
            .verifyResults(new String[]{"3000"})
            .run("show tables like 't4'")
            .verifyResult("t4")
            .run("select id from t4")
            .verifyResults(new String[]{"4000"})
            .run("select id from tp1")
            .verifyResults(new String[]{"10", "20", "30", "40", "50", "60"})
            .run("show tables like 'tp1'")
            .verifyResult("tp1")
            .run("select id from tp2")
            .verifyResults(new String[]{"100", "200"});
    listOfTables.addAll(Arrays.asList("t3", "t4", "tp2"));
    verifyCustomDBLocations(srcDb, listOfTables, dbWhManagedLoc, dbWhExternalLoc, true);
  }

  @Test
  public void testCustomWarehouseLocationsConf() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(primary.repldDir, false);
    String dbWhManagedLoc = new Path(primary.warehouseRoot.getParent(), "customManagedLoc1").toUri().getPath();
    String dbWhExternalLoc = new Path(primary.externalTableWarehouseRoot.getParent(),
            "customExternalLoc1").toUri().getPath();
    String srcDb = "srcDbConf";
    WarehouseInstance.Tuple tuple = primary
            .run("create database " + srcDb + " LOCATION '" + dbWhExternalLoc + "' MANAGEDLOCATION '" + dbWhManagedLoc
                    + "' WITH DBPROPERTIES ( '" + SOURCE_OF_REPLICATION + "' = '1,2,3')")
            .run("use " + srcDb)
            .run("create table t1 (id int)")
            .run("insert into table t1 values (500)")
            .run("create external table t2 (id int)")
            .run("insert into table t2 values (1000)")
            .dump(srcDb, withClauseOptions);

    withClauseOptions.add("'" + HiveConf.ConfVars.REPL_RETAIN_CUSTOM_LOCATIONS_FOR_DB_ON_TARGET.varname + "'='false'");
    replica.load(replicatedDbName, srcDb, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResult("500")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResult("1000");
    List<String> listOfTables = new ArrayList<>();
    listOfTables.addAll(Arrays.asList("t1", "t2"));
    verifyCustomDBLocations(srcDb, listOfTables, dbWhManagedLoc, dbWhExternalLoc, false);
    primary.run("use " + srcDb)
            .run("insert into table t1 values (1000)")
            .run("insert into table t2 values (2000)")
            .dump(srcDb, withClauseOptions);
    replica.load(replicatedDbName, srcDb, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResults(new String[]{"500", "1000"})
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResults(new String[]{"1000", "2000"});
    primary.run("use " + srcDb)
            .run("insert into table t1 values (2000)")
            .run("insert into table t2 values (3000)")
            .run("create table t3 (id int)")
            .run("insert into table t3 values (3000)")
            .run("create external table t4 (id int)")
            .run("insert into table t4 values (4000)")
            .dump(srcDb, withClauseOptions);
    replica.load(replicatedDbName, srcDb, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResults(new String[]{"500", "1000", "2000"})
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResults(new String[]{"1000", "2000", "3000"})
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("select id from t3")
            .verifyResults(new String[]{"3000"})
            .run("show tables like 't4'")
            .verifyResult("t4")
            .run("select id from t4")
            .verifyResults(new String[]{"4000"});
    listOfTables.addAll(Arrays.asList("t3", "t4"));
    verifyCustomDBLocations(srcDb, listOfTables, dbWhManagedLoc, dbWhExternalLoc, false);
  }

  private void verifyCustomDBLocations(String srcDb, List<String> listOfTables, String managedCustLocOnSrc,
                                       String externalCustLocOnSrc, boolean replaceCustPath) throws Exception {
    Database replDatabase  = replica.getDatabase(replicatedDbName);
    String managedCustLocOnTgt = new Path(replDatabase.getManagedLocationUri()).toUri().getPath();
    String externalCustLocOnTgt = new Path(replDatabase.getLocationUri()).toUri().getPath();
    if (replaceCustPath ) {
      Assert.assertEquals(managedCustLocOnSrc,  managedCustLocOnTgt);
      Assert.assertNotEquals(managedCustLocOnTgt,  replica.warehouseRoot.toUri().getPath());
      Assert.assertEquals(externalCustLocOnSrc,  externalCustLocOnTgt);
      Assert.assertNotEquals(externalCustLocOnTgt,  new Path(replica.externalTableWarehouseRoot,
              replicatedDbName.toLowerCase()  + ".db").toUri().getPath());
    } else {
      Assert.assertNotEquals(managedCustLocOnSrc,  null);
      Assert.assertEquals(managedCustLocOnTgt, new Path(replica.warehouseRoot,
              replicatedDbName.toLowerCase() + ".db").toUri().getPath());
      Assert.assertNotEquals(externalCustLocOnSrc,  externalCustLocOnTgt);
      Assert.assertEquals(externalCustLocOnTgt,  new Path(replica.externalTableWarehouseRoot,
              replicatedDbName.toLowerCase()  + ".db").toUri().getPath());
    }
    verifyTableLocations(srcDb, replDatabase, listOfTables, replaceCustPath);
  }

  private void verifyTableLocations(String srcDb, Database replDb, List<String> tables, boolean customLocOntgt)
          throws Exception {
    String tgtExtBase = replica.getConf().get(HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname);
    for (String tname: tables) {
      Table table = replica.getTable(replicatedDbName, tname);
      if ("EXTERNAL_TABLE".equals(table.getTableType())) {
        String pathOnSrc = new Path(primary.getTable(srcDb, tname).getSd().getLocation()).toUri().getPath();
        Assert.assertEquals(new Path(table.getSd().getLocation()), new Path(tgtExtBase, pathOnSrc.substring(1)));
      } else {
        //Managed Table case
        Path tblPathOnTgt  = customLocOntgt
                ? new Path(replDb.getManagedLocationUri(), tname)
                : new Path(replica.warehouseRoot, replicatedDbName.toLowerCase()  + ".db" + "/" + tname );
        Assert.assertEquals(new Path(table.getSd().getLocation()), tblPathOnTgt);
      }
    }
  }

  private void verifyTableDataExists(WarehouseInstance warehouse, Path dbDataPath, String tableName,
                                     boolean shouldExists) throws IOException {
    FileSystem fileSystem = FileSystem.get(warehouse.warehouseRoot.toUri(), warehouse.getConf());
    Path tablePath = new Path(dbDataPath, tableName);
    Path dataFilePath = new Path(tablePath, "000000_0");
    Assert.assertEquals(shouldExists, fileSystem.exists(dbDataPath));
    Assert.assertEquals(shouldExists, fileSystem.exists(tablePath));
    Assert.assertEquals(shouldExists, fileSystem.exists(dataFilePath));
  }

  private List<String> getStagingLocationConfig(String stagingLoc, boolean addDistCpConfigs) throws IOException {
    List<String> confList = new ArrayList<>();
    confList.add("'" + HiveConf.ConfVars.REPL_DIR.varname + "'='" + stagingLoc + "'");
    if (addDistCpConfigs) {
      confList.add("'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'");
      confList.add("'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'");
      confList.add("'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
              + UserGroupInformation.getCurrentUser().getUserName() + "'");
    }
    return confList;
  }

  /*
   * Method used from TestTableLevelReplicationScenarios
   */
  private void verifyTableListForPolicy(FileSystem fileSystem, String dumpLocation, String[] tableList) throws Throwable {
    String hiveDumpLocation = dumpLocation + File.separator + ReplUtils.REPL_HIVE_BASE_DIR;
    Path tableListFile = new Path(hiveDumpLocation, ReplUtils.REPL_TABLE_LIST_DIR_NAME);
    tableListFile = new Path(tableListFile, primaryDbName.toLowerCase());

    if (tableList == null) {
      Assert.assertFalse(fileSystem.exists(tableListFile));
      return;
    } else {
      Assert.assertTrue(fileSystem.exists(tableListFile));
    }

    BufferedReader reader = null;
    try {
      InputStream inputStream = fileSystem.open(tableListFile);
      reader = new BufferedReader(new InputStreamReader(inputStream));
      Set tableNames = new HashSet<>(Arrays.asList(tableList));
      int numTable = 0;
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        numTable++;
        Assert.assertTrue(tableNames.contains(line));
      }
      Assert.assertEquals(numTable, tableList.length);
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }
}
