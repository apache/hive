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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils;
import org.apache.hadoop.hive.ql.parse.repl.metric.MetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Metadata;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.ReplicationMetric;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Stage;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK_PATHS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_SNAPSHOT_DIFF_FOR_EXTERNAL_TABLE_COPY;
import static org.apache.hadoop.hive.ql.exec.repl.ReplExternalTables.externalTableDataPath;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.NEW_SNAPSHOT;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.OLD_SNAPSHOT;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.deleteSnapshotIfExists;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.firstSnapshot;
import static org.apache.hadoop.hive.ql.exec.repl.util.SnapshotUtils.secondSnapshot;
import static org.apache.hadoop.hive.ql.parse.repl.metric.event.Metadata.ReplicationType.BOOTSTRAP;
import static org.apache.hadoop.hive.ql.parse.repl.metric.event.Metadata.ReplicationType.INCREMENTAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestReplicationScenariosUsingSnapshots extends BaseReplicationAcrossInstances {

  String extraPrimaryDb;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    HashMap<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());
    overrides.put(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY.varname, "false");
    overrides.put(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname, "true");
    overrides.put(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname,
        UserGroupInformation.getCurrentUser().getUserName());
    overrides.put(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname, "false");
    overrides.put(HiveConf.ConfVars.REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK.varname, "true");
    overrides.put(REPL_SNAPSHOT_DIFF_FOR_EXTERNAL_TABLE_COPY.varname, "true");

    internalBeforeClassSetup(overrides, TestReplicationScenarios.class);
  }

  @Before
  public void setup() throws Throwable {
    super.setup();
    extraPrimaryDb = "extra_" + primaryDbName;
  }

  @After
  public void tearDown() throws Throwable {
    primary.run("drop database if exists " + extraPrimaryDb + " cascade");
    SnapshottableDirectoryStatus[] snaps =
        primary.miniDFSCluster.getFileSystem().getSnapshottableDirListing();

    // In case of tests where in the end so directory stays snapshottable the listing will return null, so ignore,
    // else clean up all snapshots to allow minidfs to delete the directories and teardown.
    if (snaps != null) {
      for (SnapshottableDirectoryStatus sn : snaps) {
        Path path = sn.getFullPath();
        SnapshotUtils.deleteSnapshotSafe(primary.miniDFSCluster.getFileSystem(), path,
            firstSnapshot(primaryDbName.toLowerCase()));
        SnapshotUtils.deleteSnapshotSafe(primary.miniDFSCluster.getFileSystem(), path,
            secondSnapshot(primaryDbName.toLowerCase()));
      }
    }
    primary.miniDFSCluster.getFileSystem().delete(new Path("/"), true);
    super.tearDown();
  }


  @Test
  public void testBasicReplicationWithSnapshots() throws Throwable {

    // Create a partitioned and non-partitioned table and call dump.
    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create external table table1 (id int)")
        .run("insert into table table1 values (1)")
        .run("insert into table table1 values (2)")
        .run("create external table table2 (place string) partitioned by "
            + "(country string)")
        .run("insert into table table2 partition(country='india') values "
            + "('bangalore')")
        .run("insert into table table2 partition(country='us') values "
            + "('austin')")
        .run("insert into table table2 partition(country='france') values "
            + "('paris')")
        .dump(primaryDbName);

    // Call load, For the first time, only snapshots would be created and distCp would run from source snapshot to
    // target.
    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 'table1'")
        .verifyResult("table1")
        .run("show tables like 'table2'")
        .verifyResult("table2")
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("select country from table2 where country = 'us'")
        .verifyResult("us")
        .run("select country from table2 where country = 'france'")
        .verifyResult("france")
        .run("show partitions table2")
        .verifyResults(new String[] {"country"
            + "=france", "country=india", "country=us"})
        .run("select * from table1").
        verifyResults(new String[]{"1","2"});

    // Verify Snapshots are created in source.
    validateInitialSnapshotsCreated(primary.getDatabase(primaryDbName).getLocationUri());

    String hiveDumpLocation = tuple.dumpLocation + File.separator + ReplUtils.REPL_HIVE_BASE_DIR;
    // Ckpt should be set on bootstrapped db.
    replica.verifyIfCkptSet(replicatedDbName, hiveDumpLocation);

    // Create a new table and do dump, for these also it should do a normal distcp and copy from snapshot directory

    tuple = primary.run("use " + primaryDbName)
        .run("create external table table3 (id int)")
        .run("insert into table table3 values (10)")
        .run("create external table table4 as select id from table3")
        .dump(primaryDbName);

    // Verify that the table info is written correctly for incremental

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 'table3'")
        .verifyResult("table3")
        .run("select id from table3")
        .verifyResult("10")
        .run("select id from table4")
        .verifyResult("10");

    // Check the new snapshots are created.
    validateDiffSnapshotsCreated(primary.getDatabase(primaryDbName).getLocationUri());

    // Try deleting a directory and add data to a already dumped and loaded table for using snapshot diff
    tuple = primary.run("use " + primaryDbName)
        .run("drop table table2")
        .run("insert into table1 values (3),(4) ")
        .dump(primaryDbName);

    // Check if the dropped table isn't there and the new data is available.
    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("select * from table1 order by id")
        .verifyResults(new String[]{"1","2","3","4"})
        .run("show tables like 'table2'")
        .verifyFailure(new String[] {"table2" });
  }

  @Test
  public void testBasicStartFromIncrementalReplication() throws Throwable {

    // Run a cycle of dump & load with snapshot disabled.
    ArrayList<String> withClause = new ArrayList<>(1);
    withClause.add("'"+ REPL_SNAPSHOT_DIFF_FOR_EXTERNAL_TABLE_COPY + "' = 'false'");
    WarehouseInstance.Tuple tuple = primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause);

    tuple = primary.run("use " + primaryDbName)
        .run("create external table t1 (place string) partitioned by (country string) row format "
            + "delimited fields terminated by ',' ")
        .run("insert into t1 partition(country='india') values ('delhi')")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("show partitions t1")
        .verifyResults(new String[] { "country=india"})
        .run("select place from t1 order by place")
        .verifyResults(new String[] {"delhi"})
        .verifyReplTargetProperty(replicatedDbName);

    // Check snapshots aren't enabled, since the replication was run without snapshots enabled.
    Path locationPath = new Path(primary.getDatabase(primaryDbName).getLocationUri());
    DistributedFileSystem dfs = (DistributedFileSystem) locationPath.getFileSystem(conf);
    assertFalse(dfs.getFileStatus(locationPath).isSnapshotEnabled());

    // Enable snapshot based copy
    primary.getConf().setBoolVar(REPL_SNAPSHOT_DIFF_FOR_EXTERNAL_TABLE_COPY, true);
    replica.getConf().setBoolVar(REPL_SNAPSHOT_DIFF_FOR_EXTERNAL_TABLE_COPY, true);

    // Add some data & then try a dump & load cycle.
    primary.run("use " + primaryDbName)
        .run("insert into t1 partition(country='india') values ('mumbai')")
        .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("select place from t1 order by place")
        .verifyResults(new String[] {"delhi", "mumbai"})
        .verifyReplTargetProperty(replicatedDbName);

    // Check if the initial snapshots are created.
    validateInitialSnapshotsCreated(primary.getDatabase(primaryDbName).getLocationUri());

    // Run one more cycle of incremental dump & load to see diff snapshots are created, and add a new table so as to
    // see if it gets included.

    tuple = primary.run("use " + primaryDbName)
        .run("insert into t1 partition(country='india') values ('pune')")
        .run("create external table t2 (place string) partitioned by (country string) row format "
            + "delimited fields terminated by ',' ")
        .run("insert into t2 partition(country='usa') values ('new york')")
        .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 't2'")
        .verifyResult("t2")
        .run("show partitions t2")
        .verifyResults(new String[] {"country=usa"})
        .run("select place from t2 order by place")
        .verifyResults(new String[] {"new york"})
        .run("select place from t1 order by place")
        .verifyResults(new String[] {"delhi", "mumbai", "pune"})
        .verifyReplTargetProperty(replicatedDbName);

    // Check if diff snapshots are created.
    validateDiffSnapshotsCreated(primary.getDatabase(primaryDbName).getLocationUri());

  }


  @Test
  public void testBasicExternalTableWithPartitions() throws Throwable {
    Path externalTableLocation =
        new Path("/" + testName.getMethodName() + "/t2/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocation, new FsPermission("777"));

    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);

    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create external table t2 (place string) partitioned by (country string) row format "
            + "delimited fields terminated by ',' location '" + externalTableLocation.toString()
            + "'")
        .run("insert into t2 partition(country='india') values ('bangalore')")
        .dump(primaryDbName, withClause);


    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 't2'")
        .verifyResults(new String[] { "t2" })
        .run("select place from t2")
        .verifyResults(new String[] { "bangalore" })
        .verifyReplTargetProperty(replicatedDbName);


    // add new  data externally, to a partition, but under the table level top directory
    Path partitionDir = new Path(externalTableLocation, "country=india");
    try (FSDataOutputStream outputStream = fs.create(new Path(partitionDir, "file.txt"))) {
      outputStream.write("pune\n".getBytes());
      outputStream.write("mumbai\n".getBytes());
    }

    tuple = primary.run("use " + primaryDbName)
        .run("insert into t2 partition(country='australia') values ('sydney')")
        .dump(primaryDbName, withClause);


    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select distinct(country) from t2")
        .verifyResults(new String[] { "india", "australia" })
        .run("select place from t2 where country='india'")
        .verifyResults(new String[] { "bangalore", "pune", "mumbai" })
        .run("select place from t2 where country='australia'")
        .verifyResults(new String[] { "sydney" })
        .run("show partitions t2")
        .verifyResults(new String[] {"country=australia", "country=india"})
        .verifyReplTargetProperty(replicatedDbName);

    String tmpLocation2 = "/tmp1/" + System.nanoTime() + "_2";
    primary.miniDFSCluster.getFileSystem().mkdirs(new Path(tmpLocation2), new FsPermission("777"));

    // Try alter table location and then dump.
    primary.run("use " + primaryDbName)
        .run("insert into table t2 partition(country='france') values ('lyon')")
        .run("alter table t2 set location '" + tmpLocation2 + "'")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("show partitions t2").verifyResults(
        new String[] {"country=australia", "country=india", "country=france"});

    // Test alter partition location.
    String tmpLocation3 = "/tmp1/" + System.nanoTime() + "_3";
    primary.miniDFSCluster.getFileSystem()
        .mkdirs(new Path(tmpLocation2), new FsPermission("777"));
    primary.run("use " + primaryDbName).run(
        "alter table t2 partition (country='australia') set location '"
            + tmpLocation3 + "'").run(
        "insert into table t2 partition(country='australia') values "
            + "('sydney')").dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("select place from t2 where country='australia'")
        .verifyResult("sydney");
  }

  @Test
  public void testSnapshotCleanupsOnDatabaseLocationChange() throws Throwable {
    Path externalDatabaseLocation = new Path("/" + testName.getMethodName() + "/externalDatabase/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalDatabaseLocation, new FsPermission("777"));

    Path externalDatabaseLocationAlter = new Path("/" + testName.getMethodName() + "/externalDatabaseAlter/");
    fs.mkdirs(externalDatabaseLocationAlter, new FsPermission("777"));

    Path externalDatabaseLocationDest =
        new Path(REPLICA_EXTERNAL_BASE, testName.getMethodName() + "/externalDatabase/");

    Path externalDatabaseLocationAlterDest =
        new Path(REPLICA_EXTERNAL_BASE, testName.getMethodName() + "/externalDatabaseAlter/");

    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);

    // Create a normal and partitioned table in the database location.

    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("alter database " + primaryDbName + " set location '" + externalDatabaseLocation + "'")
        .run("create external table emp1 (id int)")
        .run("insert into emp1 values(1),(2)")
        .run("create external table t2 (place string) partitioned by (country "
            + "string) row format delimited fields terminated by ','")
        .run("insert into t2 partition(country='india') values ('bangalore')")
        .dump(primaryDbName, withClause);

    // Do a load, post that one snapshot should have been created in the source for each table.

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 'emp1'")
        .verifyResults(new String[] {"emp1"})
        .run("select id from emp1")
        .verifyResults(new String[] {"1", "2"})
        .run("select place from t2")
        .verifyResults(new String[] {"bangalore"})
        .verifyReplTargetProperty(replicatedDbName);

    // Check the directory is snapshotable.
    assertTrue(fs.getFileStatus(externalDatabaseLocation).isSnapshotEnabled());

    // Check if the snapshot exists at source.
    assertNotNull(fs.getFileStatus(
        new Path(externalDatabaseLocation, ".snapshot/" + secondSnapshot(primaryDbName.toLowerCase()))));

    // Check if the destination is snapshottable.
    assertTrue(fs.getFileStatus(externalDatabaseLocationDest).isSnapshotEnabled());

    // Check if snapshot exist at destination.
    assertNotNull(fs.getFileStatus(
        new Path(externalDatabaseLocationDest, ".snapshot/" + firstSnapshot(primaryDbName.toLowerCase()))));

    // Alter database location and create another table inside it.

    tuple = primary.run("use " + primaryDbName).run(
        "alter database " + primaryDbName + " set location '"
            + externalDatabaseLocationAlter + "'")
        .run("create external table empnew (id int)")
        .run("insert into empnew values (3),(4)").run("drop table emp")
        .run("insert into t2 partition(country='france') " + "values('paris')")
        .dump(primaryDbName, withClause);

    // Do a load and see if everything is correct.

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 'emp'")
        .verifyFailure(new String[] {"emp"})
        .run("show tables like 'empnew'")
        .verifyResults(new String[] {"empnew"})
        .run("select id from empnew")
        .verifyResults(new String[] {"3", "4"})
        .run("select place from t2 where country='france'")
        .verifyResults(new String[] {"paris"})
        .run("select place from t2 where country='india'")
        .verifyResults(new String[] {"bangalore"})
        .verifyReplTargetProperty(replicatedDbName);

    // Check if the new database location is snapshottable.
    assertTrue(fs.getFileStatus(externalDatabaseLocationAlter).isSnapshotEnabled());

    // Check if the snapshot exists at source.
    assertNotNull(fs.getFileStatus(
        new Path(externalDatabaseLocationAlter, ".snapshot/" +  secondSnapshot(primaryDbName.toLowerCase()))));

    // Check if the old snapshot got deleted at source.
    LambdaTestUtils.intercept(FileNotFoundException.class, () -> fs
        .getFileStatus(new Path(externalDatabaseLocation, ".snapshot/" + secondSnapshot(primaryDbName.toLowerCase()))));

    // Check if the new destination database location is snapshottable.
    assertTrue(fs.getFileStatus(externalDatabaseLocationAlterDest).isSnapshotEnabled());

    // Check if snapshot exist at destination.
    assertNotNull(fs.getFileStatus(
        new Path(externalDatabaseLocationAlterDest, ".snapshot/" + firstSnapshot(primaryDbName.toLowerCase()))));

    //Check if the destination old snapshot is deleted.
    LambdaTestUtils.intercept(FileNotFoundException.class, () -> fs.getFileStatus(
        new Path(externalDatabaseLocationDest, ".snapshot/" + firstSnapshot(primaryDbName.toLowerCase()))));
  }

  @Test
  public void testFailureScenarios() throws Throwable {
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);

    // Create a table which couldn't create snapshot during dump.
    Path externalTableLocationSource = new Path("/" + testName.getMethodName() + "source1/tablesource/");

    fs.mkdirs(externalTableLocationSource, new FsPermission("777"));

    // Allow snapshot on the parent of table directory, the creation of snapshot shall fail for the table directory
    // during dump.
    fs.allowSnapshot(externalTableLocationSource.getParent());

    // Create a table which can not create snapshot on the destination.
    Path externalTableLocationDest = new Path("/" + testName.getMethodName() + "dest1/tabledest/");
    fs.mkdirs(externalTableLocationDest, new FsPermission("777"));

    Path externalTableLocationDestInDest = new Path(REPLICA_EXTERNAL_BASE, testName.getMethodName() + "dest1/tabledest/");

    withClause.add("'hive.repl.external.warehouse.single.copy.task.paths'='" + externalTableLocationSource
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "'");

    try {
      WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
          .run("create external table tablesource (place string) row format delimited fields terminated by ',' location '"
              + externalTableLocationSource.toString() + "'")
          .run("insert into tablesource values ('bangalore')")
          .dump(primaryDbName, withClause);
      fail("Should have thrown snapshot exception");
    } catch (SnapshotException se) {
      // Ignore
    }
    // Check if there is a non-recoverable error or not.
    Path baseDumpDir = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPL_DIR));
    Path nonRecoverablePath =
        TestReplicationScenarios.getNonRecoverablePath(baseDumpDir, primaryDbName, primary.hiveConf);
    assertTrue(fs.exists(nonRecoverablePath));

    // Fix the table and dump & load which should be success.
    fs.disallowSnapshot(externalTableLocationSource.getParent());
    fs.delete(nonRecoverablePath, true);

    primary.dump(primaryDbName, withClause);

    // Load and check if the data is there, we should have fallback to normal mode

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select place from tablesource")
        .verifyResults(new String[] {"bangalore"})
        .verifyReplTargetProperty(replicatedDbName);

    // Create a table for which target is non-snapshottable.

    // Allow snapshot on the parent of destination.
    fs.mkdirs(externalTableLocationDestInDest.getParent());
    fs.allowSnapshot(externalTableLocationDestInDest.getParent());
    withClause.add("'hive.repl.external.warehouse.single.copy.task.paths'='" + externalTableLocationSource
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "," + externalTableLocationDest
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "'");

    WarehouseInstance.Tuple tuple;
    try {
       tuple = primary.run("use " + primaryDbName).run(
          "create external table tabledest (place string) partitioned by (country string) row format "
              + "delimited fields terminated by ',' location '" + externalTableLocationDest.toString() + "'")
          .run("insert into tabledest partition(country='india') values ('kanpur')")
          .run("insert into tabledest partition(country='india') values ('lucknow')")
          .run("insert into tabledest partition(country='usa') values ('New York')")
          .run("insert into tablesource values('chennai')")
          .dump(primaryDbName, withClause);
      fail("Expected a snapshot exception.");
    } catch (SecurityException se) {
      // This is expected!!!
    }
    nonRecoverablePath =
        TestReplicationScenarios.getNonRecoverablePath(baseDumpDir, primaryDbName, primary.hiveConf);
    assertTrue(fs.exists(nonRecoverablePath));
   fs.delete(nonRecoverablePath, true);

    fs.disallowSnapshot(externalTableLocationDestInDest.getParent());

    primary.dump(primaryDbName, withClause);

        replica.load(replicatedDbName, primaryDbName, withClause)
            .run("use " + replicatedDbName)
            .run("select place from tabledest where country='usa'")
            .verifyResults(new String[] {"New York"})
            .run("select place from tabledest where country='india'")
            .verifyResults(new String[] { "kanpur", "lucknow" })
            .run("select place from tablesource")
            .verifyResults(new String[] {"bangalore", "chennai"});

    // Add a new table which will fail to create snapshots, post snapshots for other tables have been created.
    Path externalTableLocationSource2 = new Path("/" + testName.getMethodName() + "source2/tablesource/");

    fs.mkdirs(externalTableLocationSource2, new FsPermission("777"));
    fs.allowSnapshot(externalTableLocationSource2.getParent());

    withClause.add("'hive.repl.external.warehouse.single.copy.task.paths'='" + externalTableLocationSource
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "," + externalTableLocationDestInDest
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "," + externalTableLocationSource2
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "'");
    try {
      tuple = primary.run("use " + primaryDbName)
          .run("create external table tablesource2 (place string) row format delimited fields terminated by ',' "
              + "location '" + externalTableLocationSource2.toString() + "'")
          .run("insert into tablesource2 values ('delhi')")
          .run("insert into tablesource2 values ('noida')")
          .run("insert into tabledest partition(country='usa') values ('San Jose')")
          .run("insert into tablesource values('kolkata')")
          .dump(primaryDbName, withClause);
      fail("Expected a snapshot exception.");
    } catch (SnapshotException se) {
      // Expected, Ignore
    }

    nonRecoverablePath = TestReplicationScenarios.getNonRecoverablePath(baseDumpDir, primaryDbName, primary.hiveConf);
    assertTrue(fs.exists(nonRecoverablePath));
    fs.delete(nonRecoverablePath, true);

    fs.disallowSnapshot(externalTableLocationSource2.getParent());
    primary
        .run("insert into tablesource values('patna')")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select place from tabledest where country='usa'")
        .verifyResults(new String[] {"New York", "San Jose"})
        .run("select place from tabledest where country='india'")
        .verifyResults(new String[] { "kanpur", "lucknow" })
        .run("select place from tablesource")
        .verifyResults(new String[] {"bangalore", "chennai", "kolkata", "patna"})
        .run("select place from tablesource2")
        .verifyResults(new String[] {"delhi", "noida"});
  }

  @Test
  public void testCustomPathTableSnapshots() throws Throwable {
    Path externalTableLocation1 = new Path("/" + testName.getMethodName() + "/t1/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocation1, new FsPermission("777"));

    Path externalTableLocation2 = new Path("/" + testName.getMethodName() + "/t2/");
    fs.mkdirs(externalTableLocation1, new FsPermission("777"));

    Path externalTablePart1 = new Path("/" + testName.getMethodName() + "/part1/");
    fs.mkdirs(externalTableLocation1, new FsPermission("777"));

    Path externalTablePart2 = new Path("/" + testName.getMethodName() + "/part2/");
    fs.mkdirs(externalTableLocation1, new FsPermission("777"));

    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'hive.repl.external.warehouse.single.copy.task.paths'='" + externalTableLocation2
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "," + externalTablePart1
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "'");

    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create external table t1 (place string) partitioned by (country"
            + " string) row format "
            + "delimited fields terminated by ',' location '" + externalTableLocation1.toString()
            + "'")
        .run("insert into t1 partition(country='india') values ('bangalore')")
        .run("create external table t2 (place string) partitioned by (country"
            + " string) row format "
            + "delimited fields terminated by ',' location '" + externalTableLocation2.toString()
            + "'")
        .run("insert into t1 partition(country='australia') values "
            + "('sydney')")
        .run("insert into t2 partition(country='nepal') values "
            + "('kathmandu')")
        .run("alter table t1 add partition (country='france') location '"+externalTablePart1.toString() + "'")
        .run("insert into t1 partition(country='france') values ('paris')")
        .run("alter table t2 add partition (country='china') location '"+externalTablePart2.toString() + "'")
        .run("insert into t2 partition(country='china') values ('beejing')")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResults(new String[] {"t1"})
        .run("show tables like 't2'")
        .verifyResults(new String[] {"t2"})
        .run("select place from t1 where country='india'")
        .verifyResults(new String[] {"bangalore"})
        .run("select place from t1 where country='france'")
        .verifyResults(new String[] {"paris"})
        .run("select place from t2 where country='nepal'")
        .verifyResults(new String[] {"kathmandu"})
        .run("select place from t2 where country='china'")
        .verifyResults(new String[] {"beejing"})
        .verifyReplTargetProperty(replicatedDbName);

    // Check if the t2 directory is snapshotoble and the snapshot is there.
    validateInitialSnapshotsCreated(externalTableLocation2.toString());

    // Check if the partition directory is snapshottable and has snapshots
    validateInitialSnapshotsCreated(externalTablePart1.toString());

    // Check the table not specified as configuration is not snapshottable
    assertFalse(fs.getFileStatus(externalTableLocation1).isSnapshotEnabled());

    // Check the partition not specified as configuration is not snapshottable
    assertFalse(fs.getFileStatus(externalTablePart2).isSnapshotEnabled());

    // Drop the snapshotted table and create another table with same location and add the other table to the config.
    withClause.add("'hive.repl.external.warehouse.single.copy.task.paths'='" + externalTableLocation2
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "," + externalTableLocation1
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString()+ "," + externalTablePart1
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "'");

    primary.run("use " + primaryDbName)
        .run("drop table t2")
        .run("create external table t4 (place string) partitioned by (country string) row format delimited fields"
            + " terminated by ',' location '" + externalTableLocation2.toString() + "'")
        .run("MSCK REPAIR TABLE t4 ADD PARTITIONS")
        .run("insert into t4 partition(country='china') values ('Shanghai')") // new partition in the recreated table.
        .run("insert into t4 partition(country='nepal') values ('pokhra')") // entry in old existing partition in recreated table.
        .run("insert into t1 partition(country='india') values ('chennai')") // entry for newely added path in list.
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 't2'")
        .verifyFailure(new String[] {"t1"})
        .run("select place from t1 where country='india'")
        .verifyResults(new String[] {"bangalore", "chennai"})
        .run("select place from t1 where country='france'")
        .verifyResults(new String[] {"paris"})
        .run("select place from t4 where country='nepal'")
        .verifyResults(new String[] {"kathmandu", "pokhra"}) // Old and new entry both should be there
        .run("select place from t4 where country='china'")
        .verifyResults(new String[] {"Shanghai"}) // the newely added partition should be there.
        .verifyReplTargetProperty(replicatedDbName);

    // The newly added path should have the initial snapshot
    validateInitialSnapshotsCreated(externalTableLocation1.toString());
    // The old path should have diff snapshot though the table got deleted, since the new table uses the same path
    validateDiffSnapshotsCreated(externalTableLocation2.toString());
  }

  @Test
  public void testCustomPathTableSnapshotsCleanup() throws Throwable {
    Path externalTableLocation1 = new Path("/" + testName.getMethodName() + "/t1/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocation1, new FsPermission("777"));

    Path externalTableLocation1Target = new Path("/replica_external_base/" + testName.getMethodName() + "/t1/");

    Path externalTableLocation2 = new Path("/" + testName.getMethodName() + "/t2/");
    fs.mkdirs(externalTableLocation1, new FsPermission("777"));

    Path externalTableLocation2Target = new Path("/replica_external_base/" + testName.getMethodName() + "/t2/");

    Path externalTableLocation2New = new Path("/" + testName.getMethodName() + "/t2alter/");
    fs.mkdirs(externalTableLocation2New, new FsPermission("777"));


    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'hive.repl.external.warehouse.single.copy.task.paths'='" + externalTableLocation2
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "," + externalTableLocation1
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "'");

    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create external table t1 (place string) partitioned by (country string) row format "
            + "delimited fields terminated by ',' location '" + externalTableLocation1.toString() +"'")
        .run("insert into t1 partition(country='india') values ('bangalore')")
        .run("create external table t2 (place string) partitioned by (country string) row format "
            + "delimited fields terminated by ',' location '" + externalTableLocation2.toString() + "'")
        .run("insert into t2 partition(country='nepal') values ('kathmandu')")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResults(new String[] {"t1"})
        .run("show tables like 't2'")
        .verifyResults(new String[] {"t2"})
        .run("select place from t1 where country='india'")
        .verifyResults(new String[] {"bangalore"})
        .run("select place from t2 where country='nepal'")
        .verifyResults(new String[] {"kathmandu"})
        .verifyReplTargetProperty(replicatedDbName);

    // Check if the t1 directory is snapshotoble and the snapshot is there.
    assertTrue(fs.getFileStatus(externalTableLocation1).isSnapshotEnabled());
    assertNotNull(
        fs.getFileStatus(new Path(externalTableLocation1, ".snapshot/" + secondSnapshot(primaryDbName.toLowerCase()))));
    assertTrue(fs.getFileStatus(externalTableLocation1Target).isSnapshotEnabled());

    // Check if the t2 directory is snapshotoble and the snapshot is there.
    assertTrue(fs.getFileStatus(externalTableLocation2).isSnapshotEnabled());
    assertNotNull(
        fs.getFileStatus(new Path(externalTableLocation2, ".snapshot/" + secondSnapshot(primaryDbName.toLowerCase()))));
    assertTrue(fs.getFileStatus(externalTableLocation2Target).isSnapshotEnabled());

    // Drop table t1 and alter the location of table t2
    tuple = primary.run("use " + primaryDbName)
        .run("drop table t1")
        .run("alter table t2 SET LOCATION '" + externalTableLocation2New +"'")
        .run("alter table t2 drop partition(country='nepal')")
        .run("insert into t2 partition(country='china') values ('beejing')")
        .dump(primaryDbName, withClause);

    // Load and check if the data is there and the dropped table isn't there.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyFailure(new String[] {"t1"})
        .run("show tables like 't2'")
        .verifyResults(new String[] {"t2"})
        .run("select place from t2 where country='china'")
        .verifyResults(new String[] {"beejing"})
        .verifyReplTargetProperty(replicatedDbName);

    // Verify the new location is not snapshottable
    assertFalse(fs.getFileStatus(externalTableLocation2New).isSnapshotEnabled());

    // Verify the old location is not snapshottable now and the snapshot is cleaned up.
    assertFalse(fs.getFileStatus(externalTableLocation1).isSnapshotEnabled());
    assertFalse(fs.getFileStatus(externalTableLocation1Target).isSnapshotEnabled());
    assertFalse(fs.getFileStatus(externalTableLocation2).isSnapshotEnabled());
    assertFalse(fs.getFileStatus(externalTableLocation2Target).isSnapshotEnabled());
  }

  @Test
  public void testTargetModified() throws Throwable {

    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create external table table1 (id int)")
        .run("insert into table table1 values (1)")
        .run("insert into table table1 values (2)")
        .dump(primaryDbName);

    // Call load, For the first time, only snapshots would be created and distCp would run from source snapshot to
    // target.
    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 'table1'")
        .verifyResult("table1")
        .run("select * from table1").
        verifyResults(new String[]{"1","2"});

    // Verify Snapshots are created in source.
    validateInitialSnapshotsCreated(primary.getDatabase(primaryDbName).getLocationUri());

    // Create a new table and do dump

    tuple = primary.run("use " + primaryDbName)
        .run("create external table table3 (id int)")
        .run("insert into table table3 values (10)")
        .run("insert into table table1 values (3)")
        .dump(primaryDbName);

    // Verify that the table info is written correctly for incremental

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 'table3'")
        .verifyResult("table3")
        .run("select id from table3")
        .verifyResult("10")
        .run("select id from table1")
        .verifyResults(new String[] {"1", "2", "3"});

    // Check the new snapshots are created.
    validateDiffSnapshotsCreated(primary.getDatabase(primaryDbName).getLocationUri());

    // Now modify the target by deleting one table directory

    Path targetWhPath = externalTableDataPath(conf, REPLICA_EXTERNAL_BASE,
        new Path(primary.getDatabase(primaryDbName).getLocationUri()));
    DistributedFileSystem replicaDfs = (DistributedFileSystem) targetWhPath.getFileSystem(conf);
    assertTrue(replicaDfs.delete(new Path(targetWhPath, "table1"), true));

    // Add some data to the table and do a Dump & Load
    tuple = primary.run("use " + primaryDbName)
        .run("insert into table table3 values (11)")
        .run("insert into table table1 values (4)")
        .dump(primaryDbName);

    // Load and see the data is there.

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("select id from table3")
        .verifyResults(new String[]{"10","11"})
        .run("select id from table1")
        .verifyResults(new String[] {"1", "2", "3", "4"});

    // Modify the target by adding and modifying data
    replicaDfs.create(new Path(targetWhPath, "table1/000000_0_new")).close();
    replicaDfs.create(new Path(targetWhPath, "table1/000000_0"), true).close();

    // Add some more data to table1
    tuple = primary.run("use " + primaryDbName)
        .run("insert into table table1 values (5)")
        .dump(primaryDbName);

    // Load and see the new data is there.
    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("select id from table1")
        .verifyResults(new String[] {"1", "2", "3", "4", "5"});

  }

  @Test
  public void testSnapshotMetrics() throws Throwable {
    conf.set(Constants.SCHEDULED_QUERY_SCHEDULENAME, "metrics_test");
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    MetricCollector collector = MetricCollector.getInstance();
    Path externalDatabaseLocation = new Path("/" + testName.getMethodName() + "/externalDatabase/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalDatabaseLocation, new FsPermission("777"));

    Path externalTableLocation1 = new Path("/" + testName.getMethodName() + "/t1/");
    fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocation1, new FsPermission("777"));
    withClause.add("'hive.repl.external.warehouse.single.copy.task.paths'='" + externalTableLocation1
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "'");

    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create external table emp1 (id int)")
        .run("insert into emp1 values(1),(2)")
        .run("create external table exttab (place string) row format delimited fields terminated by ','"
            + " location '" + externalTableLocation1.toString() +"'")
        .run("insert into exttab values('lucknow')")
        .dump(primaryDbName, withClause);

    // The boootstrap stage, 2 directories for which snapshot is enabled, the database directory and the one table
    // as part of the config. This would be initial copy stage, so only 1 snapshot per directory and none to be deleted.
    assertIncrementalMetricsValues(BOOTSTRAP, collector, 2 ,0);


    Iterator<ReplicationMetric> itr = collector.getMetrics().iterator();
    while (itr.hasNext()) {
      ReplicationMetric elem = itr.next();
      assertEquals(BOOTSTRAP, elem.getMetadata().getReplicationType());
      List<Stage> stages = elem.getProgress().getStages();
      for (Stage stage : stages) {
        SnapshotUtils.ReplSnapshotCount counts = stage.getReplSnapshotCount();
        assertEquals(2, counts.getNumCreated());
        assertEquals(0, counts.getNumDeleted());
      }
    }

    // Load and check if the data and table are there.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 'emp1'")
        .verifyResults(new String[] {"emp1"})
        .run("select id from emp1")
        .verifyResults(new String[] {"1", "2"})
        .run("show tables like 'exttab'")
        .verifyResults(new String[]{"exttab"})
        .run("select place from exttab")
        .verifyResults(new String[] {"lucknow"})
        .verifyReplTargetProperty(replicatedDbName);

    // Add some data and try incremental dump.

    tuple = primary.run("use " + primaryDbName)
        .run("insert into emp1 values(3),(4)")
        .run("insert into exttab values('agra')")
        .dump(primaryDbName, withClause);

    // This is from the diff stage, 2 Directories where snapshots were enabled, 1 old snapshots got deleted and 1
    // got created, so 2 created and 2 deleted.
    assertIncrementalMetricsValues(INCREMENTAL, collector, 2 ,2);

    // Do a load
    replica.load(replicatedDbName, primaryDbName, withClause);

    // Remove the with clause, hence the external table specified as part of the config.
    tuple = primary.run("use " + primaryDbName)
        .run("insert into exttab values('lucknow')")
        .dump(primaryDbName, null);

    // Only one directory, i.e the database directory is going through snapshot based replication, so only 1 created
    // for it and 1 old deleted for it, 2 deleted for the table removed from the snapshot based replication scope.
    assertIncrementalMetricsValues(INCREMENTAL, collector, 1 ,3);
  }

  private void assertIncrementalMetricsValues(Metadata.ReplicationType replicationType, MetricCollector collector, int numCreated,
      int numDeleted) {
    Iterator<ReplicationMetric> itr;
    itr = collector.getMetrics().iterator();
    while (itr.hasNext()) {
      ReplicationMetric elem = itr.next();
      assertEquals(replicationType, elem.getMetadata().getReplicationType());
      List<Stage> stages = elem.getProgress().getStages();
      for (Stage stage : stages) {
        SnapshotUtils.ReplSnapshotCount count = stage.getReplSnapshotCount();
        assertEquals(numCreated, count.getNumCreated());
        assertEquals(numDeleted, count.getNumDeleted());
      }
    }
  }

  @Test
  public void testPurgeAndReBootstrap() throws Throwable {
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();

    Path externalTableLocation1 = new Path("/" + testName.getMethodName() + "/table1/");
    fs.mkdirs(externalTableLocation1, new FsPermission("777"));

    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'hive.repl.external.warehouse.single.copy.task.paths'='" + externalTableLocation1
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "'");

    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create external table table1 (place string) partitioned by (country string) row format "
            + "delimited fields terminated by ',' location '" + externalTableLocation1.toString() + "'")
        .run("ALTER TABLE table1 SET TBLPROPERTIES ('external.table.purge'='true')")
        .run("create external table table2 (id int)")
        .run("create external table table3 (id int)")
        .run("ALTER TABLE table3 SET TBLPROPERTIES ('external.table.purge'='true')")
        .run("insert into table1 partition(country='nepal') values ('kathmandu')")
        .run("insert into table1 partition(country='china') values ('beejing')")
        .run("insert into table2 values(1)")
        .run("insert into table3 values(5)")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 'table1'")
        .verifyResults(new String[] {"table1"})
        .run("select place from table1 where country='nepal'")
        .verifyResults(new String[] {"kathmandu"})
        .run("select place from table1 where country='china'")
        .verifyResults(new String[] {"beejing"})
        .verifyReplTargetProperty(replicatedDbName);

    // Check if the t2 directory is snapshotoble and the snapshot is there.
    validateInitialSnapshotsCreated(externalTableLocation1.toString());

    // Add some more data and do a dump & load
    primary.run("use " + primaryDbName)
        .run("insert into table1 partition(country='china') values ('wuhan')")
        .run("insert into table2 values(2)")
        .run("insert into table3 values(6)")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select place from table1 where country='china'")
        .verifyResults(new String[] {"beejing", "wuhan"})
        .verifyReplTargetProperty(replicatedDbName);

    // Verify if diff snapshots is there.
    validateDiffSnapshotsCreated(externalTableLocation1.toString());

    assertTrue(fs.listStatus(externalTableLocation1).length > 0);

    // Now purge the table
    primary.run("use " + primaryDbName)
        .run("drop table table1 purge")
        .run("insert into table2 values(3)")
        .run("insert into table3 values(7)")
        .dump(primaryDbName, withClause);

    // Check if target dir exists.
    assertFalse(fs.exists(externalTableLocation1));

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 'table1'")
        .verifyFailure(new String[] {"table1"})
        .run("select id from table2")
        .verifyResults(new String[] {"1", "2", "3"})
        .run("select id from table3")
        .verifyResults(new String[] {"5", "6", "7"})
        .verifyReplTargetProperty(replicatedDbName);

    Path externalDbLocation = new Path(replica.getDatabase(replicatedDbName).getLocationUri());

    assertTrue(fs.exists(externalDbLocation));

    replica.run("use " + replicatedDbName)
        .run("drop table table2")
        .run("drop table table3")
        .run("drop database "+ replicatedDbName + " cascade");

    assertFalse(fs.exists(externalDbLocation));

    // Delete to force a bootstrap
    Path parent = new Path(tuple.dumpLocation).getParent().getParent();

    fs.delete(parent, true);

    // Delete the external data base dir.
    Path dbLocation = new Path(REPLICA_EXTERNAL_BASE,
        Path.getPathWithoutSchemeAndAuthority(new Path(primary.getDatabase(primaryDbName).getLocationUri())).toString()
            .replaceFirst("/", ""));
    deleteReplRelatedSnapshots(fs, dbLocation, conf);
    fs.delete(REPLICA_EXTERNAL_BASE, true);

    primary.run("use " + primaryDbName)
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 'table1'")
        .verifyFailure(new String[] {"table1"})
        .run("select id from table2")
        .verifyResults(new String[] {"1", "2", "3"})
        .run("select id from table3")
        .verifyResults(new String[] {"5", "6", "7"})
        .verifyReplTargetProperty(replicatedDbName);

    validateInitialSnapshotsCreated(primary.getDatabase(primaryDbName).getLocationUri());
  }

  @Test
  public void testSnapshotsWithCustomDbLevelPaths() throws Throwable {
    // Create table1 inside database warehouse location, table2 outside db location and non snapshottable, table3
    // outside database location but snapshottable, table4 and table5 inside a custom db path and snapshottable,
    // table6 and table7 inside a custom db path but not snapshottable.

    Path externalTableLocation2 = new Path("/" + testName.getMethodName() + "/table2/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocation2, new FsPermission("777"));

    Path externalTableLocation3 = new Path("/" + testName.getMethodName() + "/table3/");
    fs.mkdirs(externalTableLocation3, new FsPermission("777"));

    Path externalCustomDb1Path =  new Path("/" + testName.getMethodName() + "/customDb1/");

    Path externalCustomDb2Path =  new Path("/" + testName.getMethodName() + "/customDb2/");

    Path externalTableLocation4 = new Path(externalCustomDb1Path, "table4");
    fs.mkdirs(externalTableLocation4, new FsPermission("777"));

    Path externalTableLocation5 = new Path(externalCustomDb1Path, "table5");
    fs.mkdirs(externalTableLocation5, new FsPermission("777"));

    Path externalTableLocation6 = new Path(externalCustomDb2Path, "table6");
    fs.mkdirs(externalTableLocation6, new FsPermission("777"));

    Path externalTableLocation7 = new Path(externalCustomDb2Path, "table7");
    fs.mkdirs(externalTableLocation7, new FsPermission("777"));

    // Specify table3 and the customDb1 location as snapshottable and customDb1 & customDb2 as custom locations for
    // single copy task.
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'"
        + REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK_PATHS.varname + "'='" + externalCustomDb1Path
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "," + externalTableLocation3
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "'");

    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create external table table1 (id int)")
        .run("create external table table2 (place string) partitioned by (country string) row format "
            + "delimited fields terminated by ',' location '" + externalTableLocation2.toString() +"'")
        .run("create external table table3 (place string) partitioned by (country string) row format "
            + "delimited fields terminated by ',' location '" + externalTableLocation3.toString() +"'")
        .run("create external table table4 (place string) partitioned by (country string) row format "
            + "delimited fields terminated by ',' location '" + externalTableLocation4.toString() +"'")
        .run("create external table table5 (place string) partitioned by (country string) row format "
            + "delimited fields terminated by ',' location '" + externalTableLocation5.toString() +"'")
        .run("create external table table6 (place string) partitioned by (country string) row format "
            + "delimited fields terminated by ',' location '" + externalTableLocation6.toString() +"'")
        .run("create external table table7 (place string) partitioned by (country string) row format "
            + "delimited fields terminated by ',' location '" + externalTableLocation7.toString() +"'")
        .run("insert into table2 partition(country='india') values ('bangalore')")
        .run("insert into table3 partition(country='china') values ('beejing')")
        .run("insert into table4 partition(country='usa') values ('new york')")
        .run("insert into table5 partition(country='japan') values ('tokyo')")
        .run("insert into table6 partition(country='nepal') values ('kathmandu')")
        .run("insert into table7 partition(country='australia') values ('sydney')")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select place from table2 where country='india'")
        .verifyResults(new String[] {"bangalore"})
        .run("select place from table3 where country='china'")
        .verifyResults(new String[] {"beejing"})
        .run("select place from table4 where country='usa'")
        .verifyResults(new String[] {"new york"})
        .run("select place from table5 where country='japan'")
        .verifyResults(new String[] {"tokyo"})
        .run("select place from table6 where country='nepal'")
        .verifyResults(new String[] {"kathmandu"})
        .run("select place from table7 where country='australia'")
        .verifyResults(new String[] {"sydney"})
        .verifyReplTargetProperty(replicatedDbName);

    // Check if the table3 and custom db1 location directory is snapshotoble and the snapshot is there.
    validateInitialSnapshotsCreated(externalTableLocation3.toString());
    validateInitialSnapshotsCreated(externalCustomDb1Path.toString());

    // Check if others aren't snapshottable.
    assertFalse(fs.getFileStatus(externalCustomDb2Path).isSnapshotEnabled());
    assertFalse(fs.getFileStatus(externalTableLocation2).isSnapshotEnabled());

    // Add some more data and do a dump & load so as to create diff snapshots.
    tuple = primary.run("use " + primaryDbName)
        .run("insert into table2 partition(country='india') values ('chennai')")
        .run("insert into table3 partition(country='china') values ('chengdu')")
        .run("insert into table4 partition(country='usa') values ('washington')")
        .run("select place from table4 where country='usa'")
        .verifyResults(new String[] {"new york", "washington"})
        .run("insert into table5 partition(country='japan') values ('osaka')")
        .run("insert into table6 partition(country='nepal') values ('pokhra')")
        .run("insert into table7 partition(country='australia') values ('perth')")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 'table%'")
        .verifyResults(new String[] {"table1", "table2", "table3", "table4", "table5", "table6", "table7"})
        .run("select place from table2 where country='india'")
        .verifyResults(new String[] {"bangalore", "chennai"})
        .run("select place from table3 where country='china'")
        .verifyResults(new String[] {"beejing", "chengdu"})
        .run("select place from table4 where country='usa'")
        .verifyResults(new String[] {"new york", "washington"})
        .run("select place from table5 where country='japan'")
        .verifyResults(new String[] {"tokyo", "osaka"})
        .run("select place from table6 where country='nepal'")
        .verifyResults(new String[] {"kathmandu", "pokhra"})
        .run("select place from table7 where country='australia'")
        .verifyResults(new String[] {"sydney", "perth"})
        .verifyReplTargetProperty(replicatedDbName);

    // Check if the diff snapshots got created for the paths.
    validateDiffSnapshotsCreated(externalTableLocation3.toString());
    validateDiffSnapshotsCreated(externalCustomDb1Path.toString());

    // Check if others doesn't become snapshottable during incremental
    assertFalse(fs.getFileStatus(externalCustomDb2Path).isSnapshotEnabled());
    assertFalse(fs.getFileStatus(externalTableLocation2).isSnapshotEnabled());

    // Add some data to customDb location and see if the diff copy is done and do a dump & load cycle.
    tuple = primary.run("use " + primaryDbName)
        .run("insert into table4 partition(country='usa') values ('chicago')")
        .run("drop table table5 purge")
        .run("insert into table6 partition(country='nepal') values ('lalitpur')")
        .run("insert into table7 partition(country='australia') values ('adelaide')")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 'table%'")
        .verifyResults(new String[] {"table1", "table2", "table3", "table4", "table6", "table7"})
        .run("select place from table4 where country='usa'")
        .verifyResults(new String[] {"new york", "washington", "chicago"})
        .run("select place from table6 where country='nepal'")
        .verifyResults(new String[] {"kathmandu", "pokhra", "lalitpur"})
        .run("select place from table7 where country='australia'")
        .verifyResults(new String[] {"sydney", "perth", "adelaide"})
        .verifyReplTargetProperty(replicatedDbName);

    // Check if the t1 directory is snapshotoble and the snapshot is there.
    validateDiffSnapshotsCreated(externalTableLocation3.toString());
    validateDiffSnapshotsCreated(externalCustomDb1Path.toString());

    // Add the other custom db path for creating snapshots and remove the already configured path as snapshottable.
    // The new db location should become snapshottable and the snapshots should be cleared for the other db location.
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK_PATHS.varname + "'='" + externalCustomDb2Path
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "," + externalTableLocation3
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "'");

    primary.run("use " + primaryDbName)
        .run("insert into table4 partition(country='usa') values ('austin')")
        .run("insert into table6 partition(country='nepal') values ('janakpur')")
        .run("insert into table7 partition(country='australia') values ('darwin')")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 'table%'")
        .verifyResults(new String[] {"table1", "table2", "table3", "table4", "table6", "table7"})
        .run("select place from table4 where country='usa'")
        .verifyResults(new String[] {"new york", "washington", "chicago", "austin"})
        .run("select place from table6 where country='nepal'")
        .verifyResults(new String[] {"kathmandu", "pokhra", "lalitpur", "janakpur"})
        .run("select place from table7 where country='australia'")
        .verifyResults(new String[] {"sydney", "perth", "adelaide", "darwin"})
        .verifyReplTargetProperty(replicatedDbName);

    // Check if the table3 directory is snapshotoble and the db path2 became snapshottable.
    validateDiffSnapshotsCreated(externalTableLocation3.toString());
    validateInitialSnapshotsCreated(externalCustomDb2Path.toString());

    // Check if others doesn't become snapshottable and the snapshots are cleared for the first custom db location.
    assertFalse(fs.getFileStatus(externalCustomDb1Path).isSnapshotEnabled());
    assertFalse(fs.getFileStatus(externalTableLocation2).isSnapshotEnabled());
  }

  // Verifies if the diff snapshots are created for source and target database.
  private void validateDiffSnapshotsCreated(String location) throws Exception {
    Path locationPath = new Path(location);
    DistributedFileSystem dfs = (DistributedFileSystem) locationPath.getFileSystem(conf);
    assertNotNull(dfs.getFileStatus(new Path(locationPath, ".snapshot/" + firstSnapshot(primaryDbName.toLowerCase()))));
    assertNotNull(
        dfs.getFileStatus(new Path(locationPath, ".snapshot/" + secondSnapshot(primaryDbName.toLowerCase()))));
  }

  @Test
  public void testSnapshotsWithFiltersCustomDbLevelPaths() throws Throwable {
    // Directory Structure:
    //    /prefix/project/   <- Specified as custom Location.(Snapshot Root)
    //                        /randomStuff <- Not to be copied as part of external data copy
    //                        /warehouse1 <- To be copied, Contains table1 & table2
    //                       /warehouse2 <- To be copied, Contains table3 & table4

    // Create /prefix/project
    Path project = new Path("/" + testName.getMethodName() + "/project");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(project);

    // Create /prefix/project/warehouse1
    Path warehouse1 = new Path(project, "warehouse1");
    fs.mkdirs(warehouse1);

    // Create /prefix/project/warehouse2
    Path warehouse2 = new Path(project, "warehouse2");
    fs.mkdirs(warehouse2);

    // Table1 Path: /prefix/project/warehouse1/table1
    Path table1 = new Path(warehouse1, "table1");
    fs.mkdirs(table1);

    // Table2 Path: /prefix/project/warehouse1/table2
    Path table2 = new Path(warehouse1, "table2");
    fs.mkdirs(table2);

    // Table3 Path: /prefix/project/warehouse2/table3
    Path table3 = new Path(warehouse2, "table3");
    fs.mkdirs(table3);

    // Table4 Path: /prefix/project/warehouse2/table4
    Path table4 = new Path(warehouse2, "table4");
    fs.mkdirs(table4);

    // Random Dir inside the /prefix/project
    Path random = new Path(project, "randomStuff");
    fs.mkdirs(random);

    fs.create(new Path(random, "file1")).close();
    fs.create(new Path(random, "file2")).close();
    fs.create(new Path(random, "file3")).close();

    // Create a filter file for DistCp
    String filterFilePath = "/tmp/filter";
    FileWriter myWriter = new FileWriter(filterFilePath);
    myWriter.write(".*randomStuff.*");
    myWriter.close();

    assertTrue(new File(filterFilePath).exists());

    // Specify the project directory as the snapshot root using the single copy task path config.
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'"
        + REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK_PATHS.varname + "'='" + project
        .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString() + "'");

    // Add Filter file
    withClause.add("'distcp.options.filters'='" + filterFilePath + "'");

    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create external table table1 (place string)  row format "
            + "delimited fields terminated by ',' location '" + table1.toString() +"'")
        .run("create external table table2 (place string)  row format "
            + "delimited fields terminated by ',' location '" + table2.toString() +"'")
        .run("create external table table3 (place string)  row format "
            + "delimited fields terminated by ',' location '" + table3.toString() +"'")
        .run("create external table table4 (place string)  row format "
            + "delimited fields terminated by ',' location '" + table4.toString() +"'")
        .run("insert into table1 values ('bangalore')")
        .run("select place from table1")
        .verifyResults(new String[] {"bangalore"})
        .run("insert into table2 values ('beejing')")
        .run("insert into table3 values ('new york')")
        .run("insert into table4 values ('tokyo')")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select place from table1")
        .verifyResults(new String[] {"bangalore"})
        .run("select place from table2")
        .verifyResults(new String[] {"beejing"})
        .run("select place from table3")
        .verifyResults(new String[] {"new york"})
        .run("select place from table4")
        .verifyResults(new String[] {"tokyo"})
        .verifyReplTargetProperty(replicatedDbName);

    // Check if the initial snapshot got created for project dir
    validateInitialSnapshotsCreated(project.toString());

    // Check if the randomStuff Directory didn't get copied.
    assertFalse(fs.exists(new Path(REPLICA_EXTERNAL_BASE, random.toUri().getPath().replaceFirst("/", ""))));

    // Diff Mode Of Snapshot.
    tuple = primary.run("use " + primaryDbName)
        .run("insert into table1 values ('delhi')")
        .run("insert into table2 values ('wuhan')")
        .run("insert into table3 values ('washington')")
        .run("insert into table4 values ('osaka')")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select place from table1")
        .verifyResults(new String[] {"bangalore", "delhi"})
        .run("select place from table2")
        .verifyResults(new String[] {"beejing", "wuhan"})
        .run("select place from table3")
        .verifyResults(new String[] {"new york", "washington"})
        .run("select place from table4")
        .verifyResults(new String[] {"tokyo", "osaka"})
        .verifyReplTargetProperty(replicatedDbName);


    // Check if diff snapshot got created.
    validateDiffSnapshotsCreated(project.toString());

    // Check if the randomStuff Directory didn't get copied, post diff copy
    assertFalse(fs.exists(new Path(REPLICA_EXTERNAL_BASE, random.toUri().getPath().replaceFirst("/", ""))));

    // Clean up the filter file.
    new File(filterFilePath).delete();
  }

  // Verifies if the initial rounds are snapshots are created for source and target database.
  private void validateInitialSnapshotsCreated(String location) throws Exception {
    Path locationPath = new Path(location);
    DistributedFileSystem dfs = (DistributedFileSystem) locationPath.getFileSystem(conf);

    // Check whether the source  location got snapshottable
    assertTrue("Snapshot not enabled for the source  location", dfs.getFileStatus(locationPath).isSnapshotEnabled());

    // Check whether the initial snapshot got created in the source db location.
    assertNotNull(dfs.getFileStatus(new Path(locationPath, ".snapshot/" + secondSnapshot(primaryDbName.toLowerCase()))));

    // Verify Snapshots are created in target.
    Path locationPathTarget = new Path(REPLICA_EXTERNAL_BASE, locationPath.toUri().getPath().replaceFirst("/", ""));
    DistributedFileSystem dfsTarget = (DistributedFileSystem) locationPathTarget.getFileSystem(conf);
    assertTrue("Snapshot not enabled for the target location",
        dfsTarget.getFileStatus(locationPathTarget).isSnapshotEnabled());

    // Check whether the snapshot got created in the target location.
    assertNotNull(dfsTarget
        .getFileStatus(new Path(locationPathTarget, ".snapshot" + "/" + firstSnapshot(primaryDbName.toLowerCase()))));
  }

  public static void deleteReplRelatedSnapshots(FileSystem fs, Path path, HiveConf conf) {
    try {
      FileStatus[] listing = fs.listStatus(new Path(path, ".snapshot"));
      for (FileStatus elem : listing) {
        if (elem.getPath().getName().contains(OLD_SNAPSHOT) || elem.getPath().getName().contains(NEW_SNAPSHOT)) {
          deleteSnapshotIfExists((DistributedFileSystem) fs, path, elem.getPath().getName(), conf);
        }
      }
    } catch (Exception e) {
      // Ignore
    }
  }
}