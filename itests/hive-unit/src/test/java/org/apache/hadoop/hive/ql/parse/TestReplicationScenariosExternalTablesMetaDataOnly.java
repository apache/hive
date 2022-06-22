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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.BehaviourInjection;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;
import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.INC_BOOTSTRAP_ROOT_DIR_NAME;
import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.REPL_HIVE_BASE_DIR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * TestMetadataReplicationScenariosExternalTables - Test metadata only replication.
 * for external tables.
 */
public class TestReplicationScenariosExternalTablesMetaDataOnly extends BaseReplicationAcrossInstances {

  String extraPrimaryDb;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());
    overrides.put(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY.varname, "false");
    overrides.put(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname, "true");
    overrides.put(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY_FOR_EXTERNAL_TABLE.varname, "true");
    overrides.put(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname,
        UserGroupInformation.getCurrentUser().getUserName());

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
    super.tearDown();
  }

  @Test
  public void replicationWithoutExternalTables() throws Throwable {
    List<String> loadWithClause = ReplicationTestUtils.includeExternalTableClause(false);
    List<String> dumpWithClause = ReplicationTestUtils.includeExternalTableClause(false);
    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create external table t1 (id int)")
        .run("insert into table t1 values (1)")
        .run("insert into table t1 values (2)")
        .run("create external table t2 (place string) partitioned by (country string)")
        .run("insert into table t2 partition(country='india') values ('bangalore')")
        .run("insert into table t2 partition(country='us') values ('austin')")
        .run("insert into table t2 partition(country='france') values ('paris')")
        .dump(primaryDbName, dumpWithClause);

    // the _file_list_external only should be created if external tables are to be replicated not otherwise
    ReplicationTestUtils.assertFalseExternalFileList(primary, tuple.dumpLocation);

    replica.load(replicatedDbName, primaryDbName, loadWithClause)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyFailure(new String[] {"t1"})
        .run("show tables like 't2'")
        .verifyFailure(new String[] {"t2"})
        .verifyReplTargetProperty(replicatedDbName);

    tuple = primary.run("use " + primaryDbName)
        .run("create external table t3 (id int)")
        .run("insert into table t3 values (10)")
        .run("insert into table t3 values (20)")
        .dump(primaryDbName, dumpWithClause);

    // the _file_list_external should be created if external tables are to be replicated not otherwise
    assertFalse(primary.miniDFSCluster.getFileSystem()
        .exists(new Path(new Path(tuple.dumpLocation,
                REPL_HIVE_BASE_DIR), EximUtil.FILE_LIST_EXTERNAL)));

    replica.load(replicatedDbName, primaryDbName, loadWithClause)
        .run("use " + replicatedDbName)
        .run("show tables like 't3'")
        .verifyFailure(new String[] {"t3"})
        .verifyReplTargetProperty(replicatedDbName);
  }

  @Test
  public void externalTableReplicationWithDefaultPaths() throws Throwable {
    //creates external tables with partitions
    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create external table t1 (id int)")
        .run("insert into table t1 values (1)")
        .run("insert into table t1 values (2)")
        .run("create external table t2 (place string) partitioned by (country string)")
        .run("insert into table t2 partition(country='india') values ('bangalore')")
        .run("insert into table t2 partition(country='us') values ('austin')")
        .run("insert into table t2 partition(country='france') values ('paris')")
        .dumpWithCommand("repl dump " + primaryDbName);

    // verify that the external table list is not written as metadata only replication
    ReplicationTestUtils.assertFalseExternalFileList(primary, tuple.dumpLocation);

    List<String> withClauseOptions = ReplicationTestUtils.includeExternalTableClause(true);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("show tables like 't2'")
        .verifyResult("t2")
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("select country from t2 where country = 'us'")
        .verifyResult(null)
        .run("select country from t2 where country = 'france'")
        .verifyResult(null)
        .run("show partitions t2").verifyResults(new String[] {"country=france", "country=india", "country=us"});

    // Ckpt should be set on bootstrapped db.
    String hiveDumpLocation = tuple.dumpLocation + File.separator + REPL_HIVE_BASE_DIR;
    replica.verifyIfCkptSet(replicatedDbName, hiveDumpLocation);

    tuple = primary.run("use " + primaryDbName)
        .run("create external table t3 (id int)")
        .run("insert into table t3 values (10)")
        .run("create external table t4 as select id from t3")
        .dumpWithCommand("repl dump " + primaryDbName);

    // verify that the external table list is written correctly for incremental
    ReplicationTestUtils.assertFalseExternalFileList(primary, tuple.dumpLocation);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
        .run("use " + replicatedDbName)
        .run("show tables like 't3'")
        .verifyResult("t3")
        .run("select id from t3")
        .verifyResult(null)
        .run("select id from t4")
        .verifyResult(null);

    tuple = primary.run("use " + primaryDbName)
        .run("drop table t1")
        .dumpWithCommand("repl dump " + primaryDbName);

    // verify that the external table list is written correctly for incremental
    ReplicationTestUtils.assertFalseExternalFileList(primary, tuple.dumpLocation);
  }

  @Test
  public void externalTableReplicationWithCustomPaths() throws Throwable {
    Path externalTableLocation =
        new Path("/" + testName.getMethodName() + "/" + primaryDbName + "/" + "a/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocation, new FsPermission("777"));

    // Create base directory but use HDFS path without schema or authority details.
    // Hive should pick up the local cluster's HDFS schema/authority.
    List<String> loadWithClause = Arrays.asList(
            "'distcp.options.update'=''"
    );

    WarehouseInstance.Tuple bootstrapTuple = primary.run("use " + primaryDbName)
        .run("create external table a (i int, j int) "
            + "row format delimited fields terminated by ',' "
            + "location '" + externalTableLocation.toUri() + "'")
        .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName, loadWithClause)
        .run("use " + replicatedDbName)
        .run("show tables like 'a'")
        .verifyResults(Collections.singletonList("a"))
        .run("select * From a").verifyResults(Collections.emptyList());

    //externally add data to location
    try (FSDataOutputStream outputStream =
        fs.create(new Path(externalTableLocation, "file1.txt"))) {
      outputStream.write("1,2\n".getBytes());
      outputStream.write("13,21\n".getBytes());
    }

    WarehouseInstance.Tuple incrementalTuple = primary.run("create table b (i int)")
        .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName, loadWithClause)
        .run("select i From a")
        .verifyResults(new String[] {})
        .run("select j from a")
        .verifyResults(new String[] {});

    // alter table location to something new.
    externalTableLocation =
        new Path("/" + testName.getMethodName() + "/" + primaryDbName + "/new_location/a/");
    incrementalTuple = primary.run("use " + primaryDbName)
        .run("alter table a set location '" + externalTableLocation + "'")
        .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName, loadWithClause)
        .run("use " + replicatedDbName)
        .run("select i From a")
        .verifyResults(Collections.emptyList());
  }

  @Test
  public void externalTableWithPartitions() throws Throwable {
    Path externalTableLocation =
        new Path("/" + testName.getMethodName() + "/t2/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocation, new FsPermission("777"));

    List<String> loadWithClause = ReplicationTestUtils.includeExternalTableClause(true);

    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create external table t2 (place string) partitioned by (country string) row format "
            + "delimited fields terminated by ',' location '" + externalTableLocation.toString()
            + "'")
        .run("insert into t2 partition(country='india') values ('bangalore')")
        .dumpWithCommand("repl dump " + primaryDbName);

    ReplicationTestUtils.assertFalseExternalFileList(primary, tuple.dumpLocation);

    replica.load(replicatedDbName, primaryDbName, loadWithClause)
        .run("use " + replicatedDbName)
        .run("show tables like 't2'")
        .verifyResults(new String[] {"t2"})
        .run("select place from t2")
        .verifyResults(new String[] {})
        .verifyReplTargetProperty(replicatedDbName)
        .run("show partitions t2")
        .verifyResults(new String[] {"country=india"});

    // add new  data externally, to a partition, but under the table level top directory
    Path partitionDir = new Path(externalTableLocation, "country=india");
    try (FSDataOutputStream outputStream = fs.create(new Path(partitionDir, "file.txt"))) {
      outputStream.write("pune\n".getBytes());
      outputStream.write("mumbai\n".getBytes());
    }

    tuple = primary.run("use " + primaryDbName)
        .run("insert into t2 partition(country='australia') values ('sydney')")
        .dump(primaryDbName);

    ReplicationTestUtils.assertFalseExternalFileList(primary, tuple.dumpLocation);

    replica.load(replicatedDbName, primaryDbName, loadWithClause)
        .run("use " + replicatedDbName)
        .run("select distinct(country) from t2")
        .verifyResults(new String[] {})
        .run("select place from t2 where country='india'")
        .verifyResults(new String[] {})
        .run("select place from t2 where country='australia'")
        .verifyResults(new String[] {})
        .run("show partitions t2")
        .verifyResults(new String[] {"country=australia", "country=india"})
        .verifyReplTargetProperty(replicatedDbName);

    Path customPartitionLocation =
        new Path("/" + testName.getMethodName() + "/partition_data/t2/country=france");
    fs.mkdirs(externalTableLocation, new FsPermission("777"));

    // add new partitions to the table, at an external location than the table level directory
    try (FSDataOutputStream outputStream = fs
        .create(new Path(customPartitionLocation, "file.txt"))) {
      outputStream.write("paris".getBytes());
    }

    tuple = primary.run("use " + primaryDbName)
        .run("ALTER TABLE t2 ADD PARTITION (country='france') LOCATION '" + customPartitionLocation
            .toString() + "'")
        .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName, loadWithClause)
        .run("use " + replicatedDbName)
        .run("select place from t2 where country='france'")
        .verifyResults(new String[] {})
        .run("show partitions t2")
        .verifyResults(new String[] {"country=australia", "country=france", "country=india"})
        .verifyReplTargetProperty(replicatedDbName);

    // change the location of the partition via alter command
    String tmpLocation = "/tmp/" + System.nanoTime();
    primary.miniDFSCluster.getFileSystem().mkdirs(new Path(tmpLocation), new FsPermission("777"));

    tuple = primary.run("use " + primaryDbName)
        .run("alter table t2 partition (country='france') set location '" + tmpLocation + "'")
        .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName, loadWithClause)
        .run("use " + replicatedDbName)
        .run("select place from t2 where country='france'")
        .verifyResults(new String[] {})
        .verifyReplTargetProperty(replicatedDbName);

    // Changing location of the external table, should result in changes to the location of
    // partition residing within the table location and not the partitions located outside.
    String tmpLocation2 = "/tmp/" + System.nanoTime() + "_2";
    primary.miniDFSCluster.getFileSystem().mkdirs(new Path(tmpLocation2), new FsPermission("777"));

    tuple = primary.run("use " + primaryDbName)
            .run("insert into table t2 partition(country='france') values ('lyon')")
            .run("alter table t2 set location '" + tmpLocation2 + "'")
            .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName, loadWithClause);
  }

  @Test
  public void externalTableIncrementalReplication() throws Throwable {
    WarehouseInstance.Tuple tuple = primary.dumpWithCommand("repl dump " + primaryDbName);
    replica.load(replicatedDbName, primaryDbName);

    Path externalTableLocation =
            new Path("/" + testName.getMethodName() + "/t1/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocation, new FsPermission("777"));

    tuple = primary.run("use " + primaryDbName)
        .run("create external table t1 (place string) partitioned by (country string) row format "
            + "delimited fields terminated by ',' location '" + externalTableLocation.toString()
            + "'")
        .run("alter table t1 add partition(country='india')")
        .run("alter table t1 add partition(country='us')")
        .dump(primaryDbName);

    ReplicationTestUtils.assertFalseExternalFileList(primary, tuple.dumpLocation);

    // Add new data externally, to a partition, but under the partition level top directory
    // Also, it is added after dumping the events but data should be seen at target after REPL LOAD.
    Path partitionDir = new Path(externalTableLocation, "country=india");
    try (FSDataOutputStream outputStream = fs.create(new Path(partitionDir, "file.txt"))) {
      outputStream.write("pune\n".getBytes());
      outputStream.write("mumbai\n".getBytes());
    }

    try (FSDataOutputStream outputStream = fs.create(new Path(partitionDir, "file1.txt"))) {
      outputStream.write("bangalore\n".getBytes());
    }

    List<String> loadWithClause = ReplicationTestUtils.includeExternalTableClause(true);
    replica.load(replicatedDbName, primaryDbName, loadWithClause)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("show partitions t1")
        .verifyResults(new String[] {"country=india", "country=us"})
        .run("select place from t1 order by place")
        .verifyResults(new String[] {})
        .verifyReplTargetProperty(replicatedDbName);

    // Delete one of the file and update another one.
    fs.delete(new Path(partitionDir, "file.txt"), true);
    fs.delete(new Path(partitionDir, "file1.txt"), true);
    try (FSDataOutputStream outputStream = fs.create(new Path(partitionDir, "file1.txt"))) {
      outputStream.write("chennai\n".getBytes());
    }

    // Repl load with zero events but external tables location info should present.
    tuple = primary.dump(primaryDbName);
    ReplicationTestUtils.assertFalseExternalFileList(primary, tuple.dumpLocation);

    replica.load(replicatedDbName, primaryDbName, loadWithClause)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show partitions t1")
            .verifyResults(new String[] {"country=india", "country=us"})
            .run("select place from t1 order by place")
            .verifyResults(new String[] {})
            .verifyReplTargetProperty(replicatedDbName);

    Hive hive = Hive.get(replica.getConf());
    Set<Partition> partitions =
        hive.getAllPartitionsOf(hive.getTable(replicatedDbName + ".t1"));
    List<String> paths = partitions.stream().map(p -> p.getDataLocation().toUri().getPath())
        .collect(Collectors.toList());

    tuple = primary
        .run("alter table t1 drop partition (country='india')")
        .run("alter table t1 drop partition (country='us')")
        .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
        .run("select * From t1")
        .verifyResults(new String[] {})
        .verifyReplTargetProperty(replicatedDbName);

    for (String path : paths) {
      assertTrue(replica.miniDFSCluster.getFileSystem().exists(new Path(path)));
    }
  }

  @Test
  public void bootstrapExternalTablesDuringIncrementalPhase() throws Throwable {
    List<String> loadWithClause = ReplicationTestUtils.includeExternalTableClause(false);
    List<String> dumpWithClause = ReplicationTestUtils.includeExternalTableClause(false);

    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (1)")
            .run("insert into table t1 values (2)")
            .run("create external table t2 (place string) partitioned by (country string)")
            .run("insert into table t2 partition(country='india') values ('bangalore')")
            .run("insert into table t2 partition(country='us') values ('austin')")
            .run("insert into table t2 partition(country='france') values ('paris')")
            .dump(primaryDbName, dumpWithClause);

    // the file _file_list_external only should be created if external tables are to be replicated not otherwise
    ReplicationTestUtils.assertFalseExternalFileList(primary, tuple.dumpLocation);

    replica.load(replicatedDbName, primaryDbName, loadWithClause)
            .status(replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyFailure(new String[] {"t1" })
            .run("show tables like 't2'")
            .verifyFailure(new String[] {"t2" })
            .verifyReplTargetProperty(replicatedDbName);

    dumpWithClause = Arrays.asList("'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='true'",
                                   "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_EXTERNAL_TABLES.varname + "'='true'",
            "'" + HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY_FOR_EXTERNAL_TABLE.varname + "'='false'",
            "'distcp.options.pugpb'=''");
    loadWithClause = ReplicationTestUtils.includeExternalTableClause(true);
    tuple = primary.run("use " + primaryDbName)
            .run("drop table t1")
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (10)")
            .run("insert into table t3 values (20)")
            .run("create table t4 as select * from t3")
            .dump(primaryDbName, dumpWithClause);

    String hiveDumpDir = tuple.dumpLocation + File.separator + REPL_HIVE_BASE_DIR;
    // the _file_list_external should be created as external tables are to be replicated.
    assertTrue(primary.miniDFSCluster.getFileSystem()
            .exists(new Path(hiveDumpDir, EximUtil.FILE_LIST_EXTERNAL)));

    // verify that the external table list is written correctly for incremental
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t2", "t3"), tuple.dumpLocation, primary);

    // _bootstrap directory should be created as bootstrap enabled on external tables.
    Path dumpPath = new Path(hiveDumpDir, INC_BOOTSTRAP_ROOT_DIR_NAME);
    assertTrue(primary.miniDFSCluster.getFileSystem().exists(dumpPath));

    // _bootstrap/metedata/<db_name>/t2
    // _bootstrap/metedata/<db_name>/t3
    Path dbPath = new Path(dumpPath, EximUtil.METADATA_PATH_NAME + File.separator + primaryDbName);
    Path tblPath = new Path(dbPath, "t2");
    assertTrue(primary.miniDFSCluster.getFileSystem().exists(tblPath));
    tblPath = new Path(dbPath, "t3");
    assertTrue(primary.miniDFSCluster.getFileSystem().exists(tblPath));

    replica.load(replicatedDbName, primaryDbName, loadWithClause)
            .status(replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyFailure(new String[] {"t1" })
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("show tables like 't4'")
            .verifyResult("t4")
            .verifyReplTargetProperty(replicatedDbName);

    // Ckpt should be set on bootstrapped tables.
    hiveDumpDir = tuple.dumpLocation + File.separator + REPL_HIVE_BASE_DIR;
    replica.verifyIfCkptSetForTables(replicatedDbName, Arrays.asList("t2", "t3"), hiveDumpDir);

    // Drop source tables to see if target points to correct data or not after bootstrap load.
    primary.run("use " + primaryDbName)
            .run("drop table t2")
            .run("drop table t3");

    // Create table event for t4 should be applied along with bootstrapping of t2 and t3
    replica.run("use " + replicatedDbName)
            .run("select place from t2 where country = 'us'")
            .verifyResult("austin")
            .run("select place from t2 where country = 'france'")
            .verifyResult("paris")
            .run("select id from t3 order by id")
            .verifyResults(Arrays.asList("10", "20"))
            .run("select id from t4 order by id")
            .verifyResults(Arrays.asList("10", "20"))
            .verifyReplTargetProperty(replicatedDbName);
  }

  @Test
  public void testExternalTablesIncReplicationWithConcurrentDropTable() throws Throwable {
    List<String> dumpWithClause = ReplicationTestUtils.includeExternalTableClause(true);
    List<String> loadWithClause = ReplicationTestUtils.includeExternalTableClause(true);
    WarehouseInstance.Tuple tupleBootstrap = primary.run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (1)")
            .dump(primaryDbName, dumpWithClause);

    replica.load(replicatedDbName, primaryDbName, loadWithClause);

    // Insert a row into "t1" and create another external table using data from "t1".
    primary.run("use " + primaryDbName)
            .run("insert into table t1 values (2)")
            .run("create external table t2 as select * from t1");

    // Inject a behavior so that getTable returns null for table "t1". This ensures the table is
    // skipped for data files listing.
    BehaviourInjection<Table, Table> tableNuller = new BehaviourInjection<Table, Table>() {
      @Nullable
      @Override
      public Table apply(@Nullable Table table) {
        LOG.info("Performing injection on table " + table.getTableName());
        if (table.getTableName().equalsIgnoreCase("t1")){
          injectionPathCalled = true;
          return null;
        } else {
          nonInjectedPathCalled = true;
          return table;
        }
      }
    };
    InjectableBehaviourObjectStore.setGetTableBehaviour(tableNuller);
    WarehouseInstance.Tuple tupleInc;
    try {
      // The t1 table will be skipped from data location listing.
      tupleInc = primary.dump(primaryDbName, dumpWithClause);
      tableNuller.assertInjectionsPerformed(true, true);
    } finally {
      InjectableBehaviourObjectStore.resetGetTableBehaviour(); // reset the behaviour
    }

    // Only table t2 should exist in the data location list file.
    ReplicationTestUtils.assertFalseExternalFileList(primary, tupleInc.dumpLocation);

    // The newly inserted data "2" should be missing in table "t1". But, table t2 should exist and have
    // inserted data.
    replica.load(replicatedDbName, primaryDbName, loadWithClause)
            .run("use " + replicatedDbName)
            .run("select id from t1 order by id")
            .verifyResult(null)
            .run("select id from t2 order by id")
            .verifyResults(new String[]{});
  }

  @Test
  public void testIncrementalDumpEmptyDumpDirectory() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (1)")
            .run("insert into table t1 values (2)")
            .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName)
            .status(replicatedDbName)
            .verifyResult(tuple.lastReplicationId);

    // This looks like an empty dump but it has the ALTER TABLE event created by the previous
    // dump. We need it here so that the next dump won't have any events.
    WarehouseInstance.Tuple incTuple = primary.dump(primaryDbName, withClause);
    replica.load(replicatedDbName, primaryDbName, withClause)
            .status(replicatedDbName)
            .verifyResult(incTuple.lastReplicationId);

    // create events for some other database and then dump the primaryDbName to dump an empty directory.
    primary.run("create database " + extraPrimaryDb + " WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')");
    WarehouseInstance.Tuple inc2Tuple = primary.run("use " + extraPrimaryDb)
            .run("create table tbl (fld int)")
            .run("use " + primaryDbName)
            .dump(primaryDbName, withClause);
    Assert.assertEquals(primary.getCurrentNotificationEventId().getEventId(),
                        Long.valueOf(inc2Tuple.lastReplicationId).longValue());

    // Incremental load to existing database with empty dump directory should set the repl id to the last event at src.
    replica.load(replicatedDbName, primaryDbName, withClause)
            .status(replicatedDbName)
            .verifyResult(inc2Tuple.lastReplicationId);
  }
}
