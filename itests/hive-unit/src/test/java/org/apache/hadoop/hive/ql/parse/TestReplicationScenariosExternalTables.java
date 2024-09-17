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
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.BehaviourInjection;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.CallerArguments;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.repl.ReplAck;
import org.apache.hadoop.hive.ql.exec.repl.ReplDumpWork;
import org.apache.hadoop.hive.ql.exec.repl.ReplExternalTables;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.StringAppender;
import org.apache.hadoop.hive.ql.parse.repl.dump.EventsDumpMetadata;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Base64;

import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK_PATHS;
import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;
import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.INC_BOOTSTRAP_ROOT_DIR_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestReplicationScenariosExternalTables extends BaseReplicationAcrossInstances {

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
    overrides.put(HiveConf.ConfVars.REPL_BATCH_INCREMENTAL_EVENTS.varname, "false");

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
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(false);
    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create external table t1 (id int)")
        .run("insert into table t1 values (1)")
        .run("insert into table t1 values (2)")
        .run("create external table t2 (place string) partitioned by (country string)")
        .run("insert into table t2 partition(country='india') values ('bangalore')")
        .run("insert into table t2 partition(country='us') values ('austin')")
        .run("insert into table t2 partition(country='france') values ('paris')")
        .dump(primaryDbName, withClause);

    // the _file_list_external only should be created if external tables are to be replicated not otherwise
    Path replHiveBasePath = new Path(tuple.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertFalse(primary.miniDFSCluster.getFileSystem()
        .exists(new Path(replHiveBasePath, EximUtil.FILE_LIST_EXTERNAL)));

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyFailure(new String[] { "t1" })
        .run("show tables like 't2'")
        .verifyFailure(new String[] { "t2" })
        .verifyReplTargetProperty(replicatedDbName);

    tuple = primary.run("use " + primaryDbName)
        .run("create external table t3 (id int)")
        .run("insert into table t3 values (10)")
        .run("insert into table t3 values (20)")
        .dump(primaryDbName, withClause);

    // _file_list_external only should be created if external tables are to be replicated not otherwise
    replHiveBasePath = new Path(tuple.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertFalse(primary.miniDFSCluster.getFileSystem()
            .exists(new Path(replHiveBasePath, EximUtil.FILE_LIST_EXTERNAL)));

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 't3'")
        .verifyFailure(new String[] { "t3" })
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
        .dump(primaryDbName);

    // verify that the external table filelist is written correctly for bootstrap
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1", "t2"), tuple.dumpLocation, primary);



    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("show tables like 't2'")
        .verifyResult("t2")
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("select country from t2 where country = 'us'")
        .verifyResult("us")
        .run("select country from t2 where country = 'france'")
        .verifyResult("france")
        .run("show partitions t2").verifyResults(new String[] {"country=france", "country=india", "country=us"});

    String hiveDumpLocation = tuple.dumpLocation + File.separator + ReplUtils.REPL_HIVE_BASE_DIR;
    // Ckpt should be set on bootstrapped db.
    replica.verifyIfCkptSet(replicatedDbName, hiveDumpLocation);

    assertTablePartitionLocation(primaryDbName + ".t1", replicatedDbName + ".t1");
    assertTablePartitionLocation(primaryDbName + ".t2", replicatedDbName + ".t2");

    tuple = primary.run("use " + primaryDbName)
        .run("create external table t3 (id int)")
        .run("insert into table t3 values (10)")
        .run("create external table t4 as select id from t3")
        .dump(primaryDbName);

    // verify that the external table list is written correctly for incremental
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1", "t2", "t3", "t4"), tuple.dumpLocation, primary);

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 't3'")
        .verifyResult("t3")
        .run("select id from t3")
        .verifyResult("10")
        .run("select id from t4")
        .verifyResult("10");

    assertTablePartitionLocation(primaryDbName + ".t3", replicatedDbName + ".t3");

    tuple = primary.run("use " + primaryDbName)
        .run("drop table t1")
        .dump(primaryDbName);

    // verify that the external table list is written correctly for incremental
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t2", "t3", "t4"), tuple.dumpLocation, primary);
  }

  @Test
  public void externalTableReplicationWithDefaultPathsLazyCopy() throws Throwable {
    List<String> lazyCopyClause = Arrays.asList("'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='true'");
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
            .dump(primaryDbName, lazyCopyClause);

    // verify that the external table list is written correctly for bootstrap
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1", "t2"), tuple.dumpLocation, primary);



    replica.load(replicatedDbName, primaryDbName, lazyCopyClause)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("repl status " + replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("select country from t2 where country = 'us'")
            .verifyResult("us")
            .run("select country from t2 where country = 'france'")
            .verifyResult("france")
            .run("show partitions t2").verifyResults(new String[] {"country=france", "country=india", "country=us"});

    String hiveDumpLocation = tuple.dumpLocation + File.separator + ReplUtils.REPL_HIVE_BASE_DIR;
    // Ckpt should be set on bootstrapped db.
    replica.verifyIfCkptSet(replicatedDbName, hiveDumpLocation);

    assertTablePartitionLocation(primaryDbName + ".t1", replicatedDbName + ".t1");
    assertTablePartitionLocation(primaryDbName + ".t2", replicatedDbName + ".t2");

    tuple = primary.run("use " + primaryDbName)
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (10)")
            .run("create external table t4 as select id from t3")
            .dump(primaryDbName, lazyCopyClause);

    // verify that the external table list is written correctly for incremental
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t1", "t2", "t3", "t4"), tuple.dumpLocation, primary);

    replica.load(replicatedDbName, primaryDbName, lazyCopyClause)
            .run("use " + replicatedDbName)
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("select id from t3")
            .verifyResult("10")
            .run("select id from t4")
            .verifyResult("10");
  }

  @Test
  public void externalTableReplicationWithCustomPathsLazyCopy() throws Throwable {
    Path externalTableLocation =
            new Path("/" + testName.getMethodName() + "/" + primaryDbName + "/" + "a/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocation, new FsPermission("777"));

    List<String> withClause = Arrays.asList(
            "'distcp.options.update'=''",
            "'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='true'"
    );

    primary.run("use " + primaryDbName)
            .run("create external table a (i int, j int) "
                    + "row format delimited fields terminated by ',' "
                    + "location '" + externalTableLocation.toUri() + "'")
            .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("use " + replicatedDbName)
            .run("show tables like 'a'")
            .verifyResults(Collections.singletonList("a"))
            .run("select * From a").verifyResults(Collections.emptyList());

    assertTablePartitionLocation(primaryDbName + ".a", replicatedDbName + ".a");

    //externally add data to location
    try (FSDataOutputStream outputStream =
                 fs.create(new Path(externalTableLocation, "file1.txt"))) {
      outputStream.write("1,2\n".getBytes());
      outputStream.write("13,21\n".getBytes());
    }
    WarehouseInstance.Tuple incrementalTuple = primary.run("create table b (i int)")
            .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("select i From a")
            .verifyResults(new String[] { "1", "13" })
            .run("select j from a")
            .verifyResults(new String[] { "2", "21" });

    // alter table location to something new.
    externalTableLocation =
            new Path("/" + testName.getMethodName() + "/" + primaryDbName + "/new_location/a/");
    primary.run("use " + primaryDbName)
            .run("alter table a set location '" + externalTableLocation + "'")
            .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("use " + replicatedDbName)
            .run("select i From a")
            .verifyResults(Collections.emptyList());
    assertTablePartitionLocation(primaryDbName + ".a", replicatedDbName + ".a");
  }

  @Test
  public void testExternalTableLocationACLPreserved() throws Throwable {

    // Create data file with data for external table.
    Path externalTableLocation = new Path(
        "/" + testName.getMethodName() + "/" + primaryDbName + "/" + "a/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocation, new FsPermission("777"));
    fs.setOwner(externalTableLocation,"user1","group1");

    Path externalFileLoc = new Path(externalTableLocation, "file1.txt");
    try (FSDataOutputStream outputStream = fs.create(externalFileLoc)) {
      outputStream.write("1,2\n".getBytes());
      outputStream.write("13,21\n".getBytes());
    }

    // Set some ACL's on the table directory and the data file.
    List<AclEntry> aclEntries = new ArrayList<>();
    AclEntry aeUser =
        new AclEntry.Builder().setName("user").setScope(AclEntryScope.ACCESS)
            .setType(AclEntryType.USER).setPermission(FsAction.ALL).build();
    AclEntry aeGroup =
        new AclEntry.Builder().setName("group").setScope(AclEntryScope.ACCESS)
            .setType(AclEntryType.GROUP).setPermission(FsAction.ALL).build();
    AclEntry aeOther = new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.OTHER).setPermission(FsAction.ALL).build();

    aclEntries.add(aeUser);
    aclEntries.add(aeGroup);
    aclEntries.add(aeOther);

    fs.modifyAclEntries(externalTableLocation, aclEntries);
    fs.modifyAclEntries(externalFileLoc, aclEntries);

    // Run bootstrap without distcp options to preserve options.
    List<String> withClause = Arrays.asList("'distcp.options.update'=''",
        "'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='true'");

    primary.run("use " + primaryDbName).run(
        "create external table a (i int, j int) "
            + "row format delimited fields terminated by ',' " + "location '"
            + externalTableLocation.toUri() + "'")
        .dump(primaryDbName, withClause);

    // Verify load is success and has the appropriate data.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName).run("select i From a")
        .verifyResults(new String[] {"1", "13"}).run("select j from a")
        .verifyResults(new String[] {"2", "21"});

    // Verify the attributes of the destination table directory and data file are
    // not same as that of source.
    Hive hiveForReplica = Hive.get(replica.hiveConf);
    org.apache.hadoop.hive.ql.metadata.Table replicaTable =
        hiveForReplica.getTable(replicatedDbName + ".a");
    Path dataLocation = replicaTable.getDataLocation();

    assertNotEquals("ACL entries are same for the data file.",
        fs.getAclStatus(externalFileLoc).getEntries().size(),
        fs.getAclStatus(new Path(dataLocation, "file1.txt")).getEntries()
            .size());
    assertNotEquals("ACL entries are same for the table directory.",
        fs.getAclStatus(externalTableLocation).getEntries().size(),
        fs.getAclStatus(dataLocation).getEntries().size());

    assertNotEquals(fs.getFileStatus(externalTableLocation).getOwner(), fs.getFileStatus(dataLocation).getOwner());
    assertNotEquals(fs.getFileStatus(externalTableLocation).getGroup(), fs.getFileStatus(dataLocation).getGroup());

    // Dump & load with preserve attributes set.
    withClause = Arrays
        .asList("'distcp.options.update'=''", "'distcp.options.pugpa'=''",
            "'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname
                + "'='true'");

    primary.run("use " + primaryDbName).dump(primaryDbName, withClause);

    // Verify load is success and has the appropriate data.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName).run("select i From a")
        .verifyResults(new String[] {"1", "13"}).run("select j from a")
        .verifyResults(new String[] {"2", "21"});

    // Verify the ACL's of the destination table directory and data file are
    // same as that of source.

    assertEquals("ACL entries are not same for the data file.",
        fs.getAclStatus(externalFileLoc).getEntries().size(),
        fs.getAclStatus(new Path(dataLocation, "file1.txt")).getEntries()
            .size());
    assertEquals("ACL entries are not same for the table directory.",
        fs.getAclStatus(externalTableLocation).getEntries().size(),
        fs.getAclStatus(dataLocation).getEntries().size());

    assertEquals(fs.getFileStatus(externalTableLocation).getOwner(), fs.getFileStatus(dataLocation).getOwner());
    assertEquals(fs.getFileStatus(externalTableLocation).getGroup(), fs.getFileStatus(dataLocation).getGroup());
  }

  /**
   * @param sourceTableName  -- Provide the fully qualified table name
   * @param replicaTableName -- Provide the fully qualified table name
   */
  private void assertTablePartitionLocation(String sourceTableName, String replicaTableName)
      throws HiveException {
    Hive hiveForPrimary = Hive.get(primary.hiveConf);
    org.apache.hadoop.hive.ql.metadata.Table sourceTable = hiveForPrimary.getTable(sourceTableName);
    Path sourceLocation = sourceTable.getDataLocation();
    Hive hiveForReplica = Hive.get(replica.hiveConf);
    org.apache.hadoop.hive.ql.metadata.Table replicaTable = hiveForReplica.getTable(replicaTableName);
    Path dataLocation = replicaTable.getDataLocation();
    assertEquals(REPLICA_EXTERNAL_BASE + sourceLocation.toUri().getPath(),
        dataLocation.toUri().getPath());
    if (sourceTable.isPartitioned()) {
      Set<Partition> sourcePartitions = hiveForPrimary.getAllPartitionsOf(sourceTable);
      Set<Partition> replicaPartitions = hiveForReplica.getAllPartitionsOf(replicaTable);
      assertEquals(sourcePartitions.size(), replicaPartitions.size());
      List<String> expectedPaths =
          sourcePartitions.stream()
              .map(p -> REPLICA_EXTERNAL_BASE + p.getDataLocation().toUri().getPath())
              .collect(Collectors.toList());
      List<String> actualPaths =
          replicaPartitions.stream()
              .map(p -> p.getDataLocation().toUri().getPath())
              .collect(Collectors.toList());
      assertTrue(expectedPaths.containsAll(actualPaths));
    }
  }

  @Test
  public void externalTableReplicationWithCustomPaths() throws Throwable {
    Path externalTableLocation =
        new Path("/" + testName.getMethodName() + "/" + primaryDbName + "/" + "a/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocation, new FsPermission("777"));

    // Create base directory but use HDFS path without schema or authority details.
    // Hive should pick up the local cluster's HDFS schema/authority.
    List<String> withClause = Arrays.asList(
            "'distcp.options.update'=''"
    );

    primary.run("use " + primaryDbName)
        .run("create external table a (i int, j int) "
            + "row format delimited fields terminated by ',' "
            + "location '" + externalTableLocation.toUri() + "'")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 'a'")
        .verifyResults(Collections.singletonList("a"))
        .run("select * From a").verifyResults(Collections.emptyList());

    assertTablePartitionLocation(primaryDbName + ".a", replicatedDbName + ".a");

    //externally add data to location
    try (FSDataOutputStream outputStream =
        fs.create(new Path(externalTableLocation, "file1.txt"))) {
      outputStream.write("1,2\n".getBytes());
      outputStream.write("13,21\n".getBytes());
    }
    WarehouseInstance.Tuple incrementalTuple = primary.run("create table b (i int)")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("select i From a")
        .verifyResults(new String[] { "1", "13" })
        .run("select j from a")
        .verifyResults(new String[] { "2", "21" });

    // alter table location to something new.
    externalTableLocation =
        new Path("/" + testName.getMethodName() + "/" + primaryDbName + "/new_location/a/");
    primary.run("use " + primaryDbName)
        .run("alter table a set location '" + externalTableLocation + "'")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select i From a")
        .verifyResults(Collections.emptyList());
    assertTablePartitionLocation(primaryDbName + ".a", replicatedDbName + ".a");
  }

  @Test
  public void externalTableWithPartitions() throws Throwable {
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

    ReplicationTestUtils.assertExternalFileList(Collections.singletonList("t2"), tuple.dumpLocation, primary);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 't2'")
        .verifyResults(new String[] { "t2" })
        .run("select place from t2")
        .verifyResults(new String[] { "bangalore" })
        .verifyReplTargetProperty(replicatedDbName);

    assertTablePartitionLocation(primaryDbName + ".t2", replicatedDbName + ".t2");

    // add new  data externally, to a partition, but under the table level top directory
    Path partitionDir = new Path(externalTableLocation, "country=india");
    try (FSDataOutputStream outputStream = fs.create(new Path(partitionDir, "file.txt"))) {
      outputStream.write("pune\n".getBytes());
      outputStream.write("mumbai\n".getBytes());
    }

    tuple = primary.run("use " + primaryDbName)
        .run("insert into t2 partition(country='australia') values ('sydney')")
        .dump(primaryDbName, withClause);

    ReplicationTestUtils.assertExternalFileList(Collections.singletonList("t2"), tuple.dumpLocation, primary);

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

    Path customPartitionLocation =
        new Path("/" + testName.getMethodName() + "/partition_data/t2/country=france");
    fs.mkdirs(externalTableLocation, new FsPermission("777"));

    // add new partitions to the table, at an external location than the table level directory
    try (FSDataOutputStream outputStream = fs
        .create(new Path(customPartitionLocation, "file.txt"))) {
      outputStream.write("paris".getBytes());
    }

    primary.run("use " + primaryDbName)
        .run("ALTER TABLE t2 ADD PARTITION (country='france') LOCATION '" + customPartitionLocation
            .toString() + "'")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select place from t2 where country='france'")
        .verifyResults(new String[] { "paris" })
        .run("show partitions t2")
        .verifyResults(new String[] {"country=australia", "country=france", "country=india"})
        .verifyReplTargetProperty(replicatedDbName);

    // change the location of the partition via alter command
    String tmpLocation = "/tmp/" + System.nanoTime();
    primary.miniDFSCluster.getFileSystem().mkdirs(new Path(tmpLocation), new FsPermission("777"));

    primary.run("use " + primaryDbName)
        .run("alter table t2 partition (country='france') set location '" + tmpLocation + "'")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select place from t2 where country='france'")
        .verifyResults(new String[] {})
        .verifyReplTargetProperty(replicatedDbName);

    // Changing location of one of the partitions shouldn't result in changing location of other
    // partitions as well as that of the table.
    assertTablePartitionLocation(primaryDbName + ".t2", replicatedDbName + ".t2");

    // Changing location of the external table, should result in changes to the location of
    // partition residing within the table location and not the partitions located outside.
    String tmpLocation2 = "/tmp/" + System.nanoTime() + "_2";
    primary.miniDFSCluster.getFileSystem().mkdirs(new Path(tmpLocation2), new FsPermission("777"));

    primary.run("use " + primaryDbName)
            .run("insert into table t2 partition(country='france') values ('lyon')")
            .run("alter table t2 set location '" + tmpLocation2 + "'")
            .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause);
    assertTablePartitionLocation(primaryDbName + ".t2", replicatedDbName + ".t2");
  }

  @Test
  public void externalTableWithPartitionsInBatch() throws Throwable {
    Path externalTableLocation =
      new Path("/" + testName.getMethodName() + "/t2/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocation, new FsPermission("777"));

    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_LOAD_PARTITIONS_BATCH_SIZE.varname + "'='" + 1 + "'");

    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
      .run("create external table t2 (place string) partitioned by (country string) row format "
        + "delimited fields terminated by ',' location '" + externalTableLocation.toString()
        + "'")
      .run("insert into t2 partition(country='india') values ('bangalore')")
      .run("insert into t2 partition(country='france') values ('paris')")
      .run("insert into t2 partition(country='australia') values ('sydney')")
      .dump(primaryDbName, withClause);

    ReplicationTestUtils.assertExternalFileList(Collections.singletonList("t2"), tuple.dumpLocation, primary);

    replica.load(replicatedDbName, primaryDbName, withClause)
      .run("use " + replicatedDbName)
      .run("show tables like 't2'")
      .verifyResults(new String[] { "t2" })
      .run("select distinct(country) from t2")
      .verifyResults(new String[] { "india", "france", "australia" })
      .run("select place from t2")
      .verifyResults(new String[] { "bangalore", "paris", "sydney" })
      .verifyReplTargetProperty(replicatedDbName);

    assertTablePartitionLocation(primaryDbName + ".t2", replicatedDbName + ".t2");
  }

  @Test
  public void externalTableIncrementalCheckpointing() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (1)")
            .run("insert into table t1 values (2)")
            .run("create external table t2 (id int)")
            .run("insert into table t2 values (3)")
            .run("insert into table t2 values (4)")
            .dump(primaryDbName, withClause);

    ReplicationTestUtils.assertExternalFileList(Arrays.asList(new String[]{"t1", "t2"}), tuple.dumpLocation, primary);

    replica.load(replicatedDbName, primaryDbName)
            .status(replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("select * from t1")
            .verifyResults(new String[] {"1", "2"})
            .run("select * from t2")
            .verifyResults(new String[] {"3", "4"})
            .verifyReplTargetProperty(replicatedDbName);

    ReplDumpWork.testDeletePreviousDumpMetaPath(true);

    withClause = ReplicationTestUtils.externalTableWithClause(new ArrayList<>(), true, true);
    WarehouseInstance.Tuple incrementalDump1 = primary.run("use " + primaryDbName)
            .run("drop table t1")
            .run("insert into table t2 values (5)")
            .run("insert into table t2 values (6)")
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (8)")
            .dump(primaryDbName, withClause);

    // verify that the external table list is written correctly for incremental
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t2", "t3"), incrementalDump1.dumpLocation, primary);

    FileSystem fs = primary.miniDFSCluster.getFileSystem();
    Path hiveDumpDir = new Path(incrementalDump1.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path ackFile = new Path(hiveDumpDir, ReplAck.DUMP_ACKNOWLEDGEMENT.toString());
    Path ackLastEventID = new Path(hiveDumpDir, ReplAck.EVENTS_DUMP.toString());
    assertTrue(fs.exists(ackFile));
    assertTrue(fs.exists(ackLastEventID));
    Path bootstrapDir = new Path(hiveDumpDir, ReplUtils.INC_BOOTSTRAP_ROOT_DIR_NAME);
    Path metaDir = new Path(bootstrapDir, EximUtil.METADATA_PATH_NAME);
    Path dataDir = new Path(bootstrapDir, EximUtil.DATA_PATH_NAME);
    assertFalse(fs.exists(dataDir));
    long oldMetadirModTime = fs.getFileStatus(metaDir).getModificationTime();
    fs.delete(ackFile, false);
    EventsDumpMetadata eventsDumpMetadata = EventsDumpMetadata.deserialize(ackLastEventID, conf);
    fs.delete(ackLastEventID, false);
    //delete all the event folders except first event
    long startEvent = -1;
    long endEvent = Long.valueOf(incrementalDump1.lastReplicationId);
    int deletedEventsCount = 0;
    for (long eventDir = Long.valueOf(tuple.lastReplicationId) + 1;  eventDir <= endEvent; eventDir++) {
      Path eventRoot = new Path(hiveDumpDir, String.valueOf(eventDir));
      if (fs.exists(eventRoot)) {
        if (startEvent == -1){
          startEvent = eventDir;
        } else {
          deletedEventsCount++;
          fs.delete(eventRoot, true);
        }
      }
    }
    Path startEventRoot = new Path(hiveDumpDir, String.valueOf(startEvent));
    Map<Path, Long> firstEventModTimeMap = new HashMap<>();
    for (FileStatus fileStatus: fs.listStatus(startEventRoot)) {
      firstEventModTimeMap.put(fileStatus.getPath(), fileStatus.getModificationTime());
    }
    assertTrue(endEvent - startEvent > 1);
    eventsDumpMetadata.setEventsDumpedCount(eventsDumpMetadata.getEventsDumpedCount() - deletedEventsCount);
    Utils.writeOutput(eventsDumpMetadata.serialize(), ackLastEventID, primary.hiveConf);
    WarehouseInstance.Tuple incrementalDump2 = primary.dump(primaryDbName, withClause);
    assertEquals(incrementalDump1.dumpLocation, incrementalDump2.dumpLocation);
    assertTrue(fs.getFileStatus(metaDir).getModificationTime() > oldMetadirModTime);
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t2", "t3"), incrementalDump2.dumpLocation, primary);
    //first event meta is not rewritten
    for (Map.Entry<Path, Long> entry: firstEventModTimeMap.entrySet()) {
      assertEquals((long)entry.getValue(), fs.getFileStatus(entry.getKey()).getModificationTime());
    }
    replica.load(replicatedDbName, primaryDbName)
            .status(replicatedDbName)
            .verifyResult(incrementalDump2.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyFailure(new String[] {"t1"})
            .run("select * from t2")
            .verifyResults(new String[] {"3", "4", "5", "6"})
            .run("select * from t3")
            .verifyResult("8")
            .verifyReplTargetProperty(replicatedDbName);
  }

  @Test
  public void externalTableIncrementalReplication() throws Throwable {
    WarehouseInstance.Tuple tuple = primary.dump(primaryDbName);
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

    ReplicationTestUtils.assertExternalFileList(Collections.singletonList("t1"), tuple.dumpLocation, primary);

    // Add new data externally, to a partition, but under the partition level top directory
    // Also, it is added after dumping the events so data should not be seen at target after REPL LOAD.
    Path partitionDir = new Path(externalTableLocation, "country=india");
    try (FSDataOutputStream outputStream = fs.create(new Path(partitionDir, "file.txt"))) {
      outputStream.write("pune\n".getBytes());
      outputStream.write("mumbai\n".getBytes());
    }

    try (FSDataOutputStream outputStream = fs.create(new Path(partitionDir, "file1.txt"))) {
      outputStream.write("bangalore\n".getBytes());
    }

    replica.load(replicatedDbName, primaryDbName)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("show partitions t1")
        .verifyResults(new String[] { "country=india", "country=us" })
        .run("select place from t1 order by place")
        .verifyResults(new String[] {})
        .verifyReplTargetProperty(replicatedDbName);

    // The Data should be seen after next dump-and-load cycle.
    tuple = primary.run("use " + primaryDbName)
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show partitions t1")
            .verifyResults(new String[] {"country=india", "country=us"})
            .run("select place from t1 order by place")
            .verifyResults(new String[] {"bangalore", "mumbai", "pune"})
            .verifyReplTargetProperty(replicatedDbName);

    // Delete one of the file and update another one.
    fs.delete(new Path(partitionDir, "file.txt"), true);
    fs.delete(new Path(partitionDir, "file1.txt"), true);
    try (FSDataOutputStream outputStream = fs.create(new Path(partitionDir, "file1.txt"))) {
      outputStream.write("chennai\n".getBytes());
    }

    // Repl load with zero events but external tables file list should present.
    tuple = primary.dump(primaryDbName);
    ReplicationTestUtils.assertExternalFileList(Collections.singletonList("t1"), tuple.dumpLocation, primary);

    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show partitions t1")
            .verifyResults(new String[] { "country=india", "country=us" })
            .run("select place from t1 order by place")
            .verifyResults(new String[] { "chennai" })
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
    List<String> dumpWithClause = Collections.singletonList(
        "'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='false'"
    );

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
    Path replHiveBasePath = new Path(tuple.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertFalse(primary.miniDFSCluster.getFileSystem()
            .exists(new Path(replHiveBasePath, EximUtil.FILE_LIST_EXTERNAL)));

    replica.load(replicatedDbName, primaryDbName)
            .status(replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyFailure(new String[] {"t1" })
            .run("show tables like 't2'")
            .verifyFailure(new String[] {"t2" })
            .verifyReplTargetProperty(replicatedDbName);

    dumpWithClause = ReplicationTestUtils.externalTableWithClause(new ArrayList<>(), true, true);

    tuple = primary.run("use " + primaryDbName)
            .run("drop table t1")
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (10)")
            .run("insert into table t3 values (20)")
            .run("create table t4 as select * from t3")
            .dump(primaryDbName, dumpWithClause);

    // the _file_list_external should be created as external tables are to be replicated.
    Path hivePath = new Path(tuple.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(primary.miniDFSCluster.getFileSystem()
            .exists(new Path(hivePath, EximUtil.FILE_LIST_EXTERNAL)));

    // verify that the external table list is written correctly for incremental
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("t2", "t3"), tuple.dumpLocation, primary);

    // _bootstrap directory should be created as bootstrap enabled on external tables.
    String hiveDumpLocation = tuple.dumpLocation + File.separator + ReplUtils.REPL_HIVE_BASE_DIR;
    Path dumpPath = new Path(hiveDumpLocation, INC_BOOTSTRAP_ROOT_DIR_NAME);
    assertTrue(primary.miniDFSCluster.getFileSystem().exists(dumpPath));

    // _bootstrap/<db_name>/t2
    // _bootstrap/<db_name>/t3
    Path dbPath = new Path(dumpPath, EximUtil.METADATA_PATH_NAME + File.separator + primaryDbName);
    Path tblPath = new Path(dbPath, "t2");
    assertTrue(primary.miniDFSCluster.getFileSystem().exists(tblPath));
    tblPath = new Path(dbPath, "t3");
    assertTrue(primary.miniDFSCluster.getFileSystem().exists(tblPath));

    replica.load(replicatedDbName, primaryDbName)
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
    replica.verifyIfCkptSetForTables(replicatedDbName, Arrays.asList("t2", "t3"), hiveDumpLocation);

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
  public void retryBootstrapExternalTablesFromDifferentDump() throws Throwable {
    List<String> loadWithClause = ReplicationTestUtils.includeExternalTableClause(true);
    List<String> dumpWithClause = ReplicationTestUtils.includeExternalTableClause(false);

    WarehouseInstance.Tuple tupleBootstrapWithoutExternal = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (1)")
            .run("create external table t2 (place string) partitioned by (country string)")
            .run("insert into table t2 partition(country='india') values ('bangalore')")
            .run("insert into table t2 partition(country='us') values ('austin')")
            .run("create table t3 as select * from t1")
            .dump(primaryDbName, dumpWithClause);

    replica.load(replicatedDbName, primaryDbName, loadWithClause)
            .status(replicatedDbName)
            .verifyResult(tupleBootstrapWithoutExternal.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResult("t3")
            .run("select id from t3")
            .verifyResult("1")
            .verifyReplTargetProperty(replicatedDbName);

    dumpWithClause = ReplicationTestUtils.externalTableWithClause(new ArrayList<>(), true, true);
    primary.run("use " + primaryDbName)
            .run("drop table t1")
            .run("create external table t4 (id int)")
            .run("insert into table t4 values (10)")
            .run("create table t5 as select * from t4")
            .dump(primaryDbName, dumpWithClause);

    // Fail setting ckpt property for table t4 but success for t2.
    BehaviourInjection<CallerArguments, Boolean> callerVerifier
            = new BehaviourInjection<CallerArguments, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable CallerArguments args) {
        if (args.tblName.equalsIgnoreCase("t4") && args.dbName.equalsIgnoreCase(replicatedDbName)) {
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
      replica.loadFailure(replicatedDbName, primaryDbName, loadWithClause);
      callerVerifier.assertInjectionsPerformed(true, false);
    } finally {
      InjectableBehaviourObjectStore.resetAlterTableModifier();
    }

    Path baseDumpDir = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPL_DIR));
    Path nonRecoverablePath = TestReplicationScenarios.getNonRecoverablePath(baseDumpDir, primaryDbName, primary.hiveConf);
    if(nonRecoverablePath != null){
      baseDumpDir.getFileSystem(primary.hiveConf).delete(nonRecoverablePath, true);
    }

    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyFailure(new String[]{"t1"})
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select country from t2 order by country")
            .verifyResults(new String[] {"india", "us"})
            .run("select id from t4")
            .verifyResults(Arrays.asList("10"))
            .run("select id from t5")
            .verifyResult("10")
            .verifyReplTargetProperty(replicatedDbName);

    // Insert into existing external table and then Drop it, add another managed table with same name
    // and dump another bootstrap dump for external tables.
    dumpWithClause = ReplicationTestUtils.includeExternalTableClause(true);
    primary.run("use " + primaryDbName)
            .run("insert into table t2 partition(country='india') values ('chennai')")
            .run("drop table t2")
            .run("create table t2 as select * from t4")
            .run("insert into table t4 values (20)")
            .dump(primaryDbName, dumpWithClause);


    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyFailure(new String[]{"t1"})
            .run("select id from t2")
            .verifyResult("10")
            .run("select id from t4")
            .verifyResults(Arrays.asList("10", "20"))
            .run("select id from t5")
            .verifyResult("10")
            .verifyReplTargetProperty(replicatedDbName);

  }

  @Test
  public void testExternalTableDataPath() throws Exception {
    HiveConf conf = primary.getConf();
    Path basePath = new Path("/");
    Path sourcePath = new Path("/abc/xyz");
    Path dataPath = ReplExternalTables.externalTableDataPath(conf, basePath, sourcePath);
    assertTrue(dataPath.toUri().getPath().equalsIgnoreCase("/abc/xyz"));

    basePath = new Path("/tmp");
    dataPath = ReplExternalTables.externalTableDataPath(conf, basePath, sourcePath);
    assertTrue(dataPath.toUri().getPath().equalsIgnoreCase("/tmp/abc/xyz"));

    basePath = new Path("/tmp/");
    dataPath = ReplExternalTables.externalTableDataPath(conf, basePath, sourcePath);
    assertTrue(dataPath.toUri().getPath().equalsIgnoreCase("/tmp/abc/xyz"));

    basePath = new Path("/tmp/tmp1//");
    dataPath = ReplExternalTables.externalTableDataPath(conf, basePath, sourcePath);
    assertTrue(dataPath.toUri().getPath().equalsIgnoreCase("/tmp/tmp1/abc/xyz"));
  }

  @Test
  public void testExternalTablesIncReplicationWithConcurrentDropTable() throws Throwable {
    List<String> dumpWithClause = ReplicationTestUtils.externalTableWithClause(new ArrayList<>(), null, true);
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
    ReplicationTestUtils.assertExternalFileList(Collections.singletonList("t2"), tupleInc.dumpLocation, primary);

    // The newly inserted data "2" should be missing in table "t1". But, table t2 should exist and have
    // inserted data.
    replica.load(replicatedDbName, primaryDbName, loadWithClause)
            .run("use " + replicatedDbName)
            .run("select id from t1 order by id")
            .verifyResult("1")
            .run("select id from t2 order by id")
            .verifyResults(Arrays.asList("1", "2"));
  }

  @Test
  public void testIncrementalDumpEmptyDumpDirectory() throws Throwable {
    List<String> loadWithClause = ReplicationTestUtils.includeExternalTableClause(true);
    List<String> dumpWithClause = ReplicationTestUtils.includeExternalTableClause(true);
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (1)")
            .run("insert into table t1 values (2)")
            .dump(primaryDbName, dumpWithClause);

    replica.load(replicatedDbName, primaryDbName)
            .status(replicatedDbName)
            .verifyResult(tuple.lastReplicationId);

    // This looks like an empty dump but it has the ALTER TABLE event created by the previous
    // dump. We need it here so that the next dump won't have any events.
    WarehouseInstance.Tuple incTuple = primary.dump(primaryDbName, dumpWithClause);
    replica.load(replicatedDbName, primaryDbName, loadWithClause)
            .status(replicatedDbName)
            .verifyResult(incTuple.lastReplicationId);

    // create events for some other database and then dump the primaryDbName to dump an empty directory.
    primary.run("create database " + extraPrimaryDb + " WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')");
    WarehouseInstance.Tuple inc2Tuple = primary.run("use " + extraPrimaryDb)
            .run("create table tbl (fld int)")
            .run("use " + primaryDbName)
            .dump(primaryDbName, dumpWithClause);
    Assert.assertEquals(primary.getCurrentNotificationEventId().getEventId(),
                        Long.valueOf(inc2Tuple.lastReplicationId).longValue());

    // Incremental load to existing database with empty dump directory should set the repl id to the last event at src.
    replica.load(replicatedDbName, primaryDbName, loadWithClause)
            .status(replicatedDbName)
            .verifyResult(inc2Tuple.lastReplicationId);
  }

  @Test
  public void testExtTableBootstrapDuringIncrementalWithoutAnyEvents() throws Throwable {
    List<String> loadWithClause = ReplicationTestUtils.includeExternalTableClause(false);
    List<String> dumpWithClause = Collections.singletonList(
            "'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='false'"
    );

    WarehouseInstance.Tuple bootstrapDump = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (1)")
            .run("create table t2 (id int)")
            .run("insert into table t2 values (1)")
            .dump(primaryDbName, dumpWithClause);

    replica.load(replicatedDbName, primaryDbName, loadWithClause)
            .status(replicatedDbName)
            .verifyResult(bootstrapDump.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyFailure(new String[] {"t1" })
            .run("show tables like 't2'")
            .verifyResult("t2")
            .verifyReplTargetProperty(replicatedDbName);

    // This looks like an empty dump but it has the ALTER TABLE event created by the previous
    // dump. We need it here so that the next dump won't have any events.
    WarehouseInstance.Tuple incTuple = primary.dump(primaryDbName, ReplicationTestUtils.includeExternalTableClause(true));
    replica.load(replicatedDbName, primaryDbName)
            .status(replicatedDbName)
            .verifyResult(incTuple.lastReplicationId);

    // Take a dump with external tables bootstrapped and load it
    dumpWithClause = ReplicationTestUtils.externalTableWithClause(new ArrayList<>(), true, true);
    WarehouseInstance.Tuple inc2Tuple = primary.run("use " + primaryDbName)
            .dump(primaryDbName, dumpWithClause);

    replica.load(replicatedDbName, primaryDbName, loadWithClause)
            .status(replicatedDbName)
            .verifyResult(inc2Tuple.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .verifyReplTargetProperty(replicatedDbName);
  }

  @Test
  public void replicationWithTableNameContainsKeywords() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);

    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create external table t1_functions (id int)")
            .run("insert into table t1_functions values (1)")
            .run("insert into table t1_functions values (2)")
            .run("create external table t2_constraints (place string) partitioned by (country string)")
            .run("insert into table t2_constraints partition(country='india') values ('bangalore')")
            .run("insert into table t2_constraints partition(country='us') values ('austin')")
            .run("insert into table t2_constraints partition(country='france') values ('paris')")
            .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("repl status " + replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables like 't1_functions'")
            .verifyResults(new String[] {"t1_functions"})
            .run("show tables like 't2_constraints'")
            .verifyResults(new String[] {"t2_constraints"})
            .run("select id from t1_functions")
            .verifyResults(new String[] {"1", "2"})
            .verifyReplTargetProperty(replicatedDbName);

    primary.run("use " + primaryDbName)
            .run("create external table t3_bootstrap (id int)")
            .run("insert into table t3_bootstrap values (10)")
            .run("insert into table t3_bootstrap values (20)")
            .run("create table t4_tables (id int)")
            .run("insert into table t4_tables values (10)")
            .run("insert into table t4_tables values (20)")
            .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
            .run("use " + replicatedDbName)
            .run("show tables like 't3_bootstrap'")
            .verifyResults(new String[] {"t3_bootstrap"})
            .run("show tables like 't4_tables'")
            .verifyResults(new String[] {"t4_tables"})
            .verifyReplTargetProperty(replicatedDbName);
  }

  @Test
  public void testExternalTableBaseDirMandatory() throws Throwable {
    List<String> withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname + "'='/extTablebase'");
    WarehouseInstance.Tuple tuple = null;
    try {
      primary.run("use " + primaryDbName)
              .run("create external table t1 (id int)")
              .run("insert into table t1 values(1)")
              .dump(primaryDbName, withClause);
    } catch (SemanticException ex) {
      assertTrue(ex.getMessage().contains(
              "Fully qualified path for 'hive.repl.replica.external.table.base.dir' is required"));
    }
    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname
            + "'='"+ fullyQualifiedReplicaExternalBase +"'");
    try {
      primary.run("use " + primaryDbName)
        .dump(primaryDbName, withClause);
    } catch (Exception e) {
      Assert.assertEquals(ErrorMsg.REPL_FAILED_WITH_NON_RECOVERABLE_ERROR.getErrorCode(),
        ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode());
    }
    //delete non recoverable marker
    Path dumpPath = new Path(primary.hiveConf.get(HiveConf.ConfVars.REPL_DIR.varname),
      Base64.getEncoder().encodeToString(primaryDbName.toLowerCase()
        .getBytes(StandardCharsets.UTF_8.name())));
    FileSystem fs = dumpPath.getFileSystem(conf);
    Path nonRecoverableMarker = new Path(fs.listStatus(dumpPath)[0].getPath(), ReplAck.NON_RECOVERABLE_MARKER
      .toString());
    fs.delete(nonRecoverableMarker, true);

    tuple = primary.run("use " + primaryDbName)
      .dump(primaryDbName, withClause);

    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname + "'=''");
    try {
      replica.load(replicatedDbName, primaryDbName, withClause);
    } catch (SemanticException ex) {
      assertTrue(ex.getMessage().contains(
              "Fully qualified path for 'hive.repl.replica.external.table.base.dir' is required"));
    }

    withClause = ReplicationTestUtils.includeExternalTableClause(true);
    withClause.add("'" + HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname
            + "'='"+ fullyQualifiedReplicaExternalBase +"'");

    try {
      replica.load(replicatedDbName, primaryDbName, withClause);
    } catch (Exception e) {
      Assert.assertEquals(ErrorMsg.REPL_FAILED_WITH_NON_RECOVERABLE_ERROR.getErrorCode(),
        ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode());
    }

    //delete non recoverable marker
    nonRecoverableMarker = new Path(tuple.dumpLocation, ReplAck.NON_RECOVERABLE_MARKER
      .toString());
    fs.delete(nonRecoverableMarker, true);

    replica.load(replicatedDbName, primaryDbName, withClause);

    replica.run("repl status " + replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResults(new String[] {"t1"})
            .run("select id from t1")
            .verifyResults(new String[] {"1"})
            .verifyReplTargetProperty(replicatedDbName);
  }

  @Test
  public void differentCatalogIncrementalReplication() throws Throwable {
    //Create the catalog
    Catalog catalog = new Catalog();
    catalog.setName("spark");
    Warehouse wh = new Warehouse(conf);
    catalog.setLocationUri(wh.getWhRootExternal().toString() + File.separator + catalog);
    catalog.setDescription("Non-hive catalog");
    Hive.get(primary.hiveConf).getMSC().createCatalog(catalog);

    //Create database and table in spark catalog
    String sparkDbName = "src_spark";
    Database sparkdb = new Database();
    sparkdb.setCatalogName("spark");
    sparkdb.setName(sparkDbName);
    Hive.get(primary.hiveConf).getMSC().createDatabase(sparkdb);

    SerDeInfo serdeInfo = new SerDeInfo("LBCSerDe", LazyBinaryColumnarSerDe.class.getCanonicalName(),
      new HashMap<String, String>());
    ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(1);
    cols.add(new FieldSchema("place", serdeConstants.STRING_TYPE_NAME, ""));
    StorageDescriptor sd
      = new StorageDescriptor(cols, null,
      "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
      "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
      false, 0, serdeInfo, null, null, null);
    Map<String, String> tableParameters = new HashMap<String, String>();

    Table sparkTable = new Table("mgt1", sparkDbName, "", 0, 0, 0,
      sd, null, tableParameters, "", "", "");
    sparkTable.setCatName("spark");
    Hive.get(primary.hiveConf).getMSC().createTable(sparkTable);

    //create same db in hive catalog
    Map<String, String> params = new HashMap<>();
    params.put(SOURCE_OF_REPLICATION, "1");
    Database hiveDb = new Database();
    hiveDb.setCatalogName("hive");
    hiveDb.setName(sparkDbName);
    hiveDb.setParameters(params);
    Hive.get(primary.hiveConf).getMSC().createDatabase(hiveDb);

    primary.dump(sparkDbName);
    //spark tables are not replicated in bootstrap
    replica.load(replicatedDbName, sparkDbName)
      .run("use " + replicatedDbName)
      .run("show tables like mgdt1")
      .verifyResult(null);

    Path externalTableLocation =
      new Path("/" + testName.getMethodName() + "/t1/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocation, new FsPermission("777"));

    //Create another table in spark
    sparkTable = new Table("mgt2", sparkDbName, "", 0, 0, 0,
      sd, null, tableParameters, "", "", "");
    sparkTable.setCatName("spark");
    Hive.get(primary.hiveConf).getMSC().createTable(sparkTable);

    //Incremental load shouldn't copy any events from spark catalog
    primary.dump(sparkDbName);
    replica.load(replicatedDbName, sparkDbName)
      .run("use " + replicatedDbName)
      .run("show tables like mgdt1")
      .verifyResult(null)
      .run("show tables like 'mgt2'")
      .verifyResult(null);

    primary.run("drop database if exists " + sparkDbName + " cascade");
  }

  @Test
  public void testDatabaseLevelCopyLazy() throws Throwable {
    testDatabaseLevelCopy(true);
  }

  @Test
  public void testDatabaseLevelCopyAtSource() throws Throwable {
    testDatabaseLevelCopy(false);
  }

  public void testDatabaseLevelCopy(boolean runCopyTasksOnTarget)
      throws Throwable {
    Path externalTableLocation =
        new Path("/" + testName.getMethodName() + "/" + primaryDbName + "/" + "a/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocation, new FsPermission("777"));

    Path externalTablePartitionLocation =
        new Path("/" + testName.getMethodName() + "/" + primaryDbName + "/" + "part/");
    fs.mkdirs(externalTableLocation, new FsPermission("777"));

    List<String> withClause = Arrays.asList("'distcp.options.update'=''",
        "'" + REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK.varname + "'='true'",
        "'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname
            + "'='" + runCopyTasksOnTarget + "'");

    // Create a table within the warehouse location, one outside and one with
    // a partition outside the default location.
    WarehouseInstance.Tuple tuple =
        primary.run("use " + primaryDbName)
            .run("create external table a (i int, j int) "
                + "row format delimited fields terminated by ',' "
                + "location '" + externalTableLocation.toUri() + "'")
            .run("insert into a values(1,2)")
            .run("create external table b (id int)")
            .run("insert into b values(5)")
            .run("create external table c (place string) partitioned by (country "
                + "string)")
            .run("insert into table c partition(country='india') values "
                + "('bangalore')")
            .run("ALTER TABLE c ADD PARTITION (country='france') LOCATION '"
                + externalTablePartitionLocation.toString() + "'")
            .run("insert into c partition(country='france') values('paris')")
            .dump(primaryDbName, withClause);

    Database primaryDb = primary.getDatabase(primaryDbName);

    // Confirm the a table is outside the db location.
    Table aTable = primary.getTable(primaryDbName, "a");
    assertFalse(FileUtils
        .isPathWithinSubtree(new Path(aTable.getSd().getLocation()),
            new Path(primaryDb.getLocationUri())));

    //Confirm the b table is inside the db location.
    Table bTable = primary.getTable(primaryDbName, "b");
    assertTrue(FileUtils
        .isPathWithinSubtree(new Path(bTable.getSd().getLocation()),
            new Path(primaryDb.getLocationUri())));

    //Confirm the c table is inside the db location.
    Table cTable = primary.getTable(primaryDbName, "c");
    assertTrue(FileUtils
        .isPathWithinSubtree(new Path(cTable.getSd().getLocation()),
            new Path(primaryDb.getLocationUri())));

    // Confirm the partition of c table is outside db location.
    String partitionBtableLocation =
        primary.getAllPartitions(primaryDbName, "c").get(0).getSd()
            .getLocation();
    assertFalse(FileUtils.isPathWithinSubtree(new Path(partitionBtableLocation),
        new Path(primaryDb.getLocationUri())));

    // Do a load and verify all the data is there.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select i from a where j=2")
        .verifyResult("1")
        .run("select id from b")
        .verifyResult("5")
        .run("select place from c where country='india'")
        .verifyResult("bangalore")
        .run("select place from c where country='france'")
        .verifyResult("paris");

    // Check the task copied post bootstrap, It should have the database loc,
    // the table 'a' since that is outside of the default location, and the
    // 'c', since its partition is out of the default location.
    ReplicationTestUtils
        .assertExternalFileList(Arrays.asList("dbPath:" + new Path(primaryDb.getLocationUri()).getName(), "a", "c"),
        tuple.dumpLocation, primary);

    // Add more data to tables and do a incremental run and create another
    // tables one inside and other outside default location.

    externalTableLocation = new Path(
        "/" + testName.getMethodName() + "/" + primaryDbName + "/" + "newout/");
    fs.mkdirs(externalTableLocation, new FsPermission("777"));
    tuple =
        primary.run("use " + primaryDbName)
            .run("insert into a values(3,4)")
            .run("insert into b values(6)")
            .run("insert into table c partition(country='india') values "
                + "('delhi')")
            .run("insert into c partition(country='france') values('lyon')")
            .run("create external table newin (id int)")
            .run("insert into newin values(1)")
            .run("create external table newout(id int) row format delimited "
                + "fields terminated by ',' location '" + externalTableLocation
                .toUri() + "'")
            .run("insert into newout values(2)")
            .dump(primaryDbName, withClause);


    // Check whether table newin is inside the database location.
    Table tableNewin = primary.getTable(primaryDbName,"newin");
    assertTrue(FileUtils
        .isPathWithinSubtree(new Path(tableNewin.getSd().getLocation()),
            new Path(primaryDb.getLocationUri())));

    // Check whether table newout is outside database location,
    Table tableNewout = primary.getTable(primaryDbName,"newout");
    assertFalse(FileUtils
        .isPathWithinSubtree(new Path(tableNewout.getSd().getLocation()),
            new Path(primaryDb.getLocationUri())));

    // Do an incremental load and check if all the old and new data is there.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select i from a where j=4")
        .verifyResult("3")
        .run("select id from b")
        .verifyResults(new String[]{"5", "6"})
        .run("select place from c where country='india'")
        .verifyResults(new String[]{"bangalore", "delhi"})
        .run("select place from c where country='france'")
        .verifyResults(new String[]{"paris", "lyon"})
        .run("select id from newin")
        .verifyResult("1")
        .run("select id from newout")
        .verifyResult("2");

    // New table in the warehouse shouldn't be there but the table created
    // outside should be there, apart from the ones in the previous run.

    ReplicationTestUtils.assertExternalFileList(
        Arrays.asList("dbPath:" + new Path(primaryDb.getLocationUri()).getName(), "a", "c", "newout"),
        tuple.dumpLocation, primary);
  }

  @Test
  public void testDatabaseLevelCopyDisabled() throws Throwable {
    Path externalTableLocation =
        new Path("/" + testName.getMethodName() + "/" + primaryDbName + "/" + "a/");
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(externalTableLocation, new FsPermission("777"));

    Path externalTablePartitionLocation =
        new Path("/" + testName.getMethodName() + "/" + primaryDbName + "/" + "part/");
    fs.mkdirs(externalTableLocation, new FsPermission("777"));

    List<String> withClause = Arrays.asList("'distcp.options.update'=''",
        "'" + REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK.varname + "'='false'");

    // Create a table within the warehouse location, one outside and one with
    // a partition outside the default location.
    WarehouseInstance.Tuple tuple =
        primary.run("use " + primaryDbName)
            .run("create external table a (i int, j int) "
                + "row format delimited fields terminated by ',' "
                + "location '" + externalTableLocation.toUri() + "'")
            .run("insert into a values(1,2)")
            .run("create external table b (id int)")
            .run("insert into b values(5)")
            .run("create external table c (place string) partitioned by (country "
                + "string)")
            .run("insert into table c partition(country='india') values "
                + "('bangalore')")
            .run("ALTER TABLE c ADD PARTITION (country='france') LOCATION '"
                + externalTablePartitionLocation.toString() + "'")
            .run("insert into c partition(country='france') values('paris')")
            .dump(primaryDbName, withClause);

    // Do a load and verify all the data is there.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select i from a where j=2")
        .verifyResult("1")
        .run("select id from b")
        .verifyResult("5")
        .run("select place from c where country='india'")
        .verifyResult("bangalore")
        .run("select place from c where country='france'")
        .verifyResult("paris");

    ReplicationTestUtils.assertExternalFileList(Arrays.asList("a", "b", "c"), tuple.dumpLocation, primary);

    // Add more data to tables and do a incremental run and create another
    // tables one inside and other outside default location.

    externalTableLocation = new Path(
        "/" + testName.getMethodName() + "/" + primaryDbName + "/" + "newout/");
    fs.mkdirs(externalTableLocation, new FsPermission("777"));
    tuple =
        primary.run("use " + primaryDbName)
            .run("insert into a values(3,4)")
            .run("insert into b values(6)")
            .run("insert into table c partition(country='india') values "
                + "('delhi')")
            .run("insert into c partition(country='france') values('lyon')")
            .run("create external table newin (id int)")
            .run("insert into newin values(1)")
            .run("create external table newout(id int) row format delimited "
                + "fields terminated by ',' location '" + externalTableLocation
                .toUri() + "'")
            .run("insert into newout values(2)")
            .dump(primaryDbName, withClause);

    // Do an incremental load and check if all the old and new data is there.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select i from a where j=4")
        .verifyResult("3")
        .run("select id from b")
        .verifyResults(new String[]{"5", "6"})
        .run("select place from c where country='india'")
        .verifyResults(new String[]{"bangalore", "delhi"})
        .run("select place from c where country='france'")
        .verifyResults(new String[]{"paris", "lyon"})
        .run("select id from newin")
        .verifyResult("1")
        .run("select id from newout")
        .verifyResult("2");

    ReplicationTestUtils.assertExternalFileList(Arrays
            .asList("a", "b", "c", "newin", "newout"), tuple.dumpLocation, primary);
  }

  @Test
  public void testDataCopyEndLogAtSource() throws Throwable {
    testDataCopyEndLog(false);
  }

  @Test
  public void testDataCopyEndLogAtTarget() throws Throwable {
    testDataCopyEndLog(true);
  }

  public void testDataCopyEndLog(boolean runCopyTasksOnTarget) throws Throwable {
    // Get the logger at the root level.
    Logger logger = LogManager.getLogger("hive.ql.metadata.Hive");
    Level oldLevel = logger.getLevel();
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();
    LoggerConfig loggerConfig = config.getLoggerConfig(logger.getName());
    loggerConfig.setLevel(Level.DEBUG);
    ctx.updateLoggers();
    // Create a String Appender to capture log output

    StringAppender appender = StringAppender.createStringAppender("%m");
    appender.addToLogger(logger.getName(), Level.DEBUG);
    appender.start();
    appender.reset();

    List<String> withClause = Arrays.asList("'distcp.options.update'=''",
        "'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='" + runCopyTasksOnTarget + "'");

    // Perform bootstrap dump & load.
    primary.run("use " + primaryDbName)
        .run("create external table a (i int)")
        .run("insert into a values (1),(2),(3),(4)")
        .run("create external table b (i int)")
        .run("insert into b values (5),(6),(7),(8)")
        .dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("show tables like 'a'")
        .verifyResults(Collections.singletonList("a"))
        .run("show tables like 'b'")
        .verifyResults(Collections.singletonList("b"));

    String logStr = appender.getOutput();
    // Check the log contains DATA_COPY_END after Distcp
    assertTrue(logStr.lastIndexOf("REPL::DATA_COPY_END:") > logStr.lastIndexOf("Completed DirCopyTask for source"));
    appender.reset();

    // Perform incremental dump & load.
    primary.run("use " + primaryDbName)
        .run("create table c (i int)")
        .run("insert into c values (10),(11)")
        .run("insert into a values (5),(6)")
        .run("insert into b values (9),(10)").dump(primaryDbName, withClause);

    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select i From c")
        .verifyResults(new String[] {"10", "11"});

    logStr = appender.getOutput();
    // Check the log contains DATA_COPY_END after Distcp
    assertTrue(logStr.indexOf("REPL::DATA_COPY_END:") > logStr.lastIndexOf("Completed DirCopyTask for source:"));
    loggerConfig.setLevel(oldLevel);
    ctx.updateLoggers();
    appender.removeFromLogger(logger.getName());
  }

  @Test
  public void testSingleCopyTasksAtSource() throws Throwable {
    testDataCopyEndLog(false);
  }

  @Test
  public void testSingleCopyTasksAtTarget() throws Throwable {
    testDataCopyEndLog(true);
  }

  public void testSingleCopyTasks(boolean runCopyTasksOnTarget)
      throws Throwable {
    // Create five tables, 2 inside each parent path and one inside the database location.
    Path parentPath1 = new Path("/" + testName.getMethodName() + "/" + "external1");
    Path parentPath2 = new Path("/" + testName.getMethodName() + "/" + "external1");
    Path parent1Table1 = new Path(parentPath1,"table1");
    Path parent1Table2 = new Path(parentPath1,"table2");
    Path parent2Table3 = new Path(parentPath2,"table3");
    Path parent2Table4 = new Path(parentPath2,"table4");

    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(parent1Table1, new FsPermission("777"));
    fs.mkdirs(parent1Table2, new FsPermission("777"));
    fs.mkdirs(parent2Table3, new FsPermission("777"));
    fs.mkdirs(parent2Table4, new FsPermission("777"));

    List<String> withClause = Arrays
        .asList("'distcp.options.update'=''", "'" + REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK.varname + "'='true'",
            "'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='" + runCopyTasksOnTarget + "'",
            "'" + REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK_PATHS.varname + "'='" + fs.makeQualified(parentPath1) + ","
                + fs.makeQualified(parentPath2) + "'");

    WarehouseInstance.Tuple tuple =
        primary.run("use " + primaryDbName)
            .run("create external table table1 (id int) row format delimited fields terminated by ',' location '"
                + parent1Table1.toUri() + "'")
            .run("create external table table2 (id int) row format delimited fields terminated by ',' location '"
                + parent1Table2.toUri() + "'")
            .run("create external table table3 (id int) row format delimited fields terminated by ',' location '"
                + parent2Table3.toUri() + "'")
            .run("create external table table4 (id int) row format delimited fields terminated by ',' location '"
                + parent2Table4.toUri() + "'")
            .run("create external table table5(id int) row format delimited fields terminated by ','")
            .run("insert into table1 values (1)")
            .run("insert into table2 values (2)")
            .run("insert into table3 values (3)")
            .run("insert into table4 values (4)")
            .run("insert into table4 values (5)")
            .dump(primaryDbName, withClause);

    Path primaryDb = new Path(primary.getDatabase(primaryDbName).getLocationUri());

    // Do a load and verify all the data is there.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select id from table1 ")
        .verifyResult("1")
        .run("select id from table2")
        .verifyResult("2")
        .run("select id from table3")
        .verifyResult("3")
        .run("select id from table4")
        .verifyResult("4")
        .run("select id from table5")
        .verifyResult("5");

    // Verify the tasks created for these tables, it should be one for the database, two for the two parent paths
    // that we configured.
    ReplicationTestUtils.assertExternalFileList(
        Arrays.asList(primaryDb.getName(), parentPath1.getName(), parentPath2.getName()), tuple.dumpLocation, primary);

    // Add more data to tables and do a incremental run and check if things stays same.
    tuple =
        primary.run("use " + primaryDbName)
            .run("insert into table1 values (4)")
            .run("insert into table2 values (3)")
            .run("insert into table3 values (2)")
            .run("insert into table4 values (1)")
            .run("insert into table5 values (6)")
            .dump(primaryDbName, withClause);

    // Do an incremental load and check if all the old and new data is there.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select id from table1 ")
        .verifyResults(new String[] {"1", "4"})
        .run("select id from table2 ")
        .verifyResults(new String[] {"2", "3"})
        .run("select id from table3 ")
        .verifyResults(new String[] {"3", "2"})
        .run("select id from table4")
        .verifyResults(new String[] {"4", "1"})
        .run("select id from table5")
        .verifyResults(new String[] {"5", "6"});;

    // The same tasks should be created.
    ReplicationTestUtils.assertExternalFileList(
        Arrays.asList(primaryDb.getName(), parentPath1.getName(), parentPath2.getName()), tuple.dumpLocation, primary);
  }

  @Test
  public void testSingleCopyTasksConfigurationAtSource() throws Throwable {
    testSingleCopyTasksConfiguration(false);
  }

  @Test
  public void testSingleCopyTasksConfigurationAtTarget() throws Throwable {
    testSingleCopyTasksConfiguration(true);
  }

  public void testSingleCopyTasksConfiguration(boolean runCopyTasksOnTarget)
      throws Throwable {
    // Create five tables, 1 inside the custom parent path, 1 inside the database location and one separate.
    Path parentPath1 = new Path("/" + testName.getMethodName() + "/" + "external1");
    Path parent1Table1 = new Path(parentPath1,"table1");

    Path externalTablePath = new Path("/" + testName.getMethodName() + "/" + "externaltable2");

    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
    fs.mkdirs(parent1Table1, new FsPermission("777"));
    fs.mkdirs(externalTablePath, new FsPermission("777"));


    try (FSDataOutputStream outputStream =
        fs.create(new Path(externalTablePath, "file1.txt"))) {
      outputStream.write("3\n".getBytes());
      outputStream.write("4\n".getBytes());
    }

    try (FSDataOutputStream outputStream =
        fs.create(new Path(parent1Table1, "file1.txt"))) {
      outputStream.write("3\n".getBytes());
      outputStream.write("4\n".getBytes());
    }

    // Create a filter file for DistCp
    String filterFilePath = "/tmp/filter";
    FileWriter myWriter = new FileWriter(filterFilePath);
    myWriter.write(".*file1.txt.*");
    myWriter.close();

    List<String> withClause = Arrays
        .asList("'distcp.options.update'=''", "'" + REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK.varname + "'='true'",
            "'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='" + runCopyTasksOnTarget + "'",
            "'" + REPL_EXTERNAL_WAREHOUSE_SINGLE_COPY_TASK_PATHS.varname + "'='" + fs.makeQualified(parentPath1) +
                "'", "'hive.dbpath.distcp.options.filters'='" + filterFilePath + "'");

    WarehouseInstance.Tuple tuple =
        primary.run("use " + primaryDbName)
            .run("create external table table1 (id int) row format delimited fields terminated by ',' location '"
                + parent1Table1.toUri() + "'")
            .run("create external table table2 (id int) row format delimited fields terminated by ',' location '"
                + externalTablePath.toUri() + "'")
            .run("create external table table3 (id int) row format delimited fields terminated by ','")
            .run("insert into table1 values (1)")
            .run("insert into table3 values (3)")
            .dump(primaryDbName, withClause);

    Path primaryDb = new Path(primary.getDatabase(primaryDbName).getLocationUri());

    // Do a load and verify all the data is there.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select id from table1 ")
        .verifyResult("1")
        .run("select id from table2")
        .verifyResults(new String[] {"3", "4"})
        .run("select id from table3")
        .verifyResult("3");

    // Verify the tasks created for these tables, it should be one for the database, two for the two parent paths
    // that we configured.
    ReplicationTestUtils.assertExternalFileList(
        Arrays.asList("dbPath:" + primaryDb.getName(), "dbPath:" + parentPath1.getName(), "table2"), tuple.dumpLocation,
        primary);

    // Verify the preserve config only worked for db level path.
    Path parent1Table1_target = new Path(REPLICA_EXTERNAL_BASE, parent1Table1.toUri().getPath().replaceFirst("/", ""));
    assertFalse(fs.exists(new Path(parent1Table1_target, "file1.txt")));

    // Verify the filter config gets used for the only db level paths.
    Path externalTablePath_target =
        new Path(REPLICA_EXTERNAL_BASE, externalTablePath.toUri().getPath().replaceFirst("/", ""));
    assertTrue(fs.exists(new Path(externalTablePath_target, "file1.txt")));

    // Add more data to tables and do a incremental run and check if things stays same.
    tuple =
        primary.run("use " + primaryDbName)
            .run("insert into table1 values (4)")
            .run("insert into table2 values (5)")
            .run("insert into table3 values (2)")
            .dump(primaryDbName, withClause);

    // Do an incremental load and check if all the old and new data is there.
    replica.load(replicatedDbName, primaryDbName, withClause)
        .run("use " + replicatedDbName)
        .run("select id from table1 ")
        .verifyResults(new String[] {"1", "4"})
        .run("select id from table2 ")
        .verifyResults(new String[] {"3", "4", "5"})
        .run("select id from table3 ")
        .verifyResults(new String[] {"3", "2"});

    // The same tasks should be created.
    ReplicationTestUtils.assertExternalFileList(
        Arrays.asList("dbPath:" + primaryDb.getName(), "dbPath:" + parentPath1.getName(), "table2"), tuple.dumpLocation,
        primary);

    // Verify the filter config gets used for the only db level paths.
    assertFalse(fs.exists(new Path(parent1Table1_target, "file1.txt")));
    assertTrue(fs.exists(new Path(externalTablePath_target, "file1.txt")));

    // Clean up the filter file.
    new File(filterFilePath).delete();
  }

  @Test
  public void testTableAndPartitionExportServiceWithParallelism() throws Throwable {
    List<String> extTableList = new ArrayList<String>();
    List<String> mgnTableList = new ArrayList<String>();
    List<String> dumpWithClause = ReplicationTestUtils.includeExternalTableClause(true);

    primary.run("use " + primaryDbName);

    // create 5 managed partitioned and 5 external un-partitioned tables.
    // create first 2 tables with  2 partitions, another 2 with 4 partitions and
    // another 2 tables with 6 partitions and so on.

    int pt = 0;
    for (int i = 0; i < 5; i++) {
      primary.run("CREATE EXTERNAL TABLE ptned" + i + " (a int)");
      extTableList.add("ptned" + i);
      primary.run("CREATE TABLE ptnmgned" + i + " (a int) partitioned by (b int)");
      mgnTableList.add("ptnmgned" + i);

      if (i % 2 == 0) {
        pt += 2;
      }
      for (int j = 0; j < pt; j++) {
        primary.run("ALTER TABLE ptnmgned" + i + " ADD PARTITION(b=" + j + ")");
        // insert some rows in each partitions of table
        for (int k = 0; k < pt; k++) {
          primary.run("INSERT INTO TABLE ptned" + i + " VALUES (" + k + ")");
          primary.run("INSERT INTO TABLE ptnmgned" + i + " PARTITION(b=" + j + ") VALUES (" + k + ")");
        }
      }
    }

    // create 5 un-partitioned table
    for (int i = 0; i < 5; i++) {
      primary.run("CREATE EXTERNAL TABLE unptned" + i + " (a int)");
      extTableList.add("unptned" + i);
      primary.run("CREATE TABLE unptnmgned" + i + " (a int)");
      mgnTableList.add("unptnmgned" + i);
      //insert some rows in each tables
      for (int j = 0; j < 2; j++) {
        primary.run("INSERT INTO TABLE unptned" + i + " VALUES (" + j + ")");
        primary.run("INSERT INTO TABLE unptnmgned" + i + " VALUES (" + j + ")");
      }
    }

    //start bootstrap dump
    WarehouseInstance.Tuple tuple = primary.dump(primaryDbName, dumpWithClause);
    // verify that the external table filelist is written correctly for bootstrap
    ReplicationTestUtils.assertExternalFileList(extTableList, tuple.dumpLocation, primary);

    List<String> newTableList = new ArrayList<String>();
    newTableList.addAll(extTableList);
    newTableList.addAll(mgnTableList);

    replica.load(replicatedDbName, primaryDbName, dumpWithClause)
        .run("use " + replicatedDbName)
        .run("show tables")
        .verifyResults(newTableList);

    primary.run("use " + primaryDbName);
    // create 5 un-partitioned table for incremental dump
    for (int i = 0; i < 5; i++) {
      primary.run("CREATE EXTERNAL TABLE incrunptned" + i + "(a int)");
      extTableList.add("incrunptned" + i);
      primary.run("CREATE TABLE incrunptnmgned" + i + "(a int)");
      mgnTableList.add("incrunptnmgned" + i);
      // insert some rows in each tables
      for (int j = 0; j < 2; j++) {
        primary.run("INSERT INTO TABLE incrunptned" + i + " VALUES (" + j + ")");
        primary.run("INSERT INTO TABLE incrunptnmgned" + i + " VALUES (" + j + ")");
      }
    }

    newTableList.clear();
    newTableList.addAll(extTableList);
    newTableList.addAll(mgnTableList);

    //start incremental dump
    WarehouseInstance.Tuple newTuple = primary.dump(primaryDbName, dumpWithClause);

    replica.load(replicatedDbName, primaryDbName, dumpWithClause)
        .run("use " + replicatedDbName)
        .run("show tables")
        .verifyResults(newTableList);

    // verify that the external table filelist is written correctly for incremental dump
    ReplicationTestUtils.assertExternalFileList(extTableList, newTuple.dumpLocation, primary);
  }

}
