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
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.CallerArguments;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static org.apache.hadoop.hive.ql.exec.repl.ReplExternalTables.FILE_NAME;
import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.INC_BOOTSTRAP_ROOT_DIR_NAME;
import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.REPL_CLEAN_TABLES_FROM_BOOTSTRAP_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestReplicationScenariosExternalTables extends BaseReplicationAcrossInstances {

  private static final String REPLICA_EXTERNAL_BASE = "/replica_external_base";

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    HashMap<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());
    overrides.put(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY.varname, "false");
    overrides.put(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname, "true");
    overrides.put(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname,
        UserGroupInformation.getCurrentUser().getUserName());

    internalBeforeClassSetup(overrides, TestReplicationScenarios.class);
  }

  @Test
  public void replicationWithoutExternalTables() throws Throwable {
    List<String> loadWithClause = externalTableBasePathWithClause();
    List<String> dumpWithClause = Collections.singletonList
        ("'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='false'");

    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create external table t1 (id int)")
        .run("insert into table t1 values (1)")
        .run("insert into table t1 values (2)")
        .run("create external table t2 (place string) partitioned by (country string)")
        .run("insert into table t2 partition(country='india') values ('bangalore')")
        .run("insert into table t2 partition(country='us') values ('austin')")
        .run("insert into table t2 partition(country='france') values ('paris')")
        .dump(primaryDbName, null, dumpWithClause);

    // the _external_tables_file info only should be created if external tables are to be replicated not otherwise
    assertFalse(primary.miniDFSCluster.getFileSystem()
        .exists(new Path(new Path(tuple.dumpLocation, primaryDbName.toLowerCase()), FILE_NAME)));

    replica.load(replicatedDbName, tuple.dumpLocation, loadWithClause)
        .run("repl status " + replicatedDbName)
        .verifyResult(tuple.lastReplicationId)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyFailure(new String[] { "t1" })
        .run("show tables like 't2'")
        .verifyFailure(new String[] { "t2" });

    tuple = primary.run("use " + primaryDbName)
        .run("create external table t3 (id int)")
        .run("insert into table t3 values (10)")
        .run("insert into table t3 values (20)")
        .dump(primaryDbName, tuple.lastReplicationId, dumpWithClause);

    // the _external_tables_file info only should be created if external tables are to be replicated not otherwise
    assertFalse(primary.miniDFSCluster.getFileSystem()
        .exists(new Path(tuple.dumpLocation, FILE_NAME)));

    replica.load(replicatedDbName, tuple.dumpLocation, loadWithClause)
        .run("use " + replicatedDbName)
        .run("show tables like 't3'")
        .verifyFailure(new String[] { "t3" });
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
        .dump("repl dump " + primaryDbName);

    // verify that the external table info is written correctly for bootstrap
    assertExternalFileInfo(Arrays.asList("t1", "t2"),
        new Path(new Path(tuple.dumpLocation, primaryDbName.toLowerCase()), FILE_NAME));

    List<String> withClauseOptions = externalTableBasePathWithClause();

    replica.load(replicatedDbName, tuple.dumpLocation, withClauseOptions)
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
        .verifyResult("france");

    // Ckpt should be set on bootstrapped db.
    replica.verifyIfCkptSet(replicatedDbName, tuple.dumpLocation);

    assertTablePartitionLocation(primaryDbName + ".t1", replicatedDbName + ".t1");
    assertTablePartitionLocation(primaryDbName + ".t2", replicatedDbName + ".t2");

    tuple = primary.run("use " + primaryDbName)
        .run("create external table t3 (id int)")
        .run("insert into table t3 values (10)")
        .run("create external table t4 as select id from t3")
        .dump("repl dump " + primaryDbName + " from " + tuple.lastReplicationId);

    // verify that the external table info is written correctly for incremental
    assertExternalFileInfo(Arrays.asList("t1", "t2", "t3", "t4"),
        new Path(tuple.dumpLocation, FILE_NAME));

    replica.load(replicatedDbName, tuple.dumpLocation, withClauseOptions)
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
        .dump("repl dump " + primaryDbName + " from " + tuple.lastReplicationId);

    // verify that the external table info is written correctly for incremental
    assertExternalFileInfo(Arrays.asList("t2", "t3", "t4"),
        new Path(tuple.dumpLocation, FILE_NAME));
  }

  /**
   * @param sourceTableName  -- Provide the fully qualified table name
   * @param replicaTableName -- Provide the fully qualified table name
   */
  private void assertTablePartitionLocation(String sourceTableName, String replicaTableName)
      throws HiveException {
    Hive hiveForPrimary = Hive.get(primary.hiveConf);
    Table sourceTable = hiveForPrimary.getTable(sourceTableName);
    Path sourceLocation = sourceTable.getDataLocation();
    Hive hiveForReplica = Hive.get(replica.hiveConf);
    Table replicaTable = hiveForReplica.getTable(replicaTableName);
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
    externalTableBasePathWithClause();
    List<String> loadWithClause = Arrays.asList(
            "'" + HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname + "'='"
                    + REPLICA_EXTERNAL_BASE + "'",
            "'distcp.options.update'=''"
    );

    WarehouseInstance.Tuple bootstrapTuple = primary.run("use " + primaryDbName)
        .run("create external table a (i int, j int) "
            + "row format delimited fields terminated by ',' "
            + "location '" + externalTableLocation.toUri() + "'")
        .dump(primaryDbName, null);

    replica.load(replicatedDbName, bootstrapTuple.dumpLocation, loadWithClause)
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
        .dump(primaryDbName, bootstrapTuple.lastReplicationId);

    replica.load(replicatedDbName, incrementalTuple.dumpLocation, loadWithClause)
        .run("select i From a")
        .verifyResults(new String[] { "1", "13" })
        .run("select j from a")
        .verifyResults(new String[] { "2", "21" });

    // alter table location to something new.
    externalTableLocation =
        new Path("/" + testName.getMethodName() + "/" + primaryDbName + "/new_location/a/");
    incrementalTuple = primary.run("use " + primaryDbName)
        .run("alter table a set location '" + externalTableLocation + "'")
        .dump(primaryDbName, incrementalTuple.lastReplicationId);

    replica.load(replicatedDbName, incrementalTuple.dumpLocation, loadWithClause)
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

    List<String> loadWithClause = externalTableBasePathWithClause();

    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create external table t2 (place string) partitioned by (country string) row format "
            + "delimited fields terminated by ',' location '" + externalTableLocation.toString()
            + "'")
        .run("insert into t2 partition(country='india') values ('bangalore')")
        .dump("repl dump " + primaryDbName);

    assertExternalFileInfo(Collections.singletonList("t2"),
        new Path(new Path(tuple.dumpLocation, primaryDbName.toLowerCase()), FILE_NAME));

    replica.load(replicatedDbName, tuple.dumpLocation, loadWithClause)
        .run("use " + replicatedDbName)
        .run("show tables like 't2'")
        .verifyResults(new String[] { "t2" })
        .run("select place from t2")
        .verifyResults(new String[] { "bangalore" });

    assertTablePartitionLocation(primaryDbName + ".t2", replicatedDbName + ".t2");

    // add new  data externally, to a partition, but under the table level top directory
    Path partitionDir = new Path(externalTableLocation, "country=india");
    try (FSDataOutputStream outputStream = fs.create(new Path(partitionDir, "file.txt"))) {
      outputStream.write("pune\n".getBytes());
      outputStream.write("mumbai\n".getBytes());
    }

    tuple = primary.run("use " + primaryDbName)
        .run("insert into t2 partition(country='australia') values ('sydney')")
        .dump(primaryDbName, tuple.lastReplicationId);

    assertExternalFileInfo(Collections.singletonList("t2"),
        new Path(tuple.dumpLocation, FILE_NAME));

    replica.load(replicatedDbName, tuple.dumpLocation, loadWithClause)
        .run("use " + replicatedDbName)
        .run("select distinct(country) from t2")
        .verifyResults(new String[] { "india", "australia" })
        .run("select place from t2 where country='india'")
        .verifyResults(new String[] { "bangalore", "pune", "mumbai" })
        .run("select place from t2 where country='australia'")
        .verifyResults(new String[] { "sydney" });

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
        .dump(primaryDbName, tuple.lastReplicationId);

    replica.load(replicatedDbName, tuple.dumpLocation, loadWithClause)
        .run("use " + replicatedDbName)
        .run("select place from t2 where country='france'")
        .verifyResults(new String[] { "paris" });

    // change the location of the partition via alter command
    String tmpLocation = "/tmp/" + System.nanoTime();
    primary.miniDFSCluster.getFileSystem().mkdirs(new Path(tmpLocation), new FsPermission("777"));

    tuple = primary.run("use " + primaryDbName)
        .run("alter table t2 partition (country='france') set location '" + tmpLocation + "'")
        .dump(primaryDbName, tuple.lastReplicationId);

    replica.load(replicatedDbName, tuple.dumpLocation, loadWithClause)
        .run("use " + replicatedDbName)
        .run("select place from t2 where country='france'")
        .verifyResults(new String[] {});
  }

  @Test
  public void externalTableIncrementalReplication() throws Throwable {
    WarehouseInstance.Tuple tuple = primary.dump("repl dump " + primaryDbName);
    replica.load(replicatedDbName, tuple.dumpLocation);

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
        .dump(primaryDbName, tuple.lastReplicationId);

    assertExternalFileInfo(Collections.singletonList("t1"), new Path(tuple.dumpLocation, FILE_NAME));

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

    List<String> loadWithClause = externalTableBasePathWithClause();
    replica.load(replicatedDbName, tuple.dumpLocation, loadWithClause)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("show partitions t1")
        .verifyResults(new String[] { "country=india", "country=us" })
        .run("select place from t1 order by place")
        .verifyResults(new String[] { "bangalore", "mumbai", "pune" });

    // Delete one of the file and update another one.
    fs.delete(new Path(partitionDir, "file.txt"), true);
    fs.delete(new Path(partitionDir, "file1.txt"), true);
    try (FSDataOutputStream outputStream = fs.create(new Path(partitionDir, "file1.txt"))) {
      outputStream.write("chennai\n".getBytes());
    }

    // Repl load with zero events but external tables location info should present.
    tuple = primary.dump(primaryDbName, tuple.lastReplicationId);
    assertExternalFileInfo(Collections.singletonList("t1"), new Path(tuple.dumpLocation, FILE_NAME));

    replica.load(replicatedDbName, tuple.dumpLocation, loadWithClause)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show partitions t1")
            .verifyResults(new String[] { "country=india", "country=us" })
            .run("select place from t1 order by place")
            .verifyResults(new String[] { "chennai" });

    Hive hive = Hive.get(replica.getConf());
    Set<Partition> partitions =
        hive.getAllPartitionsOf(hive.getTable(replicatedDbName + ".t1"));
    List<String> paths = partitions.stream().map(p -> p.getDataLocation().toUri().getPath())
        .collect(Collectors.toList());

    tuple = primary
        .run("alter table t1 drop partition (country='india')")
        .run("alter table t1 drop partition (country='us')")
        .dump(primaryDbName, tuple.lastReplicationId);

    replica.load(replicatedDbName, tuple.dumpLocation)
        .run("select * From t1")
        .verifyResults(new String[] {});

    for (String path : paths) {
      assertTrue(replica.miniDFSCluster.getFileSystem().exists(new Path(path)));
    }
  }

  @Test
  public void bootstrapExternalTablesDuringIncrementalPhase() throws Throwable {
    List<String> loadWithClause = externalTableBasePathWithClause();
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
            .dump(primaryDbName, null, dumpWithClause);

    // the _external_tables_file info only should be created if external tables are to be replicated not otherwise
    assertFalse(primary.miniDFSCluster.getFileSystem()
            .exists(new Path(new Path(tuple.dumpLocation, primaryDbName.toLowerCase()), FILE_NAME)));

    replica.load(replicatedDbName, tuple.dumpLocation, loadWithClause)
            .status(replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyFailure(new String[] {"t1" })
            .run("show tables like 't2'")
            .verifyFailure(new String[] {"t2" });

    dumpWithClause = Arrays.asList("'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='true'",
                                   "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_EXTERNAL_TABLES.varname + "'='true'");
    tuple = primary.run("use " + primaryDbName)
            .run("drop table t1")
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (10)")
            .run("insert into table t3 values (20)")
            .run("create table t4 as select * from t3")
            .dump(primaryDbName, tuple.lastReplicationId, dumpWithClause);

    // the _external_tables_file info should be created as external tables are to be replicated.
    assertTrue(primary.miniDFSCluster.getFileSystem()
            .exists(new Path(tuple.dumpLocation, FILE_NAME)));

    // verify that the external table info is written correctly for incremental
    assertExternalFileInfo(Arrays.asList("t2", "t3"),
            new Path(tuple.dumpLocation, FILE_NAME));

    // _bootstrap directory should be created as bootstrap enabled on external tables.
    Path dumpPath = new Path(tuple.dumpLocation, INC_BOOTSTRAP_ROOT_DIR_NAME);
    assertTrue(primary.miniDFSCluster.getFileSystem().exists(dumpPath));

    // _bootstrap/<db_name>/t2
    // _bootstrap/<db_name>/t3
    Path dbPath = new Path(dumpPath, primaryDbName);
    Path tblPath = new Path(dbPath, "t2");
    assertTrue(primary.miniDFSCluster.getFileSystem().exists(tblPath));
    tblPath = new Path(dbPath, "t3");
    assertTrue(primary.miniDFSCluster.getFileSystem().exists(tblPath));

    replica.load(replicatedDbName, tuple.dumpLocation, loadWithClause)
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
            .verifyResult("t4");

    // Ckpt should be set on bootstrapped tables.
    replica.verifyIfCkptSetForTables(replicatedDbName, Arrays.asList("t2", "t3"), tuple.dumpLocation);

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
            .verifyResults(Arrays.asList("10", "20"));
  }

  @Test
  public void retryBootstrapExternalTablesFromDifferentDump() throws Throwable {
    List<String> loadWithClause = new ArrayList<>();
    loadWithClause.addAll(externalTableBasePathWithClause());

    List<String> dumpWithClause = Collections.singletonList(
            "'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='false'"
    );

    WarehouseInstance.Tuple tupleBootstrapWithoutExternal = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (1)")
            .run("create external table t2 (place string) partitioned by (country string)")
            .run("insert into table t2 partition(country='india') values ('bangalore')")
            .run("insert into table t2 partition(country='us') values ('austin')")
            .run("create table t3 as select * from t1")
            .dump(primaryDbName, null, dumpWithClause);

    replica.load(replicatedDbName, tupleBootstrapWithoutExternal.dumpLocation, loadWithClause)
            .status(replicatedDbName)
            .verifyResult(tupleBootstrapWithoutExternal.lastReplicationId)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResult("t3")
            .run("select id from t3")
            .verifyResult("1");

    dumpWithClause = Arrays.asList("'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='true'",
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_EXTERNAL_TABLES.varname + "'='true'");
    WarehouseInstance.Tuple tupleIncWithExternalBootstrap = primary.run("use " + primaryDbName)
            .run("drop table t1")
            .run("create external table t4 (id int)")
            .run("insert into table t4 values (10)")
            .run("create table t5 as select * from t4")
            .dump(primaryDbName, tupleBootstrapWithoutExternal.lastReplicationId, dumpWithClause);

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
      replica.loadFailure(replicatedDbName, tupleIncWithExternalBootstrap.dumpLocation, loadWithClause);
      callerVerifier.assertInjectionsPerformed(true, false);
    } finally {
      InjectableBehaviourObjectStore.resetAlterTableModifier();
    }

    // Insert into existing external table and then Drop it, add another managed table with same name
    // and dump another bootstrap dump for external tables.
    WarehouseInstance.Tuple tupleNewIncWithExternalBootstrap = primary.run("use " + primaryDbName)
            .run("insert into table t2 partition(country='india') values ('chennai')")
            .run("drop table t2")
            .run("create table t2 as select * from t4")
            .run("insert into table t4 values (20)")
            .dump(primaryDbName, tupleIncWithExternalBootstrap.lastReplicationId, dumpWithClause);

    // Set incorrect bootstrap dump to clean tables. Here, used the full bootstrap dump which is invalid.
    // So, REPL LOAD fails.
    loadWithClause.add("'" + REPL_CLEAN_TABLES_FROM_BOOTSTRAP_CONFIG + "'='"
            + tupleBootstrapWithoutExternal.dumpLocation + "'");
    replica.loadFailure(replicatedDbName, tupleNewIncWithExternalBootstrap.dumpLocation, loadWithClause);
    loadWithClause.remove("'" + REPL_CLEAN_TABLES_FROM_BOOTSTRAP_CONFIG + "'='"
            + tupleBootstrapWithoutExternal.dumpLocation + "'");

    // Set previously failed bootstrap dump to clean-up. Now, new bootstrap should overwrite the old one.
    loadWithClause.add("'" + REPL_CLEAN_TABLES_FROM_BOOTSTRAP_CONFIG + "'='"
            + tupleIncWithExternalBootstrap.dumpLocation + "'");

    // Verify if bootstrapping with same dump is idempotent and return same result
    for (int i = 0; i < 2; i++) {
      replica.load(replicatedDbName, tupleNewIncWithExternalBootstrap.dumpLocation, loadWithClause)
              .run("use " + replicatedDbName)
              .run("show tables like 't1'")
              .verifyFailure(new String[]{"t1"})
              .run("select id from t2")
              .verifyResult("10")
              .run("select id from t4")
              .verifyResults(Arrays.asList("10", "20"))
              .run("select id from t5")
              .verifyResult("10");

      // Once the REPL LOAD is successful, the this config should be unset or else, the subsequent REPL LOAD
      // will also drop those tables which will cause data loss.
      loadWithClause.remove("'" + REPL_CLEAN_TABLES_FROM_BOOTSTRAP_CONFIG + "'='"
              + tupleIncWithExternalBootstrap.dumpLocation + "'");
    }
  }

  @Test
  public void retryIncBootstrapExternalTablesFromDifferentDumpWithoutCleanTablesConfig() throws Throwable {
    List<String> dumpWithClause = Collections.singletonList(
            "'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='false'"
    );
    List<String> loadWithClause = externalTableBasePathWithClause();

    WarehouseInstance.Tuple tupleBootstrapWithoutExternal = primary
            .dump(primaryDbName, null, dumpWithClause);

    replica.load(replicatedDbName, tupleBootstrapWithoutExternal.dumpLocation, loadWithClause);

    dumpWithClause = Arrays.asList("'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='true'",
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_EXTERNAL_TABLES.varname + "'='true'");
    WarehouseInstance.Tuple tupleIncWithExternalBootstrap = primary.run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (1)")
            .run("create table t2 as select * from t1")
            .dump(primaryDbName, tupleBootstrapWithoutExternal.lastReplicationId, dumpWithClause);
    WarehouseInstance.Tuple tupleNewIncWithExternalBootstrap
            = primary.dump(primaryDbName, tupleBootstrapWithoutExternal.lastReplicationId, dumpWithClause);

    replica.load(replicatedDbName, tupleIncWithExternalBootstrap.dumpLocation, loadWithClause);

    // Re-bootstrapping from different bootstrap dump without clean tables config should fail.
    replica.loadFailure(replicatedDbName, tupleNewIncWithExternalBootstrap.dumpLocation, loadWithClause,
            ErrorMsg.REPL_BOOTSTRAP_LOAD_PATH_NOT_VALID.getErrorCode());
  }

  @Test
  public void dynamicallyConvertManagedToExternalTable() throws Throwable {
    List<String> dumpWithClause = Collections.singletonList(
            "'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='true'"
    );
    List<String> loadWithClause = externalTableBasePathWithClause();

    WarehouseInstance.Tuple tupleBootstrapManagedTable = primary.run("use " + primaryDbName)
            .run("create table t1 (id int)")
            .run("insert into table t1 values (1)")
            .run("create table t2 (id int) partitioned by (key int)")
            .run("insert into table t2 partition(key=10) values (1)")
            .dump(primaryDbName, null, dumpWithClause);

    replica.load(replicatedDbName, tupleBootstrapManagedTable.dumpLocation, loadWithClause);

    Hive hiveForReplica = Hive.get(replica.hiveConf);
    Table replicaTable = hiveForReplica.getTable(replicatedDbName + ".t1");
    Path oldTblLocT1 = replicaTable.getDataLocation();

    replicaTable = hiveForReplica.getTable(replicatedDbName + ".t2");
    Path oldTblLocT2 = replicaTable.getDataLocation();

    WarehouseInstance.Tuple tupleIncConvertToExternalTbl = primary.run("use " + primaryDbName)
            .run("alter table t1 set tblproperties('EXTERNAL'='true')")
            .run("alter table t2 set tblproperties('EXTERNAL'='true')")
            .dump(primaryDbName, tupleBootstrapManagedTable.lastReplicationId, dumpWithClause);

    assertExternalFileInfo(Arrays.asList("t1", "t2"),
            new Path(tupleIncConvertToExternalTbl.dumpLocation, FILE_NAME));
    replica.load(replicatedDbName, tupleIncConvertToExternalTbl.dumpLocation, loadWithClause)
            .run("use " + replicatedDbName)
            .run("select id from t1")
            .verifyResult("1")
            .run("select id from t2 where key=10")
            .verifyResult("1");

    // Check if the table type is set correctly in target.
    replicaTable = hiveForReplica.getTable(replicatedDbName + ".t1");
    assertTrue(TableType.EXTERNAL_TABLE.equals(replicaTable.getTableType()));

    replicaTable = hiveForReplica.getTable(replicatedDbName + ".t2");
    assertTrue(TableType.EXTERNAL_TABLE.equals(replicaTable.getTableType()));

    // Verify if new table location is set inside the base directory.
    assertTablePartitionLocation(primaryDbName + ".t1", replicatedDbName + ".t1");
    assertTablePartitionLocation(primaryDbName + ".t2", replicatedDbName + ".t2");

    // Old location should be removed and set to new location.
    assertFalse(replica.miniDFSCluster.getFileSystem().exists(oldTblLocT1));
    assertFalse(replica.miniDFSCluster.getFileSystem().exists(oldTblLocT2));
  }

  private List<String> externalTableBasePathWithClause() throws IOException, SemanticException {
    Path externalTableLocation = new Path(REPLICA_EXTERNAL_BASE);
    DistributedFileSystem fileSystem = replica.miniDFSCluster.getFileSystem();
    externalTableLocation = PathBuilder.fullyQualifiedHDFSUri(externalTableLocation, fileSystem);
    fileSystem.mkdirs(externalTableLocation);

    // this is required since the same filesystem is used in both source and target
    return Arrays.asList(
            "'" + HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname + "'='"
                    + externalTableLocation.toString() + "'",
            "'distcp.options.pugpb'=''"
    );
  }

  private void assertExternalFileInfo(List<String> expected, Path externalTableInfoFile)
      throws IOException {
    DistributedFileSystem fileSystem = primary.miniDFSCluster.getFileSystem();
    assertTrue(fileSystem.exists(externalTableInfoFile));
    InputStream inputStream = fileSystem.open(externalTableInfoFile);
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    Set<String> tableNames = new HashSet<>();
    for (String line = reader.readLine(); line != null; line = reader.readLine()) {
      String[] components = line.split(",");
      assertEquals("The file should have tableName,base64encoded(data_location)",
          2, components.length);
      tableNames.add(components[0]);
      assertTrue(components[1].length() > 0);
    }
    assertTrue(tableNames.containsAll(expected));
    reader.close();
  }
}
