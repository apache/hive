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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.CallerArguments;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.BehaviourInjection;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import static org.apache.hadoop.hive.metastore.ReplChangeManager.SOURCE_OF_REPLICATION;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.junit.rules.TestName;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isFullAcidTable;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isTransactionalTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * TestReplicationWithTableMigration - test replication for Hive2 to Hive3 (Strict managed tables)
 */
public class TestReplicationWithTableMigration {
  private final static String AVRO_SCHEMA_FILE_NAME = "avro_table.avsc";

  @Rule
  public final TestName testName = new TestName();

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationWithTableMigration.class);
  private static WarehouseInstance primary, replica;
  private String primaryDbName, replicatedDbName;
  private Path avroSchemaFile = null;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    HashMap<String, String> overrideProperties = new HashMap<>();
    overrideProperties.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());
    internalBeforeClassSetup(overrideProperties);
  }

  static void internalBeforeClassSetup(Map<String, String> overrideConfigs) throws Exception {
    HiveConf conf = new HiveConf(TestReplicationWithTableMigration.class);
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    MiniDFSCluster miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    final DistributedFileSystem fs = miniDFSCluster.getFileSystem();
    HashMap<String, String> hiveConfigs = new HashMap<String, String>() {{
      put("fs.defaultFS", fs.getUri().toString());
      put("hive.support.concurrency", "true");
      put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
      put("hive.metastore.client.capability.check", "false");
      put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
      put("hive.exec.dynamic.partition.mode", "nonstrict");
      put("hive.strict.checks.bucketing", "false");
      put("hive.mapred.mode", "nonstrict");
      put("mapred.input.dir.recursive", "true");
      put("hive.metastore.disallow.incompatible.col.type.changes", "false");
      put("hive.strict.managed.tables", "true");
    }};
    replica = new WarehouseInstance(LOG, miniDFSCluster, hiveConfigs);

    HashMap<String, String> configsForPrimary = new HashMap<String, String>() {{
      put("fs.defaultFS", fs.getUri().toString());
      put("hive.metastore.client.capability.check", "false");
      put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
      put("hive.exec.dynamic.partition.mode", "nonstrict");
      put("hive.strict.checks.bucketing", "false");
      put("hive.mapred.mode", "nonstrict");
      put("mapred.input.dir.recursive", "true");
      put("hive.metastore.disallow.incompatible.col.type.changes", "false");
      put("hive.support.concurrency", "false");
      put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
      put("hive.strict.managed.tables", "false");
      put("hive.stats.autogather", "true");
      put("hive.stats.column.autogather", "true");
    }};
    configsForPrimary.putAll(overrideConfigs);
    primary = new WarehouseInstance(LOG, miniDFSCluster, configsForPrimary);
  }

  private static Path createAvroSchemaFile(FileSystem fs, Path testPath) throws IOException {
    Path schemaFile = new Path(testPath, AVRO_SCHEMA_FILE_NAME);
    String[] schemaVals = new String[] { "{",
        "  \"type\" : \"record\",",
        "  \"name\" : \"table1\",",
        "  \"doc\" : \"Sqoop import of table1\",",
        "  \"fields\" : [ {",
        "    \"name\" : \"col1\",",
        "    \"type\" : [ \"null\", \"string\" ],",
        "    \"default\" : null,",
        "    \"columnName\" : \"col1\",",
        "    \"sqlType\" : \"12\"",
        "  }, {",
        "    \"name\" : \"col2\",",
        "    \"type\" : [ \"null\", \"long\" ],",
        "    \"default\" : null,",
        "    \"columnName\" : \"col2\",",
        "    \"sqlType\" : \"13\"",
        "  } ],",
        "  \"tableName\" : \"table1\"",
        "}"
    };

    try (FSDataOutputStream stream = fs.create(schemaFile)) {
      for (String line : schemaVals) {
        stream.write((line + "\n").getBytes());
      }
    }
    fs.deleteOnExit(schemaFile);
    return schemaFile;
  }

  @AfterClass
  public static void classLevelTearDown() throws IOException {
    primary.close();
    replica.close();
  }

  @Before
  public void setup() throws Throwable {
    primaryDbName = testName.getMethodName() + "_" + +System.currentTimeMillis();
    replicatedDbName = "replicated_" + primaryDbName;
    primary.run("create database " + primaryDbName + " WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')");
    if (avroSchemaFile == null) {
      Path testPath = new Path("/tmp/avro_schema/definition/" + System.nanoTime());
      DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();
      fs.mkdirs(testPath, new FsPermission("777"));
      avroSchemaFile = PathBuilder.fullyQualifiedHDFSUri(createAvroSchemaFile(fs, testPath), fs);
    }
  }

  @After
  public void tearDown() throws Throwable {
    primary.run("drop database if exists " + primaryDbName + " cascade");
    replica.run("drop database if exists " + replicatedDbName + " cascade");
  }

  private WarehouseInstance.Tuple prepareDataAndDump(String primaryDbName, String fromReplId) throws Throwable {
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
        .run("create table tacid (id int) clustered by(id) into 3 buckets stored as orc ")
        .run("insert into tacid values(1)")
        .run("insert into tacid values(2)")
        .run("insert into tacid values(3)")
        .run(
            "create table tacidpart (place string) partitioned by (country string) clustered by(place) "
                +
                "into 3 buckets stored as orc ")
        .run("alter table tacidpart add partition(country='france')")
        .run("insert into tacidpart partition(country='india') values('mumbai')")
        .run("insert into tacidpart partition(country='us') values('sf')")
        .run("insert into tacidpart partition(country='france') values('paris')")
        .run(
            "create table tflat (rank int) stored as orc tblproperties(\"transactional\"=\"false\")")
        .run("insert into tflat values(11)")
        .run("insert into tflat values(22)")
        .run("create table tflattext (id int) ")
        .run("insert into tflattext values(111), (222)")
        .run("create table tflattextpart (id int) partitioned by (country string) ")
        .run("insert into tflattextpart partition(country='india') values(1111), (2222)")
        .run("insert into tflattextpart partition(country='us') values(3333)")
        .run(
            "create table tacidloc (id int) clustered by(id) into 3 buckets stored as orc  LOCATION '/tmp' ")
        .run("insert into tacidloc values(1)")
        .run("insert into tacidloc values(2)")
        .run("insert into tacidloc values(3)")
        .run(
            "create table tacidpartloc (place string) partitioned by (country string) clustered by(place) "
                +
                "into 3 buckets stored as orc ")
        .run("alter table tacidpartloc add partition(country='france') LOCATION '/tmp/part'")
        .run("insert into tacidpartloc partition(country='india') values('mumbai')")
        .run("insert into tacidpartloc partition(country='us') values('sf')")
        .run("insert into tacidpartloc partition(country='france') values('paris')")
        .run(
            "create table avro_table ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' "
                + "stored as avro tblproperties ('avro.schema.url'='" + avroSchemaFile.toUri()
                .toString() + "')")
        .run("insert into avro_table values ('str1', 10)")
        .run(
            "create table avro_table_part partitioned by (country string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' "
                + "stored as avro tblproperties ('avro.schema.url'='" + avroSchemaFile.toUri()
                .toString() + "')")
        .run("insert into avro_table_part partition (country='india') values ('another', 13)")
        .dump(primaryDbName, fromReplId);
    assertFalse(isTransactionalTable(primary.getTable(primaryDbName, "tacid")));
    assertFalse(isTransactionalTable(primary.getTable(primaryDbName, "tacidpart")));
    assertFalse(isTransactionalTable(primary.getTable(primaryDbName, "tflat")));
    assertFalse(isTransactionalTable(primary.getTable(primaryDbName, "tflattext")));
    assertFalse(isTransactionalTable(primary.getTable(primaryDbName, "tflattextpart")));
    assertFalse(isTransactionalTable(primary.getTable(primaryDbName, "tacidloc")));
    assertFalse(isTransactionalTable(primary.getTable(primaryDbName, "tacidpartloc")));
    assertAvroTableState(primaryDbName, "avro_table", "avro_table_part");
    assertAvroTableState(primaryDbName,  "avro_table_part");
    return tuple;
  }

  private void assertAvroTableState(String primaryDbName, String... tableNames) throws Exception {
    for (String tableName : tableNames) {
      Table avroTable = primary.getTable(primaryDbName, tableName);
      assertFalse(isTransactionalTable(avroTable));
      assertFalse(MetaStoreUtils.isExternalTable(avroTable));
    }
  }

  private void verifyLoadExecution(String replicatedDbName, String lastReplId) throws Throwable {
    replica.run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"tacid", "tacidpart", "tflat", "tflattext", "tflattextpart",
                "tacidloc", "tacidpartloc", "avro_table", "avro_table_part" })
            .run("repl status " + replicatedDbName)
            .verifyResult(lastReplId)
            .run("select id from tacid order by id")
            .verifyResults(new String[]{"1", "2", "3"})
            .run("select country from tacidpart order by country")
            .verifyResults(new String[] {"france", "india", "us"})
            .run("select rank from tflat order by rank")
            .verifyResults(new String[] {"11", "22"})
            .run("select id from tflattext order by id")
            .verifyResults(new String[] {"111", "222"})
            .run("select id from tflattextpart order by id")
            .verifyResults(new String[] {"1111", "2222", "3333"})
            .run("select id from tacidloc order by id")
            .verifyResults(new String[]{"1", "2", "3"})
            .run("select country from tacidpartloc order by country")
            .verifyResults(new String[] {"france", "india", "us"})
            .run("select col1 from avro_table")
            .verifyResults(new String[] { "str1" })
            .run("select col1 from avro_table_part")
            .verifyResults(new String[] { "another" });

    assertTrue(isFullAcidTable(replica.getTable(replicatedDbName, "tacid")));
    assertTrue(isFullAcidTable(replica.getTable(replicatedDbName, "tacidpart")));
    assertTrue(isFullAcidTable(replica.getTable(replicatedDbName, "tflat")));
    assertTrue(!isFullAcidTable(replica.getTable(replicatedDbName, "tflattext")));
    assertTrue(!isFullAcidTable(replica.getTable(replicatedDbName, "tflattextpart")));
    assertTrue(isTransactionalTable(replica.getTable(replicatedDbName, "tflattext")));
    assertTrue(isTransactionalTable(replica.getTable(replicatedDbName, "tflattextpart")));
    assertTrue(isFullAcidTable(replica.getTable(replicatedDbName, "tacidloc")));
    assertTrue(isFullAcidTable(replica.getTable(replicatedDbName, "tacidpartloc")));
    assertTablePath(replicatedDbName, "avro_table");
    assertPartitionPath(replicatedDbName, "avro_table_part");
  }

  private void assertPartitionPath(String replicatedDbName, String tableName) throws Exception {
    Path tablePath = assertTablePath(replicatedDbName, tableName);
    List<Partition> partitions = replica.getAllPartitions(replicatedDbName, tableName);
    assertEquals(1, partitions.size());
    String actualPartitionPath = partitions.iterator().next().getSd().getLocation().toLowerCase();
    String expectedPartitionPath = new PathBuilder(tablePath.toString())
        .addDescendant("country=india").build().toUri().toString().toLowerCase();
    assertEquals(expectedPartitionPath, actualPartitionPath);
  }

  private Path assertTablePath(String replicatedDbName, String tableName) throws Exception {
    Table avroTable = replica.getTable(replicatedDbName, tableName);
    assertTrue(MetaStoreUtils.isExternalTable(avroTable));
    Path tablePath = new PathBuilder(replica.externalTableWarehouseRoot.toString())
        .addDescendant(replicatedDbName + ".db").addDescendant(tableName).build();
    String expectedTablePath = tablePath.toUri().toString().toLowerCase();
    String actualTablePath = avroTable.getSd().getLocation().toLowerCase();
    assertEquals(expectedTablePath, actualTablePath);
    return tablePath;
  }

  private void loadWithFailureInAddNotification(String tbl, String dumpLocation) throws Throwable {
    BehaviourInjection<CallerArguments, Boolean> callerVerifier
            = new BehaviourInjection<CallerArguments, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable InjectableBehaviourObjectStore.CallerArguments args) {
        injectionPathCalled = true;
        if (!args.dbName.equalsIgnoreCase(replicatedDbName) || (args.constraintTblName != null)) {
          LOG.warn("Verifier - DB: " + args.dbName
                  + " Constraint Table: " + args.constraintTblName);
          return false;
        }
        if (args.tblName != null) {
          LOG.warn("Verifier - Table: " + args.tblName);
          return args.tblName.equalsIgnoreCase(tbl);
        }
        return true;
      }
    };
    InjectableBehaviourObjectStore.setCallerVerifier(callerVerifier);
    try {
      replica.loadFailure(replicatedDbName, dumpLocation);
    } finally {
      InjectableBehaviourObjectStore.resetCallerVerifier();
    }
    callerVerifier.assertInjectionsPerformed(true, false);
  }

  @Test
  public void testBootstrapLoadMigrationManagedToAcid() throws Throwable {
    WarehouseInstance.Tuple tuple = prepareDataAndDump(primaryDbName, null);
    replica.load(replicatedDbName, tuple.dumpLocation);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);
  }

  @Test
  public void testIncrementalLoadMigrationManagedToAcid() throws Throwable {
    WarehouseInstance.Tuple tuple = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, tuple.dumpLocation);
    tuple = prepareDataAndDump(primaryDbName, tuple.lastReplicationId);
    replica.load(replicatedDbName, tuple.dumpLocation);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);
  }

  @Test
  public void testIncrementalLoadMigrationManagedToAcidFailure() throws Throwable {
    WarehouseInstance.Tuple tuple = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, tuple.dumpLocation);
    tuple = prepareDataAndDump(primaryDbName, tuple.lastReplicationId);
    loadWithFailureInAddNotification("tacid", tuple.dumpLocation);
    replica.run("use " + replicatedDbName)
            .run("show tables like tacid")
            .verifyResult(null);
    replica.load(replicatedDbName, tuple.dumpLocation);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);
  }

  @Test
  public void testIncrementalLoadMigrationManagedToAcidFailurePart() throws Throwable {
    WarehouseInstance.Tuple tuple = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, tuple.dumpLocation);
    tuple = prepareDataAndDump(primaryDbName, tuple.lastReplicationId);
    loadWithFailureInAddNotification("tacidpart", tuple.dumpLocation);
    replica.run("use " + replicatedDbName)
            .run("show tables like tacidpart")
            .verifyResult(null);
    replica.load(replicatedDbName, tuple.dumpLocation);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);
  }

  @Test
  public void testIncrementalLoadMigrationManagedToAcidAllOp() throws Throwable {
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);
    List<String> selectStmtList = new ArrayList<>();
    List<String[]> expectedValues = new ArrayList<>();
    String tableName = testName.getMethodName() + "testInsert";
    String tableNameMM = tableName + "_MM";

    ReplicationTestUtils.appendInsert(primary, primaryDbName, null,
            tableName, tableNameMM, selectStmtList, expectedValues);
    ReplicationTestUtils.appendTruncate(primary, primaryDbName,
            null, selectStmtList, expectedValues);
    ReplicationTestUtils.appendInsertIntoFromSelect(primary, primaryDbName,
            null, tableName, tableNameMM, selectStmtList, expectedValues);
    ReplicationTestUtils.appendCreateAsSelect(primary, primaryDbName,
            null, tableName, tableNameMM, selectStmtList, expectedValues);
    ReplicationTestUtils.appendImport(primary, primaryDbName,
            null, tableName, tableNameMM, selectStmtList, expectedValues);
    ReplicationTestUtils.appendInsertOverwrite(primary, primaryDbName,
            null, tableName, tableNameMM, selectStmtList, expectedValues);
    ReplicationTestUtils.appendLoadLocal(primary, primaryDbName,
            null, tableName, tableNameMM, selectStmtList, expectedValues);
    ReplicationTestUtils.appendInsertUnion(primary, primaryDbName,
            null, tableName, tableNameMM, selectStmtList, expectedValues);
    ReplicationTestUtils.appendAlterTable(primary, primaryDbName,
            null, selectStmtList, expectedValues);

    ReplicationTestUtils.verifyIncrementalLoad(primary, replica, primaryDbName,
            replicatedDbName, selectStmtList, expectedValues, bootStrapDump.lastReplicationId);
  }

  @Test
  public void testBootstrapConvertedExternalTableAutoPurgeDataOnDrop() throws Throwable {
    WarehouseInstance.Tuple bootstrap = primary.run("use " + primaryDbName)
            .run("create table avro_tbl partitioned by (country string) ROW FORMAT SERDE "
                    + "'org.apache.hadoop.hive.serde2.avro.AvroSerDe' stored as avro "
                    + "tblproperties ('avro.schema.url'='" + avroSchemaFile.toUri().toString() + "')")
            .run("insert into avro_tbl partition (country='india') values ('another', 13)")
            .dump(primaryDbName, null);

    replica.load(replicatedDbName, bootstrap.dumpLocation);
    Path dataLocation = assertTablePath(replicatedDbName, "avro_tbl");

    WarehouseInstance.Tuple incremental = primary.run("use " + primaryDbName)
            .run("drop table avro_tbl")
            .dump(primaryDbName, bootstrap.lastReplicationId);
    replica.load(replicatedDbName, incremental.dumpLocation);

    // After drop, the external table data location should be auto deleted as it is converted one.
    assertFalse(replica.miniDFSCluster.getFileSystem().exists(dataLocation));
  }

  @Test
  public void testIncConvertedExternalTableAutoDeleteDataDirOnDrop() throws Throwable {
    WarehouseInstance.Tuple bootstrap = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootstrap.dumpLocation);

    WarehouseInstance.Tuple incremental = primary.run("use " + primaryDbName)
            .run("create table avro_tbl ROW FORMAT SERDE "
                    + "'org.apache.hadoop.hive.serde2.avro.AvroSerDe' stored as avro "
                    + "tblproperties ('avro.schema.url'='" + avroSchemaFile.toUri().toString() + "')")
            .run("insert into avro_tbl values ('str', 13)")
            .dump(primaryDbName, bootstrap.lastReplicationId);
    replica.load(replicatedDbName, incremental.dumpLocation);

    // Data location is valid and is under default external warehouse directory.
    Table avroTable = replica.getTable(replicatedDbName, "avro_tbl");
    assertTrue(MetaStoreUtils.isExternalTable(avroTable));
    Path dataLocation = new Path(avroTable.getSd().getLocation());
    assertTrue(replica.miniDFSCluster.getFileSystem().exists(dataLocation));

    incremental = primary.run("use " + primaryDbName)
            .run("drop table avro_tbl")
            .dump(primaryDbName, incremental.lastReplicationId);
    replica.load(replicatedDbName, incremental.dumpLocation);

    // After drop, the external table data location should be auto deleted as it is converted one.
    assertFalse(replica.miniDFSCluster.getFileSystem().exists(dataLocation));
  }

  @Test
  public void testBootstrapLoadMigrationToAcidWithMoveOptimization() throws Throwable {
    List<String> withConfigs =
            Collections.singletonList("'hive.repl.enable.move.optimization'='true'");
    WarehouseInstance.Tuple tuple = prepareDataAndDump(primaryDbName, null);
    replica.load(replicatedDbName, tuple.dumpLocation, withConfigs);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);
  }

  @Test
  public void testIncrementalLoadMigrationToAcidWithMoveOptimization() throws Throwable {
    List<String> withConfigs =
            Collections.singletonList("'hive.repl.enable.move.optimization'='true'");
    WarehouseInstance.Tuple tuple = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, tuple.dumpLocation);
    tuple = prepareDataAndDump(primaryDbName, tuple.lastReplicationId);
    replica.load(replicatedDbName, tuple.dumpLocation, withConfigs);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);
  }

  @Test
  public void dynamicallyConvertManagedToExternalTable() throws Throwable {
    // With Strict managed disabled but Db enabled for replication, it is not possible to convert
    // external table to managed table.
    primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc ")
            .run("insert into t1 values(1)")
            .run("create table t2 partitioned by (country string) ROW FORMAT SERDE "
                    + "'org.apache.hadoop.hive.serde2.avro.AvroSerDe' stored as avro "
                    + "tblproperties ('avro.schema.url'='" + avroSchemaFile.toUri().toString() + "')")
            .run("insert into t2 partition (country='india') values ('another', 13)")
            .runFailure("alter table t1 set tblproperties('EXTERNAL'='true')")
            .runFailure("alter table t2 set tblproperties('EXTERNAL'='true')");
  }

  @Test
  public void dynamicallyConvertExternalToManagedTable() throws Throwable {
    // With Strict managed disabled but Db enabled for replication, it is not possible to convert
    // external table to managed table.
    primary.run("use " + primaryDbName)
            .run("create external table t1 (id int) stored as orc")
            .run("insert into table t1 values (1)")
            .run("create external table t2 (place string) partitioned by (country string)")
            .run("insert into table t2 partition(country='india') values ('bangalore')")
            .runFailure("alter table t1 set tblproperties('EXTERNAL'='false')")
            .runFailure("alter table t2 set tblproperties('EXTERNAL'='false')");
  }

  @Test
  public void testMigrationWithUpgrade() throws Throwable {
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .run("create table tacid (id int) clustered by(id) into 3 buckets stored as orc ")
            .run("insert into tacid values (3)")
            .run("create table texternal (id int) ")
            .run("insert into texternal values (1)")
            .dump(primaryDbName, null);
    replica.load(replicatedDbName, tuple.dumpLocation)
            .run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("select id from tacid")
            .verifyResult("3")
            .run("select id from texternal")
            .verifyResult("1");

    assertTrue(isFullAcidTable(replica.getTable(replicatedDbName, "tacid")));
    assertFalse(MetaStoreUtils.isExternalTable(replica.getTable(replicatedDbName, "texternal")));

    // forcefully (setting db property) alter the table type. For acid table, set the bootstrap acid table to true. For
    // external table, the alter event should alter the table type at target cluster and then distcp should copy the
    // files. This is done to mock the upgrade done using HiveStrictManagedMigration.
    HiveConf hiveConf = primary.getConf();

    try {
      //Set the txn config required for this test. This will not enable the full acid functionality in the warehouse.
      hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
      hiveConf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");

      primary.run("use " + primaryDbName)
              .run("alter database " + primaryDbName + " set DBPROPERTIES ('" + SOURCE_OF_REPLICATION + "' = '')")
              .run("insert into tacid values (1)")
              .run("insert into texternal values (2)")
              .run("alter table tacid set tblproperties ('transactional'='true')")
              .run("alter table texternal SET TBLPROPERTIES('EXTERNAL'='TRUE')")
              .run("insert into texternal values (3)")
              .run("alter database " + primaryDbName + " set DBPROPERTIES ('" + SOURCE_OF_REPLICATION + "' = '1,2,3')");
    } finally {
      hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
      hiveConf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
    }

    assertTrue(isFullAcidTable(primary.getTable(primaryDbName, "tacid")));
    assertTrue(MetaStoreUtils.isExternalTable(primary.getTable(primaryDbName, "texternal")));

    List<String> withConfigs = new ArrayList();
    withConfigs.add("'hive.repl.bootstrap.acid.tables'='true'");
    withConfigs.add("'hive.repl.dump.include.acid.tables'='true'");
    withConfigs.add("'hive.repl.include.external.tables'='true'");
    withConfigs.add("'hive.distcp.privileged.doAs' = '" + UserGroupInformation.getCurrentUser().getUserName() + "'");
    tuple = primary.dump(primaryDbName, tuple.lastReplicationId, withConfigs);
    replica.load(replicatedDbName, tuple.dumpLocation, withConfigs);
    replica.run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("select id from tacid")
            .verifyResults(new String[] { "1", "3" })
            .run("select id from texternal")
            .verifyResults(new String[] { "1", "2", "3" });
    assertTrue(isFullAcidTable(replica.getTable(replicatedDbName, "tacid")));
    assertTrue(MetaStoreUtils.isExternalTable(replica.getTable(replicatedDbName, "texternal")));
  }
}
