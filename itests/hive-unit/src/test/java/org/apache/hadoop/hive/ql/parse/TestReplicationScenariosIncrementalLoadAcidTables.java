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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.hive.ql.parse.WarehouseInstance;
import static org.apache.hadoop.hive.metastore.ReplChangeManager.SOURCE_OF_REPLICATION;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isFullAcidTable;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isTransactionalTable;

import org.apache.hadoop.hive.ql.parse.ReplicationTestUtils;

import org.junit.rules.TestName;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import com.google.common.collect.Lists;

/**
 * TestReplicationScenariosAcidTables - test replication for ACID tables
 */
public class TestReplicationScenariosIncrementalLoadAcidTables {
  @Rule
  public final TestName testName = new TestName();

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationScenariosIncrementalLoadAcidTables.class);
  static WarehouseInstance primary;
  private static WarehouseInstance replica, replicaNonAcid, replicaMigration, primaryMigration;
  private static HiveConf conf;
  private String primaryDbName, replicatedDbName, primaryDbNameExtra;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    HashMap<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());

    internalBeforeClassSetup(overrides, TestReplicationScenariosIncrementalLoadAcidTables.class);
  }

  static void internalBeforeClassSetup(Map<String, String> overrides, Class clazz)
      throws Exception {
    conf = new HiveConf(clazz);
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    MiniDFSCluster miniDFSCluster =
           new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    HashMap<String, String> acidConfs = new HashMap<String, String>() {{
        put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
        put("hive.support.concurrency", "true");
        put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        put("hive.metastore.client.capability.check", "false");
        put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
        put("hive.exec.dynamic.partition.mode", "nonstrict");
        put("hive.strict.checks.bucketing", "false");
        put("hive.mapred.mode", "nonstrict");
        put("mapred.input.dir.recursive", "true");
        put("hive.metastore.disallow.incompatible.col.type.changes", "false");
    }};

    acidConfs.putAll(overrides);
    primary = new WarehouseInstance(LOG, miniDFSCluster, acidConfs);
    replica = new WarehouseInstance(LOG, miniDFSCluster, acidConfs);
    Map<String, String> overridesForHiveConf1 = new HashMap<String, String>() {{
        put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
        put("hive.support.concurrency", "false");
        put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
        put("hive.metastore.client.capability.check", "false");
    }};
    replicaNonAcid = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConf1);

    HashMap<String, String> overridesForHiveConfReplicaMigration = new HashMap<String, String>() {{
      put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
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
    replicaMigration = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConfReplicaMigration);

    HashMap<String, String> overridesForHiveConfPrimaryMigration = new HashMap<String, String>() {{
      put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
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
    }};
    primaryMigration = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConfPrimaryMigration);
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
    primaryDbNameExtra = primaryDbName+"_extra";
    primary.run("create database " + primaryDbNameExtra + " WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')");
  }

  @After
  public void tearDown() throws Throwable {
    primary.run("drop database if exists " + primaryDbName + " cascade");
    replica.run("drop database if exists " + replicatedDbName + " cascade");
    replicaNonAcid.run("drop database if exists " + replicatedDbName + " cascade");
    primary.run("drop database if exists " + primaryDbName + "_extra cascade");
  }

  @Test
  public void testAcidTableIncrementalReplication() throws Throwable {
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);
    List<String> selectStmtList = new ArrayList<>();
    List<String[]> expectedValues = new ArrayList<>();
    String tableName = testName.getMethodName() + "testInsert";
    String tableNameMM = tableName + "_MM";

    ReplicationTestUtils.appendInsert(primary, primaryDbName, primaryDbNameExtra, tableName,
            tableNameMM, selectStmtList, expectedValues);
    appendDelete(primary, primaryDbName, primaryDbNameExtra, selectStmtList, expectedValues);
    appendUpdate(primary, primaryDbName, primaryDbNameExtra, selectStmtList, expectedValues);
    ReplicationTestUtils.appendTruncate(primary, primaryDbName, primaryDbNameExtra,
            selectStmtList, expectedValues);
    ReplicationTestUtils.appendInsertIntoFromSelect(primary, primaryDbName, primaryDbNameExtra,
            tableName, tableNameMM, selectStmtList, expectedValues);
    ReplicationTestUtils.appendMerge(primary, primaryDbName, primaryDbNameExtra, selectStmtList, expectedValues);
    ReplicationTestUtils.appendCreateAsSelect(primary, primaryDbName, primaryDbNameExtra, tableName,
            tableNameMM, selectStmtList, expectedValues);
    ReplicationTestUtils.appendImport(primary, primaryDbName, primaryDbNameExtra, tableName,
            tableNameMM, selectStmtList, expectedValues);
    ReplicationTestUtils.appendInsertOverwrite(primary, primaryDbName, primaryDbNameExtra, tableName,
            tableNameMM, selectStmtList, expectedValues);
    ReplicationTestUtils.appendLoadLocal(primary, primaryDbName, primaryDbNameExtra, tableName,
            tableNameMM, selectStmtList, expectedValues);
    ReplicationTestUtils.appendInsertUnion(primary, primaryDbName, primaryDbNameExtra, tableName,
            tableNameMM, selectStmtList, expectedValues);
    ReplicationTestUtils.appendMultiStatementTxn(primary, primaryDbName, primaryDbNameExtra,
            selectStmtList, expectedValues);
    appendMultiStatementTxnUpdateDelete(primary, primaryDbName, primaryDbNameExtra, selectStmtList, expectedValues);
    ReplicationTestUtils.appendAlterTable(primary, primaryDbName, primaryDbNameExtra, selectStmtList, expectedValues);

    verifyIncrementalLoadInt(selectStmtList, expectedValues, bootStrapDump.lastReplicationId);
  }

  private void appendDelete(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                            List<String> selectStmtList, List<String[]> expectedValues) throws Throwable {
    String tableName = "testDelete";
    ReplicationTestUtils.insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableName, null, false, ReplicationTestUtils.OperationType.REPL_TEST_ACID_INSERT);
    deleteRecords(tableName);
    selectStmtList.add("select count(*) from " + tableName);
    expectedValues.add(new String[] {"0"});
  }

  private void appendUpdate(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                            List<String> selectStmtList, List<String[]> expectedValues) throws Throwable {
    String tableName = "testUpdate";
    ReplicationTestUtils.insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableName, null, false, ReplicationTestUtils.OperationType.REPL_TEST_ACID_INSERT);
    updateRecords(tableName);
    selectStmtList.add("select value from " + tableName + " order by value");
    expectedValues.add(new String[] {"1", "100", "100", "100", "100"});
  }

  private void appendMultiStatementTxnUpdateDelete(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                                                   List<String> selectStmtList, List<String[]> expectedValues)
          throws Throwable {
    String tableName = "testMultiStatementTxnUpdate";
    String tableNameDelete = "testMultiStatementTxnDelete";
    String[] resultArray = new String[]{"1", "2", "3", "4", "5"};
    String tableProperty = "'transactional'='true'";
    String tableStorage = "STORED AS ORC";

    ReplicationTestUtils.insertIntoDB(primary, primaryDbName, tableName, tableProperty,
            tableStorage, resultArray, true);
    updateRecords(tableName);
    selectStmtList.add("select value from " + tableName + " order by value");
    expectedValues.add(new String[] {"1", "100", "100", "100", "100"});

    ReplicationTestUtils.insertIntoDB(primary, primaryDbName, tableNameDelete, tableProperty,
            tableStorage, resultArray, true);
    deleteRecords(tableNameDelete);
    selectStmtList.add("select count(*) from " + tableNameDelete);
    expectedValues.add(new String[] {"0"});
  }

  @Test
  public void testReplCM() throws Throwable {
    String tableName = "testcm";
    String tableNameMM = tableName + "_MM";
    String[] result = new String[]{"5"};

    WarehouseInstance.Tuple incrementalDump;
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    ReplicationTestUtils.insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableName, null, false, ReplicationTestUtils.OperationType.REPL_TEST_ACID_INSERT);
    incrementalDump = primary.dump(primaryDbName, bootStrapDump.lastReplicationId);
    primary.run("drop table " + primaryDbName + "." + tableName);
    replica.loadWithoutExplain(replicatedDbName, incrementalDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName).verifyResult(incrementalDump.lastReplicationId);
    verifyResultsInReplicaInt(Lists.newArrayList("select count(*) from " + tableName,
                                              "select count(*) from " + tableName + "_nopart"),
                            Lists.newArrayList(result, result));

    ReplicationTestUtils.insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableNameMM, null, true, ReplicationTestUtils.OperationType.REPL_TEST_ACID_INSERT);
    incrementalDump = primary.dump(primaryDbName, bootStrapDump.lastReplicationId);
    primary.run("drop table " + primaryDbName + "." + tableNameMM);
    replica.loadWithoutExplain(replicatedDbName, incrementalDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName).verifyResult(incrementalDump.lastReplicationId);
    verifyResultsInReplicaInt(Lists.newArrayList("select count(*) from " + tableNameMM,
            "select count(*) from " + tableNameMM + "_nopart"),
            Lists.newArrayList(result, result));
  }

  private void verifyResultsInReplicaInt(List<String> selectStmtList, List<String[]> expectedValues) throws Throwable  {
    ReplicationTestUtils.verifyResultsInReplica(replica, replicatedDbName, selectStmtList, expectedValues);
  }


  private WarehouseInstance.Tuple verifyIncrementalLoadInt(List<String> selectStmtList,
                                                List<String[]> expectedValues, String lastReplId) throws Throwable {
    return ReplicationTestUtils.verifyIncrementalLoad(primary, replica, primaryDbName,
            replicatedDbName, selectStmtList, expectedValues, lastReplId);

  }

  private void deleteRecords(String tableName) throws Throwable {
    primary.run("use " + primaryDbName)
            .run("delete from " + tableName)
            .run("select count(*) from " + tableName)
            .verifyResult("0");
  }

  private void updateRecords(String tableName) throws Throwable {
    primary.run("use " + primaryDbName)
            .run("update " + tableName + " set value = 100 where key >= 2")
            .run("select value from " + tableName + " order by value")
            .verifyResults(new String[] {"1", "100", "100", "100", "100"});
  }

  private WarehouseInstance.Tuple prepareDataAndDump(String primaryDbName, String fromReplId) throws Throwable {
    WarehouseInstance.Tuple tuple =  primaryMigration.run("use " + primaryDbName)
            .run("create table tacid (id int) clustered by(id) into 3 buckets stored as orc ")
            .run("insert into tacid values(1)")
            .run("insert into tacid values(2)")
            .run("insert into tacid values(3)")
            .run("create table tacidpart (place string) partitioned by (country string) clustered by(place) " +
                    "into 3 buckets stored as orc ")
            .run("alter table tacidpart add partition(country='france')")
            .run("insert into tacidpart partition(country='india') values('mumbai')")
            .run("insert into tacidpart partition(country='us') values('sf')")
            .run("insert into tacidpart partition(country='france') values('paris')")
            .run("create table tflat (rank int) stored as orc tblproperties(\"transactional\"=\"false\")")
            .run("insert into tflat values(11)")
            .run("insert into tflat values(22)")
            .run("create table tflattext (id int) ")
            .run("insert into tflattext values(111), (222)")
            .run("create table tflattextpart (id int) partitioned by (country string) ")
            .run("insert into tflattextpart partition(country='india') values(1111), (2222)")
            .run("insert into tflattextpart partition(country='us') values(3333)")
            .run("create table avro_table ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                    "stored as avro tblproperties ('avro.schema.url'='" + primaryMigration.avroSchemaFile.toUri().toString() + "')")
            .run("insert into avro_table values('str1', 10)")
            .dump(primaryDbName, fromReplId);
    assertFalse(isTransactionalTable(primaryMigration.getTable(primaryDbName, "tacid")));
    assertFalse(isTransactionalTable(primaryMigration.getTable(primaryDbName, "tacidpart")));
    assertFalse(isTransactionalTable(primaryMigration.getTable(primaryDbName, "tflat")));
    assertFalse(isTransactionalTable(primaryMigration.getTable(primaryDbName, "tflattext")));
    assertFalse(isTransactionalTable(primaryMigration.getTable(primaryDbName, "tflattextpart")));
    Table avroTable = primaryMigration.getTable(replicatedDbName, "avro_table");
    assertFalse(isTransactionalTable(avroTable));
    assertFalse(MetaStoreUtils.isExternalTable(avroTable));
    return tuple;
  }

  private void verifyLoadExecution(String replicatedDbName, String lastReplId) throws Throwable {
    replicaMigration.run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"tacid", "tacidpart", "tflat", "tflattext", "tflattextpart",
                    "avro_table"})
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
            .run("select col1 from avro_table")
            .verifyResults(new String[] {"str1"});

    assertTrue(isFullAcidTable(replicaMigration.getTable(replicatedDbName, "tacid")));
    assertTrue(isFullAcidTable(replicaMigration.getTable(replicatedDbName, "tacidpart")));
    assertTrue(isFullAcidTable(replicaMigration.getTable(replicatedDbName, "tflat")));
    assertTrue(!isFullAcidTable(replicaMigration.getTable(replicatedDbName, "tflattext")));
    assertTrue(!isFullAcidTable(replicaMigration.getTable(replicatedDbName, "tflattextpart")));
    assertTrue(isTransactionalTable(replicaMigration.getTable(replicatedDbName, "tflattext")));
    assertTrue(isTransactionalTable(replicaMigration.getTable(replicatedDbName, "tflattextpart")));

    Table avroTable = replicaMigration.getTable(replicatedDbName, "avro_table");
    assertTrue(MetaStoreUtils.isExternalTable(avroTable));
    Path tablePath = new PathBuilder(replicaMigration.externalTableWarehouseRoot.toString()).addDescendant(replicatedDbName + ".db")
            .addDescendant("avro_table")
            .build();
    assertEquals(avroTable.getSd().getLocation().toLowerCase(), tablePath.toUri().toString().toLowerCase());
  }

  @Test
  public void testMigrationManagedToAcid() throws Throwable {
    WarehouseInstance.Tuple tupleForBootStrap = primaryMigration.dump(primaryDbName, null);
    WarehouseInstance.Tuple tuple = prepareDataAndDump(primaryDbName, null);
    WarehouseInstance.Tuple tupleForIncremental = primaryMigration.dump(primaryDbName, tupleForBootStrap.lastReplicationId);
    replicaMigration.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);

    replicaMigration.run("drop database if exists " + replicatedDbName + " cascade");
    replicaMigration.loadWithoutExplain(replicatedDbName, tupleForBootStrap.dumpLocation);
    replicaMigration.loadWithoutExplain(replicatedDbName, tupleForIncremental.dumpLocation);
    verifyLoadExecution(replicatedDbName, tupleForIncremental.lastReplicationId);
  }
}
