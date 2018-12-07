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
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.CallerArguments;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.BehaviourInjection;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.hive.ql.parse.WarehouseInstance;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import static org.apache.hadoop.hive.metastore.ReplChangeManager.SOURCE_OF_REPLICATION;
import org.apache.hadoop.hive.ql.parse.ReplicationTestUtils;

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
import javax.annotation.Nullable;
import org.junit.rules.TestName;
import com.google.common.collect.Lists;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isFullAcidTable;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isTransactionalTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * TestReplicationWithTableMigration - test replication for Hive2 to Hive3 (Strict managed tables)
 */
public class TestReplicationWithTableMigration {
  @Rule
  public final TestName testName = new TestName();

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationWithTableMigration.class);
  private static WarehouseInstance primary, replica;
  private String primaryDbName, replicatedDbName;
  private static HiveConf conf;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    conf = new HiveConf(TestReplicationWithTableMigration.class);
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    MiniDFSCluster miniDFSCluster =
           new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    HashMap<String, String> overridesForHiveConf = new HashMap<String, String>() {{
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
    replica = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConf);

    HashMap<String, String> overridesForHiveConf1 = new HashMap<String, String>() {{
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
    primary = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConf1);
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
  }

  @After
  public void tearDown() throws Throwable {
    primary.run("drop database if exists " + primaryDbName + " cascade");
    replica.run("drop database if exists " + replicatedDbName + " cascade");
  }

  private WarehouseInstance.Tuple prepareDataAndDump(String primaryDbName, String fromReplId) throws Throwable {
    WarehouseInstance.Tuple tuple =  primary.run("use " + primaryDbName)
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
            .run("create table tacidloc (id int) clustered by(id) into 3 buckets stored as orc  LOCATION '/tmp' ")
            .run("insert into tacidloc values(1)")
            .run("insert into tacidloc values(2)")
            .run("insert into tacidloc values(3)")
            .run("create table tacidpartloc (place string) partitioned by (country string) clustered by(place) " +
                    "into 3 buckets stored as orc ")
            .run("alter table tacidpartloc add partition(country='france') LOCATION '/tmp/part'")
            .run("insert into tacidpartloc partition(country='india') values('mumbai')")
            .run("insert into tacidpartloc partition(country='us') values('sf')")
            .run("insert into tacidpartloc partition(country='france') values('paris')")
            .run("create table avro_table ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                    "stored as avro tblproperties ('avro.schema.url'='" + primary.avroSchemaFile.toUri().toString() + "')")
            .run("insert into avro_table values('str1', 10)")
            .dump(primaryDbName, fromReplId);
    assertFalse(isTransactionalTable(primary.getTable(primaryDbName, "tacid")));
    assertFalse(isTransactionalTable(primary.getTable(primaryDbName, "tacidpart")));
    assertFalse(isTransactionalTable(primary.getTable(primaryDbName, "tflat")));
    assertFalse(isTransactionalTable(primary.getTable(primaryDbName, "tflattext")));
    assertFalse(isTransactionalTable(primary.getTable(primaryDbName, "tflattextpart")));
    assertFalse(isTransactionalTable(primary.getTable(primaryDbName, "tacidloc")));
    assertFalse(isTransactionalTable(primary.getTable(primaryDbName, "tacidpartloc")));
    Table avroTable = replica.getTable(replicatedDbName, "avro_table");
    assertFalse(isTransactionalTable(avroTable));
    assertFalse(MetaStoreUtils.isExternalTable(avroTable));
    return tuple;
  }

  private void verifyLoadExecution(String replicatedDbName, String lastReplId) throws Throwable {
    replica.run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"tacid", "tacidpart", "tflat", "tflattext", "tflattextpart",
                    "tacidloc", "tacidpartloc", "avro_table"})
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
            .verifyResults(new String[] {"str1"});

    assertTrue(isFullAcidTable(replica.getTable(replicatedDbName, "tacid")));
    assertTrue(isFullAcidTable(replica.getTable(replicatedDbName, "tacidpart")));
    assertTrue(isFullAcidTable(replica.getTable(replicatedDbName, "tflat")));
    assertTrue(!isFullAcidTable(replica.getTable(replicatedDbName, "tflattext")));
    assertTrue(!isFullAcidTable(replica.getTable(replicatedDbName, "tflattextpart")));
    assertTrue(isTransactionalTable(replica.getTable(replicatedDbName, "tflattext")));
    assertTrue(isTransactionalTable(replica.getTable(replicatedDbName, "tflattextpart")));
    assertTrue(isFullAcidTable(replica.getTable(replicatedDbName, "tacidloc")));
    assertTrue(isFullAcidTable(replica.getTable(replicatedDbName, "tacidpartloc")));

    /*Path databasePath = new Path(replica.warehouseRoot, replica.getDatabase(replicatedDbName).getLocationUri());
    assertEquals(replica.getTable(replicatedDbName, "tacidloc").getSd().getLocation(),
            new Path(databasePath,"tacidloc").toUri().toString());

    Path tablePath = new Path(databasePath, "tacidpartloc");
    List<Partition> partitions = replica.getAllPartitions(replicatedDbName, "tacidpartloc");
    for (Partition part : partitions) {
      tablePath.equals(new Path(part.getSd().getLocation()).getParent());
    }*/

    Table avroTable = replica.getTable(replicatedDbName, "avro_table");
    assertTrue(MetaStoreUtils.isExternalTable(avroTable));
    Path tablePath = new PathBuilder(replica.externalTableWarehouseRoot.toString()).addDescendant(replicatedDbName + ".db")
            .addDescendant("avro_table")
            .build();
    assertEquals(avroTable.getSd().getLocation().toLowerCase(), tablePath.toUri().toString().toLowerCase());
  }

  private void loadWithFailureInAddNotification(String tbl, String dumpLocation) throws Throwable {
    BehaviourInjection<CallerArguments, Boolean> callerVerifier
            = new BehaviourInjection<CallerArguments, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable InjectableBehaviourObjectStore.CallerArguments args) {
        injectionPathCalled = true;
        if (!args.dbName.equalsIgnoreCase(replicatedDbName) || (args.constraintTblName != null)) {
          LOG.warn("Verifier - DB: " + String.valueOf(args.dbName)
                  + " Constraint Table: " + String.valueOf(args.constraintTblName));
          return false;
        }
        if (args.tblName != null) {
          LOG.warn("Verifier - Table: " + String.valueOf(args.tblName));
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
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);
  }

  @Test
  public void testIncrementalLoadMigrationManagedToAcid() throws Throwable {
    WarehouseInstance.Tuple tuple = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, tuple.dumpLocation);
    tuple = prepareDataAndDump(primaryDbName, tuple.lastReplicationId);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
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
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
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
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);
  }
}
