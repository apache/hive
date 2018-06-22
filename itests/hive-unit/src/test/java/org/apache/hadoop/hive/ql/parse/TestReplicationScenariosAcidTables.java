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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.CallerArguments;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.BehaviourInjection;
import static org.apache.hadoop.hive.metastore.ReplChangeManager.SOURCE_OF_REPLICATION;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import javax.annotation.Nullable;

/**
 * TestReplicationScenariosAcidTables - test replication for ACID tables
 */
public class TestReplicationScenariosAcidTables {
  @Rule
  public final TestName testName = new TestName();

  @Rule
  public TestRule replV1BackwardCompat;

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationScenarios.class);
  private static WarehouseInstance primary, replica, replicaNonAcid;
  private String primaryDbName, replicatedDbName;
  private static HiveConf conf;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    conf = new HiveConf(TestReplicationScenariosAcidTables.class);
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    MiniDFSCluster miniDFSCluster =
           new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    HashMap<String, String> overridesForHiveConf = new HashMap<String, String>() {{
        put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
        put("hive.support.concurrency", "true");
        put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        put("hive.repl.dump.include.acid.tables", "true");
        put("hive.metastore.client.capability.check", "false");
        put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
    }};
    primary = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConf);
    replica = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConf);
    HashMap<String, String> overridesForHiveConf1 = new HashMap<String, String>() {{
        put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
        put("hive.support.concurrency", "false");
        put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
        put("hive.repl.dump.include.acid.tables", "true");
        put("hive.metastore.client.capability.check", "false");
    }};
    replicaNonAcid = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConf1);
  }

  @AfterClass
  public static void classLevelTearDown() throws IOException {
    primary.close();
    replica.close();
  }

  @Before
  public void setup() throws Throwable {
    replV1BackwardCompat = primary.getReplivationV1CompatRule(new ArrayList<>());
    primaryDbName = testName.getMethodName() + "_" + +System.currentTimeMillis();
    replicatedDbName = "replicated_" + primaryDbName;
    primary.run("create database " + primaryDbName + " WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')");
  }

  @After
  public void tearDown() throws Throwable {
    primary.run("drop database if exists " + primaryDbName + " cascade");
    replica.run("drop database if exists " + replicatedDbName + " cascade");
    replicaNonAcid.run("drop database if exists " + replicatedDbName + " cascade");
  }

  @Test
  public void testAcidTablesBootstrap() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = primary
            .run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(1)")
            .run("insert into t1 values(2)")
            .run("create table t2 (place string) partitioned by (country string) clustered by(place) " +
                    "into 3 buckets stored as orc tblproperties (\"transactional\"=\"true\")")
            .run("insert into t2 partition(country='india') values ('bangalore')")
            .run("insert into t2 partition(country='us') values ('austin')")
            .run("insert into t2 partition(country='france') values ('paris')")
            .run("alter table t2 add partition(country='italy')")
            .run("create table t3 (rank int) tblproperties(\"transactional\"=\"true\", " +
                    "\"transactional_properties\"=\"insert_only\")")
            .run("insert into t3 values(11)")
            .run("insert into t3 values(22)")
            .run("create table t4 (id int)")
            .run("insert into t4 values(111), (222)")
            .run("create table t5 (id int) stored as orc ")
            .run("insert into t5 values(1111), (2222)")
            .run("alter table t5 set tblproperties (\"transactional\"=\"true\")")
            .run("insert into t5 values(3333)")
            .dump(primaryDbName, null);

    replica.load(replicatedDbName, bootstrapDump.dumpLocation)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"t1", "t2", "t3", "t4", "t5"})
            .run("repl status " + replicatedDbName)
            .verifyResult(bootstrapDump.lastReplicationId)
            .run("select id from t1 order by id")
            .verifyResults(new String[]{"1", "2"})
            .run("select country from t2 order by country")
            .verifyResults(new String[] {"france", "india", "us"})
            .run("select rank from t3 order by rank")
            .verifyResults(new String[] {"11", "22"})
            .run("select id from t4 order by id")
            .verifyResults(new String[] {"111", "222"})
            .run("select id from t5 order by id")
            .verifyResults(new String[] {"1111", "2222", "3333"});
  }

  @Test
  public void testAcidTablesBootstrapWithOpenTxnsTimeout() throws Throwable {
    // Open 5 txns
    HiveConf primaryConf = primary.getConf();
    TxnStore txnHandler = TxnUtils.getTxnStore(primary.getConf());
    OpenTxnsResponse otResp = txnHandler.openTxns(new OpenTxnRequest(5, "u1", "localhost"));
    List<Long> txns = otResp.getTxn_ids();
    String txnIdRange = " txn_id >= " + txns.get(0) + " and txn_id <= " + txns.get(4);
    Assert.assertEquals(TxnDbUtil.queryToString(primaryConf, "select * from TXNS"),
            5, TxnDbUtil.countQueryAgent(primaryConf,
                  "select count(*) from TXNS where txn_state = 'o' and " + txnIdRange));

    // Create 2 tables, one partitioned and other not. Also, have both types of full ACID and MM tables.
    primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(1)")
            .run("create table t2 (rank int) partitioned by (name string) tblproperties(\"transactional\"=\"true\", " +
                    "\"transactional_properties\"=\"insert_only\")")
            .run("insert into t2 partition(name='Bob') values(11)")
            .run("insert into t2 partition(name='Carl') values(10)");
    // Allocate write ids for both tables t1 and t2 for all txns
    // t1=5+1(insert) and t2=5+2(insert)
    AllocateTableWriteIdsRequest rqst = new AllocateTableWriteIdsRequest(primaryDbName, "t1");
    rqst.setTxnIds(txns);
    txnHandler.allocateTableWriteIds(rqst);
    rqst.setTableName("t2");
    txnHandler.allocateTableWriteIds(rqst);
    Assert.assertEquals(TxnDbUtil.queryToString(primaryConf, "select * from TXN_TO_WRITE_ID"),
            6, TxnDbUtil.countQueryAgent(primaryConf,
                    "select count(*) from TXN_TO_WRITE_ID where t2w_database = '" + primaryDbName.toLowerCase()
                            + "' and t2w_table = 't1'"));
    Assert.assertEquals(TxnDbUtil.queryToString(primaryConf, "select * from TXN_TO_WRITE_ID"),
            7, TxnDbUtil.countQueryAgent(primaryConf,
                    "select count(*) from TXN_TO_WRITE_ID where t2w_database = '" + primaryDbName.toLowerCase()
                            + "' and t2w_table = 't2'"));

    // Bootstrap dump with open txn timeout as 1s.
    List<String> withConfigs = Arrays.asList(
            "'hive.repl.bootstrap.dump.open.txn.timeout'='1s'");
    WarehouseInstance.Tuple bootstrapDump = primary
            .run("use " + primaryDbName)
            .dump(primaryDbName, null, withConfigs);

    // After bootstrap dump, all the opened txns should be aborted. Verify it.
    Assert.assertEquals(TxnDbUtil.queryToString(primaryConf, "select * from TXNS"),
            0, TxnDbUtil.countQueryAgent(primaryConf,
                    "select count(*) from TXNS where txn_state = 'o' and " + txnIdRange));
    Assert.assertEquals(TxnDbUtil.queryToString(primaryConf, "select * from TXNS"),
            5, TxnDbUtil.countQueryAgent(primaryConf,
                    "select count(*) from TXNS where txn_state = 'a' and " + txnIdRange));

    // Verify the next write id
    String[] nextWriteId = TxnDbUtil.queryToString(primaryConf, "select nwi_next from NEXT_WRITE_ID where "
            + " nwi_database = '" + primaryDbName.toLowerCase() + "' and nwi_table = 't1'")
            .split("\n");
    Assert.assertEquals(Long.parseLong(nextWriteId[1].trim()), 7L);
    nextWriteId = TxnDbUtil.queryToString(primaryConf, "select nwi_next from NEXT_WRITE_ID where "
            + " nwi_database = '" + primaryDbName.toLowerCase() + "' and nwi_table = 't2'")
            .split("\n");
    Assert.assertEquals(Long.parseLong(nextWriteId[1].trim()), 8L);

    // Bootstrap load which should also replicate the aborted write ids on both tables.
    HiveConf replicaConf = replica.getConf();
    replica.load(replicatedDbName, bootstrapDump.dumpLocation)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"t1", "t2"})
            .run("repl status " + replicatedDbName)
            .verifyResult(bootstrapDump.lastReplicationId)
            .run("select id from t1")
            .verifyResults(new String[]{"1"})
            .run("select rank from t2 order by rank")
            .verifyResults(new String[] {"10", "11"});

    // Verify if HWM is properly set after REPL LOAD
    nextWriteId = TxnDbUtil.queryToString(replicaConf, "select nwi_next from NEXT_WRITE_ID where "
            + " nwi_database = '" + replicatedDbName.toLowerCase() + "' and nwi_table = 't1'")
            .split("\n");
    Assert.assertEquals(Long.parseLong(nextWriteId[1].trim()), 7L);
    nextWriteId = TxnDbUtil.queryToString(replicaConf, "select nwi_next from NEXT_WRITE_ID where "
            + " nwi_database = '" + replicatedDbName.toLowerCase() + "' and nwi_table = 't2'")
            .split("\n");
    Assert.assertEquals(Long.parseLong(nextWriteId[1].trim()), 8L);

    // Verify if all the aborted write ids are replicated to the replicated DB
    Assert.assertEquals(TxnDbUtil.queryToString(replicaConf, "select * from TXN_TO_WRITE_ID"),
            5, TxnDbUtil.countQueryAgent(replicaConf,
                    "select count(*) from TXN_TO_WRITE_ID where t2w_database = '" + replicatedDbName.toLowerCase()
                            + "' and t2w_table = 't1'"));
    Assert.assertEquals(TxnDbUtil.queryToString(replicaConf, "select * from TXN_TO_WRITE_ID"),
            5, TxnDbUtil.countQueryAgent(replicaConf,
                    "select count(*) from TXN_TO_WRITE_ID where t2w_database = '" + replicatedDbName.toLowerCase()
                            + "' and t2w_table = 't2'"));

    // Verify if entries added in COMPACTION_QUEUE for each table/partition
    // t1-> 1 entry and t2-> 2 entries (1 per partition)
    Assert.assertEquals(TxnDbUtil.queryToString(replicaConf, "select * from COMPACTION_QUEUE"),
            1, TxnDbUtil.countQueryAgent(replicaConf,
                    "select count(*) from COMPACTION_QUEUE where cq_database = '" + replicatedDbName
                            + "' and cq_table = 't1'"));
    Assert.assertEquals(TxnDbUtil.queryToString(replicaConf, "select * from COMPACTION_QUEUE"),
            2, TxnDbUtil.countQueryAgent(replicaConf,
                    "select count(*) from COMPACTION_QUEUE where cq_database = '" + replicatedDbName
                            + "' and cq_table = 't2'"));
  }

  @Test
  public void testOpenTxnEvent() throws Throwable {
    String tableName = testName.getMethodName();
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    // create table will start and coomit the transaction
    primary.run("use " + primaryDbName)
           .run("CREATE TABLE " + tableName +
            " (key int, value int) PARTITIONED BY (load_date date) " +
            "CLUSTERED BY(key) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')")
            .run("SHOW TABLES LIKE '" + tableName + "'")
            .verifyResult(tableName)
            .run("INSERT INTO " + tableName + " partition (load_date='2016-03-01') VALUES (1, 1)")
            .run("select key from " + tableName)
            .verifyResult("1");

    WarehouseInstance.Tuple incrementalDump =
            primary.dump(primaryDbName, bootStrapDump.lastReplicationId);
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId);

    // Test the idempotent behavior of Open and Commit Txn
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId);
  }

  @Test
  public void testAbortTxnEvent() throws Throwable {
    String tableName = testName.getMethodName();
    String tableNameFail = testName.getMethodName() + "Fail";
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    // this should fail
    primary.run("use " + primaryDbName)
            .runFailure("CREATE TABLE " + tableNameFail +
            " (key int, value int) PARTITIONED BY (load_date date) " +
            "CLUSTERED BY(key) ('transactional'='true')")
            .run("SHOW TABLES LIKE '" + tableNameFail + "'")
            .verifyFailure(new String[]{tableNameFail});

    WarehouseInstance.Tuple incrementalDump =
            primary.dump(primaryDbName, bootStrapDump.lastReplicationId);
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId);

    // Test the idempotent behavior of Abort Txn
    replica.load(replicatedDbName, incrementalDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId);
  }

  @Test
  public void testTxnEventNonAcid() throws Throwable {
    String tableName = testName.getMethodName();
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replicaNonAcid.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    primary.run("use " + primaryDbName)
            .run("CREATE TABLE " + tableName +
            " (key int, value int) PARTITIONED BY (load_date date) " +
            "CLUSTERED BY(key) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')")
            .run("SHOW TABLES LIKE '" + tableName + "'")
            .verifyResult(tableName)
            .run("INSERT INTO " + tableName +
                    " partition (load_date='2016-03-01') VALUES (1, 1)")
            .run("select key from " + tableName)
            .verifyResult("1");

    WarehouseInstance.Tuple incrementalDump =
            primary.dump(primaryDbName, bootStrapDump.lastReplicationId);
    replicaNonAcid.runFailure("REPL LOAD " + replicatedDbName + " FROM '" + incrementalDump.dumpLocation + "'")
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);
  }

  @Test
  public void testAcidBootstrapReplLoadRetryAfterFailure() throws Throwable {
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(1)")
            .run("create table t2 (rank int) partitioned by (name string) tblproperties(\"transactional\"=\"true\", " +
                    "\"transactional_properties\"=\"insert_only\")")
            .run("insert into t2 partition(name='bob') values(11)")
            .run("insert into t2 partition(name='carl') values(10)")
            .dump(primaryDbName, null);

    WarehouseInstance.Tuple tuple2 = primary
            .run("use " + primaryDbName)
            .dump(primaryDbName, null);

    // Inject a behavior where REPL LOAD failed when try to load table "t2", it fails.
    BehaviourInjection<CallerArguments, Boolean> callerVerifier
            = new BehaviourInjection<CallerArguments, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable CallerArguments args) {
        injectionPathCalled = true;
        if (!args.dbName.equalsIgnoreCase(replicatedDbName)) {
          LOG.warn("Verifier - DB: " + String.valueOf(args.dbName));
          return false;
        }
        if (args.tblName != null) {
          LOG.warn("Verifier - Table: " + String.valueOf(args.tblName));
          return args.tblName.equals("t1");
        }
        return true;
      }
    };
    InjectableBehaviourObjectStore.setCallerVerifier(callerVerifier);

    List<String> withConfigs = Arrays.asList("'hive.repl.approx.max.load.tasks'='1'");
    replica.loadFailure(replicatedDbName, tuple.dumpLocation, withConfigs);
    callerVerifier.assertInjectionsPerformed(true, false);
    InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour

    replica.run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult("null")
            .run("show tables like t2")
            .verifyResults(new String[] { });

    // Retry with different dump should fail.
    replica.loadFailure(replicatedDbName, tuple2.dumpLocation);

    // Verify if no create table on t1. Only table t2 should  be created in retry.
    callerVerifier = new BehaviourInjection<CallerArguments, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable CallerArguments args) {
        injectionPathCalled = true;
        if (!args.dbName.equalsIgnoreCase(replicatedDbName)) {
          LOG.warn("Verifier - DB: " + String.valueOf(args.dbName));
          return false;
        }
        return true;
      }
    };
    InjectableBehaviourObjectStore.setCallerVerifier(callerVerifier);

    // Retry with same dump with which it was already loaded should resume the bootstrap load.
    // This time, it completes by adding just constraints for table t4.
    replica.load(replicatedDbName, tuple.dumpLocation);
    callerVerifier.assertInjectionsPerformed(true, false);
    InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour

    replica.run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("show tables")
            .verifyResults(new String[] { "t1", "t2" })
            .run("select id from t1")
            .verifyResults(Arrays.asList("1"))
            .run("select name from t2 order by name")
            .verifyResults(Arrays.asList("bob", "carl"));
  }

  @Test
  public void testDumpAcidTableWithPartitionDirMissing() throws Throwable {
    String dbName = testName.getMethodName();
    primary.run("CREATE DATABASE " + dbName + " WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')")
    .run("CREATE TABLE " + dbName + ".normal (a int) PARTITIONED BY (part int)" +
            " STORED AS ORC TBLPROPERTIES ('transactional'='true')")
    .run("INSERT INTO " + dbName + ".normal partition (part= 124) values (1)");

    Path path = new Path(primary.warehouseRoot, dbName.toLowerCase()+".db");
    path = new Path(path, "normal");
    path = new Path(path, "part=124");
    FileSystem fs = path.getFileSystem(conf);
    fs.delete(path);

    CommandProcessorResponse ret = primary.runCommand("REPL DUMP " + dbName +
            " with ('hive.repl.dump.include.acid.tables' = 'true')");
    Assert.assertEquals(ret.getResponseCode(), ErrorMsg.FILE_NOT_FOUND.getErrorCode());

    primary.run("DROP TABLE " + dbName + ".normal");
    primary.run("drop database " + dbName);
  }

  @Test
  public void testDumpAcidTableWithTableDirMissing() throws Throwable {
    String dbName = testName.getMethodName();
    primary.run("CREATE DATABASE " + dbName + " WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')")
            .run("CREATE TABLE " + dbName + ".normal (a int) " +
                    " STORED AS ORC TBLPROPERTIES ('transactional'='true')")
            .run("INSERT INTO " + dbName + ".normal values (1)");

    Path path = new Path(primary.warehouseRoot, dbName.toLowerCase()+".db");
    path = new Path(path, "normal");
    FileSystem fs = path.getFileSystem(conf);
    fs.delete(path);

    CommandProcessorResponse ret = primary.runCommand("REPL DUMP " + dbName +
            " with ('hive.repl.dump.include.acid.tables' = 'true')");
    Assert.assertEquals(ret.getResponseCode(), ErrorMsg.FILE_NOT_FOUND.getErrorCode());

    primary.run("DROP TABLE " + dbName + ".normal");
    primary.run("drop database " + dbName);
  }
}
