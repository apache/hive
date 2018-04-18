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
import java.util.Collections;
import com.google.common.collect.Lists;

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
  private static HiveConf conf;
  private String primaryDbName, replicatedDbName, primaryDbNameExtra;
  private enum OperationType {
    REPL_TEST_ACID_INSERT, REPL_TEST_ACID_INSERT_SELECT, REPL_TEST_ACID_CTAS,
    REPL_TEST_ACID_INSERT_OVERWRITE, REPL_TEST_ACID_INSERT_IMPORT, REPL_TEST_ACID_INSERT_LOADLOCAL,
    REPL_TEST_ACID_INSERT_UNION
  }

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
        put("hive.exec.dynamic.partition.mode", "nonstrict");
        put("hive.strict.checks.bucketing", "false");
        put("hive.mapred.mode", "nonstrict");
        put("hive.metastore.disallow.incompatible.col.type.changes", "false");
        put("metastore.client.capability.check", "false");
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
    primaryDbNameExtra = primaryDbName+"_extra";
    primary.run("create database " + primaryDbNameExtra + " WITH DBPROPERTIES ( '" +
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

  @Test
  public void testInsert() throws Throwable {
    String tableName = testName.getMethodName();
    String tableNameMM = testName.getMethodName() + "_MM";
    WarehouseInstance.Tuple incrementalDump;
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);
    insertRecords(tableName, null, false, OperationType.REPL_TEST_ACID_INSERT);
    incrementalDump = verifyLoad(tableName, null, bootStrapDump.lastReplicationId);
    insertRecords(tableNameMM, null, true, OperationType.REPL_TEST_ACID_INSERT);
    incrementalDump = verifyLoad(tableNameMM, null, incrementalDump.lastReplicationId);
  }

  @Test
  public void testDelete() throws Throwable {
    String tableName = testName.getMethodName();
    WarehouseInstance.Tuple incrementalDump;

    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    insertRecords(tableName, null, false, OperationType.REPL_TEST_ACID_INSERT);
    deleteRecords(tableName);
    incrementalDump = verifyIncrementalLoad(Collections.singletonList("select count(*) from " + tableName),
            Collections.singletonList(new String[] {"0"}), bootStrapDump.lastReplicationId);
  }

  @Test
  public void testUpdate() throws Throwable {
    String tableName = testName.getMethodName();
    WarehouseInstance.Tuple incrementalDump;

    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    insertRecords(tableName, null, false, OperationType.REPL_TEST_ACID_INSERT);
    updateRecords(tableName);
    incrementalDump = verifyIncrementalLoad(Collections.singletonList("select value from " + tableName),
            Collections.singletonList(new String[] {"1", "100" , "100"}),
            bootStrapDump.lastReplicationId);
  }

  @Test
  public void testTruncate() throws Throwable {
    String tableName = testName.getMethodName();
    String tableNameMM = testName.getMethodName() + "_MM";
    WarehouseInstance.Tuple incrementalDump;

    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    insertRecords(tableName, null, false, OperationType.REPL_TEST_ACID_INSERT);
    truncateTable(tableName);
    incrementalDump = verifyIncrementalLoad(Collections.singletonList("select count(*) from " + tableName),
            Collections.singletonList(new String[] {"0"}), bootStrapDump.lastReplicationId);

    insertRecords(tableNameMM, null, true, OperationType.REPL_TEST_ACID_INSERT);
    truncateTable(tableNameMM);
    incrementalDump = verifyIncrementalLoad(Collections.singletonList("select count(*) from " + tableNameMM),
            Collections.singletonList(new String[] {"0"}), bootStrapDump.lastReplicationId);
  }

  @Test
  public void testInsertIntoFromSelect() throws Throwable {
    String tableName = testName.getMethodName();
    String tableNameMM = testName.getMethodName() + "_MM";
    String tableNameSelect = testName.getMethodName() + "_Select";
    String tableNameSelectMM = testName.getMethodName() + "_SelectMM";
    WarehouseInstance.Tuple incrementalDump;

    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    insertRecords(tableName, null, false, OperationType.REPL_TEST_ACID_INSERT);
    insertRecords(tableName, tableNameSelect, false, OperationType.REPL_TEST_ACID_INSERT_SELECT);
    incrementalDump = verifyLoad(tableName, tableNameSelect, bootStrapDump.lastReplicationId);

    insertRecords(tableNameMM, null, true, OperationType.REPL_TEST_ACID_INSERT);
    insertRecords(tableNameMM, tableNameSelectMM, true, OperationType.REPL_TEST_ACID_INSERT_SELECT);
    incrementalDump = verifyLoad(tableNameMM, tableNameSelectMM, incrementalDump.lastReplicationId);
  }

  @Test
  public void testMerge() throws Throwable {
    String tableName = testName.getMethodName();
    String tableNameMM = testName.getMethodName() + "_MM";
    String tableNameMerge = testName.getMethodName() + "_Merge";
    String tableNameMergeMM = testName.getMethodName() + "_MergeMM";
    WarehouseInstance.Tuple incrementalDump;

    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    insertForMerge(tableName, tableNameMerge, false);
    incrementalDump = verifyIncrementalLoad(Lists.newArrayList("select last_update_user from " + tableName,
                                             "select key from " + tableNameMerge,
                                             "select ID from " + tableNameMerge),
                          Lists.newArrayList(new String[] {"creation","creation","creation","creation","creation",
                                          "creation","creation","merge_update","merge_insert","merge_insert"},
                                  new String[] {"1","2","3","4","5","6"}, new String[] {"1", "4", "7", "8", "8", "11"}),
                          bootStrapDump.lastReplicationId);
  }

  @Test
  public void testCreateAsSelect() throws Throwable {
    String tableName = testName.getMethodName();
    String tableNameMM = testName.getMethodName() + "_MM";
    String tableNameCTAS = testName.getMethodName() + "_CTAS";
    String tableNameCTASMM = testName.getMethodName() + "_CTASMM";
    WarehouseInstance.Tuple incrementalDump;

    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    insertRecords(tableName, null, false, OperationType.REPL_TEST_ACID_INSERT);
    insertRecords(tableName, tableNameCTAS, false, OperationType.REPL_TEST_ACID_CTAS);
    incrementalDump = verifyLoad(tableName, tableNameCTAS, bootStrapDump.lastReplicationId);

    insertRecords(tableNameMM, null, true, OperationType.REPL_TEST_ACID_INSERT);
    insertRecords(tableNameMM, tableNameCTASMM, true, OperationType.REPL_TEST_ACID_CTAS);
    incrementalDump = verifyLoad(tableNameMM, tableNameCTASMM, incrementalDump.lastReplicationId);
  }

  @Test
  public void testImport() throws Throwable {
    String tableName = testName.getMethodName();
    String tableNameMM = testName.getMethodName() + "_MM";
    String tableNameImport = testName.getMethodName() + "_Import";
    String tableNameImportMM = testName.getMethodName() + "_ImportMM";
    WarehouseInstance.Tuple incrementalDump;

    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    insertRecords(tableName, null, false, OperationType.REPL_TEST_ACID_INSERT);
    insertRecords(tableName, tableNameImport, false, OperationType.REPL_TEST_ACID_INSERT_IMPORT);
    incrementalDump = verifyLoad(tableName, tableNameImport, bootStrapDump.lastReplicationId);

    /*insertRecords(tableNameMM, null, true, OperationType.REPL_TEST_ACID_INSERT);
    insertRecords(tableNameMM, tableNameImportMM, true, OperationType.REPL_TEST_ACID_INSERT_IMPORT);
    incrementalDump = verifyLoad(tableNameMM, tableNameImportMM, incrementalDump.lastReplicationId);*/
  }

  @Test
  public void testInsertOverwrite() throws Throwable {
    String tableName = testName.getMethodName();
    String tableNameOW = testName.getMethodName() +"_OW";
    String tableNameMM = testName.getMethodName() + "_MM";
    String tableNameOWMM = testName.getMethodName() +"_OWMM";
    WarehouseInstance.Tuple incrementalDump;
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    insertRecords(tableName, null, false, OperationType.REPL_TEST_ACID_INSERT);
    insertRecords(tableName, tableNameOW, false, OperationType.REPL_TEST_ACID_INSERT_OVERWRITE);
    incrementalDump = verifyLoad(tableName, tableNameOW, bootStrapDump.lastReplicationId);

    insertRecords(tableNameMM, null, true, OperationType.REPL_TEST_ACID_INSERT);
    insertRecords(tableNameMM, tableNameOWMM, true, OperationType.REPL_TEST_ACID_INSERT_OVERWRITE);
    incrementalDump = verifyLoad(tableNameMM, tableNameOWMM, incrementalDump.lastReplicationId);
  }

  public void testLoodLocal() throws Throwable {
    String tableName = testName.getMethodName();
    String tableNameLL = testName.getMethodName() +"_LL";
    String tableNameMM = testName.getMethodName() + "_MM";
    String tableNameLLMM = testName.getMethodName() +"_LLMM";

    WarehouseInstance.Tuple incrementalDump;
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    insertRecords(tableName, null, false, OperationType.REPL_TEST_ACID_INSERT);
    insertRecords(tableName, tableNameLL, false, OperationType.REPL_TEST_ACID_INSERT_LOADLOCAL);
    incrementalDump = verifyLoad(tableName, tableNameLL, bootStrapDump.lastReplicationId);

    insertRecords(tableNameMM, null, true, OperationType.REPL_TEST_ACID_INSERT);
    insertRecords(tableNameMM, tableNameLLMM, true, OperationType.REPL_TEST_ACID_INSERT_LOADLOCAL);
    incrementalDump = verifyLoad(tableNameMM, tableNameLLMM, incrementalDump.lastReplicationId);
  }

  public void testInsertUnion() throws Throwable {
    String tableName = testName.getMethodName();
    String tableNameUnion = testName.getMethodName() +"_UNION";
    String tableNameMM = testName.getMethodName() + "_MM";
    String tableNameUnionMM = testName.getMethodName() +"_UNIONMM";

    WarehouseInstance.Tuple incrementalDump;
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    insertRecords(tableName, null, false, OperationType.REPL_TEST_ACID_INSERT);
    insertRecords(tableName, tableNameUnion, false, OperationType.REPL_TEST_ACID_INSERT_UNION);
    incrementalDump = verifyLoad(tableName, tableNameUnion, bootStrapDump.lastReplicationId);

    insertRecords(tableNameMM, null, true, OperationType.REPL_TEST_ACID_INSERT);
    insertRecords(tableNameMM, tableNameUnionMM, true, OperationType.REPL_TEST_ACID_INSERT_UNION);
    incrementalDump = verifyLoad(tableNameMM, tableNameUnionMM, incrementalDump.lastReplicationId);
  }

  /*@Test
  public void testImportOverWrite() throws Throwable {
    String tableName = testName.getMethodName();
    String path = "hdfs:///tmp/" + primaryDbName + "/";
    String exportPath = "'" + path + tableName + "/'";
    String importPath = "'" + path + tableName + "/data'";

    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    primary.run("use " + primaryDbName)
            .run("create table " + tableName + " (i int) CLUSTERED BY (i) into 5 buckets STORED AS ORC TBLPROPERTIES" +
                     "('transactional'='true')")
            .run("insert into table " + tableName + " values (1),(2)")
            .run("export table " + tableName + " to " + exportPath)
            .run("create table " + tableName + "_t2 like " + tableName)
            .run("load data inpath " + importPath + " overwrite into table " + tableName + "_t2")
            .run("select * from " + tableName + "_t2")
            .verifyResults(new String[] { "1", "2" });

    verifyIncrementalLoad(Collections.singletonList("select * from " + tableName + "_t2"),
            Collections.singletonList(new String[] { "1", "2" }), bootStrapDump.lastReplicationId);
  }

  @Test
  public void testImportMetadataOnly() throws Throwable {
    String tableName = testName.getMethodName();
    String importTblname = tableName + "_import";
    String path = "hdfs:///tmp/" + primaryDbName + "/";
    String exportMDPath = "'" + path + tableName + "_1/'";
    String exportDataPath = "'" + path + tableName + "_2/'";

    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName, null);
    replica.load(replicatedDbName, bootStrapDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

    primary.run("use " + primaryDbName)
            .run("create table " + tableName + " (i int) CLUSTERED BY (i) into 5 buckets STORED AS ORC TBLPROPERTIES" +
                    "('transactional'='true')")
            .run("insert into table " + tableName + " values (1),(2)")
            .run("export table " + tableName + " to " + exportMDPath + " for metadata replication('1')")
            .run("export table " + tableName + " to " + exportDataPath + " for replication('2')")
            .run("import table " + importTblname + " from " + exportMDPath)
            .run("import table " + importTblname + " from " + exportDataPath)
            .run("select * from " + importTblname)
            .verifyResults(new String[] { "1", "2" });

    verifyIncrementalLoad(Collections.singletonList("select * from " + importTblname),
            Collections.singletonList(new String[] { "1", "2" }), bootStrapDump.lastReplicationId);
  }*/

  private WarehouseInstance.Tuple verifyIncrementalLoad(List<String> selectStmtList,
                                                  List<String[]> expectedValues, String lastReplId) throws Throwable {
    WarehouseInstance.Tuple incrementalDump = primary.dump(primaryDbName, lastReplId);
    replica.loadWithoutExplain(replicatedDbName, incrementalDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName).verifyResult(incrementalDump.lastReplicationId);
    for (int idx = 0; idx > selectStmtList.size(); idx++) {
      replica.run("use " + replicatedDbName).run(selectStmtList.get(idx)).verifyResults(expectedValues.get(idx));
    }
    return incrementalDump;
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
            .run("select value from " + tableName)
            .verifyResults(new String[] {"1", "100", "100", "100", "100"});
  }

  private void truncateTable(String tableName) throws Throwable {
    primary.run("use " + primaryDbName)
            .run("truncate table " + tableName)
            .run("select count(*) from " + tableName)
            .verifyResult("0");
  }

  private WarehouseInstance.Tuple verifyLoad(String tableName, String tableNameOp, String lastReplId) throws Throwable {
    String[] resultArray = new String[]{"1", "2", "3", "4", "5"};
    if (tableNameOp == null) {
      return verifyIncrementalLoad(Collections.singletonList("select key from " + tableName),
              Collections.singletonList(resultArray), lastReplId);
    }
    return verifyIncrementalLoad(Lists.newArrayList("select key from " + tableName, "select key from " + tableNameOp),
            Lists.newArrayList(resultArray, resultArray),
            lastReplId);
  }

  private void insertIntoDB(String dbName, String tableName, String tableProperty, String[] resultArray)
          throws Throwable {
    primary.run("use " + dbName);
    primary.run("CREATE TABLE " + tableName + " (key int, value int) PARTITIONED BY (load_date date) " +
            "CLUSTERED BY(key) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ( " + tableProperty + ")")
            .run("SHOW TABLES LIKE '" + tableName + "'")
            .verifyResult(tableName)
            .run("INSERT INTO " + tableName + " partition (load_date='2016-03-01') VALUES (1, 1)")
            .run("INSERT INTO " + tableName + " partition (load_date='2016-03-01') VALUES (2, 2)")
            .run("INSERT INTO " + tableName + " partition (load_date='2016-03-02') VALUES (3, 3)")
            .run("ALTER TABLE " + tableName + " ADD PARTITION (load_date='2016-03-03')")
            .run("INSERT INTO " + tableName + " partition (load_date='2016-03-03') VALUES (4, 4)")
            .run("INSERT INTO " + tableName + " partition (load_date='2016-03-02') VALUES (5, 5)")
            .run("select key from " + tableName)
            .verifyResults(resultArray)
            .run("CREATE TABLE " + tableName + "_nopart (key int, value int) " +
            "CLUSTERED BY(key) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ( " + tableProperty + ")")
            .run("SHOW TABLES LIKE '" + tableName + "_nopart'")
            .run("INSERT INTO " + tableName + "_nopart (key, value) select key, value from " + tableName)
            .run("select key from " + tableName + "_nopart")
            .verifyResults(resultArray);
  }

  private void insertRecords(String tableName, String tableNameOp, boolean isMMTable,
                             OperationType opType) throws Throwable {
    String[] resultArray = new String[]{"1", "2", "3", "4", "5"};
    String tableProperty = "'transactional'='true'";
    if (isMMTable) {
      tableProperty = setMMtableProperty(tableProperty);
    }
    primary.run("use " + primaryDbName);

    switch (opType) {
      case REPL_TEST_ACID_INSERT:
        insertIntoDB(primaryDbName, tableName, tableProperty, resultArray);
        insertIntoDB(primaryDbNameExtra, tableName, tableProperty, resultArray);
        return;
      case REPL_TEST_ACID_INSERT_OVERWRITE:
        primary.run("CREATE TABLE " + tableNameOp + " (key int, value int) PARTITIONED BY (load_date date) " +
              "CLUSTERED BY(key) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ( "+ tableProperty + " )")
        .run("INSERT INTO " + tableNameOp + " partition (load_date='2016-03-01') VALUES (2, 2)")
        .run("INSERT INTO " + tableNameOp + " partition (load_date='2016-03-01') VALUES (10, 12)")
        .run("select key from " + tableNameOp)
        .verifyResults(new String[]{"2", "10"})
        .run("insert overwrite table " + tableNameOp + " partition (load_date) select * from " + tableName);
        break;
      case REPL_TEST_ACID_INSERT_SELECT:
        primary.run("CREATE TABLE " + tableNameOp + " (key int, value int) PARTITIONED BY (load_date date) " +
            "CLUSTERED BY(key) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ( "+ tableProperty + " )")
        .run("insert into " + tableNameOp + " partition (load_date) select * from " + tableName);
        break;
      case REPL_TEST_ACID_INSERT_IMPORT:
        String path = "hdfs:///tmp/" + primaryDbName + "/";
        String exportPath = "'" + path + tableName + "/'";
        primary.run("export table " + tableName + " to " + exportPath)
        .run("import table " + tableNameOp + " from " + exportPath);
        break;
      case REPL_TEST_ACID_CTAS:
        primary.run("create table " + tableNameOp + " as select * from " + tableName);
        break;
      case REPL_TEST_ACID_INSERT_LOADLOCAL:
        primary.run("CREATE TABLE " + tableNameOp + " (key int, value int) PARTITIONED BY (load_date date) " +
              "CLUSTERED BY(key) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ( " + tableProperty + ")")
        .run("SHOW TABLES LIKE '" + tableNameOp + "'")
        .verifyResult(tableNameOp)
        .run("INSERT OVERWRITE LOCAL DIRECTORY '/tmp/' SELECT a.* FROM" + tableName + " a")
        .run("LOAD DATA LOCAL INPATH '/tmp/' OVERWRITE INTO TABLE " + tableNameOp +
                " PARTITION (load_date='2008-08-15')");
      case REPL_TEST_ACID_INSERT_UNION:
        primary.run("CREATE TABLE " + tableNameOp + " (key int, value int) PARTITIONED BY (load_date date) " +
                "CLUSTERED BY(key) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ( " + tableProperty + ")")
                .run("SHOW TABLES LIKE '" + tableNameOp + "'")
                .verifyResult(tableNameOp)
                .run("insert overwrite table " + tableNameOp + " partition (load_date) select * from " + tableName +
                    " union all select * from " + tableName);
        resultArray = new String[]{"1", "2", "3", "4", "5", "1", "2", "3", "4", "5"};
        break;
      default:
        return;
    }
    primary.run("select key from " + tableNameOp).verifyResults(resultArray);
  }

  private String setMMtableProperty(String tableProperty) throws Throwable  {
    return tableProperty.concat(", 'transactional_properties' = 'insert_only'");
  }

  private void insertForMerge(String tableName, String tableNameMerge, boolean isMMTable) throws Throwable  {
    String tableProperty = "'transactional'='true'";
    if (isMMTable) {
      tableProperty = setMMtableProperty(tableProperty);
    }
    primary.run("use " + primaryDbName)
        .run("CREATE TABLE " + tableName + "( ID int, TranValue string, last_update_user string) PARTITIONED BY " +
                "(tran_date string) CLUSTERED BY (ID) into 5 buckets STORED AS ORC TBLPROPERTIES " +
                " ( "+ tableProperty + " )")
        .run("SHOW TABLES LIKE '" + tableName + "'")
        .verifyResult(tableName)
        .run("CREATE TABLE " + tableNameMerge + " ( ID int, TranValue string, tran_date string) STORED AS ORC ")
        .run("SHOW TABLES LIKE '" + tableNameMerge + "'")
        .verifyResult(tableNameMerge)
        .run("INSERT INTO " + tableName + " PARTITION (tran_date) VALUES (1, 'value_01', 'creation', '20170410')," +
                " (2, 'value_02', 'creation', '20170410'), (3, 'value_03', 'creation', '20170410'), " +
                " (4, 'value_04', 'creation', '20170410'), (5, 'value_05', 'creation', '20170413'), " +
                " (6, 'value_06', 'creation', '20170413'), (7, 'value_07', 'creation', '20170413'),  " +
                " (8, 'value_08', 'creation', '20170413'), (9, 'value_09', 'creation', '20170413'), " +
                " (10, 'value_10','creation', '20170413')")
        .run("select ID from " + tableName)
        .verifyResults(new String[] {"1", "2" , "3", "4", "5", "6", "7", "8", "9", "10"})
        .run("INSERT INTO " + tableNameMerge + " VALUES (1, 'value_01', '20170410'), " +
                " (4, NULL, '20170410'), (7, 'value_77777', '20170413'), " +
                " (8, NULL, '20170413'), (8, 'value_08', '20170415'), " +
                "(11, 'value_11', '20170415')")
        .run("select ID from " + tableNameMerge)
        .verifyResults(new String[] {"1", "4", "7", "8", "8", "11"})
        .run("MERGE INTO " + tableName + " AS T USING " + tableNameMerge + " AS S ON T.ID = S.ID and" +
                " T.tran_date = S.tran_date WHEN MATCHED AND (T.TranValue != S.TranValue AND S.TranValue " +
                " IS NOT NULL) THEN UPDATE SET TranValue = S.TranValue, last_update_user = " +
                " 'merge_update' WHEN MATCHED AND S.TranValue IS NULL THEN DELETE WHEN NOT MATCHED " +
                " THEN INSERT VALUES (S.ID, S.TranValue,'merge_insert', S.tran_date)")
        .run("select last_update_user from " + tableName)
        .verifyResults(new String[] {"creation","creation","creation","creation","creation",
                "creation","creation","merge_update","merge_insert","merge_insert"});
>>>>>>> HIVE-19267 : Create/Replicate ACID Write event
>>>>>>> HIVE-19267 : Create/Replicate ACID Write event
  }
}
