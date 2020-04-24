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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.CallerArguments;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.BehaviourInjection;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.Utils;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.BeforeClass;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Collections;
import java.util.Map;

import static org.apache.hadoop.hive.metastore.ReplChangeManager.SOURCE_OF_REPLICATION;
import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.DUMP_ACKNOWLEDGEMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * TestReplicationScenariosAcidTables - test replication for ACID tables.
 */
public class TestReplicationScenariosAcidTables extends BaseReplicationScenariosAcidTables {

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());

    internalBeforeClassSetup(overrides, TestReplicationScenariosAcidTables.class);
  }

  static void internalBeforeClassSetup(Map<String, String> overrides,
      Class clazz) throws Exception {

    conf = new HiveConf(clazz);
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("metastore.warehouse.tenant.colocation", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    MiniDFSCluster miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    Map<String, String> acidEnableConf = new HashMap<String, String>() {{
        put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
        put("hive.support.concurrency", "true");
        put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        put("hive.metastore.client.capability.check", "false");
        put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
        put("hive.strict.checks.bucketing", "false");
        put("hive.mapred.mode", "nonstrict");
        put("mapred.input.dir.recursive", "true");
        put("hive.metastore.disallow.incompatible.col.type.changes", "false");
        put("metastore.warehouse.tenant.colocation", "true");
        put("hive.in.repl.test", "true");
      }};

    acidEnableConf.putAll(overrides);

    primary = new WarehouseInstance(LOG, miniDFSCluster, acidEnableConf);
    acidEnableConf.put(MetastoreConf.ConfVars.REPLDIR.getHiveName(), primary.repldDir);
    replica = new WarehouseInstance(LOG, miniDFSCluster, acidEnableConf);
    Map<String, String> overridesForHiveConf1 = new HashMap<String, String>() {{
          put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
          put("hive.support.concurrency", "false");
          put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
          put("hive.metastore.client.capability.check", "false");
      }};
    overridesForHiveConf1.put(MetastoreConf.ConfVars.REPLDIR.getHiveName(), primary.repldDir);
    replicaNonAcid = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConf1);
  }

  @Before
  public void setup() throws Throwable {
    super.setup();
  }

  @After
  public void tearDown() throws Throwable {
    primary.run("drop database if exists " + primaryDbName + " cascade");
    replica.run("drop database if exists " + replicatedDbName + " cascade");
    replicaNonAcid.run("drop database if exists " + replicatedDbName + " cascade");
    primary.run("drop database if exists " + primaryDbName + "_extra cascade");
  }

  @Test
  public void testAcidTablesBootstrap() throws Throwable {
    // Bootstrap
    WarehouseInstance.Tuple bootstrapDump = prepareDataAndDump(primaryDbName, null);
    replica.load(replicatedDbName, primaryDbName);
    verifyLoadExecution(replicatedDbName, bootstrapDump.lastReplicationId, true);

    // First incremental, after bootstrap
    prepareIncNonAcidData(primaryDbName);
    prepareIncAcidData(primaryDbName);
    LOG.info(testName.getMethodName() + ": first incremental dump and load.");
    WarehouseInstance.Tuple incDump = primary.run("use " + primaryDbName)
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);
    verifyIncLoad(replicatedDbName, incDump.lastReplicationId);

    // Second incremental, after bootstrap
    prepareInc2NonAcidData(primaryDbName, primary.hiveConf);
    prepareInc2AcidData(primaryDbName, primary.hiveConf);
    LOG.info(testName.getMethodName() + ": second incremental dump and load.");
    WarehouseInstance.Tuple inc2Dump = primary.run("use " + primaryDbName)
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);
    verifyInc2Load(replicatedDbName, inc2Dump.lastReplicationId);
  }

  @Test
  public void testAcidTablesMoveOptimizationBootStrap() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = prepareDataAndDump(primaryDbName, null);
    replica.load(replicatedDbName, primaryDbName,
            Collections.singletonList("'hive.repl.enable.move.optimization'='true'"));
    verifyLoadExecution(replicatedDbName, bootstrapDump.lastReplicationId, true);
  }

  @Test
  public void testAcidTablesMoveOptimizationIncremental() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName,
            Collections.singletonList("'hive.repl.enable.move.optimization'='true'"));
    WarehouseInstance.Tuple incrDump = prepareDataAndDump(primaryDbName, null);
    replica.load(replicatedDbName, primaryDbName,
            Collections.singletonList("'hive.repl.enable.move.optimization'='true'"));
    verifyLoadExecution(replicatedDbName, incrDump.lastReplicationId, true);
  }

  @Test
  public void testAcidTablesBootstrapWithOpenTxnsTimeout() throws Throwable {
    int numTxns = 5;
    HiveConf primaryConf = primary.getConf();
    TxnStore txnHandler = TxnUtils.getTxnStore(primary.getConf());
    // Open 5 txns
    List<Long> txns = openTxns(numTxns, txnHandler, primaryConf);

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
    Map<String, Long> tables = new HashMap<>();
    tables.put("t1", numTxns+1L);
    tables.put("t2", numTxns+2L);
    allocateWriteIdsForTables(primaryDbName, tables, txnHandler, txns, primaryConf);

    // Bootstrap dump with open txn timeout as 1s.
    List<String> withConfigs = Arrays.asList(
            "'hive.repl.bootstrap.dump.open.txn.timeout'='1s'");
    WarehouseInstance.Tuple bootstrapDump = primary
            .run("use " + primaryDbName)
            .dump(primaryDbName, withConfigs);

    // After bootstrap dump, all the opened txns should be aborted. Verify it.
    verifyAllOpenTxnsAborted(txns, primaryConf);
    verifyNextId(tables, primaryDbName, primaryConf);

    // Bootstrap load which should also replicate the aborted write ids on both tables.
    HiveConf replicaConf = replica.getConf();
    replica.load(replicatedDbName, primaryDbName)
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
    verifyNextId(tables, replicatedDbName, replicaConf);

    // Verify if all the aborted write ids are replicated to the replicated DB
    for(Map.Entry<String, Long> entry : tables.entrySet()) {
      entry.setValue((long) numTxns);
    }
    verifyWriteIdsForTables(tables, replicaConf, replicatedDbName);

    // Verify if entries added in COMPACTION_QUEUE for each table/partition
    // t1-> 1 entry and t2-> 2 entries (1 per partition)
    tables.clear();
    tables.put("t1", 1L);
    tables.put("t2", 2L);
    verifyCompactionQueue(tables, replicatedDbName, replicaConf);
  }

  @Test
  public void testAcidTablesBootstrapWithConcurrentWrites() throws Throwable {
    HiveConf primaryConf = primary.getConf();
    primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(1)");

    // Perform concurrent write on the acid table t1 when bootstrap dump in progress. Bootstrap
    // won't see the written data but the subsequent incremental repl should see it.
    BehaviourInjection<CallerArguments, Boolean> callerInjectedBehavior
            = new BehaviourInjection<CallerArguments, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable CallerArguments args) {
        if (injectionPathCalled) {
          nonInjectedPathCalled = true;
        } else {
          // Insert another row to t1 from another txn when bootstrap dump in progress.
          injectionPathCalled = true;
          Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
              LOG.info("Entered new thread");
              IDriver driver = DriverFactory.newDriver(primaryConf);
              SessionState.start(new CliSessionState(primaryConf));
              try {
                driver.run("insert into " + primaryDbName + ".t1 values(2)");
              } catch (CommandProcessorException e) {
                throw new RuntimeException(e);
              }
              LOG.info("Exit new thread success");
            }
          });
          t.start();
          LOG.info("Created new thread {}", t.getName());
          try {
            t.join();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return true;
      }
    };

    InjectableBehaviourObjectStore.setCallerVerifier(callerInjectedBehavior);
    WarehouseInstance.Tuple bootstrapDump = null;
    try {
      bootstrapDump = primary.dump(primaryDbName);
      callerInjectedBehavior.assertInjectionsPerformed(true, true);
    } finally {
      InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour
    }

    // Bootstrap dump has taken snapshot before concurrent tread performed write. So, it won't see data "2".
    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(bootstrapDump.lastReplicationId)
            .run("select id from t1 order by id")
            .verifyResults(new String[]{"1" });

    // Incremental should include the concurrent write of data "2" from another txn.
    WarehouseInstance.Tuple incrementalDump = primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId)
            .run("select id from t1 order by id")
            .verifyResults(new String[]{"1", "2" });
  }

  @Test
  public void testAcidTablesBootstrapWithConcurrentDropTable() throws Throwable {
    HiveConf primaryConf = primary.getConf();
    primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(1)");

    // Perform concurrent write + drop on the acid table t1 when bootstrap dump in progress. Bootstrap
    // won't dump the table but the subsequent incremental repl with new table with same name should be seen.
    BehaviourInjection<CallerArguments, Boolean> callerInjectedBehavior
            = new BehaviourInjection<CallerArguments, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable CallerArguments args) {
        if (injectionPathCalled) {
          nonInjectedPathCalled = true;
        } else {
          // Insert another row to t1 and drop the table from another txn when bootstrap dump in progress.
          injectionPathCalled = true;
          Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
              LOG.info("Entered new thread");
              IDriver driver = DriverFactory.newDriver(primaryConf);
              SessionState.start(new CliSessionState(primaryConf));
              try {
                driver.run("insert into " + primaryDbName + ".t1 values(2)");
                driver.run("drop table " + primaryDbName + ".t1");
              } catch (CommandProcessorException e) {
                throw new RuntimeException(e);
              }
              LOG.info("Exit new thread success");
            }
          });
          t.start();
          LOG.info("Created new thread {}", t.getName());
          try {
            t.join();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return true;
      }
    };

    InjectableBehaviourObjectStore.setCallerVerifier(callerInjectedBehavior);
    WarehouseInstance.Tuple bootstrapDump = null;
    try {
      bootstrapDump = primary.dump(primaryDbName);
      callerInjectedBehavior.assertInjectionsPerformed(true, true);
    } finally {
      InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour
    }

    // Bootstrap dump has taken latest list of tables and hence won't see table t1 as it is dropped.
    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(bootstrapDump.lastReplicationId)
            .run("show tables")
            .verifyResult(null);

    // Create another ACID table with same name and insert a row. It should be properly replicated.
    WarehouseInstance.Tuple incrementalDump = primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(100)")
            .dump(primaryDbName);

    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId)
            .run("select id from t1 order by id")
            .verifyResult("100");
  }

  @Test
  public void testOpenTxnEvent() throws Throwable {
    String tableName = testName.getMethodName();
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(bootStrapDump.lastReplicationId);

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
            primary.dump(primaryDbName);

    long lastReplId = Long.parseLong(bootStrapDump.lastReplicationId);
    primary.testEventCounts(primaryDbName, lastReplId, null, null, 22);

    // Test load
    replica.load(replicatedDbName, primaryDbName)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId);

    // Test the idempotent behavior of Open and Commit Txn
    replica.load(replicatedDbName, primaryDbName)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId);
  }

  @Test
  public void testAbortTxnEvent() throws Throwable {
    String tableNameFail = testName.getMethodName() + "Fail";
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
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
            primary.dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId);

    // Test the idempotent behavior of Abort Txn
    replica.load(replicatedDbName, primaryDbName)
            .run("REPL STATUS " + replicatedDbName)
            .verifyResult(incrementalDump.lastReplicationId);
  }

  @Test
  public void testTxnEventNonAcid() throws Throwable {
    String tableName = testName.getMethodName();
    WarehouseInstance.Tuple bootStrapDump = primary.dump(primaryDbName);
    replicaNonAcid.load(replicatedDbName, primaryDbName)
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
            primary.dump(primaryDbName);
    replicaNonAcid.runFailure("REPL LOAD " + primaryDbName + " INTO " + replicatedDbName + "'")
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
            .dump(primaryDbName);

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
    replica.loadFailure(replicatedDbName, primaryDbName, withConfigs);
    callerVerifier.assertInjectionsPerformed(true, false);
    InjectableBehaviourObjectStore.resetCallerVerifier(); // reset the behaviour

    replica.run("use " + replicatedDbName)
            .run("repl status " + replicatedDbName)
            .verifyResult("null")
            .run("show tables like t2")
            .verifyResults(new String[] { })
            .verifyReplTargetProperty(replicatedDbName);

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
    replica.load(replicatedDbName, primaryDbName);
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
            .verifyResults(Arrays.asList("bob", "carl"))
            .verifyReplTargetProperty(replicatedDbName);
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

    try {
      primary.runCommand("REPL DUMP " + dbName + " with ('hive.repl.dump.include.acid.tables' = 'true')");
      assert false;
    } catch (CommandProcessorException e) {
      Assert.assertEquals(e.getResponseCode(), ErrorMsg.FILE_NOT_FOUND.getErrorCode());
    }

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

    try {
      primary.runCommand("REPL DUMP " + dbName + " with ('hive.repl.dump.include.acid.tables' = 'true')");
      assert false;
    } catch (CommandProcessorException e) {
      Assert.assertEquals(e.getResponseCode(), ErrorMsg.FILE_NOT_FOUND.getErrorCode());
    }

    primary.run("DROP TABLE " + dbName + ".normal");
    primary.run("drop database " + dbName);
  }

  @Test
  public void testMultiDBTxn() throws Throwable {
    String tableName = testName.getMethodName();
    String dbName1 = tableName + "_db1";
    String dbName2 = tableName + "_db2";
    String[] resultArray = new String[]{"1", "2", "3", "4", "5"};
    String tableProperty = "'transactional'='true'";
    String txnStrStart = "START TRANSACTION";
    String txnStrCommit = "COMMIT";

    primary.run("alter database default set dbproperties ('repl.source.for' = '1, 2, 3')");

    primary.run("use " + primaryDbName)
          .run("create database " + dbName1 + " WITH DBPROPERTIES ( '" + SOURCE_OF_REPLICATION + "' = '1,2,3')")
          .run("create database " + dbName2 + " WITH DBPROPERTIES ( '" + SOURCE_OF_REPLICATION + "' = '1,2,3')")
          .run("CREATE TABLE " + dbName1 + "." + tableName + " (key int, value int) PARTITIONED BY (load_date date) " +
                  "CLUSTERED BY(key) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ( " + tableProperty + ")")
          .run("use " + dbName1)
          .run("SHOW TABLES LIKE '" + tableName + "'")
          .verifyResult(tableName)
          .run("CREATE TABLE " + dbName2 + "." + tableName + " (key int, value int) PARTITIONED BY (load_date date) " +
                  "CLUSTERED BY(key) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ( " + tableProperty + ")")
          .run("use " + dbName2)
          .run("SHOW TABLES LIKE '" + tableName + "'")
          .verifyResult(tableName)
          .run(txnStrStart)
          .run("INSERT INTO " + dbName2 + "." + tableName + " partition (load_date='2016-03-02') VALUES (5, 5)")
          .run("INSERT INTO " + dbName1 + "." + tableName + " partition (load_date='2016-03-01') VALUES (1, 1)")
          .run("INSERT INTO " + dbName1 + "." + tableName + " partition (load_date='2016-03-01') VALUES (2, 2)")
          .run("INSERT INTO " + dbName2 + "." + tableName + " partition (load_date='2016-03-01') VALUES (2, 2)")
          .run("INSERT INTO " + dbName2 + "." + tableName + " partition (load_date='2016-03-02') VALUES (3, 3)")
          .run("INSERT INTO " + dbName1 + "." + tableName + " partition (load_date='2016-03-02') VALUES (3, 3)")
          .run("INSERT INTO " + dbName1 + "." + tableName + " partition (load_date='2016-03-03') VALUES (4, 4)")
          .run("INSERT INTO " + dbName1 + "." + tableName + " partition (load_date='2016-03-02') VALUES (5, 5)")
          .run("INSERT INTO " + dbName2 + "." + tableName + " partition (load_date='2016-03-01') VALUES (1, 1)")
          .run("INSERT INTO " + dbName2 + "." + tableName + " partition (load_date='2016-03-03') VALUES (4, 4)")
          .run("select key from " + dbName2 + "." + tableName + " order by key")
          .verifyResults(resultArray)
          .run("select key from " + dbName1 + "." + tableName + " order by key")
          .verifyResults(resultArray)
          .run(txnStrCommit);

    primary.dump("`*`");

    // Due to the limitation that we can only have one instance of Persistence Manager Factory in a JVM
    // we are not able to create multiple embedded derby instances for two different MetaStore instances.
    primary.run("drop database " + primaryDbName + " cascade");
    primary.run("drop database " + dbName1 + " cascade");
    primary.run("drop database " + dbName2 + " cascade");
    //End of additional steps
    try {
      replica.loadWithoutExplain("", "`*`");
    } catch (SemanticException e) {
      assertEquals("REPL LOAD * is not supported", e.getMessage());
    }
  }

  @Test
  public void testCheckPointingDataDumpFailure() throws Throwable {
    //To force distcp copy
    List<String> dumpClause = Arrays.asList(
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
            "'" + HiveConf.ConfVars.HIVE_IN_TEST.varname + "'='false'",
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
            "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
                    + UserGroupInformation.getCurrentUser().getUserName() + "'");

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("insert into t1 values (3)")
            .run("insert into t2 values (11)")
            .run("insert into t2 values (21)")
            .dump(primaryDbName);

    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    Path metadataPath = new Path(dumpPath, EximUtil.METADATA_PATH_NAME);
    long modifiedTimeMetadata = fs.getFileStatus(metadataPath).getModificationTime();
    Path dataPath = new Path(dumpPath, EximUtil.DATA_PATH_NAME);
    Path dbDataPath = new Path(dataPath, primaryDbName.toLowerCase());
    Path tablet1Path = new Path(dbDataPath, "t1");
    Path tablet2Path = new Path(dbDataPath, "t2");
    //Delete dump ack and t2 data, metadata should be rewritten, data should be same for t1 but rewritten for t2
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), true);
    assertFalse(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    FileStatus[] statuses = fs.listStatus(tablet2Path);
    //Delete t2 data
    fs.delete(statuses[0].getPath(), true);
    long modifiedTimeTable1 = fs.getFileStatus(tablet1Path).getModificationTime();
    long modifiedTimeTable1CopyFile = fs.listStatus(tablet1Path)[0].getModificationTime();
    long modifiedTimeTable2 = fs.getFileStatus(tablet2Path).getModificationTime();
    //Do another dump. It should only dump table t2. Modification time of table t1 should be same while t2 is greater
    WarehouseInstance.Tuple nextDump = primary.dump(primaryDbName, dumpClause);
    assertEquals(nextDump.dumpLocation, bootstrapDump.dumpLocation);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertEquals(modifiedTimeTable1, fs.getFileStatus(tablet1Path).getModificationTime());
    assertEquals(modifiedTimeTable1CopyFile, fs.listStatus(tablet1Path)[0].getModificationTime());
    assertTrue(modifiedTimeTable2 < fs.getFileStatus(tablet2Path).getModificationTime());
    assertTrue(modifiedTimeMetadata < fs.getFileStatus(metadataPath).getModificationTime());
    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1", "2", "3"})
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[]{"11", "21"});
  }

  @Test
  public void testCheckPointingDataDumpFailureRegularCopy() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("insert into t1 values (3)")
            .run("insert into t2 values (11)")
            .run("insert into t2 values (21)")
            .dump(primaryDbName);
    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    Path metadataPath = new Path(dumpPath, EximUtil.METADATA_PATH_NAME);
    long modifiedTimeMetadata = fs.getFileStatus(metadataPath).getModificationTime();
    Path dataPath = new Path(dumpPath, EximUtil.DATA_PATH_NAME);
    Path dbPath = new Path(dataPath, primaryDbName.toLowerCase());
    Path tablet1Path = new Path(dbPath, "t1");
    Path tablet2Path = new Path(dbPath, "t2");
    //Delete dump ack and t2 data, metadata should be rewritten, data should be same for t1 but rewritten for t2
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), true);
    assertFalse(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    FileStatus[] statuses = fs.listStatus(tablet2Path);
    //Delete t2 data
    fs.delete(statuses[0].getPath(), true);
    long modifiedTimeTable1 = fs.getFileStatus(tablet1Path).getModificationTime();
    long modifiedTimeTable1CopyFile = fs.listStatus(tablet1Path)[0].getModificationTime();
    long modifiedTimeTable2 = fs.getFileStatus(tablet2Path).getModificationTime();
    //Do another dump. It should only dump table t2. Modification time of table t1 should be same while t2 is greater
    WarehouseInstance.Tuple nextDump = primary.dump(primaryDbName);
    assertEquals(nextDump.dumpLocation, bootstrapDump.dumpLocation);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    //File is copied again as we are using regular copy
    assertTrue(modifiedTimeTable1 < fs.getFileStatus(tablet1Path).getModificationTime());
    assertTrue(modifiedTimeTable1CopyFile < fs.listStatus(tablet1Path)[0].getModificationTime());
    assertTrue(modifiedTimeTable2 < fs.getFileStatus(tablet2Path).getModificationTime());
    assertTrue(modifiedTimeMetadata < fs.getFileStatus(metadataPath).getModificationTime());
    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1", "2", "3"})
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[]{"11", "21"});
  }

  @Test
  public void testCheckPointingDataDumpFailureORCTableRegularCopy() throws Throwable {
    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a int) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("CREATE TABLE t2(a string) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("CREATE TABLE t3(a string) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("insert into t1 values (3)")
            .run("insert into t2 values (11)")
            .run("insert into t2 values (21)")
            .dump(primaryDbName);
    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    Path metadataPath = new Path(dumpPath, EximUtil.METADATA_PATH_NAME);
    long modifiedTimeMetadata = fs.getFileStatus(metadataPath).getModificationTime();
    Path dataPath = new Path(dumpPath, EximUtil.DATA_PATH_NAME);
    Path dbPath = new Path(dataPath, primaryDbName.toLowerCase());
    Path tablet1Path = new Path(dbPath, "t1");
    Path tablet2Path = new Path(dbPath, "t2");
    //Delete dump ack and t2 data, metadata should be rewritten,
    // data will also be rewritten for t1 and t2 as regular copy
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), true);
    assertFalse(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    FileStatus[] statuses = fs.listStatus(tablet2Path);
    //Delete t2 data
    fs.delete(statuses[0].getPath(), true);
    long modifiedTimeTable1 = fs.getFileStatus(tablet1Path).getModificationTime();
    long modifiedTimeTable1CopyFile = fs.listStatus(tablet1Path)[0].getModificationTime();
    long modifiedTimeTable2 = fs.getFileStatus(tablet2Path).getModificationTime();
    WarehouseInstance.Tuple nextDump = primary.dump(primaryDbName);
    assertEquals(nextDump.dumpLocation, bootstrapDump.dumpLocation);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    //File is copied again as we are using regular copy
    assertTrue(modifiedTimeTable1 < fs.getFileStatus(tablet1Path).getModificationTime());
    assertTrue(modifiedTimeTable1CopyFile < fs.listStatus(tablet1Path)[0].getModificationTime());
    assertTrue(modifiedTimeTable2 < fs.getFileStatus(tablet2Path).getModificationTime());
    assertTrue(modifiedTimeMetadata < fs.getFileStatus(metadataPath).getModificationTime());
    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("show tables ")
            .verifyResults(new String[]{"t1", "t2", "t3"})
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1", "2", "3"})
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[]{"11", "21"});
  }

  @Test
  public void testCheckPointingDataDumpFailureORCTableDistcpCopy() throws Throwable {
    //Distcp copy
    List<String> dumpClause = Arrays.asList(
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
            "'" + HiveConf.ConfVars.HIVE_IN_TEST.varname + "'='false'",
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
            "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
                    + UserGroupInformation.getCurrentUser().getUserName() + "'");
    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a int) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("CREATE TABLE t2(a string) clustered by (a) into 2 buckets" +
                    " stored as orc TBLPROPERTIES ('transactional'='true')")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("insert into t1 values (3)")
            .run("insert into t2 values (11)")
            .run("insert into t2 values (21)")
            .dump(primaryDbName, dumpClause);
    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    Path metadataPath = new Path(dumpPath, EximUtil.METADATA_PATH_NAME);
    long modifiedTimeMetadata = fs.getFileStatus(metadataPath).getModificationTime();
    Path dataPath = new Path(dumpPath, EximUtil.DATA_PATH_NAME);
    Path dbPath = new Path(dataPath, primaryDbName.toLowerCase());
    Path tablet1Path = new Path(dbPath, "t1");
    Path tablet2Path = new Path(dbPath, "t2");
    //Delete dump ack and t2 data, metadata should be rewritten, data should be same for t1 but rewritten for t2
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), true);
    assertFalse(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    FileStatus[] statuses = fs.listStatus(tablet2Path);
    //Delete t2 data
    fs.delete(statuses[0].getPath(), true);
    long modifiedTimeTable1 = fs.getFileStatus(tablet1Path).getModificationTime();
    long modifiedTimeTable1CopyFile = fs.listStatus(tablet1Path)[0].getModificationTime();
    long modifiedTimeTable2 = fs.getFileStatus(tablet2Path).getModificationTime();
    //Do another dump. It should only dump table t2. Modification time of table t1 should be same while t2 is greater
    WarehouseInstance.Tuple nextDump = primary.dump(primaryDbName, dumpClause);
    assertEquals(nextDump.dumpLocation, bootstrapDump.dumpLocation);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertEquals(modifiedTimeTable1, fs.getFileStatus(tablet1Path).getModificationTime());
    assertEquals(modifiedTimeTable1CopyFile, fs.listStatus(tablet1Path)[0].getModificationTime());
    assertTrue(modifiedTimeTable2 < fs.getFileStatus(tablet2Path).getModificationTime());
    assertTrue(modifiedTimeMetadata < fs.getFileStatus(metadataPath).getModificationTime());
    replica.load(replicatedDbName, primaryDbName)
            .run("select * from " + replicatedDbName + ".t1")
            .verifyResults(new String[] {"1", "2", "3"})
            .run("select * from " + replicatedDbName + ".t2")
            .verifyResults(new String[]{"11", "21"});
  }

  @Test
  public void testCheckPointingWithSourceTableDataInserted() throws Throwable {
    //To force distcp copy
    List<String> dumpClause = Arrays.asList(
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
            "'" + HiveConf.ConfVars.HIVE_IN_TEST.varname + "'='false'",
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
            "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
                    + UserGroupInformation.getCurrentUser().getUserName() + "'");

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("insert into t1 values (3)")
            .run("insert into t2 values (11)")
            .run("insert into t2 values (21)")
            .dump(primaryDbName);

    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    Path dataPath = new Path(dumpPath, EximUtil.DATA_PATH_NAME);
    Path dbPath = new Path(dataPath, primaryDbName.toLowerCase());
    Path tablet1Path = new Path(dbPath, "t1");
    Path tablet2Path = new Path(dbPath, "t2");
    long modifiedTimeTable2 = fs.getFileStatus(tablet2Path).getModificationTime();
    //Delete table 2 data
    FileStatus[] statuses = fs.listStatus(tablet2Path);
    //Delete t2 data
    fs.delete(statuses[0].getPath(), true);
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), true);
    long modifiedTimeTable1CopyFile = fs.listStatus(tablet1Path)[0].getModificationTime();

    //Do another dump. It should only dump table t2. Also insert new data in existing tables.
    // New data should be there in target
    primary.run("use " + primaryDbName)
            .run("insert into t2 values (13)")
            .run("insert into t2 values (24)")
            .run("insert into t1 values (4)")
            .dump(primaryDbName, dumpClause);

    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select * from t1")
            .verifyResults(new String[]{"1", "2", "3", "4"})
            .run("select * from t2")
            .verifyResults(new String[]{"11", "21", "13", "24"});
    assertEquals(modifiedTimeTable1CopyFile, fs.listStatus(tablet1Path)[0].getModificationTime());
    assertTrue(modifiedTimeTable2 < fs.getFileStatus(tablet2Path).getModificationTime());
  }

  @Test
  public void testCheckPointingWithNewTablesAdded() throws Throwable {
    //To force distcp copy
    List<String> dumpClause = Arrays.asList(
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
            "'" + HiveConf.ConfVars.HIVE_IN_TEST.varname + "'='false'",
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
            "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
                    + UserGroupInformation.getCurrentUser().getUserName() + "'");

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("insert into t1 values (3)")
            .run("insert into t2 values (11)")
            .run("insert into t2 values (21)")
            .dump(primaryDbName);

    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    Path dataPath = new Path(dumpPath, EximUtil.DATA_PATH_NAME);
    Path dbPath = new Path(dataPath, primaryDbName.toLowerCase());
    Path tablet1Path = new Path(dbPath, "t1");
    Path tablet2Path = new Path(dbPath, "t2");
    long modifiedTimeTable1 = fs.getFileStatus(tablet1Path).getModificationTime();
    long modifiedTimeTable2 = fs.getFileStatus(tablet2Path).getModificationTime();
    //Delete table 2 data
    FileStatus[] statuses = fs.listStatus(tablet2Path);
    fs.delete(statuses[0].getPath(), true);
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), true);
    long modifiedTimeTable1CopyFile = fs.listStatus(tablet1Path)[0].getModificationTime();

    // Do another dump. It should only dump table t2 and next table.
    // Also insert new tables. New tables will be there in target
    primary.run("use " + primaryDbName)
            .run("insert into t2 values (13)")
            .run("insert into t2 values (24)")
            .run("create table t3(a string) STORED AS TEXTFILE")
            .run("insert into t3 values (1)")
            .run("insert into t3 values (2)")
            .run("insert into t3 values (3)")
            .dump(primaryDbName, dumpClause);

    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select * from t1")
            .verifyResults(new String[]{"1", "2", "3"})
            .run("select * from t2")
            .verifyResults(new String[]{"11", "21", "13", "24"})
            .run("show tables")
            .verifyResults(new String[]{"t1", "t2", "t3"})
            .run("select * from t3")
            .verifyResults(new String[]{"1", "2", "3"});
    assertEquals(modifiedTimeTable1, fs.getFileStatus(tablet1Path).getModificationTime());
    assertEquals(modifiedTimeTable1CopyFile, fs.listStatus(tablet1Path)[0].getModificationTime());
    assertTrue(modifiedTimeTable2 < fs.getFileStatus(tablet2Path).getModificationTime());
  }

  @Test
  public void testCheckPointingWithSourceTableDeleted() throws Throwable {
    //To force distcp copy
    List<String> dumpClause = Arrays.asList(
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
            "'" + HiveConf.ConfVars.HIVE_IN_TEST.varname + "'='false'",
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
            "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
                    + UserGroupInformation.getCurrentUser().getUserName() + "'");

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("insert into t1 values (3)")
            .run("insert into t2 values (11)")
            .run("insert into t2 values (21)")
            .dump(primaryDbName);

    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));


    //Delete dump ack and t2 data, Also drop table. New data will be there in target
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), true);
    assertFalse(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    Path dataPath = new Path(dumpPath, EximUtil.DATA_PATH_NAME);
    Path dbPath = new Path(dataPath, primaryDbName.toLowerCase());
    Path tablet2Path = new Path(dbPath, "t2");
    FileStatus[] statuses = fs.listStatus(tablet2Path);
    //Delete t2 data.
    fs.delete(statuses[0].getPath(), true);
    //Drop table t1. Target shouldn't have t1 table as metadata dump is rewritten
    primary.run("use " + primaryDbName)
            .run("drop table t1")
            .dump(primaryDbName, dumpClause);

    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[]{"t2"})
            .run("select * from t2")
            .verifyResults(new String[]{"11", "21"});
  }

  @Test
  public void testCheckPointingMetadataDumpFailure() throws Throwable {
    //To force distcp copy
    List<String> dumpClause = Arrays.asList(
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
            "'" + HiveConf.ConfVars.HIVE_IN_TEST.varname + "'='false'",
            "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
            "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
                    + UserGroupInformation.getCurrentUser().getUserName() + "'");

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
            .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
            .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
            .run("insert into t1 values (1)")
            .run("insert into t1 values (2)")
            .run("insert into t1 values (3)")
            .run("insert into t2 values (11)")
            .run("insert into t2 values (21)")
            .dump(primaryDbName);
    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));

    //Delete dump ack and metadata ack, everything should be rewritten in a new dump dir
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), true);
    fs.delete(new Path(dumpPath, "_dumpmetadata"), true);
    assertFalse(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    //Insert new data
    primary.run("insert into "+ primaryDbName +".t1 values (12)");
    primary.run("insert into "+ primaryDbName +".t1 values (13)");
    //Do another dump. It should dump everything in a new dump dir
    // checkpointing will not be used
    WarehouseInstance.Tuple nextDump = primary.dump(primaryDbName, dumpClause);
    replica.load(replicatedDbName, primaryDbName)
            .run("use " + replicatedDbName)
            .run("select * from t2")
            .verifyResults(new String[]{"11", "21"})
            .run("select * from t1")
            .verifyResults(new String[]{"1", "2", "3", "12", "13"});
    assertNotEquals(nextDump.dumpLocation, bootstrapDump.dumpLocation);
    dumpPath = new Path(nextDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
  }
}
