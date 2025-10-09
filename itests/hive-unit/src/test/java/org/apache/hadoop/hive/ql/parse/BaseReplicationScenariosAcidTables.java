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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.Utils;
import org.junit.rules.TestName;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Collections;
import java.util.Map;

import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;

/**
 * TestReplicationScenariosAcidTablesBase - base class for replication for ACID tables tests
 */
public class BaseReplicationScenariosAcidTables {
  @Rule
  public final TestName testName = new TestName();

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationScenarios.class);
  private static final Path REPLICA_EXTERNAL_BASE = new Path("/replica_external_base");
  protected static String fullyQualifiedReplicaExternalBase;
  static WarehouseInstance primary;
  static WarehouseInstance replica, replicaNonAcid;
  static HiveConf conf;
  String primaryDbName, replicatedDbName;
  List<String> acidTableNames = new LinkedList<>();
  private List<String> nonAcidTableNames = new LinkedList<>();

  static void internalBeforeClassSetup(Map<String, String> overrides, Class clazz)
          throws Exception {
    conf = new HiveConfForTest(clazz);
    //TODO: HIVE-28044: Replication tests to run on Tez
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    MiniDFSCluster miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).build();
    HashMap<String, String> acidEnableConf = new HashMap<String, String>() {{
      put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
      put("hive.support.concurrency", "true");
      put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
      put("hive.metastore.client.capability.check", "false");
      put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
      put("hive.strict.checks.bucketing", "false");
      put("hive.mapred.mode", "nonstrict");
      put("mapred.input.dir.recursive", "true");
      put("hive.metastore.disallow.incompatible.col.type.changes", "false");
      put("hive.in.repl.test", "true");
      put("metastore.warehouse.tenant.colocation", "true");
      put(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname, "false");
    }};

    acidEnableConf.putAll(overrides);

    setReplicaExternalBase(miniDFSCluster.getFileSystem(), acidEnableConf);
    primary = new WarehouseInstance(LOG, miniDFSCluster, acidEnableConf);
    acidEnableConf.put(MetastoreConf.ConfVars.REPLDIR.getHiveName(), primary.repldDir);
    replica = new WarehouseInstance(LOG, miniDFSCluster, acidEnableConf);
    HashMap<String, String> overridesForHiveConf1 = new HashMap<String, String>() {{
        put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
        put("hive.support.concurrency", "false");
        put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
        put("hive.metastore.client.capability.check", "false");
    }};
    overridesForHiveConf1.put(MetastoreConf.ConfVars.REPLDIR.getHiveName(), primary.repldDir);
    replicaNonAcid = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConf1);
  }

  protected static void setReplicaExternalBase(FileSystem fs, Map<String, String> confMap) throws IOException {
    fs.mkdirs(REPLICA_EXTERNAL_BASE);
    fullyQualifiedReplicaExternalBase =  fs.getFileStatus(REPLICA_EXTERNAL_BASE).getPath().toString();
    confMap.put(HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname, fullyQualifiedReplicaExternalBase);
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
    String primaryDbNameExtra = primaryDbName+"_extra";
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

  private void prepareAcidData(String primaryDbName) throws Throwable {
    primary.run("use " + primaryDbName)
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
            .run("create table t5 (id int) stored as orc tblproperties (\"transactional\"=\"true\")")
            .run("insert into t5 values(1111), (2222), (3333)");
    acidTableNames.add("t1");
    acidTableNames.add("t2");
    acidTableNames.add("t3");
    acidTableNames.add("t5");
  }

  private void prepareNonAcidData(String primaryDbName) throws Throwable {
    primary.run("use " + primaryDbName)
            .run("create table t4 (id int)")
            .run("insert into t4 values(111), (222)");
    nonAcidTableNames.add("t4");
  }

  WarehouseInstance.Tuple prepareDataAndDump(String primaryDbName,
                                                     List<String> withClause) throws Throwable {
    prepareAcidData(primaryDbName);
    prepareNonAcidData(primaryDbName);
    return primary.run("use " + primaryDbName)
            .dump(primaryDbName, withClause != null ?
                    withClause : Collections.emptyList());
  }

  private void verifyNonAcidTableLoad(String replicatedDbName) throws Throwable {
    replica.run("use " + replicatedDbName)
            .run("select id from t4 order by id")
            .verifyResults(new String[] {"111", "222"});
  }

  private void verifyAcidTableLoad(String replicatedDbName) throws Throwable {
    replica.run("use " + replicatedDbName)
            .run("select id from t1 order by id")
            .verifyResults(new String[]{"1", "2"})
            .run("select country from t2 order by country")
            .verifyResults(new String[] {"france", "india", "us"})
            .run("select rank from t3 order by rank")
            .verifyResults(new String[] {"11", "22"})
            .run("select id from t5 order by id")
            .verifyResults(new String[] {"1111", "2222", "3333"});
  }

  void verifyLoadExecution(String replicatedDbName, String lastReplId, boolean includeAcid)
          throws Throwable {
    List<String> tableNames = new LinkedList<>(nonAcidTableNames);
    if (includeAcid) {
      tableNames.addAll(acidTableNames);
    }
    replica.run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(tableNames)
            .run("repl status " + replicatedDbName)
            .verifyResult(lastReplId)
            .verifyReplTargetProperty(replicatedDbName);
    verifyNonAcidTableLoad(replicatedDbName);
    if (includeAcid) {
      verifyAcidTableLoad(replicatedDbName);
    }
  }

  void prepareIncAcidData(String dbName) throws Throwable {
    primary.run("use " + dbName)
            .run("create table t6 stored as orc tblproperties (\"transactional\"=\"true\")" +
                    " as select * from t1")
            .run("alter table t2 add columns (placetype string)")
            .run("update t2 set placetype = 'city'");
    acidTableNames.add("t6");
  }

  private void verifyIncAcidLoad(String dbName) throws Throwable {
    replica.run("use " + dbName)
            .run("select id from t6 order by id")
            .verifyResults(new String[]{"1", "2"})
            .run("select country from t2 order by country")
            .verifyResults(new String[] {"france", "india", "us"})
            .run("select distinct placetype from t2")
            .verifyResult("city")
            .run("select id from t1 order by id")
            .verifyResults(new String[]{"1", "2"})
            .run("select rank from t3 order by rank")
            .verifyResults(new String[] {"11", "22"})
            .run("select id from t5 order by id")
            .verifyResults(new String[] {"1111", "2222", "3333"});
  }

  private void runUsingDriver(IDriver driver, String command) throws Throwable {
    driver.run(command);
  }

  void prepareInc2AcidData(String dbName, HiveConf hiveConf) throws Throwable {
    IDriver driver = DriverFactory.newDriver(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));
    runUsingDriver(driver, "use " + dbName);
    runUsingDriver(driver, "insert into t1 values (3)");
    runUsingDriver(driver, "insert into t5 values (4444)");
  }

  private void verifyInc2AcidLoad(String dbName) throws Throwable {
    replica.run("use " + dbName)
            .run("select id from t6 order by id")
            .verifyResults(new String[]{"1", "2"})
            .run("select country from t2 order by country")
            .verifyResults(new String[] {"france", "india", "us"})
            .run("select distinct placetype from t2")
            .verifyResult("city")
            .run("select id from t1 order by id")
            .verifyResults(new String[]{"1", "2", "3"})
            .run("select rank from t3 order by rank")
            .verifyResults(new String[] {"11", "22"})
            .run("select id from t5 order by id")
            .verifyResults(new String[] {"1111", "2222", "3333", "4444"});
  }

  void prepareIncNonAcidData(String dbName) throws Throwable {
    primary.run("use " + dbName)
            .run("insert into t4 values (333)")
            .run("create table t7 (str string)")
            .run("insert into t7 values ('aaa')");
    nonAcidTableNames.add("t7");
  }

  private void verifyIncNonAcidLoad(String dbName) throws Throwable {
    replica.run("use " + dbName)
            .run("select * from t4 order by id")
            .verifyResults(new String[] {"111", "222", "333"})
            .run("select * from t7")
            .verifyResult("aaa");
  }

  void prepareInc2NonAcidData(String dbName, HiveConf hiveConf) throws Throwable {
    IDriver driver = DriverFactory.newDriver(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));
    runUsingDriver(driver, "use " + dbName);
    runUsingDriver(driver, "insert into t4 values (444)");
    runUsingDriver(driver, "insert into t7 values ('bbb')");
  }

  private void verifyInc2NonAcidLoad(String dbName) throws Throwable {
    replica.run("use " + dbName)
            .run("select * from t4 order by id")
            .verifyResults(new String[] {"111", "222", "333", "444"})
            .run("select * from t7")
            .verifyResults(new String[] {"aaa", "bbb"});
  }

  void verifyIncLoad(String dbName, String lastReplId)
          throws Throwable {
    List<String> tableNames = new LinkedList<>(nonAcidTableNames);
    tableNames.addAll(acidTableNames);
    replica.run("use " + dbName)
            .run("show tables")
            .verifyResults(tableNames)
            .run("repl status " + dbName)
            .verifyResult(lastReplId)
            .verifyReplTargetProperty(replicatedDbName);
    verifyIncNonAcidLoad(dbName);
    verifyIncAcidLoad(dbName);
  }

  void verifyInc2Load(String dbName, String lastReplId)
          throws Throwable {
    List<String> tableNames = new LinkedList<>(nonAcidTableNames);
    tableNames.addAll(acidTableNames);
    replica.run("use " + dbName)
            .run("show tables")
            .verifyResults(tableNames)
            .run("repl status " + dbName)
            .verifyResult(lastReplId)
            .verifyReplTargetProperty(replicatedDbName);
    verifyInc2NonAcidLoad(dbName);
    verifyInc2AcidLoad(dbName);
  }

  List<Long> openTxns(int numTxns, TxnStore txnHandler, HiveConf primaryConf) throws Throwable {
    OpenTxnsResponse otResp = txnHandler.openTxns(new OpenTxnRequest(numTxns, "u1", "localhost"));
    List<Long> txns = otResp.getTxn_ids();
    String txnIdRange = " txn_id >= " + txns.get(0) + " and txn_id <= " + txns.get(numTxns - 1);
    Assert.assertEquals(TestTxnDbUtil.queryToString(primaryConf, "select * from TXNS"),
            numTxns, TestTxnDbUtil.countQueryAgent(primaryConf,
                    "select count(*) from TXNS where txn_state = 'o' and " + txnIdRange));
    return txns;
  }

  List<Long> allocateWriteIdsForTablesAndAcquireLocks(String primaryDbName, Map<String, Long> tables,
                                                     TxnStore txnHandler,
                                                     List<Long> txns, HiveConf primaryConf) throws Throwable {
    AllocateTableWriteIdsRequest rqst = new AllocateTableWriteIdsRequest();
    rqst.setDbName(primaryDbName);
    List<Long> lockIds = new ArrayList<>();
    for(Map.Entry<String, Long> entry : tables.entrySet()) {
      rqst.setTableName(entry.getKey());
      rqst.setTxnIds(txns);
      txnHandler.allocateTableWriteIds(rqst);
      for (long txnId : txns) {
        LockComponent comp = new LockComponent(LockType.SHARED_WRITE, LockLevel.TABLE,
          primaryDbName);
        comp.setTablename(entry.getKey());
        comp.setOperationType(DataOperationType.UPDATE);
        List<LockComponent> components = new ArrayList<LockComponent>(1);
        components.add(comp);
        LockRequest lockRequest = new LockRequest(components, "u1", "hostname");
        lockRequest.setTxnid(txnId);
        lockIds.add(txnHandler.lock(lockRequest).getLockid());
      }
    }
    verifyWriteIdsForTables(tables, primaryConf, primaryDbName);
    return lockIds;
  }

  void verifyWriteIdsForTables(Map<String, Long> tables, HiveConf conf, String dbName)
          throws Throwable {
    for(Map.Entry<String, Long> entry : tables.entrySet()) {
      Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from TXN_TO_WRITE_ID"),
                          entry.getValue().longValue(),
                          TestTxnDbUtil.countQueryAgent(conf,
          "select count(*) from TXN_TO_WRITE_ID where t2w_database = '"
                    + dbName.toLowerCase()
                    + "' and t2w_table = '" + entry.getKey() + "'"));
    }
  }

  void verifyAllOpenTxnsAborted(List<Long> txns, HiveConf primaryConf) throws Throwable {
    int numTxns = txns.size();
    String txnIdRange = " txn_id >= " + txns.get(0) + " and txn_id <= " + txns.get(numTxns - 1);
    Assert.assertEquals(TestTxnDbUtil.queryToString(primaryConf, "select * from TXNS"),
            0, TestTxnDbUtil.countQueryAgent(primaryConf,
                    "select count(*) from TXNS where txn_state = 'o' and " + txnIdRange));
    Assert.assertEquals(TestTxnDbUtil.queryToString(primaryConf, "select * from TXNS"),
            numTxns, TestTxnDbUtil.countQueryAgent(primaryConf,
                    "select count(*) from TXNS where txn_state = 'a' and " + txnIdRange));
  }

  void verifyAllOpenTxnsNotAborted(List<Long> txns, HiveConf primaryConf) throws Throwable {
    int numTxns = txns.size();
    String txnIdRange = " txn_id >= " + txns.get(0) + " and txn_id <= " + txns.get(numTxns - 1);
    Assert.assertEquals(TestTxnDbUtil.queryToString(primaryConf, "select * from TXNS"),
      numTxns, TestTxnDbUtil.countQueryAgent(primaryConf,
        "select count(*) from TXNS where txn_state = 'o' and " + txnIdRange));
    Assert.assertEquals(TestTxnDbUtil.queryToString(primaryConf, "select * from TXNS"),
      0, TestTxnDbUtil.countQueryAgent(primaryConf,
        "select count(*) from TXNS where txn_state = 'a' and " + txnIdRange));
  }

  void verifyNextId(Map<String, Long> tables, String dbName, HiveConf conf) throws Throwable {
    // Verify the next write id
    for(Map.Entry<String, Long> entry : tables.entrySet()) {
      String[] nextWriteId =
              TestTxnDbUtil.queryToString(conf,
                      "select nwi_next from NEXT_WRITE_ID where  nwi_database = '"
                              + dbName.toLowerCase() + "' and nwi_table = '"
                              + entry.getKey() + "'").split("\n");
      Assert.assertEquals(Long.parseLong(nextWriteId[1].trim()), entry.getValue() + 1);
    }
  }

  void verifyCompactionQueue(Map<String, Long> tables, String dbName, HiveConf conf)
          throws Throwable {
    for(Map.Entry<String, Long> entry : tables.entrySet()) {
      Assert.assertEquals(TestTxnDbUtil.queryToString(conf, "select * from COMPACTION_QUEUE"),
                          entry.getValue().longValue(),
                          TestTxnDbUtil.countQueryAgent(conf,
                    "select count(*) from COMPACTION_QUEUE where cq_database = '" + dbName
                            + "' and cq_table = '" + entry.getKey() + "'"));
    }
  }

  void releaseLocks(TxnStore txnStore, List<Long> lockIds) throws NoSuchLockException,
    TxnOpenException, MetaException {
    for (Long lockId : lockIds) {
      txnStore.unlock(new UnlockRequest(lockId));
    }
  }
}
