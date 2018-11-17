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
import org.junit.Ignore;

/**
 * TestReplicationScenariosAcidTables - test replication for ACID tables
 */
public class TestReplicationScenariosIncrementalLoadAcidTables {
  @Rule
  public final TestName testName = new TestName();

  @Rule
  public TestRule replV1BackwardCompat;

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationScenariosIncrementalLoadAcidTables.class);
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
        put("hive.metastore.client.capability.check", "false");
        put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
        put("hive.exec.dynamic.partition.mode", "nonstrict");
        put("hive.strict.checks.bucketing", "false");
        put("hive.mapred.mode", "nonstrict");
        put("mapred.input.dir.recursive", "true");
        put("hive.metastore.disallow.incompatible.col.type.changes", "false");
    }};
    primary = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConf);
    replica = new WarehouseInstance(LOG, miniDFSCluster, overridesForHiveConf);
    HashMap<String, String> overridesForHiveConf1 = new HashMap<String, String>() {{
        put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
        put("hive.support.concurrency", "false");
        put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
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

    appendInsert(primary, primaryDbName, primaryDbNameExtra, tableName, tableNameMM, selectStmtList, expectedValues);
    appendDelete(primary, primaryDbName, primaryDbNameExtra, selectStmtList, expectedValues);
    appendUpdate(primary, primaryDbName, primaryDbNameExtra, selectStmtList, expectedValues);
    appendTruncate(primary, primaryDbName, primaryDbNameExtra, selectStmtList, expectedValues);
    appendInsertIntoFromSelect(primary, primaryDbName, primaryDbNameExtra, tableName, tableNameMM, selectStmtList, expectedValues);
    appendMerge(primary, primaryDbName, primaryDbNameExtra, selectStmtList, expectedValues);
    appendCreateAsSelect(primary, primaryDbName, primaryDbNameExtra, tableName, tableNameMM, selectStmtList, expectedValues);
    appendImport(primary, primaryDbName, primaryDbNameExtra, tableName, tableNameMM, selectStmtList, expectedValues);
    appendInsertOverwrite(primary, primaryDbName, primaryDbNameExtra, tableName, tableNameMM, selectStmtList, expectedValues);
    appendLoadLocal(primary, primaryDbName, primaryDbNameExtra, tableName, tableNameMM, selectStmtList, expectedValues);
    appendInsertUnion(primary, primaryDbName, primaryDbNameExtra, tableName, tableNameMM, selectStmtList, expectedValues);
    appendMultiStatementTxn(primary, primaryDbName, primaryDbNameExtra, selectStmtList, expectedValues);
    appendMultiStatementTxnUpdateDelete(primary, primaryDbName, primaryDbNameExtra, selectStmtList, expectedValues);
    appendAlterTable(primary, primaryDbName, primaryDbNameExtra, selectStmtList, expectedValues);

    verifyIncrementalLoad(selectStmtList, expectedValues, bootStrapDump.lastReplicationId);
  }

  public static void appendInsert(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                            String tableName, String tableNameMM,
                            List<String> selectStmtList, List<String[]> expectedValues) throws Throwable {
    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableName, null, false, OperationType.REPL_TEST_ACID_INSERT);
    selectStmtList.add("select key from " + tableName + " order by key");
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});
    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableNameMM, null, true, OperationType.REPL_TEST_ACID_INSERT);
    selectStmtList.add("select key from " + tableNameMM + " order by key");
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});
  }

  private void appendDelete(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                            List<String> selectStmtList, List<String[]> expectedValues) throws Throwable {
    String tableName = "testDelete";
    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableName, null, false, OperationType.REPL_TEST_ACID_INSERT);
    deleteRecords(tableName);
    selectStmtList.add("select count(*) from " + tableName);
    expectedValues.add(new String[] {"0"});
  }

  private void appendUpdate(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                            List<String> selectStmtList, List<String[]> expectedValues) throws Throwable {
    String tableName = "testUpdate";
    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableName, null, false, OperationType.REPL_TEST_ACID_INSERT);
    updateRecords(tableName);
    selectStmtList.add("select value from " + tableName + " order by value");
    expectedValues.add(new String[] {"1", "100", "100", "100", "100"});
  }

  public static void appendTruncate(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                              List<String> selectStmtList, List<String[]> expectedValues) throws Throwable {
    String tableName = "testTruncate";
    String tableNameMM = tableName + "_MM";

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableName, null, false, OperationType.REPL_TEST_ACID_INSERT);
    truncateTable(primary, primaryDbName, tableName);
    selectStmtList.add("select count(*) from " + tableName);
    expectedValues.add(new String[] {"0"});
    selectStmtList.add("select count(*) from " + tableName + "_nopart");
    expectedValues.add(new String[] {"0"});

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableNameMM, null, true, OperationType.REPL_TEST_ACID_INSERT);
    truncateTable(primary, primaryDbName, tableNameMM);
    selectStmtList.add("select count(*) from " + tableNameMM);
    expectedValues.add(new String[] {"0"});
    selectStmtList.add("select count(*) from " + tableNameMM + "_nopart");
    expectedValues.add(new String[] {"0"});
  }

  public static  void appendAlterTable(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                                List<String> selectStmtList, List<String[]> expectedValues) throws Throwable {
    String tableName = "testAlterTable";
    String tableNameMM = tableName + "_MM";

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableName, null, false, OperationType.REPL_TEST_ACID_INSERT);
    primary.run("use " + primaryDbName)
            .run("alter table " + tableName + " change value value1 int ")
            .run("select value1 from " + tableName)
            .verifyResults(new String[]{"1", "2", "3", "4", "5"})
            .run("alter table " + tableName + "_nopart change value value1 int ")
            .run("select value1 from " + tableName + "_nopart")
            .verifyResults(new String[]{"1", "2", "3", "4", "5"});
    selectStmtList.add("select value1 from " + tableName );
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});
    selectStmtList.add("select value1 from " + tableName + "_nopart");
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableNameMM, null, true, OperationType.REPL_TEST_ACID_INSERT);
    primary.run("use " + primaryDbName)
            .run("alter table " + tableNameMM + " change value value1 int ")
            .run("select value1 from " + tableNameMM)
            .verifyResults(new String[]{"1", "2", "3", "4", "5"})
            .run("alter table " + tableNameMM + "_nopart change value value1 int ")
            .run("select value1 from " + tableNameMM + "_nopart")
            .verifyResults(new String[]{"1", "2", "3", "4", "5"});
    selectStmtList.add("select value1 from " + tableNameMM );
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});
    selectStmtList.add("select value1 from " + tableNameMM + "_nopart");
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});
  }

  public static void appendInsertIntoFromSelect(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                                          String tableName, String tableNameMM,
                                          List<String> selectStmtList, List<String[]> expectedValues) throws Throwable {
    String tableNameSelect = tableName + "_Select";
    String tableNameSelectMM = tableName + "_SelectMM";

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableName, tableNameSelect, false, OperationType.REPL_TEST_ACID_INSERT_SELECT);
    selectStmtList.add("select key from " + tableNameSelect + " order by key");
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableNameMM, tableNameSelectMM, true, OperationType.REPL_TEST_ACID_INSERT_SELECT);
    selectStmtList.add("select key from " + tableNameSelectMM + " order by key");
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});
  }

  public static void appendMerge(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                           List<String> selectStmtList, List<String[]> expectedValues) throws Throwable {
    String tableName = "testMerge";
    String tableNameMerge = tableName + "_Merge";

    insertForMerge(primary, primaryDbName, tableName, tableNameMerge, false);
    selectStmtList.add("select last_update_user from " + tableName + " order by last_update_user");
    expectedValues.add(new String[] {"creation", "creation", "creation", "creation", "creation",
            "creation", "creation", "merge_update", "merge_insert", "merge_insert"});
    selectStmtList.add("select ID from " + tableNameMerge + " order by ID");
    expectedValues.add(new String[] {"1", "4", "7", "8", "8", "11"});
  }

  public static void appendCreateAsSelect(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                                    String tableName, String tableNameMM,
                                    List<String> selectStmtList, List<String[]> expectedValues) throws Throwable {
     String tableNameCTAS = tableName + "_CTAS";
    String tableNameCTASMM = tableName + "_CTASMM";

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableName, tableNameCTAS, false, OperationType.REPL_TEST_ACID_CTAS);
    selectStmtList.add("select key from " + tableNameCTAS + " order by key");
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableNameMM, tableNameCTASMM, true, OperationType.REPL_TEST_ACID_CTAS);
    selectStmtList.add("select key from " + tableNameCTASMM + " order by key");
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});
  }

  public static void appendImport(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                            String tableName, String tableNameMM,
                            List<String> selectStmtList, List<String[]> expectedValues) throws Throwable {
    String tableNameImport = tableName + "_Import";
    String tableNameImportMM = tableName + "_ImportMM";

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableName, tableNameImport, false, OperationType.REPL_TEST_ACID_INSERT_IMPORT);
    selectStmtList.add("select key from " + tableNameImport + " order by key");
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableNameMM, tableNameImportMM, true, OperationType.REPL_TEST_ACID_INSERT_IMPORT);
    selectStmtList.add("select key from " + tableNameImportMM + " order by key");
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});
  }

  public static void appendInsertOverwrite(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                                     String tableName, String tableNameMM,
                                     List<String> selectStmtList, List<String[]> expectedValues) throws Throwable {
    String tableNameOW = tableName + "_OW";
    String tableNameOWMM = tableName +"_OWMM";

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableName, tableNameOW, false, OperationType.REPL_TEST_ACID_INSERT_OVERWRITE);
    selectStmtList.add("select key from " + tableNameOW + " order by key");
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableNameMM, tableNameOWMM, true, OperationType.REPL_TEST_ACID_INSERT_OVERWRITE);
    selectStmtList.add("select key from " + tableNameOWMM + " order by key");
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});
  }

  //TODO: need to check why its failing. Loading to acid table from local path is failing.
  public static void appendLoadLocal(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                               String tableName, String tableNameMM,
                               List<String> selectStmtList, List<String[]> expectedValues) throws Throwable {
    String tableNameLL = tableName +"_LL";
    String tableNameLLMM = tableName +"_LLMM";

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableName, tableNameLL, false, OperationType.REPL_TEST_ACID_INSERT_LOADLOCAL);
    selectStmtList.add("select key from " + tableNameLL + " order by key");
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableNameMM, tableNameLLMM, true, OperationType.REPL_TEST_ACID_INSERT_LOADLOCAL);
    selectStmtList.add("select key from " + tableNameLLMM + " order by key");
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});
  }

  public static void appendInsertUnion(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                                 String tableName, String tableNameMM,
                                 List<String> selectStmtList, List<String[]> expectedValues) throws Throwable {
    String tableNameUnion = tableName +"_UNION";
    String tableNameUnionMM = tableName +"_UNIONMM";
    String[] resultArrayUnion = new String[]{"1", "1", "2", "2", "3", "3", "4", "4", "5", "5"};

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableName, tableNameUnion, false, OperationType.REPL_TEST_ACID_INSERT_UNION);
    selectStmtList.add( "select key from " + tableNameUnion + " order by key");
    expectedValues.add(resultArrayUnion);
    selectStmtList.add("select key from " + tableNameUnion + "_nopart" + " order by key");
    expectedValues.add(resultArrayUnion);

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableNameMM, tableNameUnionMM, true, OperationType.REPL_TEST_ACID_INSERT_UNION);
    selectStmtList.add( "select key from " + tableNameUnionMM + " order by key");
    expectedValues.add(resultArrayUnion);
    selectStmtList.add("select key from " + tableNameUnionMM + "_nopart" + " order by key");
    expectedValues.add(resultArrayUnion);
  }

  public static void appendMultiStatementTxn(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                                       List<String> selectStmtList, List<String[]> expectedValues) throws Throwable {
    String tableName = "testMultiStatementTxn";
    String[] resultArray = new String[]{"1", "2", "3", "4", "5"};
    String tableNameMM = tableName + "_MM";
    String tableProperty = "'transactional'='true'";
    String tableStorage = "STORED AS ORC";

    insertIntoDB(primary, primaryDbName, tableName, tableProperty, tableStorage, resultArray, true);
    selectStmtList.add("select key from " + tableName + " order by key");
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});

    tableProperty = setMMtableProperty(tableProperty);
    insertIntoDB(primary, primaryDbName, tableNameMM, tableProperty, tableStorage, resultArray, true);
    selectStmtList.add("select key from " + tableNameMM + " order by key");
    expectedValues.add(new String[]{"1", "2", "3", "4", "5"});
  }

  private void appendMultiStatementTxnUpdateDelete(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                                                   List<String> selectStmtList, List<String[]> expectedValues)
          throws Throwable {
    String tableName = "testMultiStatementTxnUpdate";
    String tableNameDelete = "testMultiStatementTxnDelete";
    String[] resultArray = new String[]{"1", "2", "3", "4", "5"};
    String tableProperty = "'transactional'='true'";
    String tableStorage = "STORED AS ORC";

    insertIntoDB(primary, primaryDbName, tableName, tableProperty, tableStorage, resultArray, true);
    updateRecords(tableName);
    selectStmtList.add("select value from " + tableName + " order by value");
    expectedValues.add(new String[] {"1", "100", "100", "100", "100"});

    insertIntoDB(primary, primaryDbName, tableNameDelete, tableProperty, tableStorage, resultArray, true);
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

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableName, null, false, OperationType.REPL_TEST_ACID_INSERT);
    incrementalDump = primary.dump(primaryDbName, bootStrapDump.lastReplicationId);
    primary.run("drop table " + primaryDbName + "." + tableName);
    replica.loadWithoutExplain(replicatedDbName, incrementalDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName).verifyResult(incrementalDump.lastReplicationId);
    verifyResultsInReplica(Lists.newArrayList("select count(*) from " + tableName,
                                              "select count(*) from " + tableName + "_nopart"),
                            Lists.newArrayList(result, result));

    insertRecords(primary, primaryDbName, primaryDbNameExtra,
            tableNameMM, null, true, OperationType.REPL_TEST_ACID_INSERT);
    incrementalDump = primary.dump(primaryDbName, bootStrapDump.lastReplicationId);
    primary.run("drop table " + primaryDbName + "." + tableNameMM);
    replica.loadWithoutExplain(replicatedDbName, incrementalDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName).verifyResult(incrementalDump.lastReplicationId);
    verifyResultsInReplica(Lists.newArrayList("select count(*) from " + tableNameMM,
            "select count(*) from " + tableNameMM + "_nopart"),
            Lists.newArrayList(result, result));
  }

  private void verifyResultsInReplica(List<String> selectStmtList, List<String[]> expectedValues) throws Throwable  {
    verifyResultsInReplica(replica, replicatedDbName, selectStmtList, expectedValues);
  }

  private static void verifyResultsInReplica(WarehouseInstance replica ,String replicatedDbName,
                                      List<String> selectStmtList, List<String[]> expectedValues) throws Throwable  {
    for (int idx = 0; idx < selectStmtList.size(); idx++) {
      replica.run("use " + replicatedDbName)
              .run(selectStmtList.get(idx))
              .verifyResults(expectedValues.get(idx));
    }
  }

  private WarehouseInstance.Tuple verifyIncrementalLoad(List<String> selectStmtList,
                                                List<String[]> expectedValues, String lastReplId) throws Throwable {
    return verifyIncrementalLoad(primary, replica, primaryDbName,
            replicatedDbName, selectStmtList, expectedValues, lastReplId);

  }
  public static WarehouseInstance.Tuple verifyIncrementalLoad(WarehouseInstance primary, WarehouseInstance replica,
                                                              String primaryDbName, String replicatedDbName,
                                                              List<String> selectStmtList,
                                                  List<String[]> expectedValues, String lastReplId) throws Throwable {
    WarehouseInstance.Tuple incrementalDump = primary.dump(primaryDbName, lastReplId);
    replica.loadWithoutExplain(replicatedDbName, incrementalDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName).verifyResult(incrementalDump.lastReplicationId);
    verifyResultsInReplica(replica, replicatedDbName, selectStmtList, expectedValues);

    replica.loadWithoutExplain(replicatedDbName, incrementalDump.dumpLocation)
            .run("REPL STATUS " + replicatedDbName).verifyResult(incrementalDump.lastReplicationId);
    verifyResultsInReplica(replica, replicatedDbName, selectStmtList, expectedValues);
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
            .run("select value from " + tableName + " order by value")
            .verifyResults(new String[] {"1", "100", "100", "100", "100"});
  }

  private static void truncateTable(WarehouseInstance primary, String dbName, String tableName) throws Throwable {
    primary.run("use " + dbName)
            .run("truncate table " + tableName)
            .run("select count(*) from " + tableName)
            .verifyResult("0")
            .run("truncate table " + tableName + "_nopart")
            .run("select count(*) from " + tableName + "_nopart")
            .verifyResult("0");
  }

  private WarehouseInstance.Tuple verifyLoad(String tableName, String tableNameOp, String lastReplId) throws Throwable {
    String[] resultArray = new String[]{"1", "2", "3", "4", "5"};
    if (tableNameOp == null) {
      return verifyIncrementalLoad(Lists.newArrayList("select key from " + tableName + " order by key",
              "select key from " + tableName + "_nopart order by key"),
              Lists.newArrayList(resultArray, resultArray), lastReplId);
    }
    return verifyIncrementalLoad(Lists.newArrayList("select key from " + tableName + " order by key",
                                                    "select key from " + tableNameOp + " order by key",
                                                    "select key from " + tableName + "_nopart" + " order by key",
                                                    "select key from " + tableNameOp + "_nopart" + " order by key"),
                    Lists.newArrayList(resultArray, resultArray, resultArray, resultArray), lastReplId);
  }

  private static void insertIntoDB(WarehouseInstance primary, String dbName, String tableName,
                                   String tableProperty, String storageType, String[] resultArray, boolean isTxn)
          throws Throwable {
    String txnStrStart = "START TRANSACTION";
    String txnStrCommit = "COMMIT";
    if (!isTxn) {
      txnStrStart = "use " + dbName; //dummy
      txnStrCommit = "use " + dbName; //dummy
    }
    primary.run("use " + dbName);
    primary.run("CREATE TABLE " + tableName + " (key int, value int) PARTITIONED BY (load_date date) " +
            "CLUSTERED BY(key) INTO 3 BUCKETS " + storageType + " TBLPROPERTIES ( " + tableProperty + ")")
            .run("SHOW TABLES LIKE '" + tableName + "'")
            .verifyResult(tableName)
            .run("CREATE TABLE " + tableName + "_nopart (key int, value int) " +
                    "CLUSTERED BY(key) INTO 3 BUCKETS " + storageType + " TBLPROPERTIES ( " + tableProperty + ")")
            .run("SHOW TABLES LIKE '" + tableName + "_nopart'")
            .run("ALTER TABLE " + tableName + " ADD PARTITION (load_date='2016-03-03')")
            .run(txnStrStart)
            .run("INSERT INTO " + tableName + " partition (load_date='2016-03-01') VALUES (1, 1)")
            .run("INSERT INTO " + tableName + " partition (load_date='2016-03-01') VALUES (2, 2)")
            .run("INSERT INTO " + tableName + " partition (load_date='2016-03-02') VALUES (3, 3)")
            .run("INSERT INTO " + tableName + " partition (load_date='2016-03-03') VALUES (4, 4)")
            .run("INSERT INTO " + tableName + " partition (load_date='2016-03-02') VALUES (5, 5)")
            .run("select key from " + tableName + " order by key")
            .verifyResults(resultArray)
            .run("INSERT INTO " + tableName + "_nopart (key, value) select key, value from " + tableName)
            .run("select key from " + tableName + "_nopart" + " order by key")
            .verifyResults(resultArray)
            .run(txnStrCommit);
  }

  private static void insertIntoDB(WarehouseInstance primary, String dbName, String tableName,
                                   String tableProperty, String storageType, String[] resultArray)
          throws Throwable {
    insertIntoDB(primary, dbName, tableName, tableProperty, storageType, resultArray, false);
  }

  private static void insertRecords(WarehouseInstance primary, String primaryDbName, String primaryDbNameExtra,
                                    String tableName, String tableNameOp, boolean isMMTable,
                             OperationType opType) throws Throwable {
    insertRecordsIntoDB(primary, primaryDbName, primaryDbNameExtra, tableName, tableNameOp, isMMTable, opType);
  }

  private static void insertRecordsIntoDB(WarehouseInstance primary, String DbName, String primaryDbNameExtra,
                                          String tableName, String tableNameOp, boolean isMMTable,
                             OperationType opType) throws Throwable {
    String[] resultArray = new String[]{"1", "2", "3", "4", "5"};
    String tableProperty;
    String tableStorage;

    if (primary.isAcidEnabled()) {
      tableProperty = "'transactional'='true'";
      if (isMMTable) {
        tableProperty = setMMtableProperty(tableProperty);
      }
      tableStorage = "STORED AS ORC";
    } else {
      tableProperty = "'transactional'='false'";
      if (isMMTable) {
        tableStorage = "";
      } else {
        tableStorage = "STORED AS ORC";
      }
    }

    primary.run("use " + DbName);

    switch (opType) {
      case REPL_TEST_ACID_INSERT:
        insertIntoDB(primary, DbName, tableName, tableProperty, tableStorage, resultArray);
        if (primaryDbNameExtra != null) {
          insertIntoDB(primary, primaryDbNameExtra, tableName, tableProperty, tableStorage, resultArray);
        }
        return;
      case REPL_TEST_ACID_INSERT_OVERWRITE:
        primary.run("CREATE TABLE " + tableNameOp + " (key int, value int) PARTITIONED BY (load_date date) " +
              "CLUSTERED BY(key) INTO 3 BUCKETS " + tableStorage + " TBLPROPERTIES ( "+ tableProperty + " )")
        .run("INSERT INTO " + tableNameOp + " partition (load_date='2016-03-01') VALUES (2, 2)")
        .run("INSERT INTO " + tableNameOp + " partition (load_date='2016-03-01') VALUES (10, 12)")
        .run("INSERT INTO " + tableNameOp + " partition (load_date='2016-03-02') VALUES (11, 1)")
        .run("select key from " + tableNameOp + " order by key")
        .verifyResults(new String[]{"2", "10", "11"})
        .run("insert overwrite table " + tableNameOp + " select * from " + tableName)
        .run("CREATE TABLE " + tableNameOp + "_nopart (key int, value int) " +
                "CLUSTERED BY(key) INTO 3 BUCKETS " + tableStorage + " TBLPROPERTIES ( "+ tableProperty + " )")
        .run("INSERT INTO " + tableNameOp + "_nopart VALUES (2, 2)")
        .run("INSERT INTO " + tableNameOp + "_nopart VALUES (10, 12)")
        .run("INSERT INTO " + tableNameOp + "_nopart VALUES (11, 1)")
        .run("select key from " + tableNameOp + "_nopart" + " order by key")
        .verifyResults(new String[]{"2", "10", "11"})
        .run("insert overwrite table " + tableNameOp + "_nopart select * from " + tableName + "_nopart")
        .run("select key from " + tableNameOp + "_nopart" + " order by key");
        break;
      case REPL_TEST_ACID_INSERT_SELECT:
        primary.run("CREATE TABLE " + tableNameOp + " (key int, value int) PARTITIONED BY (load_date date) " +
            "CLUSTERED BY(key) INTO 3 BUCKETS " + tableStorage + " TBLPROPERTIES ( " + tableProperty + " )")
        .run("insert into " + tableNameOp + " partition (load_date) select * from " + tableName)
        .run("CREATE TABLE " + tableNameOp + "_nopart (key int, value int) " +
                "CLUSTERED BY(key) INTO 3 BUCKETS " + tableStorage + " TBLPROPERTIES ( " + tableProperty + " )")
        .run("insert into " + tableNameOp + "_nopart select * from " + tableName + "_nopart");
        break;
      case REPL_TEST_ACID_INSERT_IMPORT:
        String path = "hdfs:///tmp/" + DbName + "/";
        String exportPath = "'" + path + tableName + "/'";
        String exportPathNoPart = "'" + path + tableName + "_nopart/'";
        primary.run("export table " + tableName + " to " + exportPath)
        .run("import table " + tableNameOp + " from " + exportPath)
        .run("export table " + tableName + "_nopart to " + exportPathNoPart)
        .run("import table " + tableNameOp + "_nopart from " + exportPathNoPart);
        break;
      case REPL_TEST_ACID_CTAS:
        primary.run("create table " + tableNameOp + " as select * from " + tableName)
                .run("create table " + tableNameOp + "_nopart as select * from " + tableName + "_nopart");
        break;
      case REPL_TEST_ACID_INSERT_LOADLOCAL:
        // For simplicity setting key and value as same value
        StringBuilder buf = new StringBuilder();
        boolean nextVal = false;
        for (String key : resultArray) {
          if (nextVal) {
            buf.append(',');
          }
          buf.append('(');
          buf.append(key);
          buf.append(',');
          buf.append(key);
          buf.append(')');
          nextVal = true;
        }

        primary.run("CREATE TABLE " + tableNameOp + "_temp (key int, value int) " + tableStorage + "")
        .run("INSERT INTO TABLE " + tableNameOp + "_temp VALUES " + buf.toString())
        .run("SELECT key FROM " + tableNameOp + "_temp")
        .verifyResults(resultArray)
        .run("CREATE TABLE " + tableNameOp + " (key int, value int) PARTITIONED BY (load_date date) " +
              "CLUSTERED BY(key) INTO 3 BUCKETS " + tableStorage + " TBLPROPERTIES ( " + tableProperty + ")")
        .run("SHOW TABLES LIKE '" + tableNameOp + "'")
        .verifyResult(tableNameOp)
        .run("INSERT OVERWRITE LOCAL DIRECTORY './test.dat' " + tableStorage + " SELECT * FROM " + tableNameOp + "_temp")
        .run("LOAD DATA LOCAL INPATH './test.dat/000000_0' OVERWRITE INTO TABLE " + tableNameOp +
                " PARTITION (load_date='2008-08-15')")
        .run("CREATE TABLE " + tableNameOp + "_nopart (key int, value int) " +
                      "CLUSTERED BY(key) INTO 3 BUCKETS " + tableStorage + " TBLPROPERTIES ( " + tableProperty + ")")
        .run("SHOW TABLES LIKE '" + tableNameOp + "_nopart'")
        .verifyResult(tableNameOp + "_nopart")
        .run("LOAD DATA LOCAL INPATH './test.dat/000000_0' OVERWRITE INTO TABLE " + tableNameOp + "_nopart");
        break;
      case REPL_TEST_ACID_INSERT_UNION:
        primary.run("CREATE TABLE " + tableNameOp + " (key int, value int) PARTITIONED BY (load_date date) " +
                "CLUSTERED BY(key) INTO 3 BUCKETS " + tableStorage + " TBLPROPERTIES ( " + tableProperty + ")")
                .run("SHOW TABLES LIKE '" + tableNameOp + "'")
                .verifyResult(tableNameOp)
                .run("insert overwrite table " + tableNameOp + " partition (load_date) select * from " + tableName +
                    " union all select * from " + tableName)
                .run("CREATE TABLE " + tableNameOp + "_nopart (key int, value int) " +
                "CLUSTERED BY(key) INTO 3 BUCKETS " + tableStorage + " TBLPROPERTIES ( " + tableProperty + ")")
                .run("insert overwrite table " + tableNameOp + "_nopart select * from " + tableName +
                        "_nopart union all select * from " + tableName + "_nopart");
        resultArray = new String[]{"1", "2", "3", "4", "5", "1", "2", "3", "4", "5"};
        break;
      default:
        return;
    }
    primary.run("select key from " + tableNameOp + " order by key").verifyResults(resultArray);
    primary.run("select key from " + tableNameOp + "_nopart" + " order by key").verifyResults(resultArray);
  }

  private static String setMMtableProperty(String tableProperty) throws Throwable  {
    return tableProperty.concat(", 'transactional_properties' = 'insert_only'");
  }

  private static void insertForMerge(WarehouseInstance primary, String primaryDbName,
                                     String tableName, String tableNameMerge, boolean isMMTable) throws Throwable  {
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
        .run("select ID from " + tableName + " order by ID")
        .verifyResults(new String[] {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"})
        .run("INSERT INTO " + tableNameMerge + " VALUES (1, 'value_01', '20170410'), " +
                " (4, NULL, '20170410'), (7, 'value_77777', '20170413'), " +
                " (8, NULL, '20170413'), (8, 'value_08', '20170415'), " +
                "(11, 'value_11', '20170415')")
        .run("select ID from " + tableNameMerge + " order by ID")
        .verifyResults(new String[] {"1", "4", "7", "8", "8", "11"})
        .run("MERGE INTO " + tableName + " AS T USING " + tableNameMerge + " AS S ON T.ID = S.ID and" +
                " T.tran_date = S.tran_date WHEN MATCHED AND (T.TranValue != S.TranValue AND S.TranValue " +
                " IS NOT NULL) THEN UPDATE SET TranValue = S.TranValue, last_update_user = " +
                " 'merge_update' WHEN MATCHED AND S.TranValue IS NULL THEN DELETE WHEN NOT MATCHED " +
                " THEN INSERT VALUES (S.ID, S.TranValue,'merge_insert', S.tran_date)")
        .run("select last_update_user from " + tableName + " order by last_update_user")
        .verifyResults(new String[] {"creation", "creation", "creation", "creation", "creation",
                "creation", "creation", "merge_update", "merge_insert", "merge_insert"});
  }
}
