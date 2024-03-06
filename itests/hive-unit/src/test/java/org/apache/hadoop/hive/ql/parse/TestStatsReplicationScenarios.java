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

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.BehaviourInjection;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.CallerArguments;
import org.apache.hadoop.hive.ql.metadata.HiveMetaStoreClientWithLocalCache;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;

/**
 * Tests for statistics replication.
 */
public class TestStatsReplicationScenarios {
  @Rule
  public final TestName testName = new TestName();

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationScenarios.class);
  static WarehouseInstance primary;
  private static WarehouseInstance replica;
  private String primaryDbName, replicatedDbName;
  private static HiveConf conf;
  private static boolean hasAutogather;

  enum AcidTableKind {
    FULL_ACID,
    INSERT_ONLY
  }

  private static AcidTableKind acidTableKindToUse;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());

    internalBeforeClassSetup(overrides, overrides, TestReplicationScenarios.class, true, null);
  }

  static void internalBeforeClassSetup(Map<String, String> primaryOverrides,
                                       Map<String, String> replicaOverrides, Class clazz,
                                       boolean autogather, AcidTableKind acidTableKind)
      throws Exception {
    conf = new HiveConf(clazz);
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    MiniDFSCluster miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).build();
    Map<String, String> additionalOverrides = new HashMap<String, String>() {{
        put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
        put(HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname, "true");
        put(HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname, "false");
        put(HiveConf.ConfVars.REPL_RETAIN_CUSTOM_LOCATIONS_FOR_DB_ON_TARGET.varname, "false");
        put(MetastoreConf.ConfVars.TXN_OPENTXN_TIMEOUT.getVarname(), "2000");
      }};
    Map<String, String> replicatedOverrides = new HashMap<>();

    replicatedOverrides.putAll(additionalOverrides);
    replicatedOverrides.putAll(replicaOverrides);

    // Run with autogather false on primary if requested
    Map<String, String> sourceOverrides = new HashMap<>();
    hasAutogather = autogather;
    additionalOverrides.put(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER.varname,
            autogather ? "true" : "false");
    sourceOverrides.putAll(additionalOverrides);
    sourceOverrides.putAll(primaryOverrides);
    primary = new WarehouseInstance(LOG, miniDFSCluster, sourceOverrides);
    replicatedOverrides.put(MetastoreConf.ConfVars.REPLDIR.getHiveName(), primary.repldDir);
    replica = new WarehouseInstance(LOG, miniDFSCluster, replicatedOverrides);

    // Use transactional tables
    acidTableKindToUse = acidTableKind;
  }

  @AfterClass
  public static void classLevelTearDown() throws IOException {
    primary.close();
    replica.close();
  }

  @Before
  public void setup() throws Throwable {
    // set up metastore client cache
    if (conf.getBoolVar(HiveConf.ConfVars.MSC_CACHE_ENABLED)) {
      HiveMetaStoreClientWithLocalCache.init(conf);
    }
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


  private Map<String, String> collectStatsParams(Map<String, String> allParams) {
    Map<String, String> statsParams = new HashMap<>();
    List<String> params = new ArrayList<>(StatsSetupConst.SUPPORTED_STATS);
    params.add(StatsSetupConst.COLUMN_STATS_ACCURATE);
    for (String param : params) {
      String value = allParams.get(param);
      if (value != null) {
        statsParams.put(param, value);
      }
    }

    return statsParams;
  }

  private void verifyReplicatedStatsForTable(String tableName) throws Throwable {
    // Test column stats
    Assert.assertEquals("Mismatching column statistics for  table " + tableName,
                        primary.getTableColumnStatistics(primaryDbName, tableName),
                        replica.getTableColumnStatistics(replicatedDbName, tableName));

    // Test table level stats
    Map<String, String> rParams =
            collectStatsParams(replica.getTable(replicatedDbName, tableName).getParameters());
    Map<String, String> pParams =
            collectStatsParams(primary.getTable(primaryDbName, tableName).getParameters());
    Assert.assertEquals("Mismatch in stats parameters for table " + tableName, pParams, rParams);

    primary.getTable(primaryDbName, tableName).getPartitionKeys();
    verifyReplicatedStatsForPartitionsOfTable(tableName);
  }

  private void verifyReplicatedStatsForPartitionsOfTable(String tableName)
          throws Throwable {
    // Test partition level stats
    List<Partition> pParts = primary.getAllPartitions(primaryDbName, tableName);

    if (pParts == null || pParts.isEmpty()) {
      // Not a partitioned table, nothing to verify.
      return;
    }

    List<FieldSchema> partKeys = primary.getTable(primaryDbName, tableName).getPartitionKeys();
    for (Partition pPart : pParts) {
      Partition rPart = replica.getPartition(replicatedDbName, tableName,
              pPart.getValues());

      Map<String, String> rParams = collectStatsParams(rPart.getParameters());
      Map<String, String> pParams = collectStatsParams(pPart.getParameters());
      String partName = Warehouse.makePartName(partKeys, pPart.getValues());
      Assert.assertEquals("Mismatch in stats parameters for partition " + partName + " of table " + tableName,
                          pParams, rParams);

      // Test partition column stats for the partition
      Assert.assertEquals("Mismatching column statistics for partition " + partName + "of table " + tableName,
                          primary.getPartitionColumnStatistics(primaryDbName, tableName, partName,
                                  StatsSetupConst.getColumnsHavingStats(pParams)),
                          replica.getPartitionColumnStatistics(replicatedDbName, tableName, partName,
                                  StatsSetupConst.getColumnsHavingStats(rParams)));
    }
  }

  private void verifyNoStatsReplicationForMetadataOnly(String tableName) throws Throwable {
    // Test column stats
    Assert.assertTrue(replica.getTableColumnStatistics(replicatedDbName, tableName).isEmpty());

    // When no data is replicated, the basic stats parameters for table should look as if it's a
    // new table created on replica i.e. zero or null.
    Map<String, String> rParams =
            collectStatsParams(replica.getTable(replicatedDbName, tableName).getParameters());
    for (String param : StatsSetupConst.SUPPORTED_STATS) {
      String val = rParams.get(param);
      Assert.assertTrue("parameter " + param + " of table " + tableName + " is expected to be " +
              "null or 0", val == null || val.trim().equals("0"));
    }

    // As long as the above conditions are met, it doesn't matter whether basic and column stats
    // state are set to true or false. If those are false, actual values are immaterial. If they
    // are true, the values assured above represent the correct state of no data.

    verifyNoPartitionStatsReplicationForMetadataOnly(tableName);
  }

  private void verifyNoPartitionStatsReplicationForMetadataOnly(String tableName) throws Throwable {
    // Test partition level stats
    List<Partition> pParts = primary.getAllPartitions(primaryDbName, tableName);

    if (pParts == null || pParts.isEmpty()) {
      // Not a partitioned table, nothing to verify.
      return;
    }

    // Partitions are not replicated in metadata only replication.
    List<Partition> rParts = replica.getAllPartitions(replicatedDbName, tableName);
    Assert.assertTrue("Partitions replicated in a metadata only dump",
            rParts == null || rParts.isEmpty());

    // Test partition column stats for all partitions
    Map<String, List<ColumnStatisticsObj>> rPartColStats =
            replica.getAllPartitionColumnStatistics(replicatedDbName, tableName);
    for (Map.Entry<String, List<ColumnStatisticsObj>> entry: rPartColStats.entrySet()) {
      List<ColumnStatisticsObj> colStats = entry.getValue();
      Assert.assertTrue(colStats == null || colStats.isEmpty());
    }
  }

  private String getCreateTableProperties() {
    if (acidTableKindToUse == AcidTableKind.FULL_ACID) {
      return " stored as orc TBLPROPERTIES('transactional'='true')";
    }

    if (acidTableKindToUse == AcidTableKind.INSERT_ONLY) {
      return " TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only')";
    }

    return "";
  }

  private List<String> createBootStrapData() throws Throwable {
    // Unpartitioned table with data
    String simpleTableName = "sTable";
    // partitioned table with data
    String partTableName = "pTable";
    // Unpartitioned table without data during bootstrap and hence no stats
    String ndTableName = "ndTable";
    // Partitioned table without data during bootstrap and hence no stats.
    String ndPartTableName = "ndPTable";
    String tblCreateExtra = getCreateTableProperties();

    primary.run("use " + primaryDbName)
            .run("create table " + simpleTableName + " (id int)" + tblCreateExtra)
            .run("insert into " + simpleTableName + " values (1), (2)")
            .run("create table " + partTableName + " (place string) partitioned by (country string)"
                    + tblCreateExtra)
            .run("insert into " + partTableName + " partition(country='india') values ('bangalore')")
            .run("insert into " + partTableName + " partition(country='us') values ('austin')")
            .run("insert into " + partTableName + " partition(country='france') values ('paris')")
            .run("create table " + ndTableName + " (str string)" + tblCreateExtra)
            .run("create table " + ndPartTableName + " (val string) partitioned by (pk int)" +
                    tblCreateExtra);

    List<String> tableNames = new ArrayList<>(Arrays.asList(simpleTableName, partTableName,
            ndTableName, ndPartTableName));

    // Run analyze on each of the tables, if they are not being gathered automatically.
    if (!hasAutogather) {
      for (String name : tableNames) {
        Assert.assertTrue(primary.getTableColumnStatistics(primaryDbName, name).isEmpty());
        primary.run("use " + primaryDbName)
                .run("analyze table " + name + " compute statistics for columns");
      }
    }

    return tableNames;
  }

  /**
   * Dumps primarydb on primary, loads it on replica as replicadb, verifies that the statistics
   * loaded are same as the ones on primary.
   * @param tableNames, names of tables on primary expected to be loaded
   * @param parallelLoad, if true, parallel bootstrap load is used
   * @param metadataOnly, only metadata is dumped and loaded.
   * @param lastReplicationId of the last dump, for incremental dump/load
   * @param failRetry
   * @return lastReplicationId of the dump performed.
   */
  private String dumpLoadVerify(List<String> tableNames, String lastReplicationId,
                                boolean parallelLoad, boolean metadataOnly, boolean failRetry)
          throws Throwable {
    List<String> withClauseList;
    // Parallel load works only for bootstrap.
    parallelLoad = parallelLoad && (lastReplicationId == null);

    // With clause construction for REPL DUMP command.
    if (metadataOnly) {
      withClauseList = Collections.singletonList("'hive.repl.dump.metadata.only'='true'");
    } else {
      withClauseList = Collections.emptyList();
    }

    // Take dump
    WarehouseInstance.Tuple dumpTuple = primary.run("use " + primaryDbName)
            .dump(primaryDbName, withClauseList);


    // Load, if necessary changing configuration.
    if (parallelLoad) {
      replica.hiveConf.setBoolVar(HiveConf.ConfVars.EXEC_PARALLEL, true);
    }

    // Fail load if for testing failure and retry scenario. Fail the load while setting
    // checkpoint for a table in the middle of list of tables.
    if (failRetry) {
      if (lastReplicationId == null) {
        failBootstrapLoad(tableNames.size()/2);
      } else {
        failIncrementalLoad();
      }
    }
    
    Path baseDumpDir = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPL_DIR));
    Path nonRecoverablePath = TestReplicationScenarios.getNonRecoverablePath(baseDumpDir, primaryDbName, primary.hiveConf);
    if(nonRecoverablePath != null){
      baseDumpDir.getFileSystem(primary.hiveConf).delete(nonRecoverablePath, true);
    }
    
    // Load, possibly a retry
    replica.load(replicatedDbName, primaryDbName);

    // Metadata load may not load all the events.
    if (!metadataOnly) {
      replica.run("repl status " + replicatedDbName)
              .verifyResult(dumpTuple.lastReplicationId);
    }

    if (parallelLoad) {
      replica.hiveConf.setBoolVar(HiveConf.ConfVars.EXEC_PARALLEL, false);
    }

    // Test statistics
    for (String name : tableNames) {
      if (metadataOnly) {
        verifyNoStatsReplicationForMetadataOnly(name);
      } else {
        verifyReplicatedStatsForTable(name);
      }
    }

    return dumpTuple.lastReplicationId;
  }

  /**
   * Run a bootstrap that will fail.
   */
  private void failBootstrapLoad(int failAfterNumTables) throws Throwable {
    // fail setting ckpt directory property for the second table so that we test the case when
    // bootstrap load fails after some but not all tables are loaded.
    BehaviourInjection<CallerArguments, Boolean> callerVerifier
            = new BehaviourInjection<CallerArguments, Boolean>() {
      int cntTables = 0;
      String prevTable = null;
      @Nullable
      @Override
      public Boolean apply(@Nullable CallerArguments args) {
        if (prevTable == null ||
                !prevTable.equalsIgnoreCase(args.tblName)) {
          cntTables++;
        }
        prevTable = args.tblName;
        if (args.dbName.equalsIgnoreCase(replicatedDbName) && cntTables > failAfterNumTables) {
          injectionPathCalled = true;
          LOG.warn("Verifier - DB : " + args.dbName + " TABLE : " + args.tblName);
          return false;
        }
        return true;
      }
    };

    InjectableBehaviourObjectStore.setAlterTableModifier(callerVerifier);
    try {
      replica.loadFailure(replicatedDbName, primaryDbName);
      callerVerifier.assertInjectionsPerformed(true, false);
    } finally {
      InjectableBehaviourObjectStore.resetAlterTableModifier();
    }
  }

  private void failIncrementalLoad() throws Throwable {
    // fail add notification when second update table stats event is encountered. Thus we
    // test successful application as well as failed application of this event.
    BehaviourInjection<NotificationEvent, Boolean> callerVerifier
            = new BehaviourInjection<NotificationEvent, Boolean>() {
      int cntEvents = 0;
      @Override
      public Boolean apply(NotificationEvent entry) {
        cntEvents++;
        if (entry.getEventType().equalsIgnoreCase(EventMessage.EventType.UPDATE_TABLE_COLUMN_STAT.toString()) &&
            cntEvents > 1) {
          injectionPathCalled = true;
          LOG.warn("Verifier - DB: " + entry.getDbName()
                  + " Table: " + entry.getTableName()
                  + " Event: " + entry.getEventType());
          return false;
        }
        return true;
      }
    };

    InjectableBehaviourObjectStore.setAddNotificationModifier(callerVerifier);
    try {
      replica.loadFailure(replicatedDbName, primaryDbName);
    } finally {
      InjectableBehaviourObjectStore.resetAddNotificationModifier();
    }
    callerVerifier.assertInjectionsPerformed(true, false);

    // fail second call to update partition column stats. Thus we test
    // successful application as well as failed application of this event.
    BehaviourInjection<Table, Boolean> callerVerifier2 = new BehaviourInjection<Table, Boolean>() {
      int cntEvents = 0;

      @Override
      public Boolean apply(Table entry) {
        cntEvents++;
        if (cntEvents > 1) {
          injectionPathCalled = true;
          LOG.warn("Verifier - DB: " + entry.getDbName()
                  + " Table: " + entry.getTableName());
          return false;
        }
        return true;
      }
    };

    InjectableBehaviourObjectStore.setUpdatePartColStatsBehaviour(callerVerifier2);
    try {
      replica.loadFailure(replicatedDbName, primaryDbName);
    } finally {
      InjectableBehaviourObjectStore.setUpdatePartColStatsBehaviour(null);
    }
    callerVerifier.assertInjectionsPerformed(true, false);
  }

  private void createIncrementalData(List<String> tableNames) throws Throwable {
    // Annotations for this table are same as createBootStrapData
    String simpleTableName = "sTable";
    String partTableName = "pTable";
    String ndTableName = "ndTable";
    String ndPartTableName = "ndPTable";
    String tblCreateExtra = getCreateTableProperties();

    Assert.assertTrue(tableNames.containsAll(Arrays.asList(simpleTableName, partTableName,
                                                         ndTableName, ndPartTableName)));
    // New tables created during incremental phase and thus loaded with data and stats during
    // incremental phase.
    String incTableName = "iTable"; // New table
    String incPartTableName = "ipTable"; // New partitioned table

    primary.run("use " + primaryDbName)
            .run("insert into " + simpleTableName + " values (3), (4)")
            // new data inserted into table
            .run("insert into " + ndTableName + " values ('string1'), ('string2')")
            // two partitions changed and one unchanged
            .run("insert into " + partTableName + "(country, place) values ('india', 'pune')")
            .run("insert into " + partTableName + "(country, place) values ('us', 'chicago')")
            // new partition
            .run("insert into " + partTableName + "(country, place) values ('australia', 'perth')")
            .run("create table " + incTableName + " (config string, enabled boolean)" +
                    tblCreateExtra)
            .run("insert into " + incTableName + " values ('conf1', true)")
            .run("insert into " + incTableName + " values ('conf2', false)")
            .run("insert into " + ndPartTableName + "(pk, val) values (1, 'one')")
            .run("insert into " + ndPartTableName + "(pk, val) values (1, 'another one')")
            .run("insert into " + ndPartTableName + "(pk, val) values (2, 'two')")
            .run("create table " + incPartTableName +
                    "(val string) partitioned by (tvalue boolean)" + tblCreateExtra)
            .run("insert into " + incPartTableName + "(tvalue, val) values (true, 'true')")
            .run("insert into " + incPartTableName + "(tvalue, val) values (false, 'false')");

    tableNames.add(incTableName);
    tableNames.add(incPartTableName);

    // Run analyze on each of the tables, if they are not being gathered automatically.
    if (!hasAutogather) {
      for (String name : tableNames) {
        primary.run("use " + primaryDbName)
                .run("analyze table " + name + " compute statistics for columns");
      }
    }
  }

  private void applyDMLOperations(List<String> tableNames) throws Throwable {
    // Annotations for this table are same as createBootStrapData
    String simpleTableName = "sTable";
    String partTableName = "pTable";
    String ndTableName = "ndTable";
    String ndPartTableName = "ndPTable";
    String incTableName = "iTable"; // New table
    String tblCreateExtra = getCreateTableProperties();

    Assert.assertTrue(tableNames.containsAll(Arrays.asList(simpleTableName, partTableName,
            ndTableName, ndPartTableName, incTableName)));

    String ctasTableName = "ctasTable"; // Table created through CTAS
    String ctasPartTableName = "ctasPartTable"; // Table created through CTAS
    // Tables created through import
    String eximTableName = "eximTable";
    String eximPartTableName = "eximPartTable";
    // Tables created through load
    String loadTableName = "loadTable";
    String loadPartTableName = "loadPartTable";

    String exportPath = "'hdfs:///tmp/" + primaryDbName + "/" + incTableName + "/'";
    String exportPartPath = "'hdfs:///tmp/" + primaryDbName + "/" + partTableName + "/'";
    String localDir = "./test.dat";
    String inPath = localDir + "/000000_0";
    String tableStorage = "";
    if (acidTableKindToUse == AcidTableKind.FULL_ACID) {
      tableStorage = "stored as orc";
    }

    primary.run("use " + primaryDbName)
            // insert overwrite
            .run("insert overwrite table " + simpleTableName + " values (5), (6), (7)")
            .run("insert overwrite table " + partTableName + " partition (country='india') " +
                    " values ('bombay')")
            // truncate
            .run("truncate table " + ndTableName)
            .run("truncate table " + ndPartTableName + " partition (pk=1)")
            // CTAS
            .run("create table " + ctasTableName + " as select * from " + incTableName)
            .run("create table " + ctasPartTableName + " as select * from " + partTableName)
            // Import
            .run("export table " + partTableName + " to " + exportPartPath)
            .run("import table " + eximPartTableName + " from " + exportPartPath)
            .run("export table " + incTableName + " to " + exportPath)
            .run("import table " + eximTableName + " from " + exportPath)
            // load
            .run("insert overwrite local directory '" + localDir + "'" + tableStorage + " select " +
                    "* from " + simpleTableName)
            .run("create table " + loadTableName + " (id int)" + tblCreateExtra)
            .run("load data local inpath '" + inPath + "' overwrite into table " + loadTableName)
            .run("create table " + loadPartTableName + " (id int) partitioned by (key int) " + tblCreateExtra)
            .run("load data local inpath '" + inPath + "' overwrite into table "
                    + loadPartTableName + " partition (key=1)");

    tableNames.add(ctasTableName);
    tableNames.add(ctasPartTableName);
    tableNames.add(eximTableName);
    tableNames.add(eximPartTableName);
    tableNames.add(loadTableName);
    tableNames.add(loadPartTableName);

    // Run analyze on each of the tables, if they are not being gathered automatically.
    if (!hasAutogather) {
      for (String name : tableNames) {
        primary.run("use " + primaryDbName)
                .run("analyze table " + name + " compute statistics for columns");
      }
    }
  }

  private void applyTransactionalDMLOperations(List<String> tableNames) throws Throwable {
    // Annotations for this table are same as createBootStrapData
    String partTableName = "pTable";
    String ndTableName = "ndTable";
    String incTableName = "iTable";
    String eximTableName = "eximTable";
    String eximPartTableName = "eximPartTable";

    Assert.assertTrue(tableNames.containsAll(Arrays.asList(partTableName, ndTableName,
            eximPartTableName, eximTableName, incTableName)));

    primary.run("update " + partTableName + " set place = 'mumbai' where place = 'bombay'")
           .run("delete from " + partTableName + " where place = 'chicago'")
            .run("merge into " + eximPartTableName + " as T using " + partTableName + " as U "
                    + " on T.country = U.country "
                    + " when matched and T.place != U.place then update set place = U.place"
                    + " when not matched then insert values (U.country, U.place)")
            .run("update " + incTableName + " set enabled = false where config = 'conf1'")
            .run("merge into " + eximTableName + " as T using " + incTableName + " as U "
                    + " on T.config = U.config"
                    + " when matched and T.enabled != U.enabled then update set enabled = U.enabled"
                    + " when not matched then insert values (U.config, U.enabled)")
           .run("delete from " + ndTableName);

    // Run analyze on each of the tables, if they are not being gathered automatically.
    if (!hasAutogather) {
      for (String name : tableNames) {
        primary.run("use " + primaryDbName)
                .run("analyze table " + name + " compute statistics for columns");
      }
    }
  }

  private void applyDDLOperations(List<String> tableNames) throws Throwable {
    // Annotations for this table are same as createBootStrapData
    String simpleTableName = "sTable";
    String partTableName = "pTable";
    String incTableName = "iTable";
    String ctasTableName = "ctasTable"; // Table created through CTAS

    Assert.assertTrue(tableNames.containsAll(Arrays.asList(simpleTableName, partTableName,
            incTableName, ctasTableName)));

    String renamedTableName = "rnTable";

    primary.run("use " + primaryDbName)
            .run("alter table " + simpleTableName + " add columns (val int)")
            .run("alter table " + incTableName + " change config configuration string")
            .run("alter table " + ctasTableName + " rename to " + renamedTableName)
            .run("alter table " + partTableName +
                    " partition(country='us') rename to partition (country='usa')");

    tableNames.remove(ctasTableName);
    tableNames.add(renamedTableName);
  }

  private void testStatsReplicationCommon(boolean parallelBootstrap, boolean metadataOnly,
                                          boolean failRetry) throws Throwable {
    List<String> tableNames = createBootStrapData();
    String lastReplicationId = dumpLoadVerify(tableNames, null, parallelBootstrap,
            metadataOnly, failRetry);

    // Incremental dump
    createIncrementalData(tableNames);
    lastReplicationId = dumpLoadVerify(tableNames, lastReplicationId, parallelBootstrap,
            metadataOnly, failRetry);

    // Incremental dump with Insert overwrite operation
    applyDMLOperations(tableNames);
    lastReplicationId = dumpLoadVerify(tableNames, lastReplicationId, parallelBootstrap,
            metadataOnly, false);

    // Incremental dump with transactional DML operations
    if (acidTableKindToUse == AcidTableKind.FULL_ACID) {
      applyTransactionalDMLOperations(tableNames);
      lastReplicationId = dumpLoadVerify(tableNames, lastReplicationId, parallelBootstrap,
              metadataOnly, false);
    }

    // Incremental dump with DDL operations
    applyDDLOperations(tableNames);
    lastReplicationId = dumpLoadVerify(tableNames, lastReplicationId, parallelBootstrap,
            metadataOnly, false);
  }
  
  @Test
  public void testNonParallelBootstrapLoad() throws Throwable {
    LOG.info("Testing " + testName.getClass().getName() + "." + testName.getMethodName());
    testStatsReplicationCommon(false, false, false);
  }

  @Test
  public void testForParallelBootstrapLoad() throws Throwable {
    LOG.info("Testing " + testName.getClass().getName() + "." + testName.getMethodName());
    testStatsReplicationCommon(true, false, false);
  }

  @Test
  public void testMetadataOnlyDump() throws Throwable {
    LOG.info("Testing " + testName.getClass().getName() + "." + testName.getMethodName());
    testStatsReplicationCommon(false, true, false);
  }

  @Test
  public void testRetryFailure() throws Throwable {
    LOG.info("Testing " + testName.getClass().getName() + "." + testName.getMethodName());
    testStatsReplicationCommon(false, false, true);
  }
}
