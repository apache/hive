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
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.shims.Utils;
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

import static org.apache.hadoop.hive.metastore.ReplChangeManager.SOURCE_OF_REPLICATION;

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

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());

    internalBeforeClassSetup(overrides, TestReplicationScenarios.class, true);
  }

  static void internalBeforeClassSetup(Map<String, String> overrides, Class clazz,
                                       boolean autogather)
      throws Exception {
    conf = new HiveConf(clazz);
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    MiniDFSCluster miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    Map<String, String> localOverrides = new HashMap<String, String>() {{
        put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
        put(HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname, "true");
      }};
    localOverrides.putAll(overrides);
    replica = new WarehouseInstance(LOG, miniDFSCluster, localOverrides);

    // Run with autogather false on primary if requested
    hasAutogather = autogather;
    localOverrides.put(HiveConf.ConfVars.HIVESTATSAUTOGATHER.varname,
                        autogather ? "true" : "false");
    primary = new WarehouseInstance(LOG, miniDFSCluster, localOverrides);
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


  private Map<String, String> collectStatsParams(Map<String, String> allParams) {
    Map<String, String> statsParams = new HashMap<>();
    List<String> params = new ArrayList<String>(Arrays.asList(StatsSetupConst.supportedStats));
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
    Assert.assertEquals(primary.getTableColumnStatistics(primaryDbName, tableName),
                        replica.getTableColumnStatistics(replicatedDbName, tableName));

    // Test table level stats
    Map<String, String> rParams =
            collectStatsParams(replica.getTable(replicatedDbName, tableName).getParameters());
    Map<String, String> pParams =
            collectStatsParams(primary.getTable(primaryDbName, tableName).getParameters());
    Assert.assertEquals(pParams, rParams);

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

    for (Partition pPart : pParts) {
      Partition rPart = replica.getPartition(replicatedDbName, tableName,
              pPart.getValues());

      Map<String, String> rParams = collectStatsParams(rPart.getParameters());
      Map<String, String> pParams = collectStatsParams(pPart.getParameters());
      Assert.assertEquals(pParams, rParams);
    }

    // Test partition column stats for all partitions
    Assert.assertEquals(primary.getAllPartitionColumnStatistics(primaryDbName, tableName),
                        replica.getAllPartitionColumnStatistics(replicatedDbName, tableName));
  }

  private void verifyNoStatsReplicationForMetadataOnly(String tableName) throws Throwable {
    // Test column stats
    Assert.assertTrue(replica.getTableColumnStatistics(replicatedDbName, tableName).isEmpty());

    // When no data is replicated, the basic stats parameters for table should look as if it's a
    // new table created on replica. Based on the create table rules the basic stats may be true
    // or false. Either is fine with us so don't bother checking exact values.
    Map<String, String> rParams =
            collectStatsParams(replica.getTable(replicatedDbName, tableName).getParameters());
    Map<String, String> expectedFalseParams = new HashMap<>();
    Map<String, String> expectedTrueParams = new HashMap<>();
    StatsSetupConst.setStatsStateForCreateTable(expectedTrueParams,
            replica.getTableColNames(replicatedDbName, tableName), StatsSetupConst.TRUE);
    StatsSetupConst.setStatsStateForCreateTable(expectedFalseParams,
            replica.getTableColNames(replicatedDbName, tableName), StatsSetupConst.FALSE);
    Assert.assertTrue(rParams.equals(expectedFalseParams) || rParams.equals(expectedTrueParams));

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
    Assert.assertTrue(rParts == null || rParts.isEmpty());

    // Test partition column stats for all partitions
    Map<String, List<ColumnStatisticsObj>> rPartColStats =
            replica.getAllPartitionColumnStatistics(replicatedDbName, tableName);
    for (Map.Entry<String, List<ColumnStatisticsObj>> entry: rPartColStats.entrySet()) {
      List<ColumnStatisticsObj> colStats = entry.getValue();
      Assert.assertTrue(colStats == null || colStats.isEmpty());
    }
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

    primary.run("use " + primaryDbName)
            .run("create table " + simpleTableName + " (id int)")
            .run("insert into " + simpleTableName + " values (1), (2)")
            .run("create table " + partTableName + " (place string) partitioned by (country string)")
            .run("insert into " + partTableName + " partition(country='india') values ('bangalore')")
            .run("insert into " + partTableName + " partition(country='us') values ('austin')")
            .run("insert into " + partTableName + " partition(country='france') values ('paris')")
            .run("create table " + ndTableName + " (str string)")
            .run("create table " + ndPartTableName + " (val string) partitioned by (pk int)");

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
   * @param lastReplicationId of the last dump, for incremental dump/load
   * @param parallelLoad, if true, parallel bootstrap load is used
   * @param metadataOnly, only metadata is dumped and loaded.
   * @return lastReplicationId of the dump performed.
   */
  private String dumpLoadVerify(List<String> tableNames, String lastReplicationId,
                                boolean parallelLoad, boolean metadataOnly)
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
            .dump(primaryDbName, lastReplicationId, withClauseList);

    // Load, if necessary changing configuration.
    if (parallelLoad) {
      replica.hiveConf.setBoolVar(HiveConf.ConfVars.EXECPARALLEL, true);
    }

    replica.load(replicatedDbName, dumpTuple.dumpLocation)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(tableNames.toArray(new String[1]));

    // Metadata load may not load all the events.
    if (!metadataOnly) {
      replica.run("repl status " + replicatedDbName)
              .verifyResult(dumpTuple.lastReplicationId);
    }

    if (parallelLoad) {
      replica.hiveConf.setBoolVar(HiveConf.ConfVars.EXECPARALLEL, false);
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

  private void createIncrementalData(List<String> tableNames) throws Throwable {
    // Annotations for this table are same as createBootStrapData
    String simpleTableName = "sTable";
    String partTableName = "pTable";
    String ndTableName = "ndTable";
    String ndPartTableName = "ndPTable";

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
            .run("create table " + incTableName + " (config string, enabled boolean)")
            .run("insert into " + incTableName + " values ('conf1', true)")
            .run("insert into " + incTableName + " values ('conf2', false)")
            .run("insert into " + ndPartTableName + "(pk, val) values (1, 'one')")
            .run("insert into " + ndPartTableName + "(pk, val) values (1, 'another one')")
            .run("insert into " + ndPartTableName + "(pk, val) values (2, 'two')")
            .run("create table " + incPartTableName +
                    "(val string) partitioned by (tvalue boolean)")
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

  private void testStatsReplicationCommon(boolean parallelBootstrap, boolean metadataOnly) throws Throwable {
    List<String> tableNames = createBootStrapData();
    String lastReplicationId = dumpLoadVerify(tableNames, null, parallelBootstrap,
            metadataOnly);

    // Incremental dump
    createIncrementalData(tableNames);
    lastReplicationId = dumpLoadVerify(tableNames, lastReplicationId, parallelBootstrap,
            metadataOnly);
  }

  @Test
  public void testForNonAcidTables() throws Throwable {
    testStatsReplicationCommon(false, false);
  }

  @Test
  public void testForNonAcidTablesParallelBootstrapLoad() throws Throwable {
    testStatsReplicationCommon(true, false);
  }

  @Test
  public void testNonAcidMetadataOnlyDump() throws Throwable {
    testStatsReplicationCommon(false, true);
  }
}
