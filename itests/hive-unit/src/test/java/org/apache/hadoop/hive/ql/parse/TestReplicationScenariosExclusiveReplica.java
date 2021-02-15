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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.hive.metastore.ReplChangeManager.SOURCE_OF_REPLICATION;
import static org.apache.hadoop.hive.ql.exec.repl.ReplExternalTables.FILE_NAME;

/**
 * Test replication scenarios with staging on replica.
 */
public class TestReplicationScenariosExclusiveReplica extends BaseReplicationAcrossInstances {

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());
    overrides.put(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY.varname, "false");
    overrides.put(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname, "true");
    overrides.put(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname,
        UserGroupInformation.getCurrentUser().getUserName());
    internalBeforeClassSetupExclusiveReplica(overrides, overrides, TestReplicationScenarios.class);
  }

  @Before
  public void setup() throws Throwable {
    super.setup();
  }

  @After
  public void tearDown() throws Throwable {
    super.tearDown();
  }

  @Test
  public void testDistCpCopyWithRemoteStagingAndCopyTaskOnTarget() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(replica.repldDir, true);
    withClauseOptions.add("'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='" + false + "'");
    WarehouseInstance.Tuple tuple = primary
        .run("use " + primaryDbName)
        .run("create external table t1 (id int)")
        .run("insert into table t1 values (100)")
        .run("create table t2 (id int)")
        .run("insert into table t2 values (200)")
        .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for bootstrap
    assertExternalFileInfo(Arrays.asList("t1"), tuple.dumpLocation, false, replica);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("select id from t1")
        .verifyResult("100")
        .run("show tables like 't2'")
        .verifyResult("t2")
        .run("select id from t2")
        .verifyResult("200");

    tuple = primary.run("use " + primaryDbName)
        .run("create external table t3 (id int)")
        .run("insert into table t3 values (300)")
        .run("create table t4 (id int)")
        .run("insert into table t4 values (400)")
        .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for incremental
    assertExternalFileInfo(Arrays.asList("t1", "t3"), tuple.dumpLocation, true, replica);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
        .run("use " + replicatedDbName)
        .run("show tables like 't1'")
        .verifyResult("t1")
        .run("show tables like 't2'")
        .verifyResult("t2")
        .run("show tables like 't3'")
        .verifyResult("t3")
        .run("show tables like 't4'")
        .verifyResult("t4")
        .run("select id from t1")
        .verifyResult("100")
        .run("select id from t2")
        .verifyResult("200")
        .run("select id from t3")
        .verifyResult("300")
        .run("select id from t4")
        .verifyResult("400");
  }

  @Test
  public void testTableLevelReplicationWithRemoteStaging() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(replica.repldDir, true);
    withClauseOptions.add("'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='" + false + "'");
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (100)")
            .run("create table t2 (id int)")
            .run("insert into table t2 values (200)")
            .dump(primaryDbName +".'t[0-9]+'", withClauseOptions);

    // verify that the external table info is written correctly for bootstrap
    assertExternalFileInfo(Arrays.asList("t1"), tuple.dumpLocation, false, replica);

    //verify table list
    verifyTableListForPolicy(replica.miniDFSCluster.getFileSystem(),
            tuple.dumpLocation, new String[]{"t1", "t2"});

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResult("100")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResult("200");

    tuple = primary.run("use " + primaryDbName)
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (300)")
            .run("create table t4 (id int)")
            .run("insert into table t4 values (400)")
            .run("create table t5 (id int) partitioned by (p int)")
            .run("insert into t5 partition(p=1) values(10)")
            .run("insert into t5 partition(p=2) values(20)")
            .dump(primaryDbName + ".'t[0-9]+'", withClauseOptions);

    // verify that the external table info is written correctly for incremental
    assertExternalFileInfo(Arrays.asList("t1", "t3"), tuple.dumpLocation, true, replica);

    //verify table list
    verifyTableListForPolicy(replica.miniDFSCluster.getFileSystem(),
            tuple.dumpLocation, new String[]{"t1", "t2", "t3", "t4", "t5"});

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("show tables like 't4'")
            .verifyResult("t4")
            .run("show tables like 't5'")
            .verifyResult("t5")
            .run("select id from t1")
            .verifyResult("100")
            .run("select id from t2")
            .verifyResult("200")
            .run("select id from t3")
            .verifyResult("300")
            .run("select id from t4")
            .verifyResult("400")
            .run("select id from t5")
            .verifyResults(new String[]{"10", "20"});
  }

  @Test
  public void testDistCpCopyWithLocalStagingAndCopyTaskOnTarget() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(primary.repldDir, true);
    withClauseOptions.add("'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='" + false + "'");
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (500)")
            .run("create table t2 (id int)")
            .run("insert into table t2 values (600)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for bootstrap
    assertExternalFileInfo(Arrays.asList("t1"), tuple.dumpLocation, false, primary);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResult("500")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResult("600");

    tuple = primary.run("use " + primaryDbName)
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (700)")
            .run("create table t4 (id int)")
            .run("insert into table t4 values (800)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for incremental
    assertExternalFileInfo(Arrays.asList("t1", "t3"), tuple.dumpLocation, true, primary);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("show tables like 't4'")
            .verifyResult("t4")
            .run("select id from t1")
            .verifyResult("500")
            .run("select id from t2")
            .verifyResult("600")
            .run("select id from t3")
            .verifyResult("700")
            .run("select id from t4")
            .verifyResult("800");
  }

  @Test
  public void testDistCpCopyWithRemoteStagingAndCopyTaskOnSource() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(replica.repldDir, true);
    withClauseOptions.add("'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='" + false + "'");
    withClauseOptions.add("'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='" + false + "'");
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (100)")
            .run("create table t2 (id int)")
            .run("insert into table t2 values (200)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for bootstrap
    assertExternalFileInfo(Arrays.asList("t1"), tuple.dumpLocation, false, replica);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResult("100")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResult("200");

    tuple = primary.run("use " + primaryDbName)
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (300)")
            .run("create table t4 (id int)")
            .run("insert into table t4 values (400)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for incremental
    assertExternalFileInfo(Arrays.asList("t1", "t3"), tuple.dumpLocation, true, replica);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("show tables like 't4'")
            .verifyResult("t4")
            .run("select id from t1")
            .verifyResult("100")
            .run("select id from t2")
            .verifyResult("200")
            .run("select id from t3")
            .verifyResult("300")
            .run("select id from t4")
            .verifyResult("400");
  }

  @Test
  public void testDistCpCopyWithLocalStagingAndCopyTaskOnSource() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(primary.repldDir, true);
    withClauseOptions.add("'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='" + false + "'");
    withClauseOptions.add("'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='" + false + "'");
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (500)")
            .run("create table t2 (id int)")
            .run("insert into table t2 values (600)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for bootstrap
    assertExternalFileInfo(Arrays.asList("t1"), tuple.dumpLocation, false, primary);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResult("500")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResult("600");

    tuple = primary.run("use " + primaryDbName)
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (700)")
            .run("create table t4 (id int)")
            .run("insert into table t4 values (800)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for incremental
    assertExternalFileInfo(Arrays.asList("t1", "t3"), tuple.dumpLocation, true, primary);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("show tables like 't4'")
            .verifyResult("t4")
            .run("select id from t1")
            .verifyResult("500")
            .run("select id from t2")
            .verifyResult("600")
            .run("select id from t3")
            .verifyResult("700")
            .run("select id from t4")
            .verifyResult("800");
  }

  @Test
  public void testRegularCopyRemoteStagingAndCopyTaskOnSource() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(replica.repldDir, false);
    withClauseOptions.add("'" + HiveConf.ConfVars.REPL_RUN_DATA_COPY_TASKS_ON_TARGET.varname + "'='" + false + "'");
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (500)")
            .run("create table t2 (id int)")
            .run("insert into table t2 values (600)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for bootstrap
    assertExternalFileInfo(Arrays.asList("t1"), tuple.dumpLocation, false, replica);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResult("500")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResult("600");

    tuple = primary.run("use " + primaryDbName)
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (700)")
            .run("create table t4 (id int)")
            .run("insert into table t4 values (800)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for incremental
    assertExternalFileInfo(Arrays.asList("t1", "t3"), tuple.dumpLocation, true, replica);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("show tables like 't4'")
            .verifyResult("t4")
            .run("select id from t1")
            .verifyResult("500")
            .run("select id from t2")
            .verifyResult("600")
            .run("select id from t3")
            .verifyResult("700")
            .run("select id from t4")
            .verifyResult("800");
  }

  @Test
  public void testRegularCopyWithLocalStagingAndCopyTaskOnTarget() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(primary.repldDir, false);
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into table t1 values (500)")
            .run("create table t2 (id int)")
            .run("insert into table t2 values (600)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for bootstrap
    assertExternalFileInfo(Arrays.asList("t1"), tuple.dumpLocation, false, primary);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResult("500")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResult("600");

    tuple = primary.run("use " + primaryDbName)
            .run("create external table t3 (id int)")
            .run("insert into table t3 values (700)")
            .run("create table t4 (id int)")
            .run("insert into table t4 values (800)")
            .dump(primaryDbName, withClauseOptions);

    // verify that the external table info is written correctly for incremental
    assertExternalFileInfo(Arrays.asList("t1", "t3"), tuple.dumpLocation, true, primary);

    replica.load(replicatedDbName, primaryDbName, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("show tables like 't4'")
            .verifyResult("t4")
            .run("select id from t1")
            .verifyResult("500")
            .run("select id from t2")
            .verifyResult("600")
            .run("select id from t3")
            .verifyResult("700")
            .run("select id from t4")
            .verifyResult("800");
  }

  @Test
  public void externalTableReplicationDropDatabase() throws Throwable {
    String primaryDb = "primarydb1";
    String replicaDb = "repldb1";
    String tableName = "t1";
    List<String> withClauseOptions = getStagingLocationConfig(primary.repldDir, false);
    WarehouseInstance.Tuple tuple = primary
            .run("create database " + primaryDb)
            .run("alter database "+ primaryDb + " set dbproperties('repl.source.for'='1,2,3')")
            .run("use " + primaryDb)
            .run("create external table " +  tableName + " (id int)")
            .run("insert into table " + tableName + " values (500)")
            .dump(primaryDb, withClauseOptions);

    replica.load(replicaDb, primaryDb, withClauseOptions)
            .run("use " + replicaDb)
            .run("show tables like '" + tableName + "'")
            .verifyResult(tableName)
            .run("select id from " + tableName)
            .verifyResult("500");

    Path dbDataLocPrimary = new Path(primary.externalTableWarehouseRoot, primaryDb + ".db");
    Path extTableBase = new Path(replica.getConf().get(HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname));
    Path dbDataLocReplica = new Path(extTableBase + dbDataLocPrimary.toUri().getPath());
    verifyTableDataExists(primary, dbDataLocPrimary, tableName, true);
    verifyTableDataExists(replica, dbDataLocReplica, tableName, true);

    primary.run("show databases like '" + primaryDb + "'")
            .verifyResult(primaryDb);
    replica.run("show databases like '" + replicaDb + "'")
            .verifyResult(replicaDb);
    primary.run("drop database " + primaryDb + " cascade");
    replica.run("drop database " + replicaDb + " cascade");
    primary.run("show databases like '" + primaryDb + "'")
            .verifyResult(null);
    replica.run("show databases like '" + replicaDb + "'")
            .verifyResult(null);

    verifyTableDataExists(primary, dbDataLocPrimary, tableName, false);
    verifyTableDataExists(replica, dbDataLocReplica, tableName, true);
  }

  @Test
  public void testCustomWarehouseLocations() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(primary.repldDir, false);
    String dbWhManagedLoc = new Path(primary.warehouseRoot.getParent(), "customManagedLoc").toUri().getPath();
    String dbWhExternalLoc = new Path(primary.externalTableWarehouseRoot.getParent(),
            "customExternalLoc").toUri().getPath();
    String srcDb = "srcDb";
    WarehouseInstance.Tuple tuple = primary
            .run("create database " + srcDb + " LOCATION '" + dbWhExternalLoc + "' MANAGEDLOCATION '" + dbWhManagedLoc
                    + "' WITH DBPROPERTIES ( '" + SOURCE_OF_REPLICATION + "' = '1,2,3')")
            .run("use " + srcDb)
            .run("create table t1 (id int)")
            .run("insert into table t1 values (500)")
            .run("create external table t2 (id int)")
            .run("insert into table t2 values (1000)")
            .run("create table tp1 (id int) partitioned by (p int)")
            .run("insert into tp1 partition(p=1) values(10)")
            .run("insert into tp1 partition(p=2) values(20)")
            .dump(srcDb, withClauseOptions);

    replica.load(replicatedDbName, srcDb, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResult("500")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResult("1000")
            .run("show tables like 'tp1'")
            .verifyResult("tp1")
            .run("select id from tp1")
            .verifyResults(new String[]{"10", "20"});
    List<String> listOfTables = new ArrayList<>();
    listOfTables.addAll(Arrays.asList("t1", "t2", "tp1"));
    verifyCustomDBLocations(srcDb, listOfTables, dbWhManagedLoc, dbWhExternalLoc, true);
    primary.run("use " + srcDb)
            .run("insert into table t1 values (1000)")
            .run("insert into table t2 values (2000)")
            .run("insert into tp1 partition(p=1) values(30)")
            .run("insert into tp1 partition(p=2) values(40)")
            .dump(srcDb, withClauseOptions);
    replica.load(replicatedDbName, srcDb, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResults(new String[]{"500", "1000"})
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResults(new String[]{"1000", "2000"})
            .run("show tables like 'tp1'")
            .verifyResult("tp1")
            .run("select id from tp1")
            .verifyResults(new String[]{"10", "20", "30", "40"});
    primary.run("use " + srcDb)
            .run("insert into table t1 values (2000)")
            .run("insert into table t2 values (3000)")
            .run("create table t3 (id int)")
            .run("insert into table t3 values (3000)")
            .run("create external table t4 (id int)")
            .run("insert into table t4 values (4000)")
            .run("insert into tp1 partition(p=1) values(50)")
            .run("insert into tp1 partition(p=2) values(60)")
            .run("create table tp2 (id int) partitioned by (p int)")
            .run("insert into tp2 partition(p=1) values(100)")
            .run("insert into tp2 partition(p=2) values(200)")
            .dump(srcDb, withClauseOptions);
    replica.load(replicatedDbName, srcDb, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResults(new String[]{"500", "1000", "2000"})
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResults(new String[]{"1000", "2000", "3000"})
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("select id from t3")
            .verifyResults(new String[]{"3000"})
            .run("show tables like 't4'")
            .verifyResult("t4")
            .run("select id from t4")
            .verifyResults(new String[]{"4000"})
            .run("select id from tp1")
            .verifyResults(new String[]{"10", "20", "30", "40", "50", "60"})
            .run("show tables like 'tp1'")
            .verifyResult("tp1")
            .run("select id from tp2")
            .verifyResults(new String[]{"100", "200"});
    listOfTables.addAll(Arrays.asList("t3", "t4", "tp2"));
    verifyCustomDBLocations(srcDb, listOfTables, dbWhManagedLoc, dbWhExternalLoc, true);
  }

  @Test
  public void testCustomWarehouseLocationsConf() throws Throwable {
    List<String> withClauseOptions = getStagingLocationConfig(primary.repldDir, false);
    String dbWhManagedLoc = new Path(primary.warehouseRoot.getParent(), "customManagedLoc1").toUri().getPath();
    String dbWhExternalLoc = new Path(primary.externalTableWarehouseRoot.getParent(),
            "customExternalLoc1").toUri().getPath();
    String srcDb = "srcDbConf";
    WarehouseInstance.Tuple tuple = primary
            .run("create database " + srcDb + " LOCATION '" + dbWhExternalLoc + "' MANAGEDLOCATION '" + dbWhManagedLoc
                    + "' WITH DBPROPERTIES ( '" + SOURCE_OF_REPLICATION + "' = '1,2,3')")
            .run("use " + srcDb)
            .run("create table t1 (id int)")
            .run("insert into table t1 values (500)")
            .run("create external table t2 (id int)")
            .run("insert into table t2 values (1000)")
            .dump(srcDb, withClauseOptions);

    withClauseOptions.add("'" + HiveConf.ConfVars.REPL_RETAIN_CUSTOM_LOCATIONS_FOR_DB_ON_TARGET.varname + "'='false'");
    replica.load(replicatedDbName, srcDb, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResult("500")
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResult("1000");
    List<String> listOfTables = new ArrayList<>();
    listOfTables.addAll(Arrays.asList("t1", "t2"));
    verifyCustomDBLocations(srcDb, listOfTables, dbWhManagedLoc, dbWhExternalLoc, false);
    primary.run("use " + srcDb)
            .run("insert into table t1 values (1000)")
            .run("insert into table t2 values (2000)")
            .dump(srcDb, withClauseOptions);
    replica.load(replicatedDbName, srcDb, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResults(new String[]{"500", "1000"})
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResults(new String[]{"1000", "2000"});
    primary.run("use " + srcDb)
            .run("insert into table t1 values (2000)")
            .run("insert into table t2 values (3000)")
            .run("create table t3 (id int)")
            .run("insert into table t3 values (3000)")
            .run("create external table t4 (id int)")
            .run("insert into table t4 values (4000)")
            .dump(srcDb, withClauseOptions);
    replica.load(replicatedDbName, srcDb, withClauseOptions)
            .run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1")
            .verifyResults(new String[]{"500", "1000", "2000"})
            .run("show tables like 't2'")
            .verifyResult("t2")
            .run("select id from t2")
            .verifyResults(new String[]{"1000", "2000", "3000"})
            .run("show tables like 't3'")
            .verifyResult("t3")
            .run("select id from t3")
            .verifyResults(new String[]{"3000"})
            .run("show tables like 't4'")
            .verifyResult("t4")
            .run("select id from t4")
            .verifyResults(new String[]{"4000"});
    listOfTables.addAll(Arrays.asList("t3", "t4"));
    verifyCustomDBLocations(srcDb, listOfTables, dbWhManagedLoc, dbWhExternalLoc, false);
  }

  private void verifyCustomDBLocations(String srcDb, List<String> listOfTables, String managedCustLocOnSrc,
                                       String externalCustLocOnSrc, boolean replaceCustPath) throws Exception {
    Database replDatabase  = replica.getDatabase(replicatedDbName);
    if (replaceCustPath ) {
      String managedCustLocOnTgt = new Path(replDatabase.getManagedLocationUri()).toUri().getPath();
      Assert.assertEquals(managedCustLocOnSrc,  managedCustLocOnTgt);
      Assert.assertNotEquals(managedCustLocOnTgt,  replica.warehouseRoot.toUri().getPath());
      String externalCustLocOnTgt = new Path(replDatabase.getLocationUri()).toUri().getPath();
      Assert.assertEquals(externalCustLocOnSrc,  externalCustLocOnTgt);
      Assert.assertNotEquals(externalCustLocOnTgt,  new Path(replica.externalTableWarehouseRoot,
              replicatedDbName.toLowerCase()  + ".db").toUri().getPath());
    } else {
      Assert.assertNotEquals(managedCustLocOnSrc,  null);
      Assert.assertEquals(replDatabase.getManagedLocationUri(),  null);
      String externalCustLocOnTgt = new Path(replDatabase.getLocationUri()).toUri().getPath();
      Assert.assertNotEquals(externalCustLocOnSrc,  externalCustLocOnTgt);
      Assert.assertEquals(externalCustLocOnTgt,  new Path(replica.externalTableWarehouseRoot,
              replicatedDbName.toLowerCase()  + ".db").toUri().getPath());
    }
    verifyTableLocations(srcDb, replDatabase, listOfTables, replaceCustPath);
  }

  private void verifyTableLocations(String srcDb, Database replDb, List<String> tables, boolean customLocOntgt)
          throws Exception {
    String tgtExtBase = replica.getConf().get(HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname);
    for (String tname: tables) {
      Table table = replica.getTable(replicatedDbName, tname);
      if ("EXTERNAL_TABLE".equals(table.getTableType())) {
        String pathOnSrc = new Path(primary.getTable(srcDb, tname).getSd().getLocation()).toUri().getPath();
        Assert.assertEquals(new Path(table.getSd().getLocation()), new Path(tgtExtBase, pathOnSrc.substring(1)));
      } else {
        //Managed Table case
        Path tblPathOnTgt  = customLocOntgt
                ? new Path(replDb.getManagedLocationUri(), tname)
                : new Path(replica.warehouseRoot, replicatedDbName.toLowerCase()  + ".db" + "/" + tname );
        Assert.assertEquals(new Path(table.getSd().getLocation()), tblPathOnTgt);
      }
    }
  }

  private void verifyTableDataExists(WarehouseInstance warehouse, Path dbDataPath, String tableName,
                                     boolean shouldExists) throws IOException {
    FileSystem fileSystem = FileSystem.get(warehouse.warehouseRoot.toUri(), warehouse.getConf());
    Path tablePath = new Path(dbDataPath, tableName);
    Path dataFilePath = new Path(tablePath, "000000_0");
    Assert.assertEquals(shouldExists, fileSystem.exists(dbDataPath));
    Assert.assertEquals(shouldExists, fileSystem.exists(tablePath));
    Assert.assertEquals(shouldExists, fileSystem.exists(dataFilePath));
  }

  private List<String> getStagingLocationConfig(String stagingLoc, boolean addDistCpConfigs) throws IOException {
    List<String> confList = new ArrayList<>();
    confList.add("'" + HiveConf.ConfVars.REPLDIR.varname + "'='" + stagingLoc + "'");
    if (addDistCpConfigs) {
      confList.add("'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'");
      confList.add("'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'");
      confList.add("'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
              + UserGroupInformation.getCurrentUser().getUserName() + "'");
    }
    return confList;
  }

  private void assertExternalFileInfo(List<String> expected, String dumplocation, boolean isIncremental,
                                      WarehouseInstance warehouseInstance)
      throws IOException {
    Path hivePath = new Path(dumplocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path metadataPath = new Path(hivePath, EximUtil.METADATA_PATH_NAME);
    Path externalTableInfoFile;
    if (isIncremental) {
      externalTableInfoFile = new Path(hivePath, FILE_NAME);
    } else {
      externalTableInfoFile = new Path(metadataPath, primaryDbName.toLowerCase() + File.separator + FILE_NAME);
    }
    ReplicationTestUtils.assertExternalFileInfo(warehouseInstance, expected, externalTableInfoFile);
  }

  /*
   * Method used from TestTableLevelReplicationScenarios
   */
  private void verifyTableListForPolicy(FileSystem fileSystem, String dumpLocation, String[] tableList) throws Throwable {
    String hiveDumpLocation = dumpLocation + File.separator + ReplUtils.REPL_HIVE_BASE_DIR;
    Path tableListFile = new Path(hiveDumpLocation, ReplUtils.REPL_TABLE_LIST_DIR_NAME);
    tableListFile = new Path(tableListFile, primaryDbName.toLowerCase());

    if (tableList == null) {
      Assert.assertFalse(fileSystem.exists(tableListFile));
      return;
    } else {
      Assert.assertTrue(fileSystem.exists(tableListFile));
    }

    BufferedReader reader = null;
    try {
      InputStream inputStream = fileSystem.open(tableListFile);
      reader = new BufferedReader(new InputStreamReader(inputStream));
      Set tableNames = new HashSet<>(Arrays.asList(tableList));
      int numTable = 0;
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        numTable++;
        Assert.assertTrue(tableNames.contains(line));
      }
      Assert.assertEquals(numTable, tableList.length);
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }
}
