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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.repl.ReplAck;
import org.apache.hadoop.hive.ql.exec.repl.ReplDumpWork;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.repl.dump.EventsDumpMetadata;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.apache.hadoop.hive.ql.exec.repl.ReplAck.DUMP_ACKNOWLEDGEMENT;
import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.INC_BOOTSTRAP_ROOT_DIR_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * Tests Table level replication scenarios.
 */

public class TestTableLevelReplicationScenarios extends BaseReplicationScenariosAcidTables {

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());
    overrides.put(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY.varname, "false");
    overrides.put(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname, "true");
    overrides.put(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname,
        UserGroupInformation.getCurrentUser().getUserName());
    overrides.put(HiveConf.ConfVars.REPL_BATCH_INCREMENTAL_EVENTS.varname, "false");
    internalBeforeClassSetup(overrides, TestTableLevelReplicationScenarios.class);
  }

  enum CreateTableType {
    FULL_ACID, MM_ACID, NON_ACID, EXTERNAL
  }

  class CreateTableInfo {
    String tableName;
    CreateTableType type;
    boolean isPartitioned;

    CreateTableInfo(String tableName, CreateTableType type, boolean isPartitioned) {
      this.tableName = tableName;
      this.type = type;
      this.isPartitioned = isPartitioned;
    }
  }

  private void createTables(List<CreateTableInfo> createTblsInfo) throws Throwable {
    for (CreateTableInfo tblInfo : createTblsInfo) {
      StringBuilder strBuilder = new StringBuilder("create ");
      if (tblInfo.type == CreateTableType.EXTERNAL) {
        strBuilder.append(" external ");
      }
      strBuilder.append(" table ").append(primaryDbName).append(".").append(tblInfo.tableName);

      if (tblInfo.isPartitioned) {
        strBuilder.append(" (a int) partitioned by (b int) ");
      } else {
        strBuilder.append(" (a int, b int) ");
      }

      if (tblInfo.type == CreateTableType.FULL_ACID) {
        strBuilder.append(" clustered by (a) into 2 buckets stored as orc " +
                "tblproperties (\"transactional\"=\"true\")");
      } else if (tblInfo.type == CreateTableType.MM_ACID) {
        strBuilder.append(" tblproperties(\"transactional\"=\"true\", " +
                "\"transactional_properties\"=\"insert_only\")");
      }

      String createTableCmd = strBuilder.toString();
      primary.run("use " + primaryDbName)
              .run(createTableCmd)
              .run("insert into " + tblInfo.tableName + " values(1, 10)");
    }
  }

  private void createTables(String[] tableNames, CreateTableType type) throws Throwable {
    List<CreateTableInfo> createTablesInfo = new ArrayList<>();
    for (String tblName : tableNames) {
      createTablesInfo.add(new CreateTableInfo(tblName, type, false));
    }
    createTables(createTablesInfo);
  }

  private String replicateAndVerify(String replPolicy, String lastReplId,
                                  List<String> dumpWithClause,
                                  List<String> loadWithClause,
                                  String[] expectedTables) throws Throwable {
    return replicateAndVerify(replPolicy, null, lastReplId, dumpWithClause, loadWithClause,
            null, expectedTables);
  }

  private String replicateAndVerify(String replPolicy, String oldReplPolicy, String lastReplId,
                                    List<String> dumpWithClause,
                                    List<String> loadWithClause,
                                    String[] bootstrappedTables,
                                    String[] expectedTables) throws Throwable {
    return replicateAndVerify(replPolicy, oldReplPolicy, lastReplId, dumpWithClause, loadWithClause,
            bootstrappedTables, expectedTables, null);
  }

  private String replicateAndVerify(String replPolicy, String oldReplPolicy, String lastReplId,
                                    List<String> dumpWithClause,
                                    List<String> loadWithClause,
                                    String[] bootstrappedTables,
                                    String[] expectedTables,
                                    String[] records) throws Throwable {
    if (dumpWithClause == null) {
      dumpWithClause = new ArrayList<>();
    }
    if (loadWithClause == null) {
      loadWithClause = new ArrayList<>();
    }

    // For bootstrap replication, drop the target database before triggering it.
    if (lastReplId == null) {
      replica.run("drop database if exists " + replicatedDbName + " cascade");
    }

    WarehouseInstance.Tuple tuple = primary.dump(replPolicy, dumpWithClause);

    DumpMetaData dumpMetaData = new DumpMetaData(new Path(tuple.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR), conf);
    Assert.assertEquals(oldReplPolicy != null && !replPolicy.equals(oldReplPolicy),
      dumpMetaData.isReplScopeModified());

    if (bootstrappedTables != null) {
      verifyBootstrapDirInIncrementalDump(tuple.dumpLocation, bootstrappedTables);
    }

    // If the policy contains '.'' means its table level replication.
    verifyTableListForPolicy(tuple.dumpLocation, replPolicy.contains(".'") ? expectedTables : null);

    verifyDumpMetadata(replPolicy, new Path(tuple.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR));

    replica.load(replicatedDbName, replPolicy, loadWithClause)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(expectedTables)
            .verifyReplTargetProperty(replicatedDbName);

    if (records == null) {
      records = new String[] {"1"};
    }
    for (String table : expectedTables) {
      replica.run("use " + replicatedDbName)
              .run("select a from " + table)
              .verifyResults(records);
    }
    return tuple.lastReplicationId;
  }

  private void verifyDumpMetadata(String replPolicy, Path dumpPath) throws SemanticException {
    String[] parseReplPolicy = replPolicy.split("\\.'");
    assertEquals(parseReplPolicy[0], new DumpMetaData(dumpPath, conf).getReplScope().getDbName());
    if (parseReplPolicy.length > 1) {
      parseReplPolicy[1] = parseReplPolicy[1].replaceAll("'", "");
      assertEquals(parseReplPolicy[1],
        new DumpMetaData(dumpPath, conf).getReplScope().getIncludedTableNames());
    }
    if (parseReplPolicy.length > 2) {
      parseReplPolicy[2] = parseReplPolicy[2].replaceAll("'", "");
      assertEquals(parseReplPolicy[2],
        new DumpMetaData(dumpPath, conf).getReplScope().getExcludedTableNames());
    }
  }

  private String replicateAndVerifyClearDump(String replPolicy, String oldReplPolicy, String lastReplId,
                                    List<String> dumpWithClause,
                                    List<String> loadWithClause,
                                    String[] bootstrappedTables,
                                    String[] expectedTables,
                                    String[] records) throws Throwable {
    if (dumpWithClause == null) {
      dumpWithClause = new ArrayList<>();
    }
    if (loadWithClause == null) {
      loadWithClause = new ArrayList<>();
    }

    // For bootstrap replication, drop the target database before triggering it.
    if (lastReplId == null) {
      replica.run("drop database if exists " + replicatedDbName + " cascade");
    }

    WarehouseInstance.Tuple tuple = primary.dump(replPolicy, dumpWithClause);

    if (bootstrappedTables != null) {
      verifyBootstrapDirInIncrementalDump(tuple.dumpLocation, bootstrappedTables);
    }

    // If the policy contains '.'' means its table level replication.
    verifyTableListForPolicy(tuple.dumpLocation, replPolicy.contains(".'") ? expectedTables : null);

    replica.load(replicatedDbName, replPolicy, loadWithClause)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(expectedTables)
            .verifyReplTargetProperty(replicatedDbName);

    if (records == null) {
      records = new String[] {"1"};
    }
    for (String table : expectedTables) {
      replica.run("use " + replicatedDbName)
              .run("select a from " + table)
              .verifyResults(records);
    }
    new Path(tuple.dumpLocation).getFileSystem(conf).delete(new Path(tuple.dumpLocation), true);
    return tuple.lastReplicationId;
  }

  private void verifyBootstrapDirInIncrementalDump(String dumpLocation, String[] bootstrappedTables)
          throws Throwable {
    String hiveDumpDir = dumpLocation + File.separator + ReplUtils.REPL_HIVE_BASE_DIR;
    // _bootstrap directory should be created as bootstrap enabled on external tables.
    Path dumpPath = new Path(hiveDumpDir, INC_BOOTSTRAP_ROOT_DIR_NAME);

    // If nothing to be bootstrapped.
    if (bootstrappedTables.length == 0) {
      Assert.assertFalse(primary.miniDFSCluster.getFileSystem().exists(dumpPath));
      return;
    }

    Assert.assertTrue(primary.miniDFSCluster.getFileSystem().exists(dumpPath));

    // Check if the DB dump path have any tables other than the ones listed in bootstrappedTables.
    Path dbPath = new Path(dumpPath, EximUtil.METADATA_PATH_NAME + File.separator + primaryDbName);
    FileStatus[] fileStatuses = primary.miniDFSCluster.getFileSystem().listStatus(dbPath);
    Assert.assertEquals(fileStatuses.length, bootstrappedTables.length);

    // Eg: _bootstrap/<db_name>/t2, _bootstrap/<db_name>/t3 etc
    for (String tableName : bootstrappedTables) {
      Path tblPath = new Path(dbPath, tableName);
      Assert.assertTrue(primary.miniDFSCluster.getFileSystem().exists(tblPath));
    }
  }

  private void verifyTableListForPolicy(String dumpLocation, String[] tableList) throws Throwable {
    FileSystem fileSystem = primary.miniDFSCluster.getFileSystem();
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

  @Test
  public void testBasicBootstrapWithIncludeList() throws Throwable {
    String[] originalNonAcidTables = new String[] {"t1", "t2"};
    String[] originalFullAcidTables = new String[] {"t3", "t4"};
    String[] originalMMAcidTables = new String[] {"t5"};
    createTables(originalNonAcidTables, CreateTableType.NON_ACID);
    createTables(originalFullAcidTables, CreateTableType.FULL_ACID);
    createTables(originalMMAcidTables, CreateTableType.MM_ACID);

    // Replicate and verify if only 2 tables are replicated to target.
    String replPolicy = primaryDbName + ".'(t1)|(t4)|(t5)'";
    String[] replicatedTables = new String[] {"t1", "t4", "t5"};
    replicateAndVerify(replPolicy, null, null, null, replicatedTables);
  }

  @Test
  public void testBasicBootstrapWithIncludeAndExcludeList() throws Throwable {
    String[] originalTables = new String[] {"t1", "t11", "t2", "t3", "t100"};
    createTables(originalTables, CreateTableType.NON_ACID);

    // Replicate and verify if only 3 tables are replicated to target.
    String replPolicy = primaryDbName + ".'(t1*)|(t3)'.'t100'";
    String[] replicatedTables = new String[] {"t1", "t11", "t3"};
    replicateAndVerify(replPolicy, null, null, null, replicatedTables);
  }

  @Test
  public void testBasicIncrementalWithIncludeList() throws Throwable {
    WarehouseInstance.Tuple tupleBootstrap = primary.run("use " + primaryDbName)
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);

    String[] originalNonAcidTables = new String[] {"t1", "t2"};
    String[] originalFullAcidTables = new String[] {"t3", "t4"};
    String[] originalMMAcidTables = new String[] {"t5"};
    createTables(originalNonAcidTables, CreateTableType.NON_ACID);
    createTables(originalFullAcidTables, CreateTableType.FULL_ACID);
    createTables(originalMMAcidTables, CreateTableType.MM_ACID);

    // Replicate and verify if only 2 tables are replicated to target.
    String replPolicy = primaryDbName + ".'t1|t5'";
    String[] replicatedTables = new String[] {"t1", "t5"};
    replicateAndVerify(replPolicy, primaryDbName, tupleBootstrap.lastReplicationId, null,
      null, null, replicatedTables);
  }

  @Test
  public void testBasicIncrementalWithIncludeAndExcludeList() throws Throwable {
    WarehouseInstance.Tuple tupleBootstrap = primary.run("use " + primaryDbName)
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);

    String[] originalTables = new String[] {"t1", "t11", "t2", "t3", "t111"};
    createTables(originalTables, CreateTableType.NON_ACID);

    // Replicate and verify if only 3 tables are replicated to target.
    String replPolicy = primaryDbName + ".'(t1+)|(t2)'.'t11|t3'";
    String[] replicatedTables = new String[] {"t1", "t111", "t2"};
    replicateAndVerify(replPolicy, primaryDbName, tupleBootstrap.lastReplicationId, null,
      null, null, replicatedTables);

    //remove table expression. fallback to db level.
    replicatedTables = new String[] {"t1", "t111", "t2", "t11", "t3"};
    String[] bootstrappedTables = new String[] {"t11", "t3"};
    replicateAndVerify(primaryDbName, replPolicy, tupleBootstrap.lastReplicationId, null,
      null, bootstrappedTables, replicatedTables);
  }

  @Test
  public void testIncorrectTablePolicyInReplDump() throws Throwable {
    String[] originalTables = new String[] {"t1", "t11", "t2", "t3", "t111"};
    createTables(originalTables, CreateTableType.NON_ACID);

    // Invalid repl policy where abruptly placed DOT which causes ParseException during REPL dump.
    String[] replicatedTables = new String[] {};
    boolean failed;
    String[] invalidReplPolicies = new String[] {
        primaryDbName + ".t1.t2", // Didn't enclose table pattern within single quotes.
        primaryDbName + ".'t1'.t2", // Table name and include list not allowed.
        primaryDbName + ".t1.'t2'", // Table name and exclude list not allowed.
        primaryDbName + ".'t1+'.", // Abruptly ended dot.
        primaryDbName +  ".['t1+'].['t11']", // With square brackets
        primaryDbName + "..''", // Two dots with empty list
        primaryDbName + ".'t1'.'tt2'.'t3'" // More than two list
    };
    for (String replPolicy : invalidReplPolicies) {
      failed = false;
      try {
        replicateAndVerify(replPolicy, null, null, null, replicatedTables);
      } catch (Exception ex) {
        LOG.info("Got exception: {}", ex.getMessage());
        Assert.assertTrue(ex instanceof ParseException);
        failed = true;
      }
      Assert.assertTrue(failed);
    }

    primary.run("use " + primaryDbName)
            .dump(primaryDbName);
    replica.load(replicatedDbName, primaryDbName);

    // Invalid pattern, include/exclude table list is empty.
    invalidReplPolicies = new String[] {
        primaryDbName + ".''.'t2'", // Include list is empty.
        primaryDbName + ".'t1'.''" // Exclude list is empty.
    };
    for (String invalidReplPolicy : invalidReplPolicies) {
      failed = false;
      try {
        replicateAndVerify(invalidReplPolicy, null, null, null, replicatedTables);
      } catch (Exception ex) {
        LOG.info("Got exception: {}", ex.getMessage());
        Assert.assertTrue(ex instanceof SemanticException);
        Assert.assertTrue(ex.getMessage().equals(ErrorMsg.REPL_INVALID_DB_OR_TABLE_PATTERN.getMsg()));
        Assert.assertEquals(ErrorMsg.REPL_INVALID_DB_OR_TABLE_PATTERN.getErrorCode(),
          ErrorMsg.getErrorMsg(ex.getMessage()).getErrorCode());
        failed = true;
      }
      Assert.assertTrue(failed);
    }
  }

  @Test
  public void testFullDbBootstrapReplicationWithDifferentReplPolicyFormats() throws Throwable {
    String[] originalTables = new String[] {"t1", "t200", "t3333"};
    createTables(originalTables, CreateTableType.NON_ACID);

    // List of repl policy formats that leads to Full DB replication.
    String[] fullDbReplPolicies = new String[] {
        primaryDbName + ".'.*?'",
        primaryDbName
    };

    // Replicate and verify if all 3 tables are replicated to target.
    for (String replPolicy : fullDbReplPolicies) {
      replicateAndVerifyClearDump(replPolicy, null, null, null,
              null, null, originalTables, null);
    }
  }

  @Test
  public void testCaseInSensitiveNatureOfReplPolicy() throws Throwable {
    String[] originalTables = new String[] {"a1", "aA11", "B2", "Cc3"};
    createTables(originalTables, CreateTableType.NON_ACID);

    // Replicate and verify if 2 tables are replicated as per policy.
    String replPolicy = primaryDbName.toUpperCase() + ".'(.*a1+)|(cc3)|(B2)'.'(AA1+)|(b2)'";
    String[] replicatedTables = new String[] {"a1", "cc3"};
    String lastReplId = replicateAndVerify(replPolicy, null, null, null, replicatedTables);

    // Test case insensitive nature in REPLACE clause as well.
    String oldReplPolicy = replPolicy;
    replPolicy = primaryDbName + ".'(.*a1+)|(cc3)|(B2)'.'AA1+'";
    replicatedTables = new String[] {"a1", "b2", "cc3"};
    String[] bootstrappedTables = new String[] {"b2"};
    replicateAndVerify(replPolicy, oldReplPolicy, lastReplId, null, null, bootstrappedTables, replicatedTables);
  }

  @Test
  public void testBootstrapAcidTablesIncrementalPhaseWithIncludeAndExcludeList() throws Throwable {
    String[] originalNonAcidTables = new String[] {"a1", "b2"};
    String[] originalFullAcidTables = new String[] {"a2", "b1"};
    String[] originalMMAcidTables = new String[] {"a3", "a4"};
    createTables(originalNonAcidTables, CreateTableType.NON_ACID);
    createTables(originalFullAcidTables, CreateTableType.FULL_ACID);
    createTables(originalMMAcidTables, CreateTableType.MM_ACID);

    // Replicate and verify if only non-acid tables are replicated to target.
    List<String> dumpWithoutAcidClause = Collections.singletonList(
            "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='false'");
    String replPolicy = primaryDbName + ".'(a[0-9]+)|(b1)'.'a4'";
    String[] bootstrapReplicatedTables = new String[] {"a1"};
    String lastReplId = replicateAndVerify(replPolicy, null, dumpWithoutAcidClause, null, bootstrapReplicatedTables);

    // Enable acid tables for replication.
    List<String> dumpWithAcidBootstrapClause = Arrays.asList(
            "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='true'",
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES + "'='true'");
    String[] incrementalReplicatedTables = new String[] {"a1", "a2", "a3", "b1"};
    replicateAndVerify(replPolicy, lastReplId, dumpWithAcidBootstrapClause, null, incrementalReplicatedTables);
  }

  @Test
  public void testBootstrapExternalTablesWithIncludeAndExcludeList() throws Throwable {
    String[] originalNonAcidTables = new String[] {"a1", "b2"};
    String[] originalExternalTables = new String[] {"a2", "b1"};
    createTables(originalNonAcidTables, CreateTableType.NON_ACID);
    createTables(originalExternalTables, CreateTableType.EXTERNAL);

    // Replicate and verify if only 2 tables are replicated to target.
    List<String> loadWithClause = ReplicationTestUtils.includeExternalTableClause(true);
    List<String> dumpWithClause = ReplicationTestUtils.includeExternalTableClause(true);
    String replPolicy = primaryDbName + ".'(a[0-9]+)|(b2)'.'a1'";
    String[] replicatedTables = new String[] {"a2", "b2"};
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .dump(replPolicy, dumpWithClause);

    String hiveDumpDir = tuple.dumpLocation + File.separator + ReplUtils.REPL_HIVE_BASE_DIR;
    // the _file_list_external should be created as external tables are to be replicated.
    Assert.assertTrue(primary.miniDFSCluster.getFileSystem()
            .exists(new Path(hiveDumpDir, EximUtil.FILE_LIST_EXTERNAL)));

    // Verify that _file_list_external  contains only table "a2".
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("a2"), tuple.dumpLocation, primary);

    replica.load(replicatedDbName, replPolicy, loadWithClause)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(replicatedTables)
            .verifyReplTargetProperty(replicatedDbName);
  }

  @Test
  public void testBootstrapExternalTablesIncrementalPhaseWithIncludeAndExcludeList() throws Throwable {
    String[] originalNonAcidTables = new String[] {"a1", "b2"};
    String[] originalExternalTables = new String[] {"a2", "b1"};
    createTables(originalNonAcidTables, CreateTableType.NON_ACID);
    createTables(originalExternalTables, CreateTableType.EXTERNAL);

    // Bootstrap should exclude external tables.
    List<String> dumpWithClause = ReplicationTestUtils.includeExternalTableClause(false);
    List<String> loadWithClause = ReplicationTestUtils.includeExternalTableClause(false);

    String replPolicy = primaryDbName + ".'(a[0-9]+)|(b2)'.'a1'";
    String[] bootstrapReplicatedTables = new String[] {"b2"};
    String lastReplId = replicateAndVerify(replPolicy, null,
            dumpWithClause, loadWithClause, bootstrapReplicatedTables);

    // Enable external tables replication and bootstrap in incremental phase.
    String[] incrementalReplicatedTables = new String[] {"a2", "b2"};
    dumpWithClause = ReplicationTestUtils.includeExternalTableClause(true);
    dumpWithClause.add("'" + HiveConf.ConfVars.REPL_BOOTSTRAP_EXTERNAL_TABLES.varname + "'='true'");
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .dump(replPolicy, dumpWithClause);
    loadWithClause = ReplicationTestUtils.includeExternalTableClause(true);

    String hiveDumpDir = tuple.dumpLocation + File.separator + ReplUtils.REPL_HIVE_BASE_DIR;
    // the _file_list_external should be created as external tables are to be replicated.
    Assert.assertTrue(primary.miniDFSCluster.getFileSystem()
            .exists(new Path(hiveDumpDir, EximUtil.FILE_LIST_EXTERNAL)));

    // Verify that _file_list_external contains only table "a2".
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("a2"), tuple.dumpLocation, primary);

    replica.load(replicatedDbName, replPolicy, loadWithClause)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(incrementalReplicatedTables)
            .verifyReplTargetProperty(replicatedDbName);
  }

  @Test
  public void testBasicReplaceReplPolicy() throws Throwable {
    String[] originalNonAcidTables = new String[] {"t1", "t2"};
    String[] originalFullAcidTables = new String[] {"t3", "t4"};
    String[] originalMMAcidTables = new String[] {"t5"};
    createTables(originalNonAcidTables, CreateTableType.NON_ACID);
    createTables(originalFullAcidTables, CreateTableType.FULL_ACID);
    createTables(originalMMAcidTables, CreateTableType.MM_ACID);

    // Replicate and verify if only 2 tables are replicated to target.
    String replPolicy = primaryDbName + ".'(t1)|(t4)'";
    String oldReplPolicy = null;
    String[] replicatedTables = new String[] {"t1", "t4"};
    String lastReplId = replicateAndVerify(replPolicy, null, null, null, replicatedTables);

    // Exclude t4 and include t3, t6
    createTables(new String[] {"t6"}, CreateTableType.MM_ACID);
    oldReplPolicy = replPolicy;
    replPolicy = primaryDbName + ".'t1|t3|t6'";
    replicatedTables = new String[] {"t1", "t3", "t6"};
    String[] bootstrappedTables = new String[] {"t3", "t6"};
    lastReplId = replicateAndVerify(replPolicy, oldReplPolicy, lastReplId,
            null, null, bootstrappedTables, replicatedTables);

    // Convert to Full Db repl policy. All tables should be included.
    oldReplPolicy = replPolicy;
    replPolicy = primaryDbName;
    replicatedTables = new String[] {"t1", "t2", "t3", "t4", "t5", "t6"};
    bootstrappedTables = new String[] {"t2", "t4", "t5"};
    replicateAndVerify(replPolicy, oldReplPolicy, lastReplId,
            null, null, bootstrappedTables, replicatedTables);

    // Convert to regex that excludes t3, t4 and t5.
    oldReplPolicy = replPolicy;
    replPolicy = primaryDbName + ".'.*?'.'t[3-5]+'";
    replicatedTables = new String[] {"t1", "t2", "t6"};
    bootstrappedTables = new String[] {};
    replicateAndVerify(replPolicy, oldReplPolicy, lastReplId,
            null, null, bootstrappedTables, replicatedTables);
  }

  @Test
  public void testReplacePolicyOnBootstrapAcidTablesIncrementalPhase() throws Throwable {
    String[] originalNonAcidTables = new String[] {"a1", "b1", "c1"};
    String[] originalFullAcidTables = new String[] {"a2", "b2"};
    String[] originalMMAcidTables = new String[] {"a3", "a4"};
    createTables(originalNonAcidTables, CreateTableType.NON_ACID);
    createTables(originalFullAcidTables, CreateTableType.FULL_ACID);
    createTables(originalMMAcidTables, CreateTableType.MM_ACID);

    // Replicate and verify if only non-acid tables are replicated to target.
    List<String> dumpWithoutAcidClause = Collections.singletonList(
            "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='false'");
    String replPolicy = primaryDbName + ".'(a[0-9]+)|(b[0-9]+)'.'b1'";
    String[] bootstrapReplicatedTables = new String[] {"a1"};
    String lastReplId = replicateAndVerify(replPolicy, null,
            dumpWithoutAcidClause, null, bootstrapReplicatedTables);

    // Enable acid tables for replication. Also, replace, replication policy to exclude "b1" and "a3"
    // instead of "a1" alone.
    String oldReplPolicy = replPolicy;
    replPolicy = primaryDbName + ".'[a-z]+[0-9]+'.'(a3)|(b1)'";
    List<String> dumpWithAcidBootstrapClause = Arrays.asList(
            "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='true'",
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES + "'='true'");
    String[] incrementalReplicatedTables = new String[] {"a1", "a2", "a4", "b2", "c1"};
    String[] bootstrappedTables = new String[] {"a2", "a4", "b2", "c1"};
    replicateAndVerify(replPolicy, oldReplPolicy, lastReplId,
            dumpWithAcidBootstrapClause, null, bootstrappedTables, incrementalReplicatedTables);
  }

  @Test
  public void testReplacePolicyWhenAcidTablesDisabledForRepl() throws Throwable {
    String[] originalNonAcidTables = new String[] {"a1", "b1", "c1"};
    String[] originalFullAcidTables = new String[] {"a2"};
    createTables(originalNonAcidTables, CreateTableType.NON_ACID);
    createTables(originalFullAcidTables, CreateTableType.FULL_ACID);

    // Replicate and verify if only non-acid tables are replicated to target.
    List<String> dumpWithoutAcidClause = Collections.singletonList(
            "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='false'");
    String replPolicy = primaryDbName + ".'(a[0-9]+)|(b[0-9]+)'.'b1'";
    String[] bootstrapReplicatedTables = new String[] {"a1"};
    String lastReplId = replicateAndVerify(replPolicy, null,
            dumpWithoutAcidClause, null, bootstrapReplicatedTables);

    // Continue to disable ACID tables for replication. Also, replace, replication policy to include
    // "a2" but exclude "a1" and "b1". Still ACID tables shouldn't be bootstrapped. Only non-ACID
    // table "b1" should be bootstrapped.
    String oldReplPolicy = replPolicy;
    replPolicy = primaryDbName + ".'(a[0-9]+)|(b[0-9]+)'.'a2'";
    String[] incrementalReplicatedTables = new String[] {"a1", "b1"};
    String[] bootstrappedTables = new String[] {"b1"};
    lastReplId = replicateAndVerify(replPolicy, oldReplPolicy, lastReplId,
            dumpWithoutAcidClause, null, bootstrappedTables, incrementalReplicatedTables);
  }

  @Test
  public void testReplacePolicyOnBootstrapExternalTablesIncrementalPhase() throws Throwable {
    String[] originalAcidTables = new String[] {"a1", "b1"};
    String[] originalExternalTables = new String[] {"a2", "b2", "c2"};
    createTables(originalAcidTables, CreateTableType.FULL_ACID);
    createTables(originalExternalTables, CreateTableType.EXTERNAL);

    // Bootstrap should exclude external tables.
    List<String> loadWithClause = ReplicationTestUtils.includeExternalTableClause(false);
    List<String> dumpWithClause = ReplicationTestUtils.includeExternalTableClause(false);
    String replPolicy = primaryDbName + ".'(a[0-9]+)|(b1)'.'a1'";
    String[] bootstrapReplicatedTables = new String[] {"b1"};
    String lastReplId = replicateAndVerify(replPolicy, null,
            dumpWithClause, loadWithClause, bootstrapReplicatedTables);

    // Continue to disable external tables for replication. Also, replace, replication policy to exclude
    // "b1" and include "a1".
    String oldReplPolicy = replPolicy;
    replPolicy = primaryDbName + ".'(a[0-9]+)|(b[0-9]+)'.'(a2)|(b1)'";
    String[] incrementalReplicatedTables = new String[] {"a1"};
    String[] bootstrappedTables = new String[] {"a1"};
    lastReplId = replicateAndVerify(replPolicy, oldReplPolicy, lastReplId,
            dumpWithClause, loadWithClause, bootstrappedTables, incrementalReplicatedTables);

    // Enable external tables replication and bootstrap in incremental phase. Also, replace,
    // replication policy to exclude tables with prefix "b".
    oldReplPolicy = replPolicy;
    replPolicy = primaryDbName + ".'[a-z]+[0-9]+'.'b[0-9]+'";
    incrementalReplicatedTables = new String[] {"a1", "a2", "c2"};
    bootstrappedTables = new String[] {"a2", "c2"};
    dumpWithClause = Arrays.asList("'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='true'",
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_EXTERNAL_TABLES.varname + "'='true'");
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .dump(replPolicy, dumpWithClause);
    loadWithClause = ReplicationTestUtils.includeExternalTableClause(true);

    String hiveDumpDir = tuple.dumpLocation + File.separator + ReplUtils.REPL_HIVE_BASE_DIR;
    // _file_list_external should be created as external tables are to be replicated.
    Assert.assertTrue(primary.miniDFSCluster.getFileSystem()
            .exists(new Path(hiveDumpDir, EximUtil.FILE_LIST_EXTERNAL)));

    // Verify that _file_list_external contains table "a2" and "c2".
    ReplicationTestUtils.assertExternalFileList(Arrays.asList("a2", "c2"), tuple.dumpLocation, primary);

    // Verify if the expected tables are bootstrapped.
    verifyBootstrapDirInIncrementalDump(tuple.dumpLocation, bootstrappedTables);

    replica.load(replicatedDbName, replPolicy, loadWithClause)
            .run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(incrementalReplicatedTables)
            .verifyReplTargetProperty(replicatedDbName);
  }

  @Test
  public void testRenameTableScenariosBasic() throws Throwable {
    String replPolicy = primaryDbName + ".'in[0-9]+'.'out[0-9]+'";
    String lastReplId = replicateAndVerify(replPolicy, null, null, null,
            null, new String[] {}, new String[] {});

    String[] originalNonAcidTables = new String[] {"in1", "in2", "out3", "out4", "out5", "out6"};
    createTables(originalNonAcidTables, CreateTableType.NON_ACID);

    // Replicate and verify if only 2 tables are replicated to target.
    String[] replicatedTables = new String[] {"in1", "in2"};
    String[] bootstrapTables = new String[] {};
    lastReplId = replicateAndVerify(replPolicy, replPolicy, lastReplId, null,
            null, bootstrapTables, replicatedTables);

    // Rename tables to make them satisfy the filter.
    primary.run("use " + primaryDbName)
            .run("alter table out3 rename to in3")
            .run("alter table out4 rename to in4")
            .run("alter table out5 rename to in5");

    replicatedTables = new String[] {"in1", "in2", "in3", "in4", "in5"};
    bootstrapTables = new String[] {"in3", "in4", "in5"};
    lastReplId = replicateAndVerify(replPolicy, replPolicy, lastReplId, null,
            null, bootstrapTables, replicatedTables);

    primary.run("use " + primaryDbName)
            .run("alter table in3 rename to in7")
            .run("alter table in7 rename to in8") // Double rename, both satisfying the filter, so no bootstrap.
            .run("alter table in4 rename to out9") // out9 does not match the filter so in4 should be dropped.
            .run("alter table in5 rename to out10") // Rename from satisfying name to not satisfying name.
            .run("alter table out10 rename to in11"); // from non satisfying to satisfying, should be bootstrapped

    replicatedTables = new String[] {"in1", "in2", "in8", "in11"};
    bootstrapTables = new String[] {"in11"};
    lastReplId = replicateAndVerify(replPolicy, replPolicy, lastReplId, null,
            null, bootstrapTables, replicatedTables);

    primary.run("use " + primaryDbName)
            .run("alter table in8 rename to in12") // table is renamed from satisfying to satisfying, no bootstrap
            .run("alter table out9 rename to in13") // out9 does not match the filter so in13 should be bootstrapped.
            .run("alter table in13 rename to in14") // table is renamed from satisfying to satisfying
            .run("alter table in2 rename to out200") // this will change the rename to drop in2
            .run("alter table out200 rename to in200") // this will add the bootstrap for in200
            .run("alter table in1 rename to out100") // this will change the rename to drop
            .run("alter table out100 rename to in100") // this will add the bootstrap
            .run("drop table in100");  // table in100 is dropped, so no bootstrap should happen.

    replicatedTables = new String[] {"in200", "in12", "in11", "in14"};
    bootstrapTables = new String[] {"in14", "in200"};
    replicateAndVerify(replPolicy, replPolicy, lastReplId, null,
            null, bootstrapTables, replicatedTables);
  }

  @Test
  public void testRenameTableScenariosWithDmlOperations() throws Throwable {
    String replPolicy = primaryDbName + ".'in[0-9]+'.'out[0-9]+'";
    String lastReplId = replicateAndVerify(replPolicy, null, null, null,
            null, new String[] {}, new String[] {});

    String[] originalFullAcidTables = new String[] {"in1"};
    String[] originalNonAcidTables = new String[] {"in100"};
    createTables(originalFullAcidTables, CreateTableType.FULL_ACID);
    createTables(originalNonAcidTables, CreateTableType.NON_ACID);

    // Replicate and verify if only 2 tables are replicated to target.
    String[] replicatedTables = new String[] {"in1", "in100"};
    String[] bootstrapTables = new String[] {};
    lastReplId = replicateAndVerify(replPolicy, null, lastReplId, null,
            null, bootstrapTables, replicatedTables);

    // Rename tables and do some operations.
    primary.run("use " + primaryDbName)
            .run("alter table in1 rename to out1")
            .run("insert into out1 values(2, 100)")
            .run("alter table out1 rename to in4")
            .run("alter table in100 rename to out100")
            .run("insert into out100 values(2, 100)")
            .run("alter table out100 rename to in400");

    replicatedTables = new String[] {"in4", "in400"};
    bootstrapTables = new String[] {"in4", "in400"};
    replicateAndVerify(replPolicy, null, lastReplId, null,
            null, bootstrapTables, replicatedTables, new String[] {"1", "2"});
  }

  @Test
  public void testRenameTableScenariosAcidTable() throws Throwable {
    String replPolicy = primaryDbName + ".'in[0-9]+'.'out[0-9]+'";
    List<String> dumpWithClause = Arrays.asList(
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES.varname + "'='false'",
            "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='false'"
    );
    String lastReplId = replicateAndVerify(replPolicy, null, null, dumpWithClause,
            null, new String[] {}, new String[] {});

    String[] originalNonAcidTables = new String[] {"in1", "out4"};
    String[] originalFullAcidTables = new String[] {"in2", "out5"};
    String[] originalMMAcidTables = new String[] {"out3", "out6"};
    createTables(originalNonAcidTables, CreateTableType.NON_ACID);
    createTables(originalFullAcidTables, CreateTableType.FULL_ACID);
    createTables(originalMMAcidTables, CreateTableType.MM_ACID);

    // Replicate and verify if only 1 tables are replicated to target. Acid tables are not dumped.
    String[] replicatedTables = new String[] {"in1"};
    String[] bootstrapTables = new String[] {};
    lastReplId = replicateAndVerify(replPolicy, replPolicy, lastReplId, dumpWithClause,
            null, bootstrapTables, replicatedTables);

    // Rename tables to make them satisfy the filter and enable acid tables.
    primary.run("use " + primaryDbName)
            .run("alter table out3 rename to in3")
            .run("alter table out4 rename to in4")
            .run("alter table out5 rename to in5");

    dumpWithClause = Arrays.asList("'" + HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES.varname + "'='true'",
            "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='true'");
    replicatedTables = new String[] {"in1", "in2", "in3", "in4", "in5"};
    bootstrapTables = new String[] {"in2", "in3", "in4", "in5"};
    replicateAndVerify(replPolicy, replPolicy, lastReplId, dumpWithClause,
            null, bootstrapTables, replicatedTables);
  }

  @Test
  public void testRenameTableScenariosExternalTable() throws Throwable {
    String replPolicy = primaryDbName + ".'in[0-9]+'.'out[0-9]+'";
    List<String> loadWithClause = ReplicationTestUtils.includeExternalTableClause(false);
    List<String> dumpWithClause = Arrays.asList(
            "'" +  HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='false'",
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_EXTERNAL_TABLES.varname + "'='false'",
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES.varname + "'='false'",
            "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='false'"
    );
    String lastReplId = replicateAndVerify(replPolicy, null, null, dumpWithClause,
            loadWithClause, new String[] {}, new String[] {});

    String[] originalNonAcidTables = new String[] {"in1", "out4"};
    String[] originalExternalTables = new String[] {"in2", "out5"};
    String[] originalMMAcidTables = new String[] {"in3", "out6"};
    createTables(originalNonAcidTables, CreateTableType.NON_ACID);
    createTables(originalExternalTables, CreateTableType.EXTERNAL);
    createTables(originalMMAcidTables, CreateTableType.MM_ACID);

    // Replicate and verify if only 1 tables are replicated to target. Acid and external tables are not dumped.
    String[] replicatedTables = new String[] {"in1"};
    String[] bootstrapTables = new String[] {};
    lastReplId = replicateAndVerify(replPolicy, null, lastReplId, dumpWithClause,
            loadWithClause, bootstrapTables, replicatedTables);

    // Rename tables to make them satisfy the filter and enable acid and external tables.
    primary.run("use " + primaryDbName)
            .run("alter table out4 rename to in4")
            .run("alter table out5 rename to in5");

    dumpWithClause = Arrays.asList(
            "'" +  HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='true'",
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_EXTERNAL_TABLES.varname + "'='true'",
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES.varname + "'='true'",
            "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='true'",
            "'distcp.options.pugpb'=''"
    );
    loadWithClause = ReplicationTestUtils.includeExternalTableClause(true);
    replicatedTables = new String[] {"in1", "in2", "in3", "in4", "in5"};
    bootstrapTables = new String[] {"in2", "in3", "in4", "in5"};
    lastReplId = replicateAndVerify(replPolicy, null, lastReplId, dumpWithClause,
            loadWithClause, null, replicatedTables);

    dumpWithClause = Arrays.asList("'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='true'",
            "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='true'");

    primary.run("use " + primaryDbName)
            .run("alter table out6 rename to in6")  // external table bootstrap.
            .run("alter table in5 rename to out7") // in5 should be deleted.
            .run("alter table out7 rename to in7") // MM table bootstrap.
            .run("alter table in1 rename to out10") // in1 should be deleted.
            .run("alter table out10 rename to in11"); // normal table bootstrapped

    replicatedTables = new String[] {"in2", "in3", "in4", "in11", "in6", "in7"};
    bootstrapTables = new String[] {"in11", "in6", "in7"};
    replicateAndVerify(replPolicy, null, lastReplId, dumpWithClause,
            loadWithClause, bootstrapTables, replicatedTables);
  }

  @Test
  public void testRenameTableScenariosWithReplaceExternalTable() throws Throwable {
    List<String> loadWithClause = ReplicationTestUtils.includeExternalTableClause(true);
    List<String> dumpWithClause = ReplicationTestUtils.externalTableWithClause(loadWithClause, true, true);
    String replPolicy = primaryDbName + ".'(in[0-9]+)|(out4)|(out5)|(out1500)'";
    String lastReplId = replicateAndVerify(replPolicy, null, null, dumpWithClause,
            loadWithClause, new String[] {}, new String[] {});

    String[] originalExternalTables = new String[] {"in1", "in2", "out3", "out4", "out10", "out11", "out1500"};
    createTables(originalExternalTables, CreateTableType.EXTERNAL);

    // Rename the tables to satisfy the condition also replace the policy.
    primary.run("use " + primaryDbName)
            .run("alter table out4 rename to in5") // Old name matching old, new name matching both
            .run("alter table out3 rename to in6") // Old name not matching old and new name matching both
            .run("alter table in1 rename to out5") // Old name matching old, new name matching only old.
            .run("alter table in2 rename to in7") // Old name matching old, only new name not matching new.
            .run("alter table out1500 rename to out1501") // Old name matching old, only old name not matching new.
            .run("alter table out10 rename to in10") // Old name not matching old and new name matching both
            .run("drop table in10")
            .run("alter table out11 rename to out12") // Old name not matching old and new name not matching both
            .run("alter table out12 rename to in12"); // Old name not matching old and new name matching both

    String newPolicy = primaryDbName + ".'(in[0-9]+)|(out1500)|(in2)'";
    dumpWithClause = Arrays.asList(
            "'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='true'",
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_EXTERNAL_TABLES.varname + "'='false'",
            "'distcp.options.pugpb'=''"
    );

    // in2 should be dropped.
    String[] replicatedTables = new String[] {"in5", "in6", "in7", "in12"};
    String[] bootstrapTables = new String[] {"in5", "in6", "in7", "in12"};
    replicateAndVerify(newPolicy, replPolicy, lastReplId, dumpWithClause,
            loadWithClause, bootstrapTables, replicatedTables);
  }

  @Test
  public void testRenameTableScenariosWithReplacePolicyDMLOperattion() throws Throwable {
    String replPolicy = primaryDbName + ".'(in[0-9]+)|(out5000)|(out5001)'.'(in100)|(in200)|(in305)'";
    String lastReplId = replicateAndVerify(replPolicy, null, null, null,
            null, new String[] {}, new String[] {});

    String[] originalFullAcidTables = new String[] {"in1", "in2", "out3", "out4", "out5",
        "in100", "in200", "in300", "out3000", "out4000", "out4001"};
    String[] originalNonAcidTables = new String[] {"in400", "out500"};
    createTables(originalFullAcidTables, CreateTableType.FULL_ACID);
    createTables(originalNonAcidTables, CreateTableType.NON_ACID);

    // Replicate and verify if only 4 tables are replicated to target.
    String[] replicatedTables = new String[] {"in1", "in2", "in300", "in400"};
    String[] bootstrapTables = new String[] {};
    lastReplId = replicateAndVerify(replPolicy, null, lastReplId, null,
            null, bootstrapTables, replicatedTables);

    // Rename the tables to satisfy the condition also replace the policy.
    String newPolicy = primaryDbName + ".'(in[0-9]+)|(out3000)'.'in2'";
    primary.run("use " + primaryDbName)
            .run("alter table in200 rename to in2000") // Old name not matching old, old and new matching new policy.
            .run("alter table in400 rename to out400") // Old name matching new and old policy, new matching none.
            .run("alter table out500 rename to in500")
            .run("alter table out3000 rename to in3000") // Old name not matching old policy and both name matching new
            .run("alter table in1 rename to out7")
            .run("alter table in300 rename to in301") // for rename its all matching to matching.
            .run("alter table in301 rename to in305") // ideally in305 bootstrap should not happen.
            .run("alter table out3 rename to in8")
            .run("alter table out4 rename to in9")
            .run("drop table in9")
            .run("alter table out5 rename to in10")
            .run("alter table in10 rename to out11")
            .run("drop table out11")
            .run("insert into in100 values(2, 100)")
            .run("insert into in8 values(2, 100)")
            .run("insert into in305 values(2, 100)")
            .run("insert into in3000 values (2, 100)")
            .run("insert into in2000 values (2, 100)")
            .run("insert into in500 values(2, 100)")
            .run("alter table out4000 rename to out5000")
            .run("alter table out5000 rename to in5000")
            .run("insert into in5000 values (2, 100)")
            .run("alter table out4001 rename to out5001")
            .run("alter table out5001 rename to out5002")
            .run("insert into out5002 values (2, 100)");

    // in2 should be dropped.
    replicatedTables = new String[] {"in100", "in2000", "in8", "in305", "in500", "in3000", "in5000"};
    bootstrapTables = new String[] {"in500", "in8", "in5000", "in305", "in3000", "in2000", "in100"};
    lastReplId = replicateAndVerify(newPolicy, replPolicy, lastReplId, null,
            null, bootstrapTables, replicatedTables, new String[] {"1", "2"});

    // No table filter
    replPolicy = newPolicy;
    newPolicy = primaryDbName;
    primary.run("use " + primaryDbName)
            .run("alter table in100 rename to in12") // should be just rename, but for replace its always bootstrap
            .run("alter table in2000 rename to out12") // bootstrap by replace policy handler
            .run("alter table out400 rename to in400") // bootstrap by rename
            .run("alter table out7 rename to in1")    // bootstrap by rename
            .run("alter table in305 rename to in301")  // should be just rename, but for replace its always bootstrap
            .run("alter table in301 rename to in300") // should be just rename, but for replace its always bootstrap
            .run("alter table in8 rename to out3") // should be just rename, but for replace its always bootstrap
            .run("insert into in2 values(2, 100)")
            .run("insert into in1 values(2, 100)")
            .run("insert into in400 values(2, 100)")
            .run("drop table out3");   // table will be removed from bootstrap list.

    replicatedTables = new String[] {"in12", "in400", "in1", "in300", "out12", "in500", "in3000", "in2",
        "in5000", "out5002"};
    bootstrapTables = new String[] {"out12", "in2", "in400", "in1", "in300", "in12", "out5002"};
    replicateAndVerify(newPolicy, replPolicy, lastReplId, null,
            null, bootstrapTables, replicatedTables, new String[] {"1", "2"});
  }

  @Test
  public void testRenameTableScenariosUpgrade() throws Throwable {
    // Policy with no table level filter, no ACID and external table.
    String replPolicy = primaryDbName;
    List<String> loadWithClause = ReplicationTestUtils.includeExternalTableClause(false);
    List<String> dumpWithClause = Arrays.asList(
            "'" +  HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='false'",
            "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='false'"
    );

    String[] originalNonAcidTables = new String[] {"in1", "out4"};
    String[] originalExternalTables = new String[] {"in2", "out5"};
    String[] originalAcidTables = new String[] {"in3", "out6"};
    createTables(originalNonAcidTables, CreateTableType.NON_ACID);
    createTables(originalExternalTables, CreateTableType.EXTERNAL);
    createTables(originalAcidTables, CreateTableType.FULL_ACID);

    // Only NON_ACID table replication is done.
    String[] replicatedTables = new String[] {"in1", "out4"};
    String lastReplId = replicateAndVerify(replPolicy, null, null, dumpWithClause,
            null, new String[] {}, replicatedTables);

    originalNonAcidTables = new String[] {"in7", "out10"};
    originalExternalTables = new String[] {"in8", "out11"};
    originalAcidTables = new String[] {"in9", "out12"};
    createTables(originalNonAcidTables, CreateTableType.NON_ACID);
    createTables(originalExternalTables, CreateTableType.EXTERNAL);
    createTables(originalAcidTables, CreateTableType.MM_ACID);

    primary.run("use " + primaryDbName)
            .run("alter table out4 rename to in4")
            .run("alter table out5 rename to in5")
            .run("alter table out6 rename to in6");

    // Table level replication with ACID and EXTERNAL table.
    String newReplPolicy = primaryDbName + ".'in[0-9]+'.'in8'";
    dumpWithClause = Arrays.asList(
            "'" +  HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='true'",
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_EXTERNAL_TABLES.varname + "'='true'",
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_ACID_TABLES.varname + "'='true'",
            "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='true'",
            "'distcp.options.pugpb'=''"
    );

    replicatedTables = new String[] {"in1", "in2", "in3", "in4", "in5", "in6", "in7", "in9"};
    String[] bootstrapTables = new String[] {"in2", "in3", "in4", "in5", "in6", "in9"};
    lastReplId = replicateAndVerify(newReplPolicy, replPolicy, lastReplId, dumpWithClause,
            loadWithClause, bootstrapTables, replicatedTables);

    primary.run("use " + primaryDbName)
            .run("alter table in4 rename to out4")
            .run("alter table in5 rename to out5")
            .run("alter table in6 rename to out6");

    dumpWithClause = Arrays.asList(
            "'" +  HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='true'",
            "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='true'",
            "'distcp.options.pugpb'=''"
    );

    // Database replication with ACID and EXTERNAL table.
    replicatedTables = new String[] {"in1", "in2", "in3", "out4", "out5", "out6", "in7", "in8",
            "in9", "out10", "out11", "out12"};
    bootstrapTables = new String[] {"out4", "out5", "out6", "in8", "out10", "out11", "out12"};
    replicateAndVerify(replPolicy, newReplPolicy, lastReplId, dumpWithClause,
            loadWithClause, bootstrapTables, replicatedTables);
  }

  @Test
  public void testCheckPointingDataDumpFailureSamePolicyExpression() throws Throwable {
    //To force distcp copy
    List<String> dumpClause = Arrays.asList(
      "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
      "'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='false'",
      "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
      "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
        + UserGroupInformation.getCurrentUser().getUserName() + "'");

    String replPolicy = primaryDbName + ".'(t1+)|(t2)'.'t11|t3'";

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
      .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
      .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
      .run("insert into t1 values (1)")
      .run("insert into t1 values (2)")
      .run("insert into t1 values (3)")
      .run("insert into t2 values (11)")
      .run("insert into t2 values (21)")
      .dump(replPolicy);

    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    Path metadataPath = new Path(dumpPath, EximUtil.METADATA_PATH_NAME);
    long modifiedTimeMetadata = fs.getFileStatus(metadataPath).getModificationTime();
    Path dataPath = new Path(dumpPath, EximUtil.DATA_PATH_NAME);
    Path dbDataPath = new Path(dataPath, primaryDbName.toLowerCase());
    Path tablet1Path = new Path(dbDataPath, "t1");
    Path tablet2Path = new Path(dbDataPath, "t2");
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
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
    WarehouseInstance.Tuple nextDump = primary.dump(replPolicy, dumpClause);
    assertEquals(nextDump.dumpLocation, bootstrapDump.dumpLocation);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    assertEquals(modifiedTimeTable1, fs.getFileStatus(tablet1Path).getModificationTime());
    assertEquals(modifiedTimeTable1CopyFile, fs.listStatus(tablet1Path)[0].getModificationTime());
    assertTrue(modifiedTimeTable2 < fs.getFileStatus(tablet2Path).getModificationTime());
    assertTrue(modifiedTimeMetadata < fs.getFileStatus(metadataPath).getModificationTime());
  }

  @Test
  public void testCheckPointingDataDumpFailureDiffPolicyExpression() throws Throwable {
    //To force distcp copy
    List<String> dumpClause = Arrays.asList(
      "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE.varname + "'='1'",
      "'" + HiveConf.ConfVars.HIVE_IN_TEST_REPL.varname + "'='false'",
      "'" + HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES.varname + "'='0'",
      "'" + HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname + "'='"
        + UserGroupInformation.getCurrentUser().getUserName() + "'");

    String replPolicy = primaryDbName + ".'(t1+)|(t2)'.'t11|t3'";

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
      .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
      .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
      .run("insert into t1 values (1)")
      .run("insert into t1 values (2)")
      .run("insert into t1 values (3)")
      .run("insert into t2 values (11)")
      .run("insert into t2 values (21)")
      .dump(replPolicy);

    FileSystem fs = new Path(bootstrapDump.dumpLocation).getFileSystem(conf);
    Path dumpPath = new Path(bootstrapDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    Path metadataPath = new Path(dumpPath, EximUtil.METADATA_PATH_NAME);
    Path dataPath = new Path(dumpPath, EximUtil.DATA_PATH_NAME);
    Path dbDataPath = new Path(dataPath, primaryDbName.toLowerCase());
    Path tablet2Path = new Path(dbDataPath, "t2");
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    //Delete dump ack and t2 data, metadata should be rewritten, data should be same for t1 but rewritten for t2
    fs.delete(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString()), true);
    assertFalse(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
    FileStatus[] statuses = fs.listStatus(tablet2Path);
    //Delete t2 data
    fs.delete(statuses[0].getPath(), true);
    //Do another dump with expression modified. It should redo the dump
    replPolicy = primaryDbName + ".'(t1+)|(t2)'.'t11|t3|t12'";
    WarehouseInstance.Tuple nextDump = primary.dump(replPolicy, dumpClause);
    assertNotEquals(nextDump.dumpLocation, bootstrapDump.dumpLocation);
    dumpPath = new Path(nextDump.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    assertTrue(fs.exists(new Path(dumpPath, DUMP_ACKNOWLEDGEMENT.toString())));
  }

  @Test
  public void testIncrementalDumpCheckpointingSameExpression() throws Throwable {
    String replPolicy = primaryDbName + ".'(t1+)|(t2)'.'t11|t3'";

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
      .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
      .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
      .dump(replPolicy);

    replica.load(replicatedDbName, replPolicy)
      .run("select * from " + replicatedDbName + ".t1")
      .verifyResults(new String[] {})
      .run("select * from " + replicatedDbName + ".t2")
      .verifyResults(new String[] {});


    //Case 1: When the last dump finished all the events and
    //only  _finished_dump file at the hiveDumpRoot was about to be written when it failed.
    ReplDumpWork.testDeletePreviousDumpMetaPath(true);

    WarehouseInstance.Tuple incrementalDump1 = primary.run("use " + primaryDbName)
      .run("insert into t1 values (1)")
      .run("insert into t2 values (2)")
      .dump(replPolicy);

    Path hiveDumpDir = new Path(incrementalDump1.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path ackFile = new Path(hiveDumpDir, ReplAck.DUMP_ACKNOWLEDGEMENT.toString());
    Path ackLastEventID = new Path(hiveDumpDir, ReplAck.EVENTS_DUMP.toString());
    FileSystem fs = FileSystem.get(hiveDumpDir.toUri(), primary.hiveConf);
    assertTrue(fs.exists(ackFile));
    assertTrue(fs.exists(ackLastEventID));

    fs.delete(ackFile, false);

    long firstIncEventID = Long.parseLong(bootstrapDump.lastReplicationId) + 1;
    long lastIncEventID = Long.parseLong(incrementalDump1.lastReplicationId);
    assertTrue(lastIncEventID > (firstIncEventID + 1));
    Map<Path, Long> pathModTimeMap = new HashMap<>();
    for (long eventId=firstIncEventID; eventId<=lastIncEventID; eventId++) {
      Path eventRoot = new Path(hiveDumpDir, String.valueOf(eventId));
      if (fs.exists(eventRoot)) {
        for (FileStatus fileStatus: fs.listStatus(eventRoot)) {
          pathModTimeMap.put(fileStatus.getPath(), fileStatus.getModificationTime());
        }
      }
    }

    ReplDumpWork.testDeletePreviousDumpMetaPath(false);
    WarehouseInstance.Tuple incrementalDump2 = primary.run("use " + primaryDbName)
      .dump(replPolicy);
    assertEquals(incrementalDump1.dumpLocation, incrementalDump2.dumpLocation);
    assertTrue(fs.exists(ackFile));
    //check events were not rewritten.
    for(Map.Entry<Path, Long> entry :pathModTimeMap.entrySet()) {
      assertEquals((long)entry.getValue(),
        fs.getFileStatus(new Path(hiveDumpDir, entry.getKey())).getModificationTime());
    }

    replica.load(replicatedDbName, replPolicy)
      .run("select * from " + replicatedDbName + ".t1")
      .verifyResults(new String[] {"1"})
      .run("select * from " + replicatedDbName + ".t2")
      .verifyResults(new String[] {"2"});


    //Case 2: When the last dump was half way through
    ReplDumpWork.testDeletePreviousDumpMetaPath(true);

    WarehouseInstance.Tuple incrementalDump3 = primary.run("use " + primaryDbName)
      .run("insert into t1 values (3)")
      .run("insert into t2 values (4)")
      .dump(replPolicy);

    hiveDumpDir = new Path(incrementalDump3.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    ackFile = new Path(hiveDumpDir, ReplAck.DUMP_ACKNOWLEDGEMENT.toString());
    ackLastEventID = new Path(hiveDumpDir, ReplAck.EVENTS_DUMP.toString());
    fs = FileSystem.get(hiveDumpDir.toUri(), primary.hiveConf);
    assertTrue(fs.exists(ackFile));
    assertTrue(fs.exists(ackLastEventID));

    fs.delete(ackFile, false);
    //delete last three events and test if it recovers.
    long lastEventID = Long.parseLong(incrementalDump3.lastReplicationId);
    Path lastEvtRoot = new Path(hiveDumpDir + File.separator + String.valueOf(lastEventID));
    Path secondLastEvtRoot = new Path(hiveDumpDir + File.separator + String.valueOf(lastEventID - 1));
    Path thirdLastEvtRoot = new Path(hiveDumpDir + File.separator + String.valueOf(lastEventID - 2));
    assertTrue(fs.exists(lastEvtRoot));
    assertTrue(fs.exists(secondLastEvtRoot));
    assertTrue(fs.exists(thirdLastEvtRoot));

    pathModTimeMap = new HashMap<>();
    for (long idx = Long.parseLong(incrementalDump2.lastReplicationId)+1; idx < (lastEventID - 2); idx++) {
      Path eventRoot  = new Path(hiveDumpDir, String.valueOf(idx));
      if (fs.exists(eventRoot)) {
        for (FileStatus fileStatus: fs.listStatus(eventRoot)) {
          pathModTimeMap.put(fileStatus.getPath(), fileStatus.getModificationTime());
        }
      }
    }
    long lastEvtModTimeOld = fs.getFileStatus(lastEvtRoot).getModificationTime();
    long secondLastEvtModTimeOld = fs.getFileStatus(secondLastEvtRoot).getModificationTime();
    long thirdLastEvtModTimeOld = fs.getFileStatus(thirdLastEvtRoot).getModificationTime();

    fs.delete(lastEvtRoot, true);
    fs.delete(secondLastEvtRoot, true);
    fs.delete(thirdLastEvtRoot, true);
    EventsDumpMetadata eventsDumpMetadata = EventsDumpMetadata.deserialize(ackLastEventID, conf);
    eventsDumpMetadata.setLastReplId(lastEventID - 3);
    eventsDumpMetadata.setEventsDumpedCount(eventsDumpMetadata.getEventsDumpedCount() - 3);
    org.apache.hadoop.hive.ql.parse.repl.dump.Utils.writeOutput(eventsDumpMetadata.serialize(), ackLastEventID,
            primary.hiveConf);
    ReplDumpWork.testDeletePreviousDumpMetaPath(false);

    WarehouseInstance.Tuple incrementalDump4 = primary.run("use " + primaryDbName)
      .dump(replPolicy);

    assertEquals(incrementalDump3.dumpLocation, incrementalDump4.dumpLocation);

    assertTrue(fs.getFileStatus(lastEvtRoot).getModificationTime() > lastEvtModTimeOld);
    assertTrue(fs.getFileStatus(secondLastEvtRoot).getModificationTime() > secondLastEvtModTimeOld);
    assertTrue(fs.getFileStatus(thirdLastEvtRoot).getModificationTime() > thirdLastEvtModTimeOld);

    //Check other event dump files have not been modified.
    for (Map.Entry<Path, Long> entry:pathModTimeMap.entrySet()) {
      assertEquals((long)entry.getValue(), fs.getFileStatus(entry.getKey()).getModificationTime());
    }

    replica.load(replicatedDbName, primaryDbName)
      .run("select * from " + replicatedDbName + ".t1")
      .verifyResults(new String[] {"1", "3"})
      .run("select * from " + replicatedDbName + ".t2")
      .verifyResults(new String[] {"2", "4"});
  }

  @Test
  public void testIncrementalDumpCheckpointingDiffExpression() throws Throwable {
    String replPolicy = primaryDbName + ".'(t1+)|(t2)'.'t11|t3'";

    WarehouseInstance.Tuple bootstrapDump = primary.run("use " + primaryDbName)
      .run("CREATE TABLE t1(a string) STORED AS TEXTFILE")
      .run("CREATE TABLE t2(a string) STORED AS TEXTFILE")
      .dump(replPolicy);

    replica.load(replicatedDbName, replPolicy)
      .run("select * from " + replicatedDbName + ".t1")
      .verifyResults(new String[] {})
      .run("select * from " + replicatedDbName + ".t2")
      .verifyResults(new String[] {});

    //When the last dump was half way through and expression modified
    ReplDumpWork.testDeletePreviousDumpMetaPath(true);

    WarehouseInstance.Tuple incrementalDump3 = primary.run("use " + primaryDbName)
      .run("insert into t1 values (3)")
      .run("insert into t2 values (4)")
      .dump(replPolicy);

    Path hiveDumpDir = new Path(incrementalDump3.dumpLocation, ReplUtils.REPL_HIVE_BASE_DIR);
    Path ackFile = new Path(hiveDumpDir, ReplAck.DUMP_ACKNOWLEDGEMENT.toString());
    Path ackLastEventID = new Path(hiveDumpDir, ReplAck.EVENTS_DUMP.toString());
    FileSystem fs = FileSystem.get(hiveDumpDir.toUri(), primary.hiveConf);
    assertTrue(fs.exists(ackFile));
    assertTrue(fs.exists(ackLastEventID));

    fs.delete(ackFile, false);
    //delete last three events and test if it recovers.
    long lastEventID = Long.parseLong(incrementalDump3.lastReplicationId);
    Path lastEvtRoot = new Path(hiveDumpDir + File.separator + String.valueOf(lastEventID));
    Path secondLastEvtRoot = new Path(hiveDumpDir + File.separator + String.valueOf(lastEventID - 1));
    Path thirdLastEvtRoot = new Path(hiveDumpDir + File.separator + String.valueOf(lastEventID - 2));
    assertTrue(fs.exists(lastEvtRoot));
    assertTrue(fs.exists(secondLastEvtRoot));
    assertTrue(fs.exists(thirdLastEvtRoot));

    Map<Path, Long> pathModTimeMap = new HashMap<>();
    for (long idx = Long.parseLong(bootstrapDump.lastReplicationId)+1; idx < (lastEventID - 2); idx++) {
      Path eventRoot  = new Path(hiveDumpDir, String.valueOf(idx));
      if (fs.exists(eventRoot)) {
        for (FileStatus fileStatus: fs.listStatus(eventRoot)) {
          pathModTimeMap.put(fileStatus.getPath(), fileStatus.getModificationTime());
        }
      }
    }

    fs.delete(lastEvtRoot, true);
    fs.delete(secondLastEvtRoot, true);
    fs.delete(thirdLastEvtRoot, true);
    EventsDumpMetadata eventsDumpMetadata = EventsDumpMetadata.deserialize(ackLastEventID, conf);
    eventsDumpMetadata.setLastReplId(lastEventID - 3);
    eventsDumpMetadata.setEventsDumpedCount(eventsDumpMetadata.getEventsDumpedCount() - 3);
    org.apache.hadoop.hive.ql.parse.repl.dump.Utils.writeOutput(eventsDumpMetadata.serialize(), ackLastEventID,
            primary.hiveConf);
    ReplDumpWork.testDeletePreviousDumpMetaPath(false);

    replPolicy = primaryDbName + ".'(t1+)|(t2)'.'t11|t3|t13'";
    WarehouseInstance.Tuple incrementalDump4 = primary.run("use " + primaryDbName)
      .dump(replPolicy);

    assertNotEquals(incrementalDump3.dumpLocation, incrementalDump4.dumpLocation);

    replica.load(replicatedDbName, replPolicy)
      .run("select * from " + replicatedDbName + ".t1")
      .verifyResults(new String[] {"3"})
      .run("select * from " + replicatedDbName + ".t2")
      .verifyResults(new String[] {"4"});
  }
}
