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
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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

import static org.apache.hadoop.hive.ql.exec.repl.ReplExternalTables.FILE_NAME;
import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.INC_BOOTSTRAP_ROOT_DIR_NAME;

/**
 * Tests Table level replication scenarios.
 */
public class TestTableLevelReplicationScenarios extends BaseReplicationScenariosAcidTables {

  private static final String REPLICA_EXTERNAL_BASE = "/replica_external_base";

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());
    overrides.put(HiveConf.ConfVars.REPL_DUMP_METADATA_ONLY.varname, "false");
    overrides.put(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname, "true");
    overrides.put(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname,
        UserGroupInformation.getCurrentUser().getUserName());

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
    WarehouseInstance.Tuple tuple = primary.dump(replPolicy, oldReplPolicy, lastReplId, dumpWithClause);

    if (bootstrappedTables != null) {
      verifyBootstrapDirInIncrementalDump(tuple.dumpLocation, bootstrappedTables);
    }

    // If the policy contains '.'' means its table level replication.
    verifyTableListForPolicy(tuple.dumpLocation, replPolicy.contains(".'") ? expectedTables : null);

    replica.load(replicatedDbName, tuple.dumpLocation, loadWithClause)
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

  private void verifyBootstrapDirInIncrementalDump(String dumpLocation, String[] bootstrappedTables)
          throws Throwable {
    // _bootstrap directory should be created as bootstrap enabled on external tables.
    Path dumpPath = new Path(dumpLocation, INC_BOOTSTRAP_ROOT_DIR_NAME);

    // If nothing to be bootstrapped.
    if (bootstrappedTables.length == 0) {
      Assert.assertFalse(primary.miniDFSCluster.getFileSystem().exists(dumpPath));
      return;
    }

    Assert.assertTrue(primary.miniDFSCluster.getFileSystem().exists(dumpPath));

    // Check if the DB dump path have any tables other than the ones listed in bootstrappedTables.
    Path dbPath = new Path(dumpPath, primaryDbName);
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
    Path tableListFile = new Path(dumpLocation, ReplUtils.REPL_TABLE_LIST_DIR_NAME);
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
            .dump(primaryDbName, null);
    replica.load(replicatedDbName, tupleBootstrap.dumpLocation);

    String[] originalNonAcidTables = new String[] {"t1", "t2"};
    String[] originalFullAcidTables = new String[] {"t3", "t4"};
    String[] originalMMAcidTables = new String[] {"t5"};
    createTables(originalNonAcidTables, CreateTableType.NON_ACID);
    createTables(originalFullAcidTables, CreateTableType.FULL_ACID);
    createTables(originalMMAcidTables, CreateTableType.MM_ACID);

    // Replicate and verify if only 2 tables are replicated to target.
    String replPolicy = primaryDbName + ".'t1|t5'";
    String[] replicatedTables = new String[] {"t1", "t5"};
    replicateAndVerify(replPolicy, tupleBootstrap.lastReplicationId, null, null, replicatedTables);
  }

  @Test
  public void testBasicIncrementalWithIncludeAndExcludeList() throws Throwable {
    WarehouseInstance.Tuple tupleBootstrap = primary.run("use " + primaryDbName)
            .dump(primaryDbName, null);
    replica.load(replicatedDbName, tupleBootstrap.dumpLocation);

    String[] originalTables = new String[] {"t1", "t11", "t2", "t3", "t111"};
    createTables(originalTables, CreateTableType.NON_ACID);

    // Replicate and verify if only 3 tables are replicated to target.
    String replPolicy = primaryDbName + ".'(t1+)|(t2)'.'t11|t3'";
    String[] replicatedTables = new String[] {"t1", "t111", "t2"};
    replicateAndVerify(replPolicy, tupleBootstrap.lastReplicationId, null, null, replicatedTables);
  }

  @Test
  public void testIncorrectTablePolicyInReplDump() throws Throwable {
    String[] originalTables = new String[] {"t1", "t11", "t2", "t3", "t111"};
    createTables(originalTables, CreateTableType.NON_ACID);

    // Invalid repl policy where abrubtly placed DOT which causes ParseException during REPL dump.
    String[] replicatedTables = new String[] {};
    boolean failed;
    String[] invalidReplPolicies = new String[] {
        primaryDbName + ".t1.t2", // Didn't enclose table pattern within single quotes.
        primaryDbName + ".'t1'.t2", // Table name and include list not allowed.
        primaryDbName + ".t1.'t2'", // Table name and exclude list not allowed.
        primaryDbName + ".'t1+'.", // Abrubtly ended dot.
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

    // Test incremental replication with invalid replication policies in REPLACE clause.
    String replPolicy = primaryDbName;
    WarehouseInstance.Tuple tupleBootstrap = primary.run("use " + primaryDbName)
            .dump(primaryDbName, null);
    replica.load(replicatedDbName, tupleBootstrap.dumpLocation);
    String lastReplId = tupleBootstrap.lastReplicationId;
    for (String oldReplPolicy : invalidReplPolicies) {
      failed = false;
      try {
        replicateAndVerify(replPolicy, oldReplPolicy, lastReplId, null, null, null, replicatedTables);
      } catch (Exception ex) {
        LOG.info("Got exception: {}", ex.getMessage());
        Assert.assertTrue(ex instanceof ParseException);
        failed = true;
      }
      Assert.assertTrue(failed);
    }

    // Replace with replication policy having different DB name.
    String oldReplPolicy = replPolicy;
    replPolicy = primaryDbName + "_dupe.'t1+'.'t1'";
    failed = false;
    try {
      replicateAndVerify(replPolicy, oldReplPolicy, lastReplId, null, null, null, replicatedTables);
    } catch (Exception ex) {
      LOG.info("Got exception: {}", ex.getMessage());
      Assert.assertTrue(ex instanceof SemanticException);
      failed = true;
    }
    Assert.assertTrue(failed);

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
      replicateAndVerify(replPolicy, null, null, null, originalTables);
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
    List<String> loadWithClause = ReplicationTestUtils.externalTableBasePathWithClause(REPLICA_EXTERNAL_BASE, replica);
    List<String> dumpWithClause = Collections.singletonList(
            "'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='true'"
    );
    String replPolicy = primaryDbName + ".'(a[0-9]+)|(b2)'.'a1'";
    String[] replicatedTables = new String[] {"a2", "b2"};
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .dump(replPolicy, null, dumpWithClause);

    // the _external_tables_file info should be created as external tables are to be replicated.
    Assert.assertTrue(primary.miniDFSCluster.getFileSystem()
            .exists(new Path(new Path(tuple.dumpLocation, primaryDbName.toLowerCase()), FILE_NAME)));

    // Verify that the external table info contains only table "a2".
    ReplicationTestUtils.assertExternalFileInfo(primary, Arrays.asList("a2"),
            new Path(new Path(tuple.dumpLocation, primaryDbName.toLowerCase()), FILE_NAME));

    replica.load(replicatedDbName, tuple.dumpLocation, loadWithClause)
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
    List<String> loadWithClause = ReplicationTestUtils.externalTableBasePathWithClause(REPLICA_EXTERNAL_BASE, replica);
    List<String> dumpWithClause = Collections.singletonList(
            "'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='false'"
    );
    String replPolicy = primaryDbName + ".'(a[0-9]+)|(b2)'.'a1'";
    String[] bootstrapReplicatedTables = new String[] {"b2"};
    String lastReplId = replicateAndVerify(replPolicy, null,
            dumpWithClause, loadWithClause, bootstrapReplicatedTables);

    // Enable external tables replication and bootstrap in incremental phase.
    String[] incrementalReplicatedTables = new String[] {"a2", "b2"};
    dumpWithClause = Arrays.asList("'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='true'",
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_EXTERNAL_TABLES.varname + "'='true'");
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .dump(replPolicy, lastReplId, dumpWithClause);

    // the _external_tables_file info should be created as external tables are to be replicated.
    Assert.assertTrue(primary.miniDFSCluster.getFileSystem()
            .exists(new Path(tuple.dumpLocation, FILE_NAME)));

    // Verify that the external table info contains only table "a2".
    ReplicationTestUtils.assertExternalFileInfo(primary, Arrays.asList("a2"),
            new Path(tuple.dumpLocation, FILE_NAME));

    replica.load(replicatedDbName, tuple.dumpLocation, loadWithClause)
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
    List<String> loadWithClause = ReplicationTestUtils.externalTableBasePathWithClause(REPLICA_EXTERNAL_BASE, replica);
    List<String> dumpWithClause = Collections.singletonList(
            "'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='false'"
    );
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
            .dump(replPolicy, oldReplPolicy, lastReplId, dumpWithClause);

    // the _external_tables_file info should be created as external tables are to be replicated.
    Assert.assertTrue(primary.miniDFSCluster.getFileSystem()
            .exists(new Path(tuple.dumpLocation, FILE_NAME)));

    // Verify that the external table info contains table "a2" and "c2".
    ReplicationTestUtils.assertExternalFileInfo(primary, Arrays.asList("a2", "c2"),
            new Path(tuple.dumpLocation, FILE_NAME));

    // Verify if the expected tables are bootstrapped.
    verifyBootstrapDirInIncrementalDump(tuple.dumpLocation, bootstrappedTables);

    replica.load(replicatedDbName, tuple.dumpLocation, loadWithClause)
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
    lastReplId = replicateAndVerify(replPolicy, null, lastReplId, null,
            null, bootstrapTables, replicatedTables);

    // Rename tables to make them satisfy the filter.
    primary.run("use " + primaryDbName)
            .run("alter table out3 rename to in3")
            .run("alter table out4 rename to in4")
            .run("alter table out5 rename to in5");

    replicatedTables = new String[] {"in1", "in2", "in3", "in4", "in5"};
    bootstrapTables = new String[] {"in3", "in4", "in5"};
    lastReplId = replicateAndVerify(replPolicy, null, lastReplId, null,
            null, bootstrapTables, replicatedTables);

    primary.run("use " + primaryDbName)
            .run("alter table in3 rename to in7")
            .run("alter table in7 rename to in8") // Double rename, both satisfying the filter, so no bootstrap.
            .run("alter table in4 rename to out9") // out9 does not match the filter so in4 should be dropped.
            .run("alter table in5 rename to out10") // Rename from satisfying name to not satisfying name.
            .run("alter table out10 rename to in11"); // from non satisfying to satisfying, should be bootstrapped

    replicatedTables = new String[] {"in1", "in2", "in8", "in11"};
    bootstrapTables = new String[] {"in11"};
    lastReplId = replicateAndVerify(replPolicy, null, lastReplId, null,
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
    replicateAndVerify(replPolicy, null, lastReplId, null,
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
    lastReplId = replicateAndVerify(replPolicy, null, lastReplId, dumpWithClause,
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
    replicateAndVerify(replPolicy, null, lastReplId, dumpWithClause,
            null, bootstrapTables, replicatedTables);
  }

  @Test
  public void testRenameTableScenariosExternalTable() throws Throwable {
    String replPolicy = primaryDbName + ".'in[0-9]+'.'out[0-9]+'";
    List<String> loadWithClause = ReplicationTestUtils.externalTableBasePathWithClause(REPLICA_EXTERNAL_BASE, replica);
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
            "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='true'"
    );
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
    List<String> loadWithClause = ReplicationTestUtils.externalTableBasePathWithClause(REPLICA_EXTERNAL_BASE, replica);
    List<String> dumpWithClause = Arrays.asList(
            "'" + HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname + "'='true'",
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_EXTERNAL_TABLES.varname + "'='true'"
    );
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
            "'" + HiveConf.ConfVars.REPL_BOOTSTRAP_EXTERNAL_TABLES.varname + "'='false'"
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
    List<String> loadWithClause = ReplicationTestUtils.externalTableBasePathWithClause(REPLICA_EXTERNAL_BASE, replica);
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
            "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='true'"
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
            "'" + ReplUtils.REPL_DUMP_INCLUDE_ACID_TABLES + "'='true'"
    );

    // Database replication with ACID and EXTERNAL table.
    replicatedTables = new String[] {"in1", "in2", "in3", "out4", "out5", "out6", "in7", "in8",
            "in9", "out10", "out11", "out12"};
    bootstrapTables = new String[] {"out4", "out5", "out6", "in8", "out10", "out11", "out12"};
    replicateAndVerify(replPolicy, newReplPolicy, lastReplId, dumpWithClause,
            loadWithClause, bootstrapTables, replicatedTables);
  }
}
