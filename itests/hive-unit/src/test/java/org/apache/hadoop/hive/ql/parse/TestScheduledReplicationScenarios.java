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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.repl.ReplAck;
import org.apache.hadoop.hive.ql.exec.repl.ReplDumpWork;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;
import org.apache.hadoop.hive.ql.parse.repl.metric.MetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.ReplicationMetric;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Metric;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Metadata;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Progress;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Stage;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Status;
import org.apache.hadoop.hive.ql.scheduled.ScheduledQueryExecutionService;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


/**
 * TestScheduledReplicationScenarios - test scheduled replication .
 */
public class TestScheduledReplicationScenarios extends BaseReplicationScenariosAcidTables {
  private static final long DEFAULT_PROBE_TIMEOUT = 5 * 60 * 1000L; // 5 minutes

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
        GzipJSONMessageEncoder.class.getCanonicalName());
    overrides.put(HiveConf.ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_IDLE_SLEEP_TIME.varname, "1s");
    overrides.put(HiveConf.ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_PROGRESS_REPORT_INTERVAL.varname,
            "1s");
    overrides.put(HiveConf.ConfVars.REPL_INCLUDE_EXTERNAL_TABLES.varname, "true");
    overrides.put(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER.varname,
            UserGroupInformation.getCurrentUser().getUserName());
    internalBeforeClassSetup(overrides, TestScheduledReplicationScenarios.class);
  }

  static void internalBeforeClassSetup(Map<String, String> overrides,
      Class clazz) throws Exception {

    conf = new HiveConf(clazz);
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    MiniDFSCluster miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).build();
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
        put("hive.in.repl.test", "true");
      }};

    acidEnableConf.putAll(overrides);
    primary = new WarehouseInstance(LOG, miniDFSCluster, acidEnableConf);
    acidEnableConf.put(MetastoreConf.ConfVars.REPLDIR.getHiveName(), primary.repldDir);
    replica = new WarehouseInstance(LOG, miniDFSCluster, acidEnableConf);
  }

  @Before
  public void setup() throws Throwable {
    super.setup();
  }

  @After
  public void tearDown() throws Throwable {
    primary.run("drop database if exists " + primaryDbName + " cascade");
    replica.run("drop database if exists " + replicatedDbName + " cascade");
    primary.run("drop database if exists " + primaryDbName + "_extra cascade");
  }

  @Test
  @Ignore("HIVE-23395")
  public void testAcidTablesReplLoadBootstrapIncr() throws Throwable {
    // Bootstrap
    primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(1)")
            .run("insert into t1 values(2)");
    try (ScheduledQueryExecutionService schqS =
                 ScheduledQueryExecutionService.startScheduledQueryExecutorService(primary.hiveConf)) {
      int next = -1;
      ReplDumpWork.injectNextDumpDirForTest(String.valueOf(next), true);
      primary.run("create scheduled query s1_t1 every 5 seconds as repl dump " + primaryDbName);
      replica.run("create scheduled query s2_t1 every 5 seconds as repl load " + primaryDbName + " INTO "
              + replicatedDbName);
      Path dumpRoot = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPL_DIR),
              Base64.getEncoder().encodeToString(primaryDbName.toLowerCase().getBytes(StandardCharsets.UTF_8.name())));
      FileSystem fs = FileSystem.get(dumpRoot.toUri(), primary.hiveConf);

      next = Integer.parseInt(ReplDumpWork.getTestInjectDumpDir()) + 1;
      Path ackPath = new Path(dumpRoot, String.valueOf(next) + File.separator + ReplUtils.REPL_HIVE_BASE_DIR
              + File.separator + ReplAck.LOAD_ACKNOWLEDGEMENT.toString());
      waitForAck(fs, ackPath, DEFAULT_PROBE_TIMEOUT);
      replica.run("use " + replicatedDbName)
              .run("show tables like 't1'")
              .verifyResult("t1")
              .run("select id from t1 order by id")
              .verifyResults(new String[]{"1", "2"});

      // First incremental, after bootstrap
      primary.run("use " + primaryDbName)
              .run("insert into t1 values(3)")
              .run("insert into t1 values(4)");
      next = Integer.parseInt(ReplDumpWork.getTestInjectDumpDir()) + 1;
      ackPath = new Path(dumpRoot, String.valueOf(next) + File.separator + ReplUtils.REPL_HIVE_BASE_DIR
              + File.separator + ReplAck.LOAD_ACKNOWLEDGEMENT.toString());
      waitForAck(fs, ackPath, DEFAULT_PROBE_TIMEOUT);
      replica.run("use " + replicatedDbName)
              .run("show tables like 't1'")
              .verifyResult("t1")
              .run("select id from t1 order by id")
              .verifyResults(new String[]{"1", "2", "3", "4"});

      // Second incremental
      primary.run("use " + primaryDbName)
              .run("insert into t1 values(5)")
              .run("insert into t1 values(6)");
      next = Integer.parseInt(ReplDumpWork.getTestInjectDumpDir()) + 1;
      ackPath = new Path(dumpRoot, String.valueOf(next) + File.separator + ReplUtils.REPL_HIVE_BASE_DIR
              + File.separator + ReplAck.LOAD_ACKNOWLEDGEMENT.toString());
      waitForAck(fs, ackPath, DEFAULT_PROBE_TIMEOUT);
      replica.run("use " + replicatedDbName)
              .run("show tables like 't1'")
              .verifyResult("t1")
              .run("select id from t1 order by id")
              .verifyResults(new String[]{"1", "2", "3", "4", "5", "6"})
              .run("drop table t1");


    } finally {
      primary.run("drop scheduled query s1_t1");
      replica.run("drop scheduled query s2_t1");
    }
  }

  @Test
  @Ignore("HIVE-23395")
  public void testExternalTablesReplLoadBootstrapIncr() throws Throwable {
    // Bootstrap
    String withClause = " WITH('" + HiveConf.ConfVars.REPL_INCLUDE_AUTHORIZATION_METADATA
            + "' = 'true' ,'" + HiveConf.ConfVars.REPL_INCLUDE_ATLAS_METADATA + "' = 'true' , '"
            + HiveConf.ConfVars.HIVE_IN_TEST + "' = 'true'" + ",'"+ HiveConf.ConfVars.REPL_ATLAS_ENDPOINT
        + "' = 'http://localhost:21000/atlas'" +  ",'"+ HiveConf.ConfVars.REPL_ATLAS_REPLICATED_TO_DB + "' = 'tgt'"
        +  ",'"+ HiveConf.ConfVars.REPL_SOURCE_CLUSTER_NAME + "' = 'cluster0'"
        +  ",'"+ HiveConf.ConfVars.REPL_TARGET_CLUSTER_NAME + "' = 'cluster1')";
    primary.run("use " + primaryDbName)
            .run("create external table t2 (id int)")
            .run("insert into t2 values(1)")
            .run("insert into t2 values(2)");
    try (ScheduledQueryExecutionService schqS =
                 ScheduledQueryExecutionService.startScheduledQueryExecutorService(primary.hiveConf)) {
      int next = -1;
      ReplDumpWork.injectNextDumpDirForTest(String.valueOf(next), true);
      primary.run("create scheduled query s1_t2 every 5 seconds as repl dump " + primaryDbName + withClause);
      replica.run("create scheduled query s2_t2 every 5 seconds as repl load " + primaryDbName + " INTO "
              + replicatedDbName + withClause);
      Path dumpRoot = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPL_DIR),
              Base64.getEncoder().encodeToString(primaryDbName.toLowerCase().getBytes(StandardCharsets.UTF_8.name())));
      FileSystem fs = FileSystem.get(dumpRoot.toUri(), primary.hiveConf);
      next = Integer.parseInt(ReplDumpWork.getTestInjectDumpDir()) + 1;
      Path ackPath = new Path(dumpRoot, String.valueOf(next) + File.separator + ReplUtils.REPL_HIVE_BASE_DIR
              + File.separator + ReplAck.LOAD_ACKNOWLEDGEMENT.toString());
      waitForAck(fs, ackPath, DEFAULT_PROBE_TIMEOUT);
      replica.run("use " + replicatedDbName)
              .run("show tables like 't2'")
              .verifyResult("t2")
              .run("select id from t2 order by id")
              .verifyResults(new String[]{"1", "2"});
      long lastReplId = Long.parseLong(primary.status(replicatedDbName).getOutput().get(0));
      DumpMetaData dumpMetaData = new DumpMetaData(ackPath.getParent(), primary.hiveConf);
      List<ReplicationMetric> replicationMetrics = MetricCollector.getInstance().getMetrics();
      Assert.assertEquals(2, replicationMetrics.size());
      //Generate expected metrics
      List<ReplicationMetric> expectedReplicationMetrics = new ArrayList<>();
      expectedReplicationMetrics.add(generateExpectedMetric("s1_t2", 0, primaryDbName,
          Metadata.ReplicationType.BOOTSTRAP, ackPath.getParent().toString(), lastReplId, Status.SUCCESS,
          generateDumpStages(true)));
      expectedReplicationMetrics.add(generateExpectedMetric("s2_t2",
          dumpMetaData.getDumpExecutionId(), replicatedDbName,
          Metadata.ReplicationType.BOOTSTRAP, ackPath.getParent().toString(), lastReplId, Status.SUCCESS,
          generateLoadStages(true)));
      checkMetrics(expectedReplicationMetrics, replicationMetrics);
      // First incremental, after bootstrap
      primary.run("use " + primaryDbName)
              .run("insert into t2 values(3)")
              .run("insert into t2 values(4)");
      next = Integer.parseInt(ReplDumpWork.getTestInjectDumpDir()) + 1;
      ackPath = new Path(dumpRoot, String.valueOf(next) + File.separator + ReplUtils.REPL_HIVE_BASE_DIR
              + File.separator + ReplAck.LOAD_ACKNOWLEDGEMENT.toString());
      waitForAck(fs, ackPath, DEFAULT_PROBE_TIMEOUT);
      replica.run("use " + replicatedDbName)
              .run("show tables like 't2'")
              .verifyResult("t2")
              .run("select id from t2 order by id")
              .verifyResults(new String[]{"1", "2", "3", "4"});
    } finally {
      primary.run("drop scheduled query s1_t2");
      replica.run("drop scheduled query s2_t2");
    }
  }

  @Test
  @Ignore("HIVE-25720")
  public void testCompleteFailoverWithReverseBootstrap() throws Throwable {
    String withClause = "'" + HiveConf.ConfVars.HIVE_IN_TEST + "' = 'true','"
            + HiveConf.ConfVars.REPL_RETAIN_PREV_DUMP_DIR + "'='true'" ;

    String sourceDbName = "sourceDbName";
    String replicaDbName = "replicaDbName";
    // Create a table with some data at source DB.
    primary.run("create database " + sourceDbName + " with dbproperties('repl.source.for'='a')")
            .run("use " + sourceDbName)
            .run("create table t2 (id int)").run("insert into t2 values(1)").run("insert into t2 values(2)");

    // Schedule Dump & Load and verify the data is replicated properly.
    try (ScheduledQueryExecutionService schqS = ScheduledQueryExecutionService
            .startScheduledQueryExecutorService(primary.hiveConf)) {
      int next = -1;
      ReplDumpWork.injectNextDumpDirForTest(String.valueOf(next), true);
      primary.run("create scheduled query repl_dump_p1 every 5 seconds as repl dump "
              + sourceDbName +  " WITH(" + withClause + ')');
      replica.run("create scheduled query repl_load_p1 every 5 seconds as repl load "
              + sourceDbName + " INTO " + replicaDbName +  " WITH(" + withClause + ')');

      Path dumpRoot = ReplUtils.getEncodedDumpRootPath(primary.hiveConf, sourceDbName.toLowerCase());
      FileSystem fs = FileSystem.get(dumpRoot.toUri(), primary.hiveConf);
      next = Integer.parseInt(ReplDumpWork.getTestInjectDumpDir()) + 1;
      Path ackPath = new Path(dumpRoot, String.valueOf(next) + File.separator + ReplUtils.REPL_HIVE_BASE_DIR
                      + File.separator + ReplAck.LOAD_ACKNOWLEDGEMENT.toString());
      waitForAck(fs, ackPath, DEFAULT_PROBE_TIMEOUT);
      replica.run("use " + replicaDbName).run("show tables like 't2'")
              .verifyResult("t2").run("select id from t2 order by id")
              .verifyResults(new String[] {"1", "2"});

      //Start failover from here.
      String startFailoverClause = withClause.concat(",'" + HiveConf.ConfVars.HIVE_REPL_FAILOVER_START + "'='true'");
      primary.run("alter scheduled query repl_dump_p1 defined as repl dump " + sourceDbName +  " WITH(" + startFailoverClause + ')');
      replica.run("alter scheduled query repl_load_p1 defined as repl load "
              + sourceDbName + " INTO " + replicaDbName +  " WITH(" + startFailoverClause + ')');

      next = Integer.parseInt(ReplDumpWork.getTestInjectDumpDir()) + 1;
      ackPath = new Path(dumpRoot,
              String.valueOf(next) + File.separator + ReplUtils.REPL_HIVE_BASE_DIR
                      + File.separator + ReplAck.LOAD_ACKNOWLEDGEMENT.toString());
      waitForAck(fs, ackPath, DEFAULT_PROBE_TIMEOUT);
      Path failoverReadyMarker = new Path(dumpRoot, String.valueOf(next) + File.separator + ReplUtils.REPL_HIVE_BASE_DIR
              + File.separator + ReplAck.FAILOVER_READY_MARKER.toString());
      assertTrue(fs.exists(failoverReadyMarker));
      assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(primary.getDatabase(sourceDbName),
              MetaStoreUtils.FailoverEndpoint.SOURCE));
      assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(replica.getDatabase(replicaDbName),
              MetaStoreUtils.FailoverEndpoint.TARGET));

      primary.run("alter scheduled query repl_dump_p1 disabled")
              .run("alter scheduled query repl_dump_p1 defined as repl dump "
                      + sourceDbName +  " WITH(" + withClause + ')')
              .run("alter database " + sourceDbName + " set dbproperties('" + SOURCE_OF_REPLICATION + "'='')")
              .run("drop database " + sourceDbName + " cascade");

      assertTrue(primary.getDatabase(sourceDbName) == null);

      replica.run("alter scheduled query repl_load_p1 disabled")
              .run("alter scheduled query repl_load_p1 defined as repl load "
                      + sourceDbName + " INTO " + replicaDbName +  " WITH(" + withClause + ')')
              .run("create scheduled query repl_dump_p2 every 5 seconds as repl dump " + replicaDbName +  " WITH(" + withClause + ')');

      primary.run("create scheduled query repl_load_p2 every 5 seconds as repl load "
              + replicaDbName + " INTO " + sourceDbName +  " WITH(" + withClause + ')');

      dumpRoot = ReplUtils.getEncodedDumpRootPath(replica.hiveConf, replicaDbName.toLowerCase());
      next = Integer.parseInt(ReplDumpWork.getTestInjectDumpDir()) + 1;
      ackPath = new Path(dumpRoot,
              String.valueOf(next) + File.separator + ReplUtils.REPL_HIVE_BASE_DIR
                      + File.separator + ReplAck.LOAD_ACKNOWLEDGEMENT.toString());
      waitForAck(fs, ackPath, DEFAULT_PROBE_TIMEOUT);
      assertFalse(MetaStoreUtils.isTargetOfReplication(replica.getDatabase(replicaDbName)));
      Database primaryDb = primary.getDatabase(sourceDbName);
      assertFalse(primaryDb == null);
      assertTrue(MetaStoreUtils.isTargetOfReplication(primaryDb));
      assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(primaryDb));

      next = Integer.parseInt(ReplDumpWork.getTestInjectDumpDir()) + 1;
      ackPath = new Path(dumpRoot,
              String.valueOf(next) + File.separator + ReplUtils.REPL_HIVE_BASE_DIR
                      + File.separator + ReplAck.LOAD_ACKNOWLEDGEMENT.toString());
      waitForAck(fs, ackPath, DEFAULT_PROBE_TIMEOUT);
      assertFalse(ReplUtils.isFirstIncPending(primary.getDatabase(sourceDbName).getParameters()));
      assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(replica.getDatabase(replicaDbName)));

      //Start failback from here.
      replica.run("alter  scheduled query repl_dump_p2 defined as repl dump " + replicaDbName +  " WITH(" + startFailoverClause + ')');
      primary.run("alter scheduled query repl_load_p2 defined as repl load "
              + replicaDbName + " INTO " + sourceDbName +  " WITH(" + startFailoverClause + ')');

      next = Integer.parseInt(ReplDumpWork.getTestInjectDumpDir()) + 1;
      ackPath = new Path(dumpRoot, String.valueOf(next) + File.separator + ReplUtils.REPL_HIVE_BASE_DIR
                      + File.separator + ReplAck.LOAD_ACKNOWLEDGEMENT.toString());
      waitForAck(fs, ackPath, DEFAULT_PROBE_TIMEOUT);
      failoverReadyMarker = new Path(dumpRoot, String.valueOf(next) + File.separator + ReplUtils.REPL_HIVE_BASE_DIR
              + File.separator + ReplAck.FAILOVER_READY_MARKER.toString());
      assertTrue(fs.exists(failoverReadyMarker));
      assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(replica.getDatabase(replicaDbName),
              MetaStoreUtils.FailoverEndpoint.SOURCE));
      assertTrue(MetaStoreUtils.isDbBeingPlannedFailedOverAtEndpoint(primary.getDatabase(sourceDbName),
              MetaStoreUtils.FailoverEndpoint.TARGET));

      replica.run("alter scheduled query repl_dump_p2 disabled")
              .run("alter scheduled query repl_dump_p2 defined as repl dump "
                      + replicaDbName +  " WITH(" + withClause + ')')
              .run("alter database " + replicaDbName + " set dbproperties('" + SOURCE_OF_REPLICATION + "'='')")
              .run("drop database " + replicaDbName + " cascade")
              .run("alter scheduled query repl_load_p1 enabled");

      assertTrue(replica.getDatabase(replicaDbName) == null);

      primary.run("alter scheduled query repl_load_p2 disabled")
              .run("alter scheduled query repl_load_p2 defined as repl load "
                      + replicaDbName + " INTO " + sourceDbName +  " WITH(" + withClause + ')')
              .run("alter scheduled query repl_dump_p1 enabled");

      dumpRoot = ReplUtils.getEncodedDumpRootPath(primary.hiveConf, sourceDbName.toLowerCase());
      next = Integer.parseInt(ReplDumpWork.getTestInjectDumpDir()) + 1;
      ackPath = new Path(dumpRoot, String.valueOf(next) + File.separator + ReplUtils.REPL_HIVE_BASE_DIR
              + File.separator + ReplAck.LOAD_ACKNOWLEDGEMENT.toString());
      waitForAck(fs, ackPath, DEFAULT_PROBE_TIMEOUT);

      assertFalse(MetaStoreUtils.isTargetOfReplication(primary.getDatabase(sourceDbName)));
      Database replicaDb = replica.getDatabase(replicaDbName);
      assertFalse(replicaDb == null);
      assertTrue(MetaStoreUtils.isTargetOfReplication(replicaDb));
      assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(replicaDb));

      next = Integer.parseInt(ReplDumpWork.getTestInjectDumpDir()) + 1;
      ackPath = new Path(dumpRoot,
              String.valueOf(next) + File.separator + ReplUtils.REPL_HIVE_BASE_DIR
                      + File.separator + ReplAck.LOAD_ACKNOWLEDGEMENT.toString());
      waitForAck(fs, ackPath, DEFAULT_PROBE_TIMEOUT);
      assertFalse(ReplUtils.isFirstIncPending(replica.getDatabase(replicaDbName).getParameters()));
      assertFalse(MetaStoreUtils.isDbBeingPlannedFailedOver(primary.getDatabase(sourceDbName)));

    } finally {
      primary.run("drop database if exists " + sourceDbName + " cascade").run("drop scheduled query repl_dump_p1");
      replica.run("drop database if exists " + replicaDbName + " cascade").run("drop scheduled query repl_load_p1");
      primary.run("drop scheduled query repl_load_p2");
      replica.run("drop scheduled query repl_dump_p2");
    }
  }

  @Test
  @Ignore("HIVE-25720")
  public void testSetPolicyId() throws Throwable {
    String withClause =
        " WITH('" + HiveConf.ConfVars.HIVE_IN_TEST + "' = 'true'" + ",'"
            + HiveConf.ConfVars.REPL_SOURCE_CLUSTER_NAME + "' = 'cluster0'"
            + ",'" + HiveConf.ConfVars.REPL_TARGET_CLUSTER_NAME
            + "' = 'cluster1')";

    // Create a table with some data at source DB.
    primary.run("use " + primaryDbName).run("create table t2 (id int)")
        .run("insert into t2 values(1)").run("insert into t2 values(2)");

    // Remove the SOURCE_OF_REPLICATION property from the database.
    primary.run("ALTER DATABASE " + primaryDbName + " Set DBPROPERTIES ( '"
        + SOURCE_OF_REPLICATION + "' = '')");
    assertFalse(ReplChangeManager.isSourceOfReplication(primary.getDatabase(primaryDbName)));

    // Schedule Dump & Load and verify the data is replicated properly.
    try (ScheduledQueryExecutionService schqS = ScheduledQueryExecutionService
        .startScheduledQueryExecutorService(primary.hiveConf)) {
      int next = -1;
      ReplDumpWork.injectNextDumpDirForTest(String.valueOf(next), true);
      primary.run("create scheduled query s1_t2 every 5 seconds as repl dump " + primaryDbName + withClause);
      replica.run("create scheduled query s2_t2 every 5 seconds as repl load "
          + primaryDbName + " INTO " + replicatedDbName + withClause);
      Path dumpRoot = ReplUtils.getEncodedDumpRootPath(primary.hiveConf, primaryDbName.toLowerCase());
      FileSystem fs = FileSystem.get(dumpRoot.toUri(), primary.hiveConf);
      next = Integer.parseInt(ReplDumpWork.getTestInjectDumpDir()) + 1;
      Path ackPath = new Path(dumpRoot,
          String.valueOf(next) + File.separator + ReplUtils.REPL_HIVE_BASE_DIR
              + File.separator + ReplAck.LOAD_ACKNOWLEDGEMENT.toString());
      waitForAck(fs, ackPath, DEFAULT_PROBE_TIMEOUT);
      replica.run("use " + replicatedDbName).run("show tables like 't2'")
          .verifyResult("t2").run("select id from t2 order by id")
          .verifyResults(new String[] {"1", "2"});

      // Check the database got the SOURCE_OF_REPLICATION property set.
      assertTrue(ReplChangeManager.getReplPolicyIdString(primary.getDatabase(primaryDbName)).equals("s1_t2"));

      // Remove the SOURCE_OF_REPLICATION property from the database.
      primary.run("ALTER DATABASE " + primaryDbName + " Set DBPROPERTIES ( '" + SOURCE_OF_REPLICATION + "' = '')");
      assertFalse(ReplChangeManager.isSourceOfReplication(primary.getDatabase(primaryDbName)));

      //Test to ensure that repl.source.for is added in incremental iteration of replication also.
      GenericTestUtils.waitFor(() -> {
        try {
          return ReplChangeManager.getReplPolicyIdString(primary.getDatabase(primaryDbName)).equals("s1_t2");
        } catch (Throwable e) {
          return false;
        }
      }, 100, 10000);

      // Test the new policy id is appended
      primary.run("drop scheduled query s1_t2");
      fs.delete(dumpRoot, true);
      primary.run("create scheduled query s1_t2_new every 5 seconds as repl " + "dump " + primaryDbName + withClause);
      GenericTestUtils.waitFor(() -> {
        try {
          return ReplChangeManager.getReplPolicyIdString(primary.getDatabase(primaryDbName)).equals("s1_t2, s1_t2_new");
        } catch (Throwable e) {
          return false;
        }
      }, 100, 10000);

    } finally {
      primary.run("drop scheduled query s1_t2_new");
      replica.run("drop scheduled query s2_t2");
    }
  }

  private void checkMetrics(List<ReplicationMetric> expectedReplicationMetrics,
                            List<ReplicationMetric> actualMetrics) {
    Assert.assertEquals(expectedReplicationMetrics.size(), actualMetrics.size());
    int metricCounter = 0;
    for (ReplicationMetric actualMetric : actualMetrics) {
      for (ReplicationMetric expecMetric : expectedReplicationMetrics) {
        if (actualMetric.getPolicy().equalsIgnoreCase(expecMetric.getPolicy())) {
          Assert.assertEquals(expecMetric.getDumpExecutionId(), actualMetric.getDumpExecutionId());
          Assert.assertEquals(expecMetric.getMetadata().getDbName(), actualMetric.getMetadata().getDbName());
          Assert.assertEquals(expecMetric.getMetadata().getLastReplId(),
              actualMetric.getMetadata().getLastReplId());
          Assert.assertEquals(expecMetric.getMetadata().getStagingDir(),
              actualMetric.getMetadata().getStagingDir());
          Assert.assertEquals(expecMetric.getMetadata().getReplicationType(),
              actualMetric.getMetadata().getReplicationType());
          Assert.assertEquals(expecMetric.getProgress().getStatus(), actualMetric.getProgress().getStatus());
          Assert.assertEquals(expecMetric.getProgress().getStages().size(),
              actualMetric.getProgress().getStages().size());
          List<Stage> expectedStages = expecMetric.getProgress().getStages();
          List<Stage> actualStages = actualMetric.getProgress().getStages();
          int counter = 0;
          for (Stage actualStage : actualStages) {
            for (Stage expeStage : expectedStages) {
              if (actualStage.getName().equalsIgnoreCase(expeStage.getName())) {
                Assert.assertEquals(expeStage.getStatus(), actualStage.getStatus());
                Assert.assertEquals(expeStage.getMetrics().size(), actualStage.getMetrics().size());
                for (Metric actMetric : actualStage.getMetrics()) {
                  for (Metric expMetric : expeStage.getMetrics()) {
                    if (actMetric.getName().equalsIgnoreCase(expMetric.getName())) {
                      Assert.assertEquals(expMetric.getTotalCount(), actMetric.getTotalCount());
                      Assert.assertEquals(expMetric.getCurrentCount(), actMetric.getCurrentCount());
                    }
                  }
                }
                counter++;
                if (counter == actualStages.size()) {
                  break;
                }
              }
            }
          }
          metricCounter++;
          if (metricCounter == actualMetrics.size()) {
            break;
          }
        }
      }
    }
  }

  private List<Stage> generateLoadStages(boolean isBootstrap) {
    List<Stage> stages = new ArrayList<>();
    //Ranger
    Stage rangerDump = new Stage("RANGER_LOAD", Status.SUCCESS, 0);
    Metric rangerMetric = new Metric(ReplUtils.MetricName.POLICIES.name(), 0);
    rangerDump.addMetric(rangerMetric);
    stages.add(rangerDump);
    //Atlas
    Stage atlasDump = new Stage("ATLAS_LOAD", Status.SUCCESS, 0);
    Metric atlasMetric = new Metric(ReplUtils.MetricName.ENTITIES.name(), 0);
    atlasDump.addMetric(atlasMetric);
    stages.add(atlasDump);
    //Hive
    Stage replDump = new Stage("REPL_LOAD", Status.SUCCESS, 0);
    if (isBootstrap) {
      Metric hiveMetric = new Metric(ReplUtils.MetricName.TABLES.name(), 1);
      hiveMetric.setCurrentCount(1);
      replDump.addMetric(hiveMetric);
      hiveMetric = new Metric(ReplUtils.MetricName.FUNCTIONS.name(), 0);
      replDump.addMetric(hiveMetric);
    } else {
      Metric hiveMetric = new Metric(ReplUtils.MetricName.EVENTS.name(), 1);
      hiveMetric.setCurrentCount(1);
      replDump.addMetric(hiveMetric);
    }
    stages.add(replDump);
    return stages;
  }

  private List<Stage> generateDumpStages(boolean isBootstrap) {
    List<Stage> stages = new ArrayList<>();
    //Ranger
    Stage rangerDump = new Stage("RANGER_DUMP", Status.SUCCESS, 0);
    Metric rangerMetric = new Metric(ReplUtils.MetricName.POLICIES.name(), 0);
    rangerDump.addMetric(rangerMetric);
    stages.add(rangerDump);
    //Atlas
    Stage atlasDump = new Stage("ATLAS_DUMP", Status.SUCCESS, 0);
    Metric atlasMetric = new Metric(ReplUtils.MetricName.ENTITIES.name(), 0);
    atlasDump.addMetric(atlasMetric);
    stages.add(atlasDump);
    //Hive
    Stage replDump = new Stage("REPL_DUMP", Status.SUCCESS, 0);
    if (isBootstrap) {
      Metric hiveMetric = new Metric(ReplUtils.MetricName.TABLES.name(), 1);
      hiveMetric.setCurrentCount(1);
      replDump.addMetric(hiveMetric);
      hiveMetric = new Metric(ReplUtils.MetricName.FUNCTIONS.name(), 0);
      replDump.addMetric(hiveMetric);
    } else {
      Metric hiveMetric = new Metric(ReplUtils.MetricName.EVENTS.name(), 1);
      hiveMetric.setCurrentCount(1);
      replDump.addMetric(hiveMetric);
    }
    stages.add(replDump);
    return stages;
  }

  private ReplicationMetric generateExpectedMetric(String policy, long dumpExecId, String dbName,
                                                   Metadata.ReplicationType replicationType, String staging,
                                                   long lastReplId, Status status, List<Stage> stages) {
    Metadata metadata = new Metadata(dbName, replicationType, staging);
    metadata.setLastReplId(lastReplId);
    ReplicationMetric replicationMetric = new ReplicationMetric(0, policy, dumpExecId, metadata);
    Progress progress = new Progress();
    progress.setStatus(status);
    for (Stage stage : stages) {
      progress.addStage(stage);
    }
    replicationMetric.setProgress(progress);
    return replicationMetric;
  }

  private void waitForAck(FileSystem fs, Path ackFile, long timeout) throws IOException {
    long oldTime = System.currentTimeMillis();
    long sleepInterval = 2;

    while(true) {
      if (fs.exists(ackFile)) {
        return;
      }
      try {
        Thread.sleep(sleepInterval);
      } catch (InterruptedException e) {
        //no-op
      }
      if (System.currentTimeMillis() - oldTime > timeout) {
        break;
      }
    }
    throw new IOException("Timed out waiting for the ack file: " +  ackFile.toString());
  }
}
