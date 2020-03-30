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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.exec.repl.ReplDumpWork;
import org.apache.hadoop.hive.ql.scheduled.ScheduledQueryExecutionService;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.BeforeClass;

import java.util.Map;
import java.util.HashMap;


/**
 * TestScheduledReplicationScenarios - test scheduled replication .
 */
public class TestScheduledReplicationScenarios extends BaseReplicationScenariosAcidTables {

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
  public void testAcidTablesReplLoadBootstrapIncr() throws Throwable {
    // Bootstrap
    primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(1)")
            .run("insert into t1 values(2)");
    try (ScheduledQueryExecutionService schqS =
                 ScheduledQueryExecutionService.startScheduledQueryExecutorService(primary.hiveConf)) {
      int next = 0;
      ReplDumpWork.injectNextDumpDirForTest(String.valueOf(next));
      primary.run("create scheduled query s1 every 10 minutes as repl dump " + primaryDbName);
      primary.run("alter scheduled query s1 execute");
      Thread.sleep(6000);
      replica.run("create scheduled query s2 every 10 minutes as repl load " + primaryDbName + " INTO "
              + replicatedDbName);
      replica.run("alter scheduled query s2 execute");
      Thread.sleep(20000);
      replica.run("use " + replicatedDbName)
              .run("show tables like 't1'")
              .verifyResult("t1")
              .run("select id from t1 order by id")
              .verifyResults(new String[]{"1", "2"});

      // First incremental, after bootstrap
      primary.run("use " + primaryDbName)
              .run("insert into t1 values(3)")
              .run("insert into t1 values(4)");
      next++;
      ReplDumpWork.injectNextDumpDirForTest(String.valueOf(next));
      primary.run("alter scheduled query s1 execute");
      Thread.sleep(20000);
      replica.run("alter scheduled query s2 execute");
      Thread.sleep(20000);
      replica.run("use " + replicatedDbName)
              .run("show tables like 't1'")
              .verifyResult("t1")
              .run("select id from t1 order by id")
              .verifyResults(new String[]{"1", "2", "3", "4"});

      // Second incremental
      primary.run("use " + primaryDbName)
              .run("insert into t1 values(5)")
              .run("insert into t1 values(6)");
      next++;
      ReplDumpWork.injectNextDumpDirForTest(String.valueOf(next));
      primary.run("alter scheduled query s1 execute");
      Thread.sleep(30000);
      replica.run("alter scheduled query s2 execute");
      Thread.sleep(30000);
      replica.run("use " + replicatedDbName)
              .run("show tables like 't1'")
              .verifyResult("t1")
              .run("select id from t1 order by id")
              .verifyResults(new String[]{"1", "2", "3", "4", "5", "6"})
              .run("drop table t1");


    } finally {
      primary.run("drop scheduled query s1");
      replica.run("drop scheduled query s2");
    }
  }

  @Test
  public void testExternalTablesReplLoadBootstrapIncr() throws Throwable {
    // Bootstrap
    String withClause = " WITH('" + HiveConf.ConfVars.REPL_EXTERNAL_TABLE_BASE_DIR.varname
            + "'='/replica_external_base')";
    primary.run("use " + primaryDbName)
            .run("create external table t1 (id int)")
            .run("insert into t1 values(1)")
            .run("insert into t1 values(2)");
    try (ScheduledQueryExecutionService schqS =
                 ScheduledQueryExecutionService.startScheduledQueryExecutorService(primary.hiveConf)) {
      int next = 0;
      ReplDumpWork.injectNextDumpDirForTest(String.valueOf(next));
      primary.run("create scheduled query s1 every 10 minutes as repl dump " + primaryDbName + withClause);
      primary.run("alter scheduled query s1 execute");
      Thread.sleep(80000);
      replica.run("create scheduled query s2 every 10 minutes as repl load " + primaryDbName + " INTO "
              + replicatedDbName);
      replica.run("alter scheduled query s2 execute");
      Thread.sleep(80000);
      replica.run("use " + replicatedDbName)
              .run("show tables like 't1'")
              .verifyResult("t1")
              .run("select id from t1 order by id")
              .verifyResults(new String[]{"1", "2"});

      // First incremental, after bootstrap
      primary.run("use " + primaryDbName)
              .run("insert into t1 values(3)")
              .run("insert into t1 values(4)");
      next++;
      ReplDumpWork.injectNextDumpDirForTest(String.valueOf(next));
      primary.run("alter scheduled query s1 execute");
      Thread.sleep(80000);
      replica.run("alter scheduled query s2 execute");
      Thread.sleep(80000);
      replica.run("use " + replicatedDbName)
              .run("show tables like 't1'")
              .verifyResult("t1")
              .run("select id from t1 order by id")
              .verifyResults(new String[]{"1", "2", "3", "4"});


    } finally {
      primary.run("drop scheduled query s1");
      replica.run("drop scheduled query s2");
    }
  }
}
