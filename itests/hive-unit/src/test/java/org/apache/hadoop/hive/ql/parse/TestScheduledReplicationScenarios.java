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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.ql.scheduled.ScheduledQueryExecutionService;
import org.apache.hadoop.hive.shims.Utils;
import org.junit.*;

import java.io.IOException;
import java.util.*;


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
    replica = new WarehouseInstance(LOG, miniDFSCluster, acidEnableConf);
    Map<String, String> overridesForHiveConf1 = new HashMap<String, String>() {{
          put("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
          put("hive.support.concurrency", "false");
          put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
          put("hive.metastore.client.capability.check", "false");
      }};
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
  public void testAcidTablesBootstrapIncr() throws Throwable {
    // Bootstrap
    primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                      "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(1)")
            .run("insert into t1 values(2)");
    try (ScheduledQueryExecutionService schqS =
                 ScheduledQueryExecutionService.startScheduledQueryExecutorService(primary.hiveConf)) {

    primary.run("create scheduled query s1 every 2 seconds as repl dump " + primaryDbName);

    Thread.sleep(6000);
    Path dumpRoot = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPLDIR), "next");
    replica.load(replicatedDbName, dumpRoot.toString());
    replica.run("use " + replicatedDbName)
            .run("show tables like 't1'")
            .verifyResult("t1")
            .run("select id from t1 order by id")
            .verifyResults(new String[]{"1", "2"});

    // First incremental, after bootstrap

//    primary.run("use " + primaryDbName)
//            .run("insert into t1 values(3)")
//            .run("insert into t1 values(4)");
//    Thread.sleep(4000);
//
//    dumpRoot = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPLDIR), "next");
//    replica.load(replicatedDbName, dumpRoot.toString());
//    replica.run("use " + replicatedDbName)
//            .run("show tables like 't1'")
//            .verifyResult("t1")
//            .run("select id from t1 order by id")
//            .verifyResults(new String[]{"1", "2", "3", "4"});


    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    } finally {
      primary.run("drop scheduled query s1");
    }
  }

  @Test
  public void testExternalTablesBootstrapIncr() throws Throwable {
        // Bootstrap
     primary.run("use " + primaryDbName)
             .run("create external table extt1 (id int)")
             .run("insert into extt1 values(1)")
             .run("insert into extt1 values(2)");
     try (ScheduledQueryExecutionService schqS =
                     ScheduledQueryExecutionService.startScheduledQueryExecutorService(primary.hiveConf)) {

        primary.run("create scheduled query s1 every 2 seconds as repl dump " + primaryDbName);

        Thread.sleep(6000);
        Path dumpRoot = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPLDIR), "next");
        replica.load(replicatedDbName, dumpRoot.toString());
        replica.run("use " + replicatedDbName)
                .run("show tables like 'extt1'")
                .verifyResult("extt1")
                .run("select id from extt1 order by id")
                .verifyResults(new String[]{"1", "2"});

        // First incremental, after bootstrap

//        primary.run("use " + primaryDbName)
//                .run("insert into t1 values(3)")
//                .run("insert into t1 values(4)");
//        Thread.sleep(4000);
//
//        dumpRoot = new Path(primary.hiveConf.getVar(HiveConf.ConfVars.REPLDIR), "next");
//        replica.load(replicatedDbName, dumpRoot.toString());
//        replica.run("use " + replicatedDbName)
//                .run("show tables like 't1'")
//                .verifyResult("t1")
//                .run("select id from t1 order by id")
//                .verifyResults(new String[]{"1", "2", "3", "4"});



    } catch (IOException | InterruptedException e) {
        e.printStackTrace();
    } finally {
        primary.run("drop scheduled query s1");
    }
  }
}
