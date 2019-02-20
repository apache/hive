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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.BehaviourInjection;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.CallerArguments;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import org.apache.hadoop.hive.shims.Utils;
import org.junit.*;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.hive.metastore.ReplChangeManager.SOURCE_OF_REPLICATION;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isFullAcidTable;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isTransactionalTable;
import static org.junit.Assert.*;

/**
 * TestReplicationWithTableMigration - test replication for Hive2 to Hive3 (Strict managed tables)
 */
public class TestReplicationWithTableMigrationMisc {
  @Rule
  public final TestName testName = new TestName();

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationWithTableMigrationMisc.class);
  private static WarehouseInstance primary, replica;
  private String primaryDbName, replicatedDbName;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    HashMap<String, String> overrideProperties = new HashMap<>();
    internalBeforeClassSetup(overrideProperties);
  }

  static void internalBeforeClassSetup(Map<String, String> overrideConfigs) throws Exception {
    HiveConf conf = new HiveConf(TestReplicationWithTableMigrationMisc.class);
    conf.set("dfs.client.use.datanode.hostname", "true");
    conf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    MiniDFSCluster miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    final DistributedFileSystem fs = miniDFSCluster.getFileSystem();
    HashMap<String, String> hiveConfigs = new HashMap<String, String>() {{
      put("fs.defaultFS", fs.getUri().toString());
      put("hive.support.concurrency", "true");
      put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
      put("hive.metastore.client.capability.check", "false");
      put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
      put("hive.exec.dynamic.partition.mode", "nonstrict");
      put("hive.strict.checks.bucketing", "false");
      put("hive.mapred.mode", "nonstrict");
      put("mapred.input.dir.recursive", "true");
      put("hive.metastore.disallow.incompatible.col.type.changes", "false");
      put("hive.strict.managed.tables", "true");
      put("hive.metastore.transactional.event.listeners", "");
    }};
    replica = new WarehouseInstance(LOG, miniDFSCluster, hiveConfigs);

    HashMap<String, String> configsForPrimary = new HashMap<String, String>() {{
      put("fs.defaultFS", fs.getUri().toString());
      put("hive.metastore.client.capability.check", "false");
      put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
      put("hive.exec.dynamic.partition.mode", "nonstrict");
      put("hive.strict.checks.bucketing", "false");
      put("hive.mapred.mode", "nonstrict");
      put("mapred.input.dir.recursive", "true");
      put("hive.metastore.disallow.incompatible.col.type.changes", "false");
      put("hive.support.concurrency", "false");
      put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager");
      put("hive.strict.managed.tables", "false");
    }};
    configsForPrimary.putAll(overrideConfigs);
    primary = new WarehouseInstance(LOG, miniDFSCluster, configsForPrimary);
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

  private void prepareData(String primaryDbName) throws Throwable {
    primary.run("use " + primaryDbName)
        .run("create table tacid (id int) clustered by(id) into 3 buckets stored as orc ")
        .run("create table tacidpart (place string) partitioned by (country string) clustered by(place) "
                + "into 3 buckets stored as orc ")
        .run("insert into tacid values(1)")
        .run("insert into tacid values(2)")
        .run("insert into tacid values(3)")
        .run("alter table tacidpart add partition(country='france')")
        .run("insert into tacidpart partition(country='india') values('mumbai')")
        .run("insert into tacidpart partition(country='us') values('sf')")
        .run("insert into tacidpart partition(country='france') values('paris')");
    assertFalse(isTransactionalTable(primary.getTable(primaryDbName, "tacid")));
    assertFalse(isTransactionalTable(primary.getTable(primaryDbName, "tacidpart")));
  }

  private void verifyLoadExecution(String replicatedDbName, String lastReplId) throws Throwable {
    replica.run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"tacid", "tacidpart"})
            .run("repl status " + replicatedDbName)
            .verifyResult(lastReplId)
            .run("select count(*) from tacid")
            .verifyResult("3")
            .run("select id from tacid order by id")
            .verifyResults(new String[]{"1", "2", "3"})
            .run("select count(*) from tacidpart")
            .verifyResult("3")
            .run("select country from tacidpart order by country")
            .verifyResults(new String[] {"france", "india", "us"});

    assertTrue(isFullAcidTable(replica.getTable(replicatedDbName, "tacid")));
    assertTrue(isFullAcidTable(replica.getTable(replicatedDbName, "tacidpart")));
  }

  private WarehouseInstance.Tuple dumpWithLastEventIdHacked(int eventId) throws Throwable {
    BehaviourInjection<CurrentNotificationEventId, CurrentNotificationEventId> callerVerifier
            = new BehaviourInjection<CurrentNotificationEventId, CurrentNotificationEventId>() {
      @Override
      public CurrentNotificationEventId apply(CurrentNotificationEventId id) {
        try {
          LOG.warn("GetCurrentNotificationEventIdBehaviour called");
          injectionPathCalled = true;
          // keep events to reply during incremental
          id.setEventId(eventId);
          return id;
        } catch (Throwable throwable) {
          throwable.printStackTrace();
          return null;
        }
      }
    };

    InjectableBehaviourObjectStore.setGetCurrentNotificationEventIdBehaviour(callerVerifier);
    try {
      return primary.dump(primaryDbName, null);
    } finally {
      InjectableBehaviourObjectStore.setGetCurrentNotificationEventIdBehaviour(null);
      callerVerifier.assertInjectionsPerformed(true, false);
    }
  }

  @Test
  public void testConcurrentOpDuringBootStrapDumpCreateTableReplay() throws Throwable {
    prepareData(primaryDbName);

    // dump with operation after last repl id is fetched.
    WarehouseInstance.Tuple tuple =  dumpWithLastEventIdHacked(2);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);
    assertTrue(!ReplUtils.isFirstIncDone(replica.getDatabase(replicatedDbName).getParameters()));

    // next incremental dump
    tuple = primary.dump(primaryDbName, tuple.lastReplicationId);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);
    assertTrue(ReplUtils.isFirstIncDone(replica.getDatabase(replicatedDbName).getParameters()));
  }

  @Test
  public void testConcurrentOpDuringBootStrapDumpInsertReplay() throws Throwable {
    prepareData(primaryDbName);

    // dump with operation after last repl id is fetched.
    WarehouseInstance.Tuple tuple =  dumpWithLastEventIdHacked(4);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);
    assertTrue(!ReplUtils.isFirstIncDone(replica.getDatabase(replicatedDbName).getParameters()));

    // next incremental dump
    tuple = primary.dump(primaryDbName, tuple.lastReplicationId);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);
    assertTrue(ReplUtils.isFirstIncDone(replica.getDatabase(replicatedDbName).getParameters()));
  }

  @Test
  public void testTableLevelDumpMigration() throws Throwable {
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create table t1 (i int, j int)")
            .dump(primaryDbName+".t1", null);
    replica.run("create database " + replicatedDbName);
    replica.loadWithoutExplain(replicatedDbName + ".t1", tuple.dumpLocation);
    assertTrue(ReplUtils.isFirstIncDone(replica.getDatabase(replicatedDbName).getParameters()));
    assertTrue(!ReplUtils.isFirstIncDone(replica.getTable(replicatedDbName, "t1").getParameters()));

    tuple = primary.run("use " + primaryDbName)
            .run("insert into t1 values (1, 2)")
            .dump(primaryDbName+".t1", tuple.lastReplicationId);
    replica.loadWithoutExplain(replicatedDbName + ".t1", tuple.dumpLocation);
    assertTrue(ReplUtils.isFirstIncDone(replica.getDatabase(replicatedDbName).getParameters()));
    assertTrue(ReplUtils.isFirstIncDone(replica.getTable(replicatedDbName, "t1").getParameters()));
  }
}
