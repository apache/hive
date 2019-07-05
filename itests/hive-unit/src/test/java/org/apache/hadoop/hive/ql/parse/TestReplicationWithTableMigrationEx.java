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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore;
import org.apache.hadoop.hive.metastore.InjectableBehaviourObjectStore.BehaviourInjection;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hive.hcatalog.listener.DbNotificationListener;
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
 * TestReplicationWithTableMigrationEx - test replication for Hive2 to Hive3 (Strict managed tables)
 */
public class TestReplicationWithTableMigrationEx {
  @Rule
  public final TestName testName = new TestName();

  protected static final Logger LOG = LoggerFactory.getLogger(TestReplicationWithTableMigrationEx.class);
  private static WarehouseInstance primary, replica;
  private String primaryDbName, replicatedDbName;

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    HashMap<String, String> overrideProperties = new HashMap<>();
    internalBeforeClassSetup(overrideProperties);
  }

  static void internalBeforeClassSetup(Map<String, String> overrideConfigs) throws Exception {
    HiveConf conf = new HiveConf(TestReplicationWithTableMigrationEx.class);
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
      InjectableBehaviourObjectStore.resetGetCurrentNotificationEventIdBehaviour();
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
    assertTrue(ReplUtils.isFirstIncPending(replica.getDatabase(replicatedDbName).getParameters()));

    // next incremental dump
    tuple = primary.dump(primaryDbName, tuple.lastReplicationId);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);
    assertFalse(ReplUtils.isFirstIncPending(replica.getDatabase(replicatedDbName).getParameters()));
  }

  @Test
  public void testConcurrentOpDuringBootStrapDumpInsertReplay() throws Throwable {
    prepareData(primaryDbName);

    // dump with operation after last repl id is fetched.
    WarehouseInstance.Tuple tuple =  dumpWithLastEventIdHacked(4);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);
    assertTrue(ReplUtils.isFirstIncPending(replica.getDatabase(replicatedDbName).getParameters()));

    // next incremental dump
    tuple = primary.dump(primaryDbName, tuple.lastReplicationId);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);
    assertFalse(ReplUtils.isFirstIncPending(replica.getDatabase(replicatedDbName).getParameters()));
  }

  @Test
  public void testTableLevelDumpMigration() throws Throwable {
    WarehouseInstance.Tuple tuple = primary
            .run("use " + primaryDbName)
            .run("create table t1 (i int, j int)")
            .dump(primaryDbName+".'t1'", null);
    replica.run("create database " + replicatedDbName);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    assertTrue(ReplUtils.isFirstIncPending(replica.getDatabase(replicatedDbName).getParameters()));

    tuple = primary.run("use " + primaryDbName)
            .run("insert into t1 values (1, 2)")
            .dump(primaryDbName+".'t1'", tuple.lastReplicationId);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    assertFalse(ReplUtils.isFirstIncPending(replica.getDatabase(replicatedDbName).getParameters()));
  }

  @Test
  public void testConcurrentOpDuringBootStrapDumpInsertOverwrite() throws Throwable {
    primary.run("use " + primaryDbName)
            .run("create table tacid (id int) clustered by(id) into 3 buckets stored as orc ")
            .run("insert into tacid values(1)")
            .run("insert into tacid values(2)")
            .run("insert into tacid values(3)")
            .run("insert overwrite table tacid values(4)")
            .run("insert into tacid values(5)");

    // dump with operation after last repl id is fetched.
    WarehouseInstance.Tuple tuple =  dumpWithLastEventIdHacked(2);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    replica.run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"tacid"})
            .run("repl status " + replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("select count(*) from tacid")
            .verifyResult("2")
            .run("select id from tacid order by id")
            .verifyResults(new String[]{"4", "5"});
    assertTrue(ReplUtils.isFirstIncPending(replica.getDatabase(replicatedDbName).getParameters()));

    // next incremental dump
    tuple = primary.dump(primaryDbName, tuple.lastReplicationId);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    replica.run("use " + replicatedDbName)
            .run("show tables")
            .verifyResults(new String[] {"tacid"})
            .run("repl status " + replicatedDbName)
            .verifyResult(tuple.lastReplicationId)
            .run("select count(*) from tacid")
            .verifyResult("2")
            .run("select id from tacid order by id")
            .verifyResults(new String[]{"4", "5",});
    assertFalse(ReplUtils.isFirstIncPending(replica.getDatabase(replicatedDbName).getParameters()));
  }

  private void loadWithFailureInAddNotification(String tbl, String dumpLocation) throws Throwable {
    BehaviourInjection<InjectableBehaviourObjectStore.CallerArguments, Boolean> callerVerifier
            = new BehaviourInjection<InjectableBehaviourObjectStore.CallerArguments, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable InjectableBehaviourObjectStore.CallerArguments args) {
        injectionPathCalled = true;
        LOG.warn("InjectableBehaviourObjectStore called for Verifier - Table: " + args.tblName);
        if (!args.dbName.equalsIgnoreCase(replicatedDbName) || (args.constraintTblName != null)) {
          LOG.warn("Verifier - DB: " + args.dbName
                  + " Constraint Table: " + args.constraintTblName);
          return false;
        }
        if (args.tblName != null) {
          LOG.warn("Verifier - Table: " + args.tblName);
          return !args.tblName.equalsIgnoreCase(tbl);
        }
        return true;
      }
    };
    InjectableBehaviourObjectStore.setCallerVerifier(callerVerifier);
    try {
      List<String> withClause = Collections.singletonList("'hive.metastore.transactional.event.listeners'='"
              + DbNotificationListener.class.getCanonicalName() + "'");
      replica.loadFailure(replicatedDbName, dumpLocation, withClause);
    } finally {
      InjectableBehaviourObjectStore.resetCallerVerifier();
    }
    callerVerifier.assertInjectionsPerformed(true, false);
  }

  @Test
  public void testIncLoadPenFlagPropAlterDB() throws Throwable {
    prepareData(primaryDbName);

    // dump with operation after last repl id is fetched.
    WarehouseInstance.Tuple tuple =  dumpWithLastEventIdHacked(4);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);
    assertTrue(ReplUtils.isFirstIncPending(replica.getDatabase(replicatedDbName).getParameters()));
    assertFalse(ReplUtils.isFirstIncPending(primary.getDatabase(primaryDbName).getParameters()));

    tuple = primary.run("use " + primaryDbName)
            .run("alter database " + primaryDbName + " set dbproperties('dummy_key'='dummy_val')")
           .run("create table tbl_temp (fld int)")
            .dump(primaryDbName, tuple.lastReplicationId);

    loadWithFailureInAddNotification("tbl_temp", tuple.dumpLocation);
    Database replDb = replica.getDatabase(replicatedDbName);
    assertTrue(ReplUtils.isFirstIncPending(replDb.getParameters()));
    assertFalse(ReplUtils.isFirstIncPending(primary.getDatabase(primaryDbName).getParameters()));
    assertTrue(replDb.getParameters().get("dummy_key").equalsIgnoreCase("dummy_val"));

    // next incremental dump
    tuple = primary.dump(primaryDbName, tuple.lastReplicationId);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    assertFalse(ReplUtils.isFirstIncPending(replica.getDatabase(replicatedDbName).getParameters()));
  }

  @Test
  public void testIncLoadPenFlagWithMoveOptimization() throws Throwable {
    List<String> withClause = Collections.singletonList("'hive.repl.enable.move.optimization'='true'");

    prepareData(primaryDbName);

    // dump with operation after last repl id is fetched.
    WarehouseInstance.Tuple tuple =  dumpWithLastEventIdHacked(4);
    replica.load(replicatedDbName, tuple.dumpLocation, withClause);
    verifyLoadExecution(replicatedDbName, tuple.lastReplicationId);
    assertTrue(ReplUtils.isFirstIncPending(replica.getDatabase(replicatedDbName).getParameters()));

    // next incremental dump
    tuple = primary.dump(primaryDbName, tuple.lastReplicationId);
    replica.load(replicatedDbName, tuple.dumpLocation, withClause);
    assertFalse(ReplUtils.isFirstIncPending(replica.getDatabase(replicatedDbName).getParameters()));
  }

  private void verifyUserName(String userName) throws Throwable {
    assertTrue(userName.equalsIgnoreCase(primary.getTable(primaryDbName, "tbl_own").getOwner()));
    assertTrue(userName.equalsIgnoreCase(replica.getTable(replicatedDbName, "tbl_own").getOwner()));
    assertTrue(userName.equalsIgnoreCase(primary.getTable(primaryDbName, "tacid").getOwner()));
    assertTrue(userName.equalsIgnoreCase(replica.getTable(replicatedDbName, "tacid").getOwner()));
    assertTrue(userName.equalsIgnoreCase(primary.getTable(primaryDbName, "tacidpart").getOwner()));
    assertTrue(userName.equalsIgnoreCase(replica.getTable(replicatedDbName, "tacidpart").getOwner()));
    assertTrue(userName.equalsIgnoreCase(primary.getTable(primaryDbName, "tbl_part").getOwner()));
    assertTrue(userName.equalsIgnoreCase(replica.getTable(replicatedDbName, "tbl_part").getOwner()));
    assertTrue(userName.equalsIgnoreCase(primary.getTable(primaryDbName, "view_own").getOwner()));
    assertTrue(userName.equalsIgnoreCase(replica.getTable(replicatedDbName, "view_own").getOwner()));
  }

  private void alterUserName(String userName) throws Throwable {
    primary.run("use " + primaryDbName)
            .run("alter table tbl_own set owner USER " + userName)
            .run("alter table tacid set owner USER " + userName)
            .run("alter table tacidpart set owner USER " + userName)
            .run("alter table tbl_part set owner USER " + userName)
            .run("alter table view_own set owner USER " + userName);
  }

  @Test
  public void testOnwerPropagation() throws Throwable {
    primary.run("use " + primaryDbName)
            .run("create table tbl_own (fld int)")
            .run("create table tacid (id int) clustered by(id) into 3 buckets stored as orc ")
            .run("create table tacidpart (place string) partitioned by (country string) clustered by(place) "
                    + "into 3 buckets stored as orc ")
            .run("create table tbl_part (fld int) partitioned by (country string)")
            .run("insert into tbl_own values (1)")
            .run("create view view_own as select * from tbl_own");

    // test bootstrap
    alterUserName("hive");
    WarehouseInstance.Tuple tuple = primary.dump(primaryDbName, null);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    verifyUserName("hive");

    // test incremental
    alterUserName("hive1");
    tuple = primary.dump(primaryDbName, tuple.lastReplicationId);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    verifyUserName("hive1");
  }

  @Test
  public void testOnwerPropagationInc() throws Throwable {
    WarehouseInstance.Tuple tuple = primary.dump(primaryDbName, null);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);

    primary.run("use " + primaryDbName)
            .run("create table tbl_own (fld int)")
            .run("create table tacid (id int) clustered by(id) into 3 buckets stored as orc ")
            .run("create table tacidpart (place string) partitioned by (country string) clustered by(place) "
                    + "into 3 buckets stored as orc ")
            .run("create table tbl_part (fld int) partitioned by (country string)")
            .run("insert into tbl_own values (1)")
            .run("create view view_own as select * from tbl_own");

    // test incremental when table is getting created in the same load
    alterUserName("hive");
    tuple = primary.dump(primaryDbName, tuple.lastReplicationId);
    replica.loadWithoutExplain(replicatedDbName, tuple.dumpLocation);
    verifyUserName("hive");
  }

  @Test
  public void dynamicallyConvertNonAcidToAcidTable() throws Throwable {
    // Non-acid table converted to an ACID table should be prohibited on source cluster with
    // strict managed false.
    primary.run("use " + primaryDbName)
            .run("create table t1 (id int) stored as orc")
            .run("insert into table t1 values (1)")
            .run("create table t2 (place string) partitioned by (country string) stored as orc")
            .run("insert into table t2 partition(country='india') values ('bangalore')")
            .runFailure("alter table t1 set tblproperties('transactional'='true')")
            .runFailure("alter table t2 set tblproperties('transactional'='true')")
            .runFailure("alter table t1 set tblproperties('transactional'='true', " +
                    "'transactional_properties'='insert_only')")
            .runFailure("alter table t2 set tblproperties('transactional'='true', " +
                    "'transactional_properties'='insert_only')");

  }

  @Test
  public void prohibitManagedTableLocationChangeOnReplSource() throws Throwable {
    String tmpLocation = "/tmp/" + System.nanoTime();
    primary.miniDFSCluster.getFileSystem().mkdirs(new Path(tmpLocation), new FsPermission("777"));

    // For managed tables at source, the table location shouldn't be changed for the given
    // non-partitioned table and partition location shouldn't be changed for partitioned table as
    // alter event doesn't capture the new files list. So, it may cause data inconsistsency. So,
    // if database is enabled for replication at source, then alter location on managed tables
    // should be blocked.
    primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc ")
            .run("insert into t1 values(1)")
            .run("create table t2 (place string) partitioned by (country string) stored as orc")
            .run("insert into table t2 partition(country='india') values ('bangalore')")
            .runFailure("alter table t1 set location '" + tmpLocation + "'")
            .runFailure("alter table t2 partition(country='india') set location '" + tmpLocation + "'")
            .runFailure("alter table t2 set location '" + tmpLocation + "'");

    primary.miniDFSCluster.getFileSystem().delete(new Path(tmpLocation), true);
  }
}
