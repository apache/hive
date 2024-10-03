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

import org.apache.commons.io.FileUtils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.*;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.parse.repl.PathBuilder;
import org.apache.hadoop.hive.shims.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;

/**
 * TestTxnReplicationOptimizations - Test transaction replication.
 */
public class TestReplicationFilterTransactions {
  static final private Logger LOG = LoggerFactory.getLogger(TestReplicationFilterTransactions.class);

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  // Event listener for the primary, mainly counts txn events.
  // Count totals are saved to static fields so they can be accessed
  // after dump/load.
  static public class PrimaryEventListenerTestImpl extends MetaStoreEventListener {
    public PrimaryEventListenerTestImpl(Configuration conf) {
      super(conf);
    }

    private static final AtomicInteger countOpenTxn = new AtomicInteger(0);
    private static final AtomicInteger countCommitTxn = new AtomicInteger (0);
    private static final AtomicInteger countAbortTxn = new AtomicInteger (0);

    @Override
    public void onOpenTxn(OpenTxnEvent openTxnEvent, Connection dbConn, SQLGenerator sqlGenerator) throws MetaException {
      super.onOpenTxn(openTxnEvent, dbConn, sqlGenerator);
      countOpenTxn.getAndIncrement();
    }

    @Override
    public void onCommitTxn(CommitTxnEvent commitTxnEvent, Connection dbConn, SQLGenerator sqlGenerator) throws MetaException {
      super.onCommitTxn(commitTxnEvent, dbConn, sqlGenerator);
      countCommitTxn.getAndIncrement();
    }

    @Override
    public void onAbortTxn(AbortTxnEvent abortTxnEvent, Connection dbConn, SQLGenerator sqlGenerator) throws MetaException {
      super.onAbortTxn(abortTxnEvent, dbConn, sqlGenerator);
      countAbortTxn.getAndIncrement();
    }

    public static int getCountOpenTxn() {
      return countOpenTxn.get();
    }

    public static int getCountCommitTxn() {
      return countCommitTxn.get();
    }

    public static int getCountAbortTxn() {
      return countAbortTxn.get();
    }

    public static void reset() {
      countOpenTxn.set(0);
      countCommitTxn.set(0);
      countAbortTxn.set(0);
    }
  }

  // Event listener for the replica, mainly counts txn events.
  // Count totals are saved to static fields so they can be accessed
  // after dump/load.
  static public class ReplicaEventListenerTestImpl extends MetaStoreEventListener {
    public ReplicaEventListenerTestImpl(Configuration conf) {
      super(conf);
    }

    private static final AtomicInteger countOpenTxn = new AtomicInteger(0);
    private static final AtomicInteger countCommitTxn = new AtomicInteger (0);
    private static final AtomicInteger countAbortTxn = new AtomicInteger (0);

    private static final Map<Long, Long> txnMapping = new ConcurrentHashMap<Long, Long>();
    
    @Override
    public void onOpenTxn(OpenTxnEvent openTxnEvent, Connection dbConn, SQLGenerator sqlGenerator) throws MetaException {
      super.onOpenTxn(openTxnEvent, dbConn, sqlGenerator);
      countOpenTxn.getAndIncrement();

      // Following code reads REPL_TXN_MAP, so we can check later test that primary to replica TxnId
      // mapping was done.
      try {
        TestReplicationFilterTransactions.updateTxnMapping(txnMapping);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onCommitTxn(CommitTxnEvent commitTxnEvent, Connection dbConn, SQLGenerator sqlGenerator) throws MetaException {
      super.onCommitTxn(commitTxnEvent, dbConn, sqlGenerator);
      countCommitTxn.getAndIncrement();
    }

    @Override
    public void onAbortTxn(AbortTxnEvent abortTxnEvent, Connection dbConn, SQLGenerator sqlGenerator) throws MetaException {
      super.onAbortTxn(abortTxnEvent, dbConn, sqlGenerator);
      countAbortTxn.getAndIncrement();
    }

    public static int getCountOpenTxn() {
      return countOpenTxn.get();
    }

    public static int getCountCommitTxn() {
      return countCommitTxn.get();
    }

    public static int getCountAbortTxn() {
      return countAbortTxn.get();
    }

    public static Map<Long, Long> getTxnMapping() { return new HashMap(txnMapping); }
    
    public static void reset() {
      countOpenTxn.set(0);
      countCommitTxn.set(0);
      countAbortTxn.set(0);
      txnMapping.clear();
    }
  }

  static class EventCount {
    int countOpenTxn;
    int countCommitTxn;
    int countAbortTxn;

    public EventCount(int countOpenTxn, int countCommitTxn, int countAbortTxn) {
      this.countOpenTxn = countOpenTxn;
      this.countCommitTxn = countCommitTxn;
      this.countAbortTxn = countAbortTxn;
    }

    public int getCountOpenTxn() {
      return countOpenTxn;
    }

    public int getCountCommitTxn() {
      return countCommitTxn;
    }

    public int getCountAbortTxn() {
      return countAbortTxn;
    }
  }

  @Rule
  public final TestName testName = new TestName();

  static WarehouseInstance primary;
  static WarehouseInstance replica;

  static HiveConf dfsConf;

  String primaryDbName, replicatedDbName, otherDbName;

  EventCount expected;
  int txnOffset;


  private Map<String, String> setupConf(String dfsUri, String listenerClassName) {
    Map<String, String> confMap = new HashMap<String, String>();
    confMap.put("fs.defaultFS", dfsUri);
    confMap.put("hive.support.concurrency", "true");
    confMap.put("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
    confMap.put("hive.metastore.client.capability.check", "false");
    confMap.put("hive.repl.bootstrap.dump.open.txn.timeout", "1s");
    confMap.put("hive.strict.checks.bucketing", "false");
    confMap.put("hive.mapred.mode", "nonstrict");
    confMap.put("mapred.input.dir.recursive", "true");
    confMap.put("hive.metastore.disallow.incompatible.col.type.changes", "false");
    confMap.put("hive.stats.autogather", "false");
    confMap.put("hive.in.repl.test", "true");

    // Primary and replica have different listeners so that we know exactly what
    // happened on primary and replica respectively.
    confMap.put(MetastoreConf.ConfVars.EVENT_LISTENERS.getVarname(), listenerClassName);
    confMap.put(MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY.getHiveName(),
            GzipJSONMessageEncoder.class.getCanonicalName());
    return confMap;
  }

  @Before
  public void setup() throws Throwable {
    TestReplicationFilterTransactions.dfsConf = new HiveConf(TestReplicationFilterTransactions.class);
    TestReplicationFilterTransactions.dfsConf.set("dfs.client.use.datanode.hostname", "true");
    TestReplicationFilterTransactions.dfsConf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    TestReplicationFilterTransactions.dfsConf.set("dfs.namenode.acls.enabled", "true");

    MiniDFSCluster miniDFSCluster =
            new MiniDFSCluster.Builder(TestReplicationFilterTransactions.dfsConf).numDataNodes(2).format(true).build();

    Map<String, String> conf = setupConf(miniDFSCluster.getFileSystem().getUri().toString(),
            PrimaryEventListenerTestImpl.class.getName());
    //TODO: HIVE-28044: Replication tests to run on Tez
    conf.put(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, "mr");
    primary = new WarehouseInstance(LOG, miniDFSCluster, conf);

    conf = setupConf(miniDFSCluster.getFileSystem().getUri().toString(),
            ReplicaEventListenerTestImpl.class.getName());
    conf.put(MetastoreConf.ConfVars.REPLDIR.getHiveName(), primary.repldDir);
    //TODO: HIVE-28044: Replication tests to run on Tez
    conf.put(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, "mr");
    replica = new WarehouseInstance(LOG, miniDFSCluster, conf);

    primaryDbName = testName.getMethodName() + "_" + System.currentTimeMillis();
    replicatedDbName = "replicated_" + primaryDbName;
    primary.run("create database " + primaryDbName + " WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')");
    otherDbName = testName.getMethodName() + "_other_" + System.currentTimeMillis();
    primary.run("create database " + otherDbName);

    PrimaryEventListenerTestImpl.reset();
    ReplicaEventListenerTestImpl.reset();

    // Each test always has 8 openTxns, 6 commitTxn, and 2 abortTxns.
    // Note that this is the number that was done on the primary,
    // and some are done on non-replicated database.
    expected = new EventCount(8, 6, 2);
  }

  static void updateTxnMapping(Map<Long, Long> map) throws Exception {
    // Poll REPL_TXN_MAP and add to map.
    // Do dirty read.
    try (Connection conn = TestTxnDbUtil.getConnection(replica.hiveConf)) {
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("SELECT \"RTM_TARGET_TXN_ID\", \"RTM_SRC_TXN_ID\" FROM \"REPL_TXN_MAP\"")) {
          while(rs.next()) {
            Long srcTxnId = rs.getLong(1);
            Long tgtTxnId = rs.getLong(2);
            map.put(srcTxnId, tgtTxnId);
          }
        }
      }
    }
  }

  @After
  public void tearDown() throws Throwable {
    primary.run("drop database if exists " + primaryDbName + " cascade");
    primary.run("drop database if exists " + otherDbName + " cascade");
    replica.run("drop database if exists " + replicatedDbName + " cascade");
    primary.close();
    replica.close();
  }

  private void prepareBootstrapData() throws Throwable {

    // Burn some txnids on the source. This will ensure that the primary TxnIds will be
    // greater than the replica TxnIds to make sure that TxnId mapping is occurring.
    // This will cause the primary transaction numbering to be 10 greater than the
    // replica transaction numbering.
    primary.run("use " + primaryDbName)
            .run("create table t999 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t999 values (99901)")
            .run("insert into t999 values (99902)")
            .run("insert into t999 values (99903)")
            .run("insert into t999 values (99904)")
            .run("insert into t999 values (99905)")
            .run("insert into t999 values (99906)")
            .run("insert into t999 values (99907)")
            .run("insert into t999 values (99908)")
            .run("insert into t999 values (99909)")
            .run("insert into t999 values (99910)")
            .run("drop table t999");
    txnOffset = 10;

    // primaryDbName is replicated, t2 and t2 are ACID tables with initial data.
    // t3 is an ACID table with 2 initial rows, later t3 will be locked to force aborted transaction.
    primary.run("use " + primaryDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                  "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(1)")
            .run("create table t2 (place string) partitioned by (country string) clustered by(place) " +
                    "into 3 buckets stored as orc tblproperties (\"transactional\"=\"true\")")
            .run("insert into t2 partition(country='india') values ('bangalore')")
            .run("create table t3 (id int) stored as orc tblproperties (\"transactional\"=\"true\")")
            .run("insert into t3 values(111), (222)");

    // otherDbName is not replicated, but contains ACID tables.
    primary.run("use " + otherDbName)
            .run("create table t1 (id int) clustered by(id) into 3 buckets stored as orc " +
                    "tblproperties (\"transactional\"=\"true\")")
            .run("insert into t1 values(100)")
            .run("create table t2 (place string) partitioned by (country string) clustered by(place) " +
                    "into 3 buckets stored as orc tblproperties (\"transactional\"=\"true\")")
            .run("insert into t2 partition(country='usa') values ('san francisco')")
            .run("create table t3 (id int) stored as orc tblproperties (\"transactional\"=\"true\")")
            .run("insert into t3 values(1110), (2220)");
  }

  // Intentionally corrupt or uncorrupt a file of the table to cause an AbortTxn.
  private void alterBucketFile(Path warehouseRoot, String dbName, String tableName, boolean toCorrupted) throws IOException {
    DistributedFileSystem fs = primary.miniDFSCluster.getFileSystem();

    String bucket = "bucket_00000_0";

    PathBuilder pb = new PathBuilder(warehouseRoot.toString())
            .addDescendant(dbName.toLowerCase() + ".db")
            .addDescendant(tableName)
            .addDescendant("delta_0000001_0000001_0000")
            .addDescendant(bucket);
    Path location = pb.build();
    File junkFile = new File(tempFolder.getRoot(), "junk");
    File saveFolder = new File(tempFolder.getRoot(), dbName + "_" + tableName);

    if (toCorrupted) {
      if (!junkFile.exists()) {
        File junk = tempFolder.newFile("junk");
        FileUtils.writeStringToFile(junk, "junk", StandardCharsets.UTF_8);
      }
      Path dest = new Path(saveFolder.getAbsolutePath(), bucket);
      fs.copyToLocalFile(true, location, dest);
      fs.copyFromLocalFile(false, true, new Path(junkFile.getAbsolutePath()), location);
    } else {
      Path src = new Path(saveFolder.getAbsolutePath(), bucket);
      fs.copyFromLocalFile(true, true, src, location);
    }
  }

  private void prepareAbortTxn(String dbName, int value) throws Throwable {
    // Forces an abortTxn even to be generated in the database.
    // The abortTxn needs to be generated during the execution phase of the plan,
    // to do so, the bucket file of the table is intentionally mangled to
    // induce an error and abortTxn during the execution phase.

    alterBucketFile(primary.warehouseRoot, dbName, "t3", true);
    try {
      primary.run("use " + dbName)
             .run("update t3 set id = 999 where id = " + String.valueOf(value));
      Assert.fail("Update should have failed");
    } catch (Throwable t) {
      Assert.assertTrue(t.getCause().getCause() instanceof org.apache.orc.FileFormatException);
    }
    alterBucketFile(primary.warehouseRoot, dbName, "t3", false);
  }

  private void prepareIncrementalData() throws Throwable {
    primary.run("use " + primaryDbName)
            .run("insert into t1 values (2), (3)")
            .run("insert into t2 partition(country='india') values ('chennai')")
            .run("insert into t2 partition(country='india') values ('pune')");
    prepareAbortTxn(primaryDbName, 222);
    primary.run("use " + otherDbName)
            .run("insert into t1 values (200), (300)")
            .run("insert into t2 partition(country='usa') values ('santa clara')")
            .run("insert into t2 partition(country='usa') values ('palo alto')");
    prepareAbortTxn(otherDbName, 2220);
  }

  private List<String> withTxnOptimized(boolean optimizationOn) {
    return Collections.singletonList(String.format("'%s'='%s'", HiveConf.ConfVars.REPL_FILTER_TRANSACTIONS.toString(),
            String.valueOf(optimizationOn)));
  }

  @Test
  public void testTxnEventsUnoptimized() throws Throwable {
    prepareBootstrapData();
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .dump(primaryDbName, withTxnOptimized(false));
    replica.load(replicatedDbName, primaryDbName, withTxnOptimized(false));
    assertBootstrap(tuple);

    PrimaryEventListenerTestImpl.reset();
    ReplicaEventListenerTestImpl.reset();
    
    prepareIncrementalData();
    tuple = primary.run("use " + primaryDbName)
            .dump(primaryDbName, withTxnOptimized(false));
    replica.load(replicatedDbName, primaryDbName, withTxnOptimized(false));

    EventCount filtered = new EventCount(0, 0, 0);
    assertTxnOptimization(false, tuple, filtered);
  }

  @Test
  public void testTxnEventsOptimized() throws Throwable {
    prepareBootstrapData();
    WarehouseInstance.Tuple tuple = primary.run("use " + primaryDbName)
            .dump(primaryDbName, withTxnOptimized(false));
    replica.load(replicatedDbName, primaryDbName, withTxnOptimized(false));
    assertBootstrap(tuple);
    PrimaryEventListenerTestImpl.reset();
    ReplicaEventListenerTestImpl.reset();
    
    prepareIncrementalData();
    tuple = primary.run("use " + primaryDbName)
            .dump(primaryDbName, withTxnOptimized(true));
    replica.load(replicatedDbName, primaryDbName, withTxnOptimized(true));

    EventCount filtered = new EventCount(4, 3, 1);
    assertTxnOptimization(true, tuple, filtered);
  }

  private void assertBootstrap(WarehouseInstance.Tuple tuple) throws IOException {
    List<Path> openTxns = new ArrayList<Path>();
    List<Path> commitTxns = new ArrayList<Path>();
    List<Path> abortTxns = new ArrayList<Path>();

    ReplicationTestUtils.findTxnsFromDump(tuple, primary.hiveConf, openTxns, commitTxns, abortTxns);

    Assert.assertEquals(openTxns.size(), 0);
    Assert.assertEquals(commitTxns.size(), 0);
    Assert.assertEquals(abortTxns.size(), 0);
  }

  private void assertTxnOptimization(boolean optimizationOn, WarehouseInstance.Tuple tuple, EventCount filtered) throws Exception {

    List<Path> openTxns = new ArrayList<Path>();
    List<Path> commitTxns = new ArrayList<Path>();
    List<Path> abortTxns = new ArrayList<Path>();

    // Find all Txn event files in the dump directory.
    ReplicationTestUtils.findTxnsFromDump(tuple, primary.hiveConf, openTxns, commitTxns, abortTxns);

    // Assert the number of Txn events that occurred on the primary
    Assert.assertEquals(expected.getCountOpenTxn(), PrimaryEventListenerTestImpl.getCountOpenTxn());
    Assert.assertEquals(expected.getCountCommitTxn(), PrimaryEventListenerTestImpl.getCountCommitTxn());
    Assert.assertEquals(expected.getCountAbortTxn(), PrimaryEventListenerTestImpl.getCountAbortTxn());

    // Assert the number of Txn events that occurred on the replica.
    // When optimization is on, filtered has the number of Txn events that are expected to have been filtered.
    // When optimization is off, filtered should be all all 0s.
    Assert.assertEquals(expected.getCountOpenTxn() - filtered.getCountOpenTxn(), ReplicaEventListenerTestImpl.getCountOpenTxn());
    Assert.assertEquals(expected.getCountCommitTxn() - filtered.getCountCommitTxn(), ReplicaEventListenerTestImpl.getCountCommitTxn());
    Assert.assertEquals(expected.getCountAbortTxn() - filtered.getCountAbortTxn(), ReplicaEventListenerTestImpl.getCountAbortTxn());

    // Assert the number of Txn event files found.
    // When optimization is on, filtered has the number of Txn events that are expected to have been filtered.
    // When optimization is off, filtered should be all all 0s.
    // Note that when optimization is on, there should never be optnTxn events.
    Assert.assertEquals(optimizationOn ? 0 : expected.getCountOpenTxn(), openTxns.size());
    Assert.assertEquals(expected.getCountCommitTxn() - filtered.getCountCommitTxn(), commitTxns.size());
    Assert.assertEquals(expected.getCountAbortTxn() - filtered.getCountAbortTxn(), abortTxns.size());

    // Check replica TxnMapping.
    // Since primary TxnMappings had 10 txnIds burnt, there should be a mapping on the replica.
    Map<Long, Long> replicaTxnMapping = ReplicaEventListenerTestImpl.getTxnMapping();
    Assert.assertEquals(ReplicaEventListenerTestImpl.getCountOpenTxn(), replicaTxnMapping.size());
    for (Map.Entry<Long, Long> mapping : replicaTxnMapping.entrySet()) {
      Assert.assertEquals(mapping.getKey().longValue()  - txnOffset, mapping.getValue().longValue());
    }
  }
}
