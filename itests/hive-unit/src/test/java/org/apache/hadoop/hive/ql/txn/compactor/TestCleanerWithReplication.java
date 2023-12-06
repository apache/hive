/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.shims.Utils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.security.auth.login.LoginException;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestCleanerWithReplication extends CompactorTest {
  private Path cmRootDirectory;
  private static MiniDFSCluster miniDFSCluster;
  private final String dbName = "TestCleanerWithReplication";

  @Before
  public void setup() throws Exception {
    HiveConf conf = new HiveConf();
    conf.set("fs.defaultFS", miniDFSCluster.getFileSystem().getUri().toString());
    conf.setBoolVar(HiveConf.ConfVars.REPL_CM_ENABLED, true);
    setup(conf);
    cmRootDirectory = new Path(conf.get(HiveConf.ConfVars.REPL_CM_DIR.varname));
    if (!fs.exists(cmRootDirectory)) {
      fs.mkdirs(cmRootDirectory);
    }
    Database db = new Database();
    db.putToParameters(SOURCE_OF_REPLICATION, "1,2,3");
    db.setName(dbName);
    ms.createDatabase(db);
  }

  @BeforeClass
  public static void classLevelSetup() throws LoginException, IOException {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("dfs.client.use.datanode.hostname", "true");
    hadoopConf.set("hadoop.proxyuser." + Utils.getUGI().getShortUserName() + ".hosts", "*");
    miniDFSCluster =
        new MiniDFSCluster.Builder(hadoopConf).numDataNodes(2).format(true).build();
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(cmRootDirectory, true);
    compactorTestCleanup();
    ms.dropDatabase(dbName, true, true, true);
  }

  @AfterClass
  public static void tearDownClass() {
    miniDFSCluster.shutdown();
  }

  @Test
  public void cleanupAfterMajorTableCompaction() throws Exception {
    Table t = newTable(dbName, "camtc", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    addBaseFile(t, null, 25L, 25);

    burnThroughTransactions(dbName, "camtc", 25);

    CompactionRequest rqst = new CompactionRequest(dbName, "camtc", CompactionType.MAJOR);
    compactInTxn(rqst);

    assertCleanerActions(6);
  }

  @Test
  public void cleanupAfterMajorPartitionCompaction() throws Exception {
    Table t = newTable(dbName, "campc", true);
    Partition p = newPartition(t, "today");

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);
    addBaseFile(t, p, 25L, 25);

    burnThroughTransactions(dbName, "campc", 25);

    CompactionRequest rqst = new CompactionRequest(dbName, "campc", CompactionType.MAJOR);
    rqst.setPartitionname("ds=today");
    compactInTxn(rqst);

    assertCleanerActions(6);
  }

  @Test
  public void cleanupAfterMinorTableCompaction() throws Exception {
    Table t = newTable(dbName, "camitc", false);

    addBaseFile(t, null, 20L, 20);
    addDeltaFile(t, null, 21L, 22L, 2);
    addDeltaFile(t, null, 23L, 24L, 2);
    addDeltaFile(t, null, 21L, 24L, 4);

    burnThroughTransactions(dbName, "camitc", 24);

    CompactionRequest rqst = new CompactionRequest(dbName, "camitc", CompactionType.MINOR);
    compactInTxn(rqst);

    assertCleanerActions(4);
  }

  @Test
  public void cleanupAfterMinorPartitionCompaction() throws Exception {
    Table t = newTable(dbName, "camipc", true);
    Partition p = newPartition(t, "today");

    addBaseFile(t, p, 20L, 20);
    addDeltaFile(t, p, 21L, 22L, 2);
    addDeltaFile(t, p, 23L, 24L, 2);
    addDeltaFile(t, p, 21L, 24L, 4);

    burnThroughTransactions(dbName, "camipc", 24);

    CompactionRequest rqst = new CompactionRequest(dbName, "camipc", CompactionType.MINOR);
    rqst.setPartitionname("ds=today");
    compactInTxn(rqst);

    assertCleanerActions(4);
  }

  private void assertCleanerActions(int expectedNumOCleanedFiles) throws Exception {
    assertEquals("there should be no deleted files in cm root", 0,
        fs.listStatus(cmRootDirectory).length);

    startCleaner();

    ShowCompactResponse rsp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals(1, rsp.getCompactsSize());
    String state = rsp.getCompacts().get(0).getState();
    Assert.assertTrue("unexpected state " + state, TxnStore.SUCCEEDED_RESPONSE.equals(state));

    assertEquals(
        "there should be " + String.valueOf(expectedNumOCleanedFiles) + " deleted files in cm root",
        expectedNumOCleanedFiles, fs.listStatus(cmRootDirectory).length
    );
  }

  @Override
  boolean useHive130DeltaDirName() {
    return false;
  }
}
