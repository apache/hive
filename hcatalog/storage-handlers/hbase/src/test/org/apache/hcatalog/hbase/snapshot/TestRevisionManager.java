/**
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.hbase.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hcatalog.hbase.SkeletonHBaseTest;
import org.apache.hcatalog.hbase.snapshot.transaction.thrift.StoreFamilyRevision;
import org.apache.hcatalog.hbase.snapshot.transaction.thrift.StoreFamilyRevisionList;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRevisionManager extends SkeletonHBaseTest {

  @BeforeClass
  public static void setup() throws Throwable {
    setupSkeletonHBaseTest();
  }

  @Test
  public void testBasicZNodeCreation() throws IOException, KeeperException, InterruptedException {

    int port = getHbaseConf().getInt("hbase.zookeeper.property.clientPort", 2181);
    String servers = getHbaseConf().get("hbase.zookeeper.quorum");
    String[] splits = servers.split(",");
    StringBuffer sb = new StringBuffer();
    for (String split : splits) {
      sb.append(split);
      sb.append(':');
      sb.append(port);
    }

    ZKUtil zkutil = new ZKUtil(sb.toString(), "/rm_base");
    String tableName = newTableName("testTable");
    List<String> columnFamilies = Arrays.asList("cf001", "cf002", "cf003");

    zkutil.createRootZNodes();
    ZooKeeper zk = zkutil.getSession();
    Stat tempTwo = zk.exists("/rm_base" + PathUtil.DATA_DIR, false);
    assertTrue(tempTwo != null);
    Stat tempThree = zk.exists("/rm_base" + PathUtil.CLOCK_NODE, false);
    assertTrue(tempThree != null);

    zkutil.setUpZnodesForTable(tableName, columnFamilies);
    String transactionDataTablePath = "/rm_base" + PathUtil.DATA_DIR + "/" + tableName;
    Stat result = zk.exists(transactionDataTablePath, false);
    assertTrue(result != null);

    for (String colFamiliy : columnFamilies) {
      String cfPath = transactionDataTablePath + "/" + colFamiliy;
      Stat resultTwo = zk.exists(cfPath, false);
      assertTrue(resultTwo != null);
    }

  }

  @Test
  public void testCommitTransaction() throws IOException {

    int port = getHbaseConf().getInt("hbase.zookeeper.property.clientPort", 2181);
    String servers = getHbaseConf().get("hbase.zookeeper.quorum");
    String[] splits = servers.split(",");
    StringBuffer sb = new StringBuffer();
    for (String split : splits) {
      sb.append(split);
      sb.append(':');
      sb.append(port);
    }

    Configuration conf = RevisionManagerConfiguration.create(getHbaseConf());
    conf.set(RMConstants.ZOOKEEPER_DATADIR, "/rm_base");
    ZKBasedRevisionManager manager = new ZKBasedRevisionManager();
    manager.initialize(conf);
    manager.open();
    ZKUtil zkutil = new ZKUtil(sb.toString(), "/rm_base");

    String tableName = newTableName("testTable");
    List<String> columnFamilies = Arrays.asList("cf1", "cf2", "cf3");
    Transaction txn = manager.beginWriteTransaction(tableName,
      columnFamilies);

    List<String> cfs = zkutil.getColumnFamiliesOfTable(tableName);
    assertTrue(cfs.size() == columnFamilies.size());
    for (String cf : cfs) {
      assertTrue(columnFamilies.contains(cf));
    }

    for (String colFamily : columnFamilies) {
      String path = PathUtil.getRunningTxnInfoPath("/rm_base", tableName, colFamily);
      byte[] data = zkutil.getRawData(path, null);
      StoreFamilyRevisionList list = new StoreFamilyRevisionList();
      ZKUtil.deserialize(list, data);
      assertEquals(list.getRevisionListSize(), 1);
      StoreFamilyRevision lightTxn = list.getRevisionList().get(0);
      assertEquals(lightTxn.timestamp, txn.getTransactionExpireTimeStamp());
      assertEquals(lightTxn.revision, txn.getRevisionNumber());

    }
    manager.commitWriteTransaction(txn);
    for (String colFamiliy : columnFamilies) {
      String path = PathUtil.getRunningTxnInfoPath("/rm_base", tableName, colFamiliy);
      byte[] data = zkutil.getRawData(path, null);
      StoreFamilyRevisionList list = new StoreFamilyRevisionList();
      ZKUtil.deserialize(list, data);
      assertEquals(list.getRevisionListSize(), 0);

    }

    manager.close();
  }

  @Test
  public void testAbortTransaction() throws IOException {

    int port = getHbaseConf().getInt("hbase.zookeeper.property.clientPort", 2181);
    String host = getHbaseConf().get("hbase.zookeeper.quorum");
    Configuration conf = RevisionManagerConfiguration.create(getHbaseConf());
    conf.set(RMConstants.ZOOKEEPER_DATADIR, "/rm_base");
    ZKBasedRevisionManager manager = new ZKBasedRevisionManager();
    manager.initialize(conf);
    manager.open();
    ZKUtil zkutil = new ZKUtil(host + ':' + port, "/rm_base");

    String tableName = newTableName("testTable");
    List<String> columnFamilies = Arrays.asList("cf1", "cf2", "cf3");
    Transaction txn = manager.beginWriteTransaction(tableName, columnFamilies);
    List<String> cfs = zkutil.getColumnFamiliesOfTable(tableName);

    assertTrue(cfs.size() == columnFamilies.size());
    for (String cf : cfs) {
      assertTrue(columnFamilies.contains(cf));
    }

    for (String colFamiliy : columnFamilies) {
      String path = PathUtil.getRunningTxnInfoPath("/rm_base", tableName, colFamiliy);
      byte[] data = zkutil.getRawData(path, null);
      StoreFamilyRevisionList list = new StoreFamilyRevisionList();
      ZKUtil.deserialize(list, data);
      assertEquals(list.getRevisionListSize(), 1);
      StoreFamilyRevision lightTxn = list.getRevisionList().get(0);
      assertEquals(lightTxn.timestamp, txn.getTransactionExpireTimeStamp());
      assertEquals(lightTxn.revision, txn.getRevisionNumber());

    }
    manager.abortWriteTransaction(txn);
    for (String colFamiliy : columnFamilies) {
      String path = PathUtil.getRunningTxnInfoPath("/rm_base", tableName, colFamiliy);
      byte[] data = zkutil.getRawData(path, null);
      StoreFamilyRevisionList list = new StoreFamilyRevisionList();
      ZKUtil.deserialize(list, data);
      assertEquals(list.getRevisionListSize(), 0);

    }

    for (String colFamiliy : columnFamilies) {
      String path = PathUtil.getAbortInformationPath("/rm_base", tableName, colFamiliy);
      byte[] data = zkutil.getRawData(path, null);
      StoreFamilyRevisionList list = new StoreFamilyRevisionList();
      ZKUtil.deserialize(list, data);
      assertEquals(list.getRevisionListSize(), 1);
      StoreFamilyRevision abortedTxn = list.getRevisionList().get(0);
      assertEquals(abortedTxn.getRevision(), txn.getRevisionNumber());
    }
    manager.close();
  }

  @Test
  public void testKeepAliveTransaction() throws InterruptedException, IOException {

    int port = getHbaseConf().getInt("hbase.zookeeper.property.clientPort", 2181);
    String servers = getHbaseConf().get("hbase.zookeeper.quorum");
    String[] splits = servers.split(",");
    StringBuffer sb = new StringBuffer();
    for (String split : splits) {
      sb.append(split);
      sb.append(':');
      sb.append(port);
    }

    Configuration conf = RevisionManagerConfiguration.create(getHbaseConf());
    conf.set(RMConstants.ZOOKEEPER_DATADIR, "/rm_base");
    ZKBasedRevisionManager manager = new ZKBasedRevisionManager();
    manager.initialize(conf);
    manager.open();
    String tableName = newTableName("testTable");
    List<String> columnFamilies = Arrays.asList("cf1", "cf2");
    Transaction txn = manager.beginWriteTransaction(tableName,
      columnFamilies, 40L);
    Thread.sleep(100);
    try {
      manager.commitWriteTransaction(txn);
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
      assertEquals(e.getMessage(),
        "The transaction to be removed not found in the data.");
    }

  }

  @Test
  public void testCreateSnapshot() throws IOException {
    int port = getHbaseConf().getInt("hbase.zookeeper.property.clientPort", 2181);
    String host = getHbaseConf().get("hbase.zookeeper.quorum");
    Configuration conf = RevisionManagerConfiguration.create(getHbaseConf());
    conf.set(RMConstants.ZOOKEEPER_DATADIR, "/rm_base");
    ZKBasedRevisionManager manager = new ZKBasedRevisionManager();
    manager.initialize(conf);
    manager.open();
    String tableName = newTableName("testTable");
    List<String> cfOne = Arrays.asList("cf1", "cf2");
    List<String> cfTwo = Arrays.asList("cf2", "cf3");
    Transaction tsx1 = manager.beginWriteTransaction(tableName, cfOne);
    Transaction tsx2 = manager.beginWriteTransaction(tableName, cfTwo);
    TableSnapshot snapshotOne = manager.createSnapshot(tableName);
    assertEquals(snapshotOne.getRevision("cf1"), 0);
    assertEquals(snapshotOne.getRevision("cf2"), 0);
    assertEquals(snapshotOne.getRevision("cf3"), 1);

    List<String> cfThree = Arrays.asList("cf1", "cf3");
    Transaction tsx3 = manager.beginWriteTransaction(tableName, cfThree);
    manager.commitWriteTransaction(tsx1);
    TableSnapshot snapshotTwo = manager.createSnapshot(tableName);
    assertEquals(snapshotTwo.getRevision("cf1"), 2);
    assertEquals(snapshotTwo.getRevision("cf2"), 1);
    assertEquals(snapshotTwo.getRevision("cf3"), 1);

    manager.commitWriteTransaction(tsx2);
    TableSnapshot snapshotThree = manager.createSnapshot(tableName);
    assertEquals(snapshotThree.getRevision("cf1"), 2);
    assertEquals(snapshotThree.getRevision("cf2"), 3);
    assertEquals(snapshotThree.getRevision("cf3"), 2);
    manager.commitWriteTransaction(tsx3);
    TableSnapshot snapshotFour = manager.createSnapshot(tableName);
    assertEquals(snapshotFour.getRevision("cf1"), 3);
    assertEquals(snapshotFour.getRevision("cf2"), 3);
    assertEquals(snapshotFour.getRevision("cf3"), 3);

  }


}
