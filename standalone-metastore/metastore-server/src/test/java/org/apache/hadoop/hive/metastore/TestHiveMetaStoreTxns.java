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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Unit tests for {@link org.apache.hadoop.hive.metastore.HiveMetaStoreClient}.  For now this just has
 * transaction and locking tests.  The goal here is not to test all
 * functionality possible through the interface, as all permutations of DB
 * operations should be tested in the appropriate DB handler classes.  The
 * goal is to test that we can properly pass the messages through the thrift
 * service.
 *
 * This is in the ql directory rather than the metastore directory because it
 * required the hive-exec jar, and hive-exec jar already depends on
 * hive-metastore jar, thus I can't make hive-metastore depend on hive-exec.
 */
@Category(MetastoreUnitTest.class)
public class TestHiveMetaStoreTxns {

  private final Configuration conf = MetastoreConf.newMetastoreConf();
  private IMetaStoreClient client;
  private Connection conn;

  @Test
  public void testTxns() throws Exception {
    List<Long> tids = client.openTxns("me", 3).getTxn_ids();
    Assert.assertEquals(1L, (long) tids.get(0));
    Assert.assertEquals(2L, (long) tids.get(1));
    Assert.assertEquals(3L, (long) tids.get(2));
    client.rollbackTxn(1);
    client.commitTxn(2);
    ValidTxnList validTxns = client.getValidTxns();
    Assert.assertFalse(validTxns.isTxnValid(1));
    Assert.assertTrue(validTxns.isTxnValid(2));
    Assert.assertFalse(validTxns.isTxnValid(3));
    Assert.assertFalse(validTxns.isTxnValid(4));
  }

  @Test
  public void testOpenTxnNotExcluded() throws Exception {
    List<Long> tids = client.openTxns("me", 3).getTxn_ids();
    Assert.assertEquals(1L, (long) tids.get(0));
    Assert.assertEquals(2L, (long) tids.get(1));
    Assert.assertEquals(3L, (long) tids.get(2));
    client.rollbackTxn(1);
    client.commitTxn(2);
    ValidTxnList validTxns = client.getValidTxns(3);
    Assert.assertFalse(validTxns.isTxnValid(1));
    Assert.assertTrue(validTxns.isTxnValid(2));
    Assert.assertTrue(validTxns.isTxnValid(3));
    Assert.assertFalse(validTxns.isTxnValid(4));
  }

  @Test
  public void testTxNWithKeyValue() throws Exception {
    Statement stm = conn.createStatement();

    String dbName = "mydbKeyValue";
    String tblName = "mytable";

    Database db = new DatabaseBuilder().setName(dbName).build(conf);
    db.unsetCatalogName();
    Table tbl = new TableBuilder().setDbName(dbName).setTableName(tblName)
        .addCol("id", "int").addCol("name", "string")
        .setType(TableType.MANAGED_TABLE.name()).build(conf);

    try {
      client.createDatabase(db);
      client.createTable(tbl);
      tbl = client.getTable(dbName, tblName);

      stm.executeUpdate(
          "INSERT INTO TABLE_PARAMS(TBL_ID, PARAM_KEY)" + " VALUES(" + tbl.getId() + String.format(", '%smykey')", TxnStore.TXN_KEY_START));

      List<Long> tids = client.openTxns("me", 1).getTxn_ids();
      Assert.assertEquals(1L, (long) tids.get(0));
      client.commitTxnWithKeyValue(1, tbl.getId(), TxnStore.TXN_KEY_START + "mykey", "myvalue");
      ValidTxnList validTxns = client.getValidTxns(1);
      Assert.assertTrue(validTxns.isTxnValid(1));

      ResultSet rs = stm.executeQuery("SELECT TBL_ID, PARAM_KEY, PARAM_VALUE"
          + " FROM TABLE_PARAMS WHERE TBL_ID = " + tbl.getId());

      Assert.assertTrue(rs.next());
      Assert.assertEquals(rs.getLong(1), tbl.getId());
      Assert.assertEquals(rs.getString(2),  TxnStore.TXN_KEY_START + "mykey");
      Assert.assertEquals(rs.getString(3), "myvalue");
    } finally {
      client.dropTable(dbName, tblName);
      client.dropDatabase(dbName);
      stm.execute("DELETE FROM TABLE_PARAMS WHERE TBL_ID = " + tbl.getId() + String.format(
          " AND PARAM_KEY = '%smykey'", TxnStore.TXN_KEY_START));
    }
  }

  @Test
  public void testTxNWithKeyValueNoTableId() throws Exception {
    List<Long> tids = client.openTxns("me", 1).getTxn_ids();
    Assert.assertEquals(1L, (long) tids.get(0));
    try {
      client.commitTxnWithKeyValue(1, 10, TxnStore.TXN_KEY_START + "mykey",
          "myvalue");
      Assert.fail("Should have raised exception");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("key=" + TxnStore.TXN_KEY_START + "mykey"));
      Assert.assertTrue(e.getMessage().contains("value=myvalue"));
      Assert.assertTrue(e.getMessage().contains("Only one row should have been affected but"));
    }
    ValidTxnList validTxns = client.getValidTxns(1);
    Assert.assertTrue(validTxns.isTxnValid(1));
  }

  @Test
  public void testTxNWithKeyWrongPrefix() throws Exception {
    String dbName = "mydbKeyValueWrongPrefix";
    String tblName = "mytable";
    List<Long> tids = client.openTxns("me", 1).getTxn_ids();
    Assert.assertEquals(1L, (long) tids.get(0));
    try {
      Database db = new DatabaseBuilder().setName(dbName).build(conf);
      db.unsetCatalogName();
      client.createDatabase(db);

      Table tbl = new TableBuilder().setDbName(dbName).setTableName(tblName)
          .addCol("id", "int").addCol("name", "string")
          .setType(TableType.MANAGED_TABLE.name()).build(conf);
      client.createTable(tbl);
      tbl = client.getTable(dbName, tblName);

      client.commitTxnWithKeyValue(1, tbl.getId(), "mykey",
          "myvalue");
      Assert.fail("Should have raised exception");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("key=mykey"));
      Assert.assertTrue(e.getMessage().contains("value=myvalue"));
      Assert.assertTrue(e.getMessage().contains("key should start with"));
    } finally {
      client.dropTable(dbName, tblName);
      client.dropDatabase(dbName);
    }
    ValidTxnList validTxns = client.getValidTxns(1);
    Assert.assertTrue(validTxns.isTxnValid(1));
  }

  @Test
  public void testLocks() throws Exception {
    LockRequestBuilder rqstBuilder = new LockRequestBuilder();
    rqstBuilder.addLockComponent(new LockComponentBuilder()
        .setDbName("mydb")
        .setTableName("mytable")
        .setPartitionName("mypartition")
        .setExclusive()
        .setOperationType(DataOperationType.NO_TXN)
        .build());
    rqstBuilder.addLockComponent(new LockComponentBuilder()
        .setDbName("mydb")
        .setTableName("yourtable")
        .setSemiShared()
        .setOperationType(DataOperationType.NO_TXN)
        .build());
    rqstBuilder.addLockComponent(new LockComponentBuilder()
        .setDbName("yourdb")
        .setOperationType(DataOperationType.NO_TXN)
        .setShared()
        .build());
    rqstBuilder.setUser("fred");

    LockResponse res = client.lock(rqstBuilder.build());
    Assert.assertEquals(1L, res.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    res = client.checkLock(1);
    Assert.assertEquals(1L, res.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    client.heartbeat(0, 1);

    client.unlock(1);
  }

  @Test
  public void testLocksWithTxn() throws Exception {
    long txnid = client.openTxn("me");

    LockRequestBuilder rqstBuilder = new LockRequestBuilder();
    rqstBuilder.setTransactionId(txnid)
      .addLockComponent(new LockComponentBuilder()
        .setDbName("mydb")
        .setTableName("mytable")
        .setPartitionName("mypartition")
        .setSemiShared()
        .setOperationType(DataOperationType.UPDATE)
        .build())
      .addLockComponent(new LockComponentBuilder()
        .setDbName("mydb")
        .setTableName("yourtable")
        .setSemiShared()
        .setOperationType(DataOperationType.UPDATE)
        .build())
      .addLockComponent(new LockComponentBuilder()
        .setDbName("yourdb")
        .setShared()
        .setOperationType(DataOperationType.SELECT)
        .build())
      .setUser("fred");

    LockResponse res = client.lock(rqstBuilder.build());
    Assert.assertEquals(1L, res.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    res = client.checkLock(1);
    Assert.assertEquals(1L, res.getLockid());
    Assert.assertEquals(LockState.ACQUIRED, res.getState());

    client.heartbeat(txnid, 1);

    client.commitTxn(txnid);
  }

  @Test
  public void stringifyValidTxns() throws Exception {
    // Test with just high water mark
    ValidTxnList validTxns = new ValidReadTxnList("1:" + Long.MAX_VALUE + "::");
    String asString = validTxns.toString();
    Assert.assertEquals("1:" + Long.MAX_VALUE + "::", asString);
    validTxns = new ValidReadTxnList(asString);
    Assert.assertEquals(1, validTxns.getHighWatermark());
    Assert.assertNotNull(validTxns.getInvalidTransactions());
    Assert.assertEquals(0, validTxns.getInvalidTransactions().length);
    asString = validTxns.toString();
    Assert.assertEquals("1:" + Long.MAX_VALUE + "::", asString);
    validTxns = new ValidReadTxnList(asString);
    Assert.assertEquals(1, validTxns.getHighWatermark());
    Assert.assertNotNull(validTxns.getInvalidTransactions());
    Assert.assertEquals(0, validTxns.getInvalidTransactions().length);

    // Test with open transactions
    validTxns = new ValidReadTxnList("10:3:5:3");
    asString = validTxns.toString();
    if (!asString.equals("10:3:3:5") && !asString.equals("10:3:5:3")) {
      Assert.fail("Unexpected string value " + asString);
    }
    validTxns = new ValidReadTxnList(asString);
    Assert.assertEquals(10, validTxns.getHighWatermark());
    Assert.assertNotNull(validTxns.getInvalidTransactions());
    Assert.assertEquals(2, validTxns.getInvalidTransactions().length);
    boolean sawThree = false, sawFive = false;
    for (long tid : validTxns.getInvalidTransactions()) {
      if (tid == 3)  sawThree = true;
      else if (tid == 5) sawFive = true;
      else  Assert.fail("Unexpected value " + tid);
    }
    Assert.assertTrue(sawThree);
    Assert.assertTrue(sawFive);
  }

  @Test
  public void testOpenTxnWithType() throws Exception {
    long txnId = client.openTxn("me", TxnType.DEFAULT);
    client.commitTxn(txnId);
    ValidTxnList validTxns = client.getValidTxns();
    Assert.assertTrue(validTxns.isTxnValid(txnId));
  }

  @Before
  public void setUp() throws Exception {
    conf.setBoolean(ConfVars.HIVE_IN_TEST.getVarname(), true);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    TxnDbUtil.setConfValues(conf);
    TxnDbUtil.prepDb(conf);
    client = new HiveMetaStoreClient(conf);
    String connectionStr = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY);

    conn = DriverManager.getConnection(connectionStr);
  }

  @After
  public void tearDown() throws Exception {
    conn.close();
    TxnDbUtil.cleanDb(conf);
  }
}
