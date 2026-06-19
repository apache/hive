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

package org.apache.hadoop.hive.metastore.client;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetAllWriteEventInfoRequest;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.metastore.model.MTxnWriteNotificationLog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jdo.PersistenceManager;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for API getAllWriteEventInfo.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestGetAllWriteEventInfo extends MetaStoreClientTest {
  private final AbstractMetaStoreService metaStore;
  private ObjectStore objectStore = null;
  private IMetaStoreClient client = null;
  private MTxnWriteNotificationLog notificationLog = null;

  private static final String DB_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";
  private static final long TXN_ID = 1;
  private static final long WRITE_ID = 1;

  public TestGetAllWriteEventInfo(String name, AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
  }

  @Before
  public void setUp() throws Exception {
    client = metaStore.getClient();
    if (objectStore == null) {
      objectStore = new ObjectStore();
      objectStore.setConf(metaStore.getConf());
    }

    // Clean up the database
    client.dropDatabase(DB_NAME, true, true, true);
    metaStore.cleanWarehouseDirs();

    createDB(DB_NAME);
    createTable(DB_NAME, TABLE_NAME);
    notificationLog = insertTxnWriteNotificationLog(TXN_ID, WRITE_ID, DB_NAME, TABLE_NAME);
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (client != null) {
        client.close();
      }
    } catch (Exception e) {
      // Ignore exceptions
    } finally {
      client = null;
    }
    try {
      if (notificationLog != null) {
        objectStore.getPersistenceManager().deletePersistent(notificationLog);
      }
    } catch (Exception e) {
      // Ignore exceptions
    } finally {
      notificationLog = null;
    }
  }

  private void createDB(String dbName) throws Exception {
    new DatabaseBuilder()
        .setName(dbName)
        .create(client, metaStore.getConf());
  }

  private void createTable(String dbName, String tableName) throws Exception {
    List<FieldSchema> cols = new ArrayList<>();
    cols.add(new FieldSchema("id", "int", "test col id"));
    cols.add(new FieldSchema("value", "string", "test col value"));

    new TableBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .setType(TableType.MANAGED_TABLE.name())
        .setCols(cols)
        .create(client, metaStore.getConf());
  }

  private MTxnWriteNotificationLog insertTxnWriteNotificationLog(
      long txnId, long writeId, String dbName, String tableName) {
    // We use objectStore to insert record directly into DB so that we don't need DbNotificationListener,
    // which will introduce cyclic dependency.
    PersistenceManager pm = objectStore.getPersistenceManager();
    MTxnWriteNotificationLog log = new MTxnWriteNotificationLog(txnId, writeId, 1, dbName, tableName, "", "", "", "");
    return pm.makePersistent(log);
  }

  @Test
  public void testGetByTxnId() throws Exception {
    GetAllWriteEventInfoRequest req = new GetAllWriteEventInfoRequest();
    req.setTxnId(TXN_ID);
    List<WriteEventInfo> writeEventInfoList = client.getAllWriteEventInfo(req);
    Assert.assertEquals(1, writeEventInfoList.size());
    WriteEventInfo writeEventInfo = writeEventInfoList.get(0);
    Assert.assertEquals(TXN_ID, writeEventInfo.getWriteId());
    Assert.assertEquals(DB_NAME, writeEventInfo.getDatabase());
    Assert.assertEquals(TABLE_NAME, writeEventInfo.getTable());
  }

  @Test
  public void testGetByTxnIdAndTableName() throws Exception {
    GetAllWriteEventInfoRequest req = new GetAllWriteEventInfoRequest();
    req.setTxnId(TXN_ID);
    req.setDbName(DB_NAME);
    req.setTableName(TABLE_NAME);
    List<WriteEventInfo> writeEventInfoList = client.getAllWriteEventInfo(req);
    Assert.assertEquals(1, writeEventInfoList.size());
    WriteEventInfo writeEventInfo = writeEventInfoList.get(0);
    Assert.assertEquals(TXN_ID, writeEventInfo.getWriteId());
    Assert.assertEquals(DB_NAME, writeEventInfo.getDatabase());
    Assert.assertEquals(TABLE_NAME, writeEventInfo.getTable());
  }

  @Test
  public void testGetByWrongTxnId() throws Exception {
    GetAllWriteEventInfoRequest req = new GetAllWriteEventInfoRequest();
    req.setTxnId(-1);
    List<WriteEventInfo> writeEventInfoList = client.getAllWriteEventInfo(req);
    Assert.assertTrue(writeEventInfoList.isEmpty());
  }

  @Test
  public void testGetByWrongDB() throws Exception {
    GetAllWriteEventInfoRequest req = new GetAllWriteEventInfoRequest();
    req.setTxnId(TXN_ID);
    req.setDbName("wrong_db");
    List<WriteEventInfo> writeEventInfoList = client.getAllWriteEventInfo(req);
    Assert.assertTrue(writeEventInfoList.isEmpty());
  }

  @Test
  public void testGetByWrongTable() throws Exception {
    GetAllWriteEventInfoRequest req = new GetAllWriteEventInfoRequest();
    req.setTxnId(TXN_ID);
    req.setDbName(DB_NAME);
    req.setTableName("wrong_table");
    List<WriteEventInfo> writeEventInfoList = client.getAllWriteEventInfo(req);
    Assert.assertTrue(writeEventInfoList.isEmpty());
  }
}
