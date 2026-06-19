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

package org.apache.hadoop.hive.metastore.handler;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.MetaStoreClientTest;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.thrift.TException;
import org.datanucleus.exceptions.NucleusException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestRetryOnException extends MetaStoreClientTest {
  protected static final String DB_NAME = "testdroptable";
  protected static final String TABLE_NAME = "test_drop_table";
  private final String testTempDir =
      Paths.get(System.getProperty("java.io.tmpdir"), "testDropTable").toString();
  private AbstractMetaStoreService metaStore;
  private HiveMetaStoreClient client;

  public TestRetryOnException(String name, AbstractMetaStoreService metaStore) {
    this.metaStore = metaStore;
  }

  @BeforeClass
  public static void startMetaStores() {
    Map<MetastoreConf.ConfVars, String> msConf = new HashMap<>();
    Map<String, String> extraConf = new HashMap<>();
    extraConf.put("metastore.transactional.event.listeners", AssertExTransactionListener.class.getName());
    startMetaStores(msConf, extraConf);
  }

  @Before
  public void setUp() throws Exception {
    AssertExTransactionListener.resetTest();
    client = metaStore.getClient();
    cleanDB();
    createDB(DB_NAME);
    AssertExTransactionListener.startTest();
  }

  @Test
  public void testRetryOnException() throws Exception {
    String location = metaStore.getExternalWarehouseRoot() + "/" + TABLE_NAME;
    new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol("test_id", "int", "test col id")
        .addCol("test_value", "string", "test col value")
        .addTableParam("param_key", "param_value")
        .setType(TableType.EXTERNAL_TABLE.name())
        .addTableParam("EXTERNAL", "TRUE")
        .addStorageDescriptorParam("sd_param_key", "sd_param_value")
        .setSerdeName(TABLE_NAME)
        .setStoredAsSubDirectories(false)
        .addSerdeParam("serde_param_key", "serde_param_value")
        .setLocation(location)
        .create(client, metaStore.getConf());

    Table table = client.getTable(DB_NAME, TABLE_NAME);
    assertNotNull(table);
    String tableName = TableName.getQualified(table.getCatName(), table.getDbName(), table.getTableName());

    assertEquals(0, AssertExTransactionListener.getCalls(tableName));
    client.dropTable(DB_NAME, TABLE_NAME);
    assertEquals(AssertExTransactionListener.maxRetries, AssertExTransactionListener.getCalls(tableName));
    try {
      client.getTable(DB_NAME, TABLE_NAME);
      fail("Table " + tableName + " should be dropped");
    } catch (NoSuchObjectException nse) {
      // expected
    }

    assertEquals(0, AssertExTransactionListener.getCalls(DB_NAME));
    client.dropDatabase(DB_NAME);
    assertEquals(AssertExTransactionListener.maxRetries, AssertExTransactionListener.getCalls(tableName));
    try {
      client.getDatabase(DB_NAME);
      fail("Database " + DB_NAME + " should be dropped");
    } catch (NoSuchObjectException nse) {
      // expected
    }
  }

  public static class AssertExTransactionListener extends TransactionalMetaStoreEventListener {
    static Map<String, Integer> calls = new HashMap<>();
    static int maxRetries = 5;
    static boolean startChecking = false;

    public AssertExTransactionListener(Configuration config) {
      super(config);
    }

    @Override
    public void onDropTable(DropTableEvent tableEvent) throws MetaException {
      Table table = tableEvent.getTable();
      String tableName = TableName.getQualified(table.getCatName(), table.getDbName(), table.getTableName());
      Integer pre = calls.putIfAbsent(tableName, 1);
      if (pre != null) {
        calls.put(tableName, pre + 1);
      }
      // For a drop table call, we should see 5(maxRetries) retries in RetryingHMSHandler
      assertException(startChecking && calls.get(tableName) < maxRetries);
    }

    @Override
    public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
      String dbName = dbEvent.getDatabase().getName();
      Integer pre = calls.putIfAbsent(dbName, 1);
      if (pre != null) {
        calls.put(dbName, pre + 1);
      }
      assertException(startChecking && calls.get(dbName) < maxRetries);
    }

    private void assertException(boolean throwException) throws MetaException {
      if (throwException) {
        MetaException exception = new MetaException("An exception for RetryingHMSHandler to retry");
        exception.initCause(new NucleusException("Non-fatal exception"));
        throw exception;
      }
    }

    static void startTest() {
      startChecking = true;
      calls.clear();
    }

    static void resetTest() {
      startChecking = false;
      calls.clear();
    }

    static int getCalls(String key) {
      return calls.get(key) != null ? calls.get(key) : 0;
    }
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (client != null) {
        try {
          client.close();
        } catch (Exception e) {
        }
      }
    } finally {
      client = null;
    }

    Path path = new Path(testTempDir);
    path.getFileSystem(metaStore.getConf()).delete(path, true);
  }

  protected void cleanDB() throws Exception{
    client.dropDatabase(DB_NAME, true, true, true);
    metaStore.cleanWarehouseDirs();
  }

  protected void createDB(String dbName) throws TException {
    new DatabaseBuilder().
        setName(dbName).
        create(client, metaStore.getConf());
  }
}
