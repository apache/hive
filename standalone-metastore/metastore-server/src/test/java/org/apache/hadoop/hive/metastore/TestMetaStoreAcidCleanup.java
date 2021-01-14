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
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.ThrowingTxnHandler;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.fail;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.TXN_STORE_IMPL;

@Category(MetastoreUnitTest.class)
public class TestMetaStoreAcidCleanup {
  protected static HiveMetaStoreClient client;
  protected static Configuration conf;
  protected static Warehouse warehouse;
  private static final String dbName = "db";
  private String tableName;

  @BeforeClass
  public static void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setClass(conf, TXN_STORE_IMPL, ThrowingTxnHandler.class, TxnStore.class);
    conf.set("hive.metastore.client.capabilities", "HIVEMANAGEDINSERTWRITE,HIVEMANAGESTATS,HIVECACHEINVALIDATE,CONNECTORWRITE");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    warehouse = new Warehouse(conf);
    client = new HiveMetaStoreClient(conf);
    Database db = new DatabaseBuilder()
            .setName(dbName)
            .build(conf);
    client.createDatabase(db);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      ThrowingTxnHandler.doThrow = false;
      client.dropDatabase(dbName);
      client.close();
    } catch (Throwable e) {
      System.err.println("Unable to close metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw e;
    }
  }

  @After
  public void afterTest() throws TException {
    client.dropTable(dbName, tableName);
  }

  @Test
  public void testDropTableShouldRollback_whenAcidCleanupFails() throws Exception {
    tableName = "tableDropTable";

    Table table;
    createTable(dbName, tableName);

    ThrowingTxnHandler.doThrow = true;
    try {
      client.dropTable(dbName, tableName);
    } catch (Exception ex) {
      if (!ex.getMessage().contains("during transactional cleanup")) {
        fail("dropTable failed with unexpected exception: " + ex);
      }
    } finally {
      ThrowingTxnHandler.doThrow = false;
    }

    table = client.getTable(dbName, tableName);
    assertNotNull(table);
  }

  @Test
  public void testDropDatabaseShouldRollback_whenAcidCleanupFails() throws Exception {
    tableName = "tableDropDatabase";

    Table table;
    createTable(dbName, tableName);

    ThrowingTxnHandler.doThrow = true;
    try {
      client.dropDatabase(dbName, true, false, true);
    } catch (Exception ex) {
      if (!ex.getMessage().contains("during transactional cleanup")) {
        fail("dropTable failed with unexpected exception: " + ex);
      }
    } finally {
      ThrowingTxnHandler.doThrow = false;
    }

    table = client.getTable(dbName, tableName);
    assertNotNull(table);
  }

  private void createTable(String dbName, String tableName) throws TException {
    new TableBuilder()
            .setDbName(dbName)
            .setTableName(tableName)
            .addCol("foo", "string")
            .addCol("bar", "string")
            .addTableParam(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true")
            .addTableParam(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES, "insert_only")
            .create(client, conf);
  }
}