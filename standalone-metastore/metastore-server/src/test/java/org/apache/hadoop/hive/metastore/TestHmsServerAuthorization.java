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

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.util.List;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;

/**
 * Test the filtering behavior at HMS client and HMS server. The configuration at each test
 * changes, and therefore HMS client and server are created for each test case
 */
@Category(MetastoreUnitTest.class)
public class TestHmsServerAuthorization {

  /**
   * Implementation of MetaStorePreEventListener that throws MetaException when configured in
   * its function onEvent().
   */
  public static class DummyAuthorizationListenerImpl extends MetaStorePreEventListener {
    private static volatile boolean throwExceptionAtCall = false;
    public DummyAuthorizationListenerImpl(Configuration config) {
      super(config);
    }

    @Override
    public void onEvent(PreEventContext context)
        throws MetaException, NoSuchObjectException, InvalidOperationException {
      if (throwExceptionAtCall) {
        throw new MetaException("Authorization fails");
      }
    }
  }

  private static HiveMetaStoreClient client;
  private static Configuration conf;

  private static final int DEFAULT_LIMIT_PARTITION_REQUEST = 100;

  private static String dbName1 = "testdb1";
  private static String dbName2 = "testdb2";
  private static final String TAB1 = "tab1";
  private static final String TAB2 = "tab2";


  protected static HiveMetaStoreClient createClient(Configuration metaStoreConf) throws Exception {
    try {
      return new HiveMetaStoreClient(metaStoreConf);
    } catch (Throwable e) {
      System.err.println("Unable to open the metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }

  @BeforeClass
  public static void setUpForTest() throws Exception {

    // make sure env setup works
    TestHmsServerAuthorization.DummyAuthorizationListenerImpl.throwExceptionAtCall = false;

    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setLongVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES, 3);
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    MetastoreConf.setClass(conf, ConfVars.PRE_EVENT_LISTENERS, DummyAuthorizationListenerImpl.class,
        MetaStorePreEventListener.class);
    MetastoreConf.setBoolVar(conf, ConfVars.METRICS_ENABLED, true);
    conf.set("hive.key1", "value1");
    conf.set("hive.key2", "http://www.example.com");
    conf.set("hive.key3", "");
    conf.set("hive.key4", "0");
    conf.set("datanucleus.autoCreateTables", "false");
    conf.set("hive.in.test", "true");

    MetastoreConf.setLongVar(conf, ConfVars.BATCH_RETRIEVE_MAX, 2);
    MetastoreConf.setLongVar(conf, ConfVars.LIMIT_PARTITION_REQUEST, DEFAULT_LIMIT_PARTITION_REQUEST);
    MetastoreConf.setVar(conf, ConfVars.STORAGE_SCHEMA_READER_IMPL, "no.such.class");
    MetastoreConf.setBoolVar(conf, ConfVars.METASTORE_CLIENT_FILTER_ENABLED, false);
    MetastoreConf.setBoolVar(conf, ConfVars.METASTORE_SERVER_FILTER_ENABLED, false);

    MetaStoreTestUtils.setConfForStandloneMode(conf);

    client = createClient(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (client != null) {
      // make sure tear down works
      DummyAuthorizationListenerImpl.throwExceptionAtCall = false;

      client.dropDatabase(dbName1, true, true, true);
      client.dropDatabase(dbName2, true, true, true);
      client.close();
    }
  }

  /**
   * This is called in each test after the configuration is set in each test case.
   * @throws Exception
   */
  protected void creatEnv(Configuration conf) throws Exception {
    client.dropDatabase(dbName1, true, true, true);
    client.dropDatabase(dbName2, true, true, true);
    Database db1 = new DatabaseBuilder()
        .setName(dbName1)
        .setCatalogName(Warehouse.DEFAULT_CATALOG_NAME)
        .create(client, conf);
    Database db2 = new DatabaseBuilder()
        .setName(dbName2)
        .setCatalogName(Warehouse.DEFAULT_CATALOG_NAME)
        .create(client, conf);
    new TableBuilder()
        .setDbName(dbName1)
        .setTableName(TAB1)
        .addCol("id", "int")
        .addCol("name", "string")
        .create(client, conf);
    Table tab2 = new TableBuilder()
        .setDbName(dbName1)
        .setTableName(TAB2)
        .addCol("id", "int")
        .addPartCol("name", "string")
        .create(client, conf);
    new PartitionBuilder()
        .inTable(tab2)
        .addValue("value1")
        .addToTable(client, conf);
    new PartitionBuilder()
        .inTable(tab2)
        .addValue("value2")
        .addToTable(client, conf);
  }

  /**
   * Test the pre-event listener is called in function get_fields at HMS server.
   * @throws Exception
   */
  @Test
  public void testGetFields() throws Exception {
    dbName1 = "db_test_get_fields_1";
    dbName2 = "db_test_get_fields_2";
    creatEnv(conf);

    // enable throwing exception, so we can check pre-envent listener is called
    TestHmsServerAuthorization.DummyAuthorizationListenerImpl.throwExceptionAtCall = true;

    try {
      List<FieldSchema> tableSchema = client.getFields(dbName1, TAB1);
      fail("getFields() should fail with throw exception mode at server side");
    } catch (MetaException ex) {
      boolean isMessageAuthorization = ex.getMessage().contains("Authorization fails");
      assertEquals(true, isMessageAuthorization);
    }
  }
}
