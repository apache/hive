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

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test long running request timeout functionality in MetaStore Server
 * HiveMetaStore.HMSHandler.create_database() is used to simulate a long running method.
 */
@Category(MetastoreCheckinTest.class)
public class TestHiveMetaStoreTimeout {
  protected static HiveMetaStoreClient client;
  protected static Configuration conf;
  protected static Warehouse warehouse;

  @BeforeClass
  public static void setUp() throws Exception {
    HiveMetaStore.TEST_TIMEOUT_ENABLED = true;
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setClass(conf, ConfVars.EXPRESSION_PROXY_CLASS,
        MockPartitionExpressionForMetastore.class, PartitionExpressionProxy.class);
    MetastoreConf.setTimeVar(conf, ConfVars.CLIENT_SOCKET_TIMEOUT, 1000,
        TimeUnit.MILLISECONDS);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    warehouse = new Warehouse(conf);
    client = new HiveMetaStoreClient(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    HiveMetaStore.TEST_TIMEOUT_ENABLED = false;
    try {
      client.close();
    } catch (Throwable e) {
      System.err.println("Unable to close metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw e;
    }
  }

  @Test
  public void testNoTimeout() throws Exception {
    HiveMetaStore.TEST_TIMEOUT_VALUE = 250;

    String dbName = "db";
    client.dropDatabase(dbName, true, true);

    Database db = new Database();
    db.setName(dbName);
    try {
      client.createDatabase(db);
    } catch (MetaException e) {
      Assert.fail("should not throw timeout exception: " + e.getMessage());
    }

    client.dropDatabase(dbName, true, true);
  }

  @Test
  public void testTimeout() throws Exception {
    HiveMetaStore.TEST_TIMEOUT_VALUE = 2 * 1000;

    String dbName = "db";
    client.dropDatabase(dbName, true, true);

    Database db = new Database();
    db.setName(dbName);
    try {
      client.createDatabase(db);
      Assert.fail("should throw timeout exception.");
    } catch (MetaException e) {
      Assert.assertTrue("unexpected MetaException", e.getMessage().contains("Timeout when " +
          "executing method: create_database"));
    }

    // restore
    HiveMetaStore.TEST_TIMEOUT_VALUE = 1;
  }

  @Test
  public void testResetTimeout() throws Exception {
    HiveMetaStore.TEST_TIMEOUT_VALUE = 250;
    String dbName = "db";

    // no timeout before reset
    client.dropDatabase(dbName, true, true);
    Database db = new Database();
    db.setName(dbName);
    try {
      client.createDatabase(db);
    } catch (MetaException e) {
      Assert.fail("should not throw timeout exception: " + e.getMessage());
    }
    client.dropDatabase(dbName, true, true);

    // reset
    HiveMetaStore.TEST_TIMEOUT_VALUE = 2000;
    client.setMetaConf(ConfVars.CLIENT_SOCKET_TIMEOUT.getVarname(), "1s");

    // timeout after reset
    try {
      client.createDatabase(db);
      Assert.fail("should throw timeout exception.");
    } catch (MetaException e) {
      Assert.assertTrue("unexpected MetaException", e.getMessage().contains("Timeout when " +
          "executing method: create_database"));
    }

    // restore
    client.dropDatabase(dbName, true, true);
    client.setMetaConf(ConfVars.CLIENT_SOCKET_TIMEOUT.getVarname(), "10s");
  }
}