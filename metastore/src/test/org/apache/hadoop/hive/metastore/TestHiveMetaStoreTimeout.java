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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test long running request timeout functionality in MetaStore Server
 * HiveMetaStore.HMSHandler.create_database() is used to simulate a long running method.
 */
public class TestHiveMetaStoreTimeout {
  protected static HiveMetaStoreClient client;
  protected static HiveConf hiveConf;
  protected static Warehouse warehouse;

  @BeforeClass
  public static void setUp() throws Exception {
    HiveMetaStore.TEST_TIMEOUT_ENABLED = true;
    hiveConf = new HiveConf(TestHiveMetaStoreTimeout.class);
    hiveConf.setBoolean(HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS.varname, true);
    hiveConf.set(HiveConf.ConfVars.METASTORE_EXPRESSION_PROXY_CLASS.varname,
        MockPartitionExpressionForMetastore.class.getCanonicalName());
    hiveConf.setTimeVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, 10 * 1000,
        TimeUnit.MILLISECONDS);
    warehouse = new Warehouse(hiveConf);
    try {
      client = new HiveMetaStoreClient(hiveConf);
    } catch (Throwable e) {
      System.err.println("Unable to open the metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw e;
    }
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
    HiveMetaStore.TEST_TIMEOUT_VALUE = 5 * 1000;

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
    HiveMetaStore.TEST_TIMEOUT_VALUE = 15 * 1000;

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
    HiveMetaStore.TEST_TIMEOUT_VALUE = 5 * 1000;
  }

  @Test
  public void testResetTimeout() throws Exception {
    HiveMetaStore.TEST_TIMEOUT_VALUE = 5 * 1000;
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
    client.setMetaConf(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.varname, "3s");

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
    client.setMetaConf(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.varname, "10s");
  }
}