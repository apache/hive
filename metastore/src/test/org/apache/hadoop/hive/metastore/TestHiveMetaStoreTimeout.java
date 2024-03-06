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

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;

import org.junit.Before;
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
  protected static int port;

  @BeforeClass
  public static void startMetaStoreServer() throws Exception {
    HiveMetaStore.TEST_TIMEOUT_ENABLED = true;
    hiveConf = new HiveConf(TestHiveMetaStoreTimeout.class);
    hiveConf.setBoolean(HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS.varname, true);
    hiveConf.set(HiveConf.ConfVars.METASTORE_EXPRESSION_PROXY_CLASS.varname,
        MockPartitionExpressionForMetastore.class.getCanonicalName());
    hiveConf.setTimeVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, 10 * 1000,
        TimeUnit.MILLISECONDS);
    warehouse = new Warehouse(hiveConf);
    port = MetaStoreUtils.startMetaStoreWithRetry(hiveConf);
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, false);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    HiveMetaStore.TEST_TIMEOUT_ENABLED = false;
  }

  @Before
  public void setup() throws MetaException {
    client = new HiveMetaStoreClient(hiveConf);
  }

  @After
  public void cleanup() {
    client.close();
    client = null;
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
    } catch (TTransportException e) {
      Assert.assertTrue("unexpected Exception", e.getMessage().contains("Read timed out"));
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

  @Test
  public void testConnectionTimeout() throws Exception {
    final HiveConf newConf = new HiveConf(hiveConf);
    newConf.setTimeVar(HiveConf.ConfVars.METASTORE_CLIENT_CONNECTION_TIMEOUT, 1000,
            TimeUnit.MILLISECONDS);
    // fake host to mock connection time out
    newConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://1.1.1.1:" + port);
    newConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 1);

    Future<Void> future = Executors.newSingleThreadExecutor().submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
            HiveMetaStoreClient c = null;
            try {
                c = new HiveMetaStoreClient(newConf);
                Assert.fail("should throw connection timeout exception.");
            } catch (MetaException e) {
                Assert.assertTrue("unexpected Exception", e.getMessage().contains("connect timed out"));
            } finally {
                if (c != null) {
                    c.close();
                }
            }
            return null;
        }
    });
    future.get(5, TimeUnit.SECONDS);
  }
}