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
package org.apache.hive.hcatalog.common;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hive.hcatalog.NoExitSecurityManager;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestHiveClientCache {

  private static final Logger LOG = LoggerFactory.getLogger(TestHiveClientCache.class);
  final HiveConf hiveConf = new HiveConf();

  @BeforeClass
  public static void setUp() throws Exception {
  }

  @AfterClass
  public static void tearDown() throws Exception {
  }

  @Test
  public void testCacheHit() throws IOException, MetaException, LoginException {

    HiveClientCache cache = new HiveClientCache(1000);
    HiveMetaStoreClient client = cache.get(hiveConf);
    assertNotNull(client);
    client.close(); // close shouldn't matter

    // Setting a non important configuration should return the same client only
    hiveConf.setIntVar(HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS, 10);
    HiveMetaStoreClient client2 = cache.get(hiveConf);
    assertNotNull(client2);
    assertEquals(client, client2);
    client2.close();
  }

  @Test
  public void testCacheMiss() throws IOException, MetaException, LoginException {
    HiveClientCache cache = new HiveClientCache(1000);
    HiveMetaStoreClient client = cache.get(hiveConf);
    assertNotNull(client);

    // Set different uri as it is one of the criteria deciding whether to return the same client or not
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, " "); // URIs are checked for string equivalence, even spaces make them different
    HiveMetaStoreClient client2 = cache.get(hiveConf);
    assertNotNull(client2);
    assertNotSame(client, client2);
  }

  /**
   * Check that a new client is returned for the same configuration after the expiry time.
   * Also verify that the expiry time configuration is honoured
   */
  @Test
  public void testCacheExpiry() throws IOException, MetaException, LoginException, InterruptedException {
    HiveClientCache cache = new HiveClientCache(1);
    HiveClientCache.CacheableHiveMetaStoreClient client = (HiveClientCache.CacheableHiveMetaStoreClient) cache.get(hiveConf);
    assertNotNull(client);

    Thread.sleep(2500);
    HiveMetaStoreClient client2 = cache.get(hiveConf);
    client.close();
    assertTrue(client.isClosed()); // close() after *expiry time* and *a cache access* should  have tore down the client

    assertNotNull(client2);
    assertNotSame(client, client2);
  }

  /**
   * Check that a *new* client is created if asked from different threads even with
   * the same hive configuration
   * @throws ExecutionException
   * @throws InterruptedException
   */
  @Test
  public void testMultipleThreadAccess() throws ExecutionException, InterruptedException {
    final HiveClientCache cache = new HiveClientCache(1000);

    class GetHiveClient implements Callable<HiveMetaStoreClient> {
      @Override
      public HiveMetaStoreClient call() throws IOException, MetaException, LoginException {
        return cache.get(hiveConf);
      }
    }

    ExecutorService executor = Executors.newFixedThreadPool(2);

    Callable<HiveMetaStoreClient> worker1 = new GetHiveClient();
    Callable<HiveMetaStoreClient> worker2 = new GetHiveClient();
    Future<HiveMetaStoreClient> clientFuture1 = executor.submit(worker1);
    Future<HiveMetaStoreClient> clientFuture2 = executor.submit(worker2);
    HiveMetaStoreClient client1 = clientFuture1.get();
    HiveMetaStoreClient client2 = clientFuture2.get();
    assertNotNull(client1);
    assertNotNull(client2);
    assertNotSame(client1, client2);
  }

  @Test
  public void testCloseAllClients() throws IOException, MetaException, LoginException {
    final HiveClientCache cache = new HiveClientCache(1000);
    HiveClientCache.CacheableHiveMetaStoreClient client1 = (HiveClientCache.CacheableHiveMetaStoreClient) cache.get(hiveConf);
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, " "); // URIs are checked for string equivalence, even spaces make them different
    HiveClientCache.CacheableHiveMetaStoreClient client2 = (HiveClientCache.CacheableHiveMetaStoreClient) cache.get(hiveConf);
    cache.closeAllClientsQuietly();
    assertTrue(client1.isClosed());
    assertTrue(client2.isClosed());
  }

  /**
   * Test that a long table name actually breaks the HMSC. Subsequently check that isOpen() reflects
   * and tells if the client is broken
   */
  @Ignore("hangs indefinitely")
  @Test
  public void testHMSCBreakability() throws IOException, MetaException, LoginException, TException, AlreadyExistsException,
      InvalidObjectException, NoSuchObjectException, InterruptedException {
    // Setup
    LocalMetaServer metaServer = new LocalMetaServer();
    metaServer.start();

    final HiveClientCache cache = new HiveClientCache(1000);
    HiveClientCache.CacheableHiveMetaStoreClient client =
        (HiveClientCache.CacheableHiveMetaStoreClient) cache.get(metaServer.getHiveConf());

    assertTrue(client.isOpen());

    final String DB_NAME = "test_db";
    final String LONG_TABLE_NAME = "long_table_name_" + new BigInteger(200, new Random()).toString(2);

    try {
      client.dropTable(DB_NAME, LONG_TABLE_NAME);
    } catch (Exception e) {
    }
    try {
      client.dropDatabase(DB_NAME);
    } catch (Exception e) {
    }

    client.createDatabase(new Database(DB_NAME, "", null, null));

    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    fields.add(new FieldSchema("colname", serdeConstants.STRING_TYPE_NAME, ""));
    Table tbl = new Table();
    tbl.setDbName(DB_NAME);
    tbl.setTableName(LONG_TABLE_NAME);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(fields);
    tbl.setSd(sd);
    sd.setSerdeInfo(new SerDeInfo());

    // Break the client
    try {
      client.createTable(tbl);
      fail("Exception was expected while creating table with long name");
    } catch (Exception e) {
    }

    assertFalse(client.isOpen());
    metaServer.shutDown();
  }

  private static class LocalMetaServer implements Runnable {
    public final int MS_PORT = 20101;
    private final HiveConf hiveConf;
    private final SecurityManager securityManager;
    public final static int WAIT_TIME_FOR_BOOTUP = 30000;

    public LocalMetaServer() {
      securityManager = System.getSecurityManager();
      System.setSecurityManager(new NoExitSecurityManager());
      hiveConf = new HiveConf(TestHiveClientCache.class);
      hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:"
          + MS_PORT);
      hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
      hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES, 3);
      hiveConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
          HCatSemanticAnalyzer.class.getName());
      hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
      hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
      hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname,
          "false");
      System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
      System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");
    }

    public void start() throws InterruptedException {
      Thread thread = new Thread(this);
      thread.start();
      Thread.sleep(WAIT_TIME_FOR_BOOTUP); // Wait for the server to bootup
    }

    @Override
    public void run() {
      try {
        HiveMetaStore.main(new String[]{"-v", "-p", String.valueOf(MS_PORT)});
      } catch (Throwable t) {
        LOG.error("Exiting. Got exception from metastore: ", t);
      }
    }

    public HiveConf getHiveConf() {
      return hiveConf;
    }

    public void shutDown() {
      System.setSecurityManager(securityManager);
    }
  }
}
