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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestHiveClientCache {
  final HiveConf hiveConf = new HiveConf();

  @Test
  public void testCacheHit() throws IOException, MetaException {
    HiveClientCache cache = new HiveClientCache(1000);
    HiveClientCache.ICacheableMetaStoreClient client =
        (HiveClientCache.ICacheableMetaStoreClient) cache.get(hiveConf);
    assertNotNull(client);
    client.close(); // close shouldn't matter

    // Setting a non important configuration should return the same client only
    hiveConf.setIntVar(HiveConf.ConfVars.DYNAMIC_PARTITION_MAX_PARTS, 10);
    HiveClientCache.ICacheableMetaStoreClient client2 =
        (HiveClientCache.ICacheableMetaStoreClient) cache.get(hiveConf);
    assertNotNull(client2);
    assertSame(client, client2);
    assertEquals(client.getUsers(), client2.getUsers());
    client2.close();
  }

  @Test
  public void testCacheMiss() throws IOException, MetaException {
    HiveClientCache cache = new HiveClientCache(1000);
    IMetaStoreClient client = cache.get(hiveConf);
    assertNotNull(client);

    // Set different uri as it is one of the criteria deciding whether to return the same client or not
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_URIS, " "); // URIs are checked for string equivalence, even spaces make them different
    IMetaStoreClient client2 = cache.get(hiveConf);
    assertNotNull(client2);
    assertNotSame(client, client2);
  }

  /**
   * Check that a new client is returned for the same configuration after the expiry time.
   * Also verify that the expiry time configuration is honoured
   */
  @Test
  public void testCacheExpiry() throws IOException, MetaException, InterruptedException {
    HiveClientCache cache = new HiveClientCache(1);
    HiveClientCache.ICacheableMetaStoreClient client =
        (HiveClientCache.ICacheableMetaStoreClient) cache.get(hiveConf);
    assertNotNull(client);

    Thread.sleep(2500);
    HiveClientCache.ICacheableMetaStoreClient client2 =
        (HiveClientCache.ICacheableMetaStoreClient) cache.get(hiveConf);
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

    class GetHiveClient implements Callable<IMetaStoreClient> {
      @Override
      public IMetaStoreClient call() throws IOException, MetaException {
        return cache.get(hiveConf);
      }
    }
    Callable<IMetaStoreClient> worker1 = new GetHiveClient();
    Callable<IMetaStoreClient> worker2 = new GetHiveClient();

    Future<IMetaStoreClient> clientFuture1, clientFuture2;
    try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
      clientFuture1 = executor.submit(worker1);
      clientFuture2 = executor.submit(worker2);
    }
    IMetaStoreClient client1 = clientFuture1.get();
    IMetaStoreClient client2 = clientFuture2.get();

    assertNotNull(client1);
    assertNotNull(client2);
    assertNotSame(client1, client2);
  }

  @Test
  public void testCloseAllClients() throws IOException, MetaException {
    final HiveClientCache cache = new HiveClientCache(1000);
    HiveClientCache.ICacheableMetaStoreClient client1 =
        (HiveClientCache.ICacheableMetaStoreClient) cache.get(hiveConf);
    MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.THRIFT_URIS, " "); // URIs are checked for string equivalence, even spaces make them different
    HiveClientCache.ICacheableMetaStoreClient client2 =
        (HiveClientCache.ICacheableMetaStoreClient) cache.get(hiveConf);
    cache.closeAllClientsQuietly();
    assertTrue(client1.isClosed());
    assertTrue(client2.isClosed());
  }
}
