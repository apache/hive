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

import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Connection related tests for HiveMetaStoreClient.
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestHiveMetaStoreClient extends MetaStoreClientTest {
  private boolean isRemote;
  private AbstractMetaStoreService metaStore;

  public TestHiveMetaStoreClient(String name, AbstractMetaStoreService metaStore) {
    this.isRemote = name.equals("Remote");
    this.metaStore = metaStore;
  }

  @After
  public void cleanUp() throws Exception {
    HiveMetaStoreClient.getConnCount().set(0);
  }

  @Test
  public void testTTransport() throws MetaException {
    HiveMetaStoreClient client = metaStore.getClient();
    assertTrue(isRemote ? client.getTTransport().isOpen() : client.getTTransport() == null);

    client.close();
    assertTrue(isRemote ? !client.getTTransport().isOpen() : client.getTTransport() == null);
  }

  @Test
  public void testReconnect() throws MetaException {
    HiveMetaStoreClient client = metaStore.getClient();
    assertEquals(isRemote ? 1 : 0, HiveMetaStoreClient.getConnCount().get());

    try {
      client.reconnect();
    } catch (MetaException e) {
      // expected in local metastore
      assertFalse(isRemote);
    }
    assertEquals(isRemote ? 1 : 0, HiveMetaStoreClient.getConnCount().get());

    client.close();
    assertEquals(0, HiveMetaStoreClient.getConnCount().get());
  }

  @Test
  public void testCloseClient() throws MetaException {
    HiveMetaStoreClient client1 = metaStore.getClient();
    assertEquals(isRemote ? 1 : 0, HiveMetaStoreClient.getConnCount().get());

    HiveMetaStoreClient client2 = metaStore.getClient();
    assertEquals(isRemote ? 2 : 0, HiveMetaStoreClient.getConnCount().get());

    client1.close();
    assertEquals(isRemote ? 1 : 0, HiveMetaStoreClient.getConnCount().get());

    client1.close();
    assertEquals(isRemote ? 1 : 0, HiveMetaStoreClient.getConnCount().get());

    client2.close();
    assertEquals(0, HiveMetaStoreClient.getConnCount().get());
  }
}
