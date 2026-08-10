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

package org.apache.hive.service.cli.session.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class TestSessionStateStoreBase {

  protected SessionStateStore store;

  protected abstract SessionStateStore createStore() throws Exception;

  @Before
  public void setUp() throws Exception {
    store = createStore();
  }

  @After
  public void tearDown() {
    if (store != null) {
      store.close();
    }
  }

  @Test
  public void testSaveAndGetSnapshot() {
    String sessionId = UUID.randomUUID().toString();
    HiveSessionSnapshot snapshot = createTestSnapshot(sessionId, "testuser", "test_db");

    store.saveSnapshot(sessionId, snapshot);
    HiveSessionSnapshot retrieved = store.getSnapshot(sessionId);

    assertNotNull(retrieved);
    assertEquals(sessionId, retrieved.getSessionHandleId());
    assertEquals("testuser", retrieved.getUsername());
    assertEquals("127.0.0.1", retrieved.getIpAddress());
    assertEquals("test_db", retrieved.getCurrentDatabase());
    assertEquals(2, retrieved.getOverriddenConfigurations().size());
    assertEquals("true", retrieved.getOverriddenConfigurations().get("hive.exec.dynamic.partition"));
    assertEquals("nonstrict", retrieved.getOverriddenConfigurations().get("hive.exec.dynamic.partition.mode"));
    assertEquals(2, retrieved.getAddedJars().size());
    assertEquals("hdfs:///user/hive/udfs/my-udf.jar", retrieved.getAddedJars().get(0));
    assertEquals(1, retrieved.getTempTableDefinitions().size());
    assertEquals("CREATE TEMPORARY TABLE tmp_t (col1 STRING, col2 INT)",
        retrieved.getTempTableDefinitions().get("tmp_t"));
    assertEquals(10, retrieved.getProtocolVersion());
    assertEquals(1000L, retrieved.getCreationTime());
    assertEquals(2000L, retrieved.getLastAccessTime());
  }

  @Test
  public void testDeleteSnapshot() {
    String sessionId = UUID.randomUUID().toString();
    HiveSessionSnapshot snapshot = createTestSnapshot(sessionId, "testuser", "default");

    store.saveSnapshot(sessionId, snapshot);
    assertNotNull(store.getSnapshot(sessionId));

    store.deleteSnapshot(sessionId);
    assertNull(store.getSnapshot(sessionId));
  }

  @Test
  public void testOverwriteSnapshot() {
    String sessionId = UUID.randomUUID().toString();

    HiveSessionSnapshot snapshot1 = createTestSnapshot(sessionId, "user1", "db1");
    store.saveSnapshot(sessionId, snapshot1);

    HiveSessionSnapshot snapshot2 = createTestSnapshot(sessionId, "user1", "db2");
    store.saveSnapshot(sessionId, snapshot2);

    HiveSessionSnapshot retrieved = store.getSnapshot(sessionId);
    assertNotNull(retrieved);
    assertEquals("db2", retrieved.getCurrentDatabase());
  }

  @Test
  public void testGetNonExistent() {
    String sessionId = UUID.randomUUID().toString();
    assertNull(store.getSnapshot(sessionId));
  }

  @Test
  public void testMultipleSessions() {
    String sessionId1 = UUID.randomUUID().toString();
    String sessionId2 = UUID.randomUUID().toString();
    String sessionId3 = UUID.randomUUID().toString();

    store.saveSnapshot(sessionId1, createTestSnapshot(sessionId1, "user1", "db1"));
    store.saveSnapshot(sessionId2, createTestSnapshot(sessionId2, "user2", "db2"));
    store.saveSnapshot(sessionId3, createTestSnapshot(sessionId3, "user3", "db3"));

    assertEquals("db1", store.getSnapshot(sessionId1).getCurrentDatabase());
    assertEquals("db2", store.getSnapshot(sessionId2).getCurrentDatabase());
    assertEquals("db3", store.getSnapshot(sessionId3).getCurrentDatabase());

    store.deleteSnapshot(sessionId2);
    assertNotNull(store.getSnapshot(sessionId1));
    assertNull(store.getSnapshot(sessionId2));
    assertNotNull(store.getSnapshot(sessionId3));
  }

  @Test
  public void testDeleteNonExistent() {
    String sessionId = UUID.randomUUID().toString();
    store.deleteSnapshot(sessionId);
    assertNull(store.getSnapshot(sessionId));
  }

  @Test
  public void testSnapshotWipedOnSessionClose() {
    String sessionId1 = UUID.randomUUID().toString();
    String sessionId2 = UUID.randomUUID().toString();

    store.saveSnapshot(sessionId1, createTestSnapshot(sessionId1, "user1", "db1"));
    store.saveSnapshot(sessionId2, createTestSnapshot(sessionId2, "user2", "db2"));

    // Simulate session close — snapshot should be completely removed
    store.deleteSnapshot(sessionId1);

    assertNull("Snapshot should be wiped after session close", store.getSnapshot(sessionId1));
    // Other sessions remain unaffected
    assertNotNull("Other session should still exist", store.getSnapshot(sessionId2));
    assertEquals("db2", store.getSnapshot(sessionId2).getCurrentDatabase());
  }

  protected HiveSessionSnapshot createTestSnapshot(String sessionId, String username, String database) {
    Map<String, String> configs = new HashMap<>();
    configs.put("hive.exec.dynamic.partition", "true");
    configs.put("hive.exec.dynamic.partition.mode", "nonstrict");

    Map<String, String> tempTables = new HashMap<>();
    tempTables.put("tmp_t", "CREATE TEMPORARY TABLE tmp_t (col1 STRING, col2 INT)");

    return HiveSessionSnapshot.builder()
        .sessionHandleId(sessionId)
        .username(username)
        .ipAddress("127.0.0.1")
        .currentDatabase(database)
        .overriddenConfigurations(configs)
        .addedJars(Arrays.asList("hdfs:///user/hive/udfs/my-udf.jar", "hdfs:///user/hive/udfs/other.jar"))
        .tempTableDefinitions(tempTables)
        .protocolVersion(10)
        .creationTime(1000L)
        .lastAccessTime(2000L)
        .build();
  }
}
