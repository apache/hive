/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.metastore;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hive.search.exception.IndexNotHealthyException;
import org.apache.hive.search.testutil.InMemoryIndexStateClient;
import org.apache.hive.search.testutil.RealMetastoreServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestRealMetastoreSearchFailureRecovery {

  private static RealMetastoreServer metastore;

  @BeforeClass
  public static void startMetastore() throws Exception {
    metastore = RealMetastoreServer.start();
  }

  @AfterClass
  public static void stopMetastore() throws Exception {
    if (metastore != null) {
      metastore.close();
    }
  }

  @Test
  public void restoresIndexFromRemoteBackupAfterLocalLoss() throws Exception {
    String db = RealMetastoreSearchSession.uniqueDbName("restore");
    metastore.createDatabase(db);
    metastore.createTable(db, "orders", "daily sales orders");

    InMemoryIndexStateClient remote = RealMetastoreSearchSession.newSharedRemoteBackup();
    try (RealMetastoreSearchSession leader = RealMetastoreSearchSession.open(metastore, remote)) {
      leader.waitUntilSearchable("sales", 5);
      leader.syncBackup();
      assertTrue(remote.readManifest().isPresent());
    }

    try (RealMetastoreSearchSession restored = RealMetastoreSearchSession.open(metastore, remote)) {
      restored.refreshSearcher();
      List<Map<String, Object>> hits = restored.searchMatch("sales", 5);
      assertFalse(hits.isEmpty());
      assertTrue(hits.stream().anyMatch(hit -> hit.get("_id").toString().contains("orders")));
    }
  }

  @Test
  public void recoversIncrementalUpdatesAfterRestore() throws Exception {
    String db = RealMetastoreSearchSession.uniqueDbName("restore_inc");
    metastore.createDatabase(db);
    metastore.createTable(db, "orders", "daily sales orders");

    InMemoryIndexStateClient remote = RealMetastoreSearchSession.newSharedRemoteBackup();
    try (RealMetastoreSearchSession leader = RealMetastoreSearchSession.open(metastore, remote)) {
      leader.waitUntilSearchable("sales", 5);
      leader.syncBackup();
    }

    try (RealMetastoreSearchSession restored = RealMetastoreSearchSession.open(metastore, remote)) {
      restored.waitUntilSearchable("sales", 5);
      metastore.createTable(db, "customers", "customer master records");
      restored.waitUntilSearchable("customer", 5);
      assertEquals(2, restored.searchMatch("master", 10).size());
    }
  }

  @Test
  public void skipPoisonEventAllowsIndexingToContinue() throws Exception {
    String db = RealMetastoreSearchSession.uniqueDbName("poison");
    metastore.createDatabase(db);
    metastore.createTable(db, "orders", "daily sales orders");

    try (RealMetastoreSearchSession session = RealMetastoreSearchSession.open(metastore)) {
      session.waitUntilSearchable("sales", 5);
      session.indexManager().notifyIndexState(false,
          new IndexNotHealthyException("simulated notification failure"));
      assertThrows(IndexNotHealthyException.class, session.indexManager()::checkIndexState);

      session.indexManager().notifyIndexState(true);
      session.indexManager().checkIndexState();

      metastore.createTable(db, "customers", "customer master records");
      session.waitUntilSearchable("customer", 5);
      assertFalse(session.searchMatch("customer", 5).isEmpty());
    }
  }
}
