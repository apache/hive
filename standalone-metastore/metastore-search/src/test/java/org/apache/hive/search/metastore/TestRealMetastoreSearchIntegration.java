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
import org.apache.hive.search.testutil.RealMetastoreServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestRealMetastoreSearchIntegration {

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
  public void bootstrapIndexesExistingTables() throws Exception {
    String db = RealMetastoreSearchSession.uniqueDbName("bootstrap");
    metastore.createDatabase(db);
    metastore.createTable(db, "orders", "daily sales orders");
    metastore.createTable(db, "customers", "customer master data");

    try (RealMetastoreSearchSession session = RealMetastoreSearchSession.open(metastore)) {
      session.refreshSearcher();
      assertFalse(session.searchMatch("orders", 10).isEmpty());
      assertFalse(session.searchMatch("customers", 10).isEmpty());
    }
  }

  @Test
  public void createAndDropTableUpdatesSearchIndex() throws Exception {
    String db = RealMetastoreSearchSession.uniqueDbName("incremental");
    metastore.createDatabase(db);

    try (RealMetastoreSearchSession session = RealMetastoreSearchSession.open(metastore)) {
      metastore.createTable(db, "orders", "daily sales orders");
      session.waitUntilSearchable("sales", 5);
      assertFalse(session.searchMatch("sales", 5).isEmpty());

      metastore.dropTable(db, "orders");
      session.waitUntilNotSearchable("sales", 5);
      assertTrue(session.searchMatch("sales", 5).isEmpty());
    }
  }

  @Test
  public void alterTableCommentUpdatesKeywordSearch() throws Exception {
    String db = RealMetastoreSearchSession.uniqueDbName("alter");
    metastore.createDatabase(db);
    metastore.createTable(db, "orders", "old warehouse comment");

    try (RealMetastoreSearchSession session = RealMetastoreSearchSession.open(metastore)) {
      session.waitUntilSearchable("warehouse", 5);
      assertTrue(session.searchMatch("revenue", 5).isEmpty());

      metastore.dropTable(db, "orders");
      metastore.createTable(db, "orders", "revenue analytics dashboard");
      session.waitUntilSearchable("revenue", 5);
      assertFalse(session.searchMatch("revenue", 5).isEmpty());
    }
  }
}
