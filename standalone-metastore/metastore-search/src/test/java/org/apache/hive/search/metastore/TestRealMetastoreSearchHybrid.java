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
public class TestRealMetastoreSearchHybrid {

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
  public void keywordSearchFindsMatchingTable() throws Exception {
    String db = RealMetastoreSearchSession.uniqueDbName("keyword");
    metastore.createDatabase(db);
    metastore.createTable(db, "orders", "daily sales orders");
    metastore.createTable(db, "inventory", "spare parts catalog");

    try (RealMetastoreSearchSession session = RealMetastoreSearchSession.open(metastore)) {
      session.waitUntilSearchable("sales", 5);
      List<Map<String, Object>> hits = session.searchMatch("sales", 5);
      assertFalse(hits.isEmpty());
      assertTrue(hits.stream().anyMatch(hit -> hit.get("_id").toString().contains("orders")));
      assertTrue(session.searchMatch("parts", 5).stream()
          .anyMatch(hit -> hit.get("_id").toString().contains("inventory")));
    }
  }

  @Test
  public void hybridSearchFindsSemanticallyNamedTable() throws Exception {
    String db = RealMetastoreSearchSession.uniqueDbName("hybrid");
    metastore.createDatabase(db);
    metastore.createTable(db, "orders", "revenue analytics dashboard");
    metastore.createTable(db, "parts", "spare parts catalog");

    try (RealMetastoreSearchSession session = RealMetastoreSearchSession.open(metastore)) {
      session.waitUntilSearchable("revenue", 5);
      List<Map<String, Object>> hits = session.searchHybrid("revenue analytics", 5);
      assertFalse(hits.isEmpty());
      assertTrue(hits.stream().anyMatch(hit -> hit.get("_id").toString().contains("orders")));
    }
  }

  @Test
  public void hybridSearchRanksKeywordMatchHigherForExactToken() throws Exception {
    String db = RealMetastoreSearchSession.uniqueDbName("hybrid_rank");
    metastore.createDatabase(db);
    metastore.createTable(db, "orders", "sales orders table");
    metastore.createTable(db, "metrics", "revenue analytics dashboard");

    try (RealMetastoreSearchSession session = RealMetastoreSearchSession.open(metastore)) {
      session.waitUntilSearchable("sales", 5);
      List<Map<String, Object>> keywordHits = session.searchMatch("sales", 5);
      assertFalse(keywordHits.isEmpty());
      assertTrue(keywordHits.get(0).get("_id").toString().contains("orders"));

      List<Map<String, Object>> hybridHits = session.searchHybrid("revenue analytics", 5);
      assertFalse(hybridHits.isEmpty());
      assertTrue(hybridHits.stream().anyMatch(hit -> hit.get("_id").toString().contains("metrics")));
    }
  }
}
