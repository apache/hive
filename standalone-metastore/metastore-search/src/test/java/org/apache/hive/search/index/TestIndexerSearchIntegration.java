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

package org.apache.hive.search.index;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.search.search.InMemorySearchFixture;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestIndexerSearchIntegration {

  @Test
  public void keywordSearchFindsIndexedTable() throws Exception {
    try (InMemorySearchFixture fixture = InMemorySearchFixture.create()) {
      fixture.mutations().addTable(
          InMemorySearchFixture.table("hive", "sales", "orders", "daily sales orders"));
      fixture.mutations().addTable(
          InMemorySearchFixture.table("hive", "inventory", "parts", "spare parts catalog"));
      fixture.commit(1L);

      List<Map<String, Object>> hits = fixture.searchMatch("sales", 5);
      assertFalse(hits.isEmpty());
      assertTrue(hits.stream().anyMatch(hit -> hit.get("_id").toString().contains("sales.orders")));
    }
  }

  @Test
  public void tableKeywordMatchesTableNameWhenCommentDoesNot() throws Exception {
    try (InMemorySearchFixture fixture = InMemorySearchFixture.create()) {
      fixture.mutations().addTable(
          InMemorySearchFixture.table("hive", "sales", "customers", "customer master data"));
      fixture.commit(1L);

      assertFalse(fixture.searchMatch("customers", 5).isEmpty());
    }
  }

  @Test
  public void tableKeywordRanksTableNameAboveColumnNameAboveComment() throws Exception {
    try (InMemorySearchFixture fixture = InMemorySearchFixture.create()) {
      fixture.mutations().addTable(
          InMemorySearchFixture.table("hive", "sales", "orders", "revenue summary"));
      fixture.mutations().addTable(tableWithColumn("hive", "sales", "line_items", "revenue"));
      fixture.mutations().addTable(
          InMemorySearchFixture.table("hive", "sales", "revenue", "monthly facts"));
      fixture.commit(1L);

      List<Map<String, Object>> hits = fixture.searchMatch("revenue", 5);
      assertEquals(3, hits.size());
      assertTrue(hits.get(0).get("_id").toString().contains("sales.revenue"));
      assertTrue(hits.get(1).get("_id").toString().contains("sales.line_items"));
      assertTrue(hits.get(2).get("_id").toString().contains("sales.orders"));
    }
  }

  private static Table tableWithColumn(String catalog, String db, String name, String columnName) {
    Table table = InMemorySearchFixture.table(catalog, db, name, "unrelated comment");
    table.getSd().setCols(List.of(
        new org.apache.hadoop.hive.metastore.api.FieldSchema(columnName, "double", "amount value")));
    return table;
  }

  @Test
  public void incrementalDropRemovesTableFromSearchResults() throws Exception {
    try (InMemorySearchFixture fixture = InMemorySearchFixture.create()) {
      fixture.mutations().addTable(
          InMemorySearchFixture.table("hive", "sales", "orders", "daily sales orders"));
      fixture.commit(1L);
      assertFalse(fixture.searchMatch("sales", 5).isEmpty());

      fixture.mutations().dropTable(new TableName("hive", "sales", "orders"));
      fixture.commit(2L);

      List<Map<String, Object>> hits = fixture.searchMatch("sales", 5);
      assertTrue(hits.isEmpty());
    }
  }

  @Test
  public void alterTableUpdatesSearchText() throws Exception {
    try (InMemorySearchFixture fixture = InMemorySearchFixture.create()) {
      var before = InMemorySearchFixture.table("hive", "sales", "orders", "old comment");
      var after = InMemorySearchFixture.table("hive", "sales", "orders", "revenue analytics");
      fixture.mutations().addTable(before);
      fixture.commit(1L);
      assertTrue(fixture.searchMatch("revenue", 5).isEmpty());

      fixture.mutations().replaceTable(before, after);
      fixture.commit(2L);

      List<Map<String, Object>> hits = fixture.searchMatch("revenue", 5);
      assertEquals(1, hits.size());
      assertTrue(hits.get(0).get("_id").toString().contains("sales.orders"));
    }
  }
}
