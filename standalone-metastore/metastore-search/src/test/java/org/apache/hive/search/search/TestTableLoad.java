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

package org.apache.hive.search.search;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@Category(MetastoreUnitTest.class)
public class TestTableLoad {

  @Test
  public void loadTableReturnsStoredTableWithoutSearch() throws Exception {
    try (InMemorySearchFixture fixture = InMemorySearchFixture.create()) {
      fixture.mutations().addTable(
          InMemorySearchFixture.table("hive", "sales", "orders", "daily orders"));
      fixture.commit(1L);

      TableSearchResult loaded = fixture.loadTable("hive", "sales", "orders");
      assertEquals(1L, loaded.processedEventId());
      Table table = loaded.hits().get(0).table();
      assertNotNull(table);
      assertEquals("sales", table.getDbName());
      assertEquals("orders", table.getTableName());
      assertEquals("daily orders", table.getParameters().get("comment"));

      assertFalse(fixture.searchMatch("daily orders", 5).isEmpty());
      Table reloaded = fixture.loadTable("hive", "sales", "orders").hits().get(0).table();
      assertEquals(table.getTableName(), reloaded.getTableName());
      assertEquals(table.getParameters().get("comment"), reloaded.getParameters().get("comment"));
    }
  }

  @Test
  public void loadTablesReturnsMultipleStoredTables() throws Exception {
    try (InMemorySearchFixture fixture = InMemorySearchFixture.create()) {
      fixture.mutations().addTable(
          InMemorySearchFixture.table("hive", "sales", "orders", "daily orders"));
      fixture.mutations().addTable(
          InMemorySearchFixture.table("hive", "sales", "customers", "customer list"));
      fixture.commit(1L);

      TableSearchResult loaded = fixture.loadTables("hive.sales.orders", "hive.sales.customers");
      assertEquals(2, loaded.hits().size());
      Map<String, Table> byName = loaded.hits().stream()
          .map(TableSearchHit::table)
          .collect(Collectors.toMap(Table::getTableName, Function.identity()));
      assertEquals("daily orders", byName.get("orders").getParameters().get("comment"));
      assertEquals("customer list", byName.get("customers").getParameters().get("comment"));
    }
  }

  @Test
  public void alterRefreshesStoredTableEntry() throws Exception {
    try (InMemorySearchFixture fixture = InMemorySearchFixture.create()) {
      Table table = InMemorySearchFixture.table("hive", "sales", "orders", "daily orders");
      fixture.mutations().addTable(table);
      fixture.commit(1L);

      Table altered = new Table(table);
      altered.setParameters(new java.util.HashMap<>(table.getParameters()));
      altered.getParameters().put("transient_lastDdlTime", "999");
      fixture.mutations().replaceTable(table, altered);
      fixture.commit(2L);

      Table after = fixture.loadTable("hive", "sales", "orders").hits().get(0).table();
      assertEquals("999", after.getParameters().get("transient_lastDdlTime"));
      assertEquals("daily orders", after.getParameters().get("comment"));
      assertFalse(fixture.searchMatch("daily orders", 5).isEmpty());
    }
  }
}
