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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.metadata.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.CacheKey;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.KeyType;
import org.apache.hadoop.hive.ql.metadata.client.MetaStoreClientCacheUtils.Supplier;
import org.apache.thrift.TException;
import org.junit.Test;

public class TestHMSQueryCache {

  private static Table table(long id) {
    Table table = new Table();
    table.setId(id);
    table.setDbName("db");
    table.setTableName("t");
    return table;
  }

  private static Supplier<Table, TException> counting(AtomicInteger loads, Table table) {
    return () -> {
      loads.incrementAndGet();
      return table;
    };
  }

  private static Supplier<Table, TException> notFound(AtomicInteger loads) {
    return () -> {
      loads.incrementAndGet();
      throw new NoSuchObjectException("not found");
    };
  }

  private static void assertNotFound(HMSQueryCache cache, GetTableRequest req, String catName,
      Supplier<Table, TException> loader) throws TException {
    try {
      cache.getTable(req, catName, loader);
      fail("expected NoSuchObjectException");
    } catch (NoSuchObjectException expected) {
      // the table is missing
    }
  }

  private static GetValidWriteIdsRequest validWriteIdsRequest(String validTxnList, String... tables) {
    GetValidWriteIdsRequest rqst = new GetValidWriteIdsRequest(List.of(tables));
    rqst.setValidTxnList(validTxnList);
    return rqst;
  }

  private static List<TableValidWriteIds> writeIds(List<String> fullTableNames) {
    return fullTableNames.stream()
        .map(name -> new TableValidWriteIds(name, 1L, new ArrayList<>(), ByteBuffer.allocate(0)))
        .toList();
  }

  private static List<String> names(List<TableValidWriteIds> writeIds) {
    return writeIds.stream().map(TableValidWriteIds::getFullTableName).toList();
  }

  @Test
  public void testPutGetClear() {
    HMSQueryCache cache = new HMSQueryCache();
    CacheKey key = new CacheKey(KeyType.CONFIG_VALUE, "name", "default");
    assertNull(cache.get(key));

    cache.put(key, "value");
    assertEquals("value", cache.get(key));

    cache.clear();
    assertNull(cache.get(key));
  }

  @Test
  public void testGetOrLoadLoadsOnce() throws Exception {
    HMSQueryCache cache = new HMSQueryCache();
    AtomicInteger loads = new AtomicInteger();
    CacheKey key = new CacheKey(KeyType.CONFIG_VALUE, "name", "default");

    assertEquals("value1", cache.getOrLoad("getConfigValue", key, () -> "value" + loads.incrementAndGet()));
    assertEquals("value1", cache.getOrLoad("getConfigValue", key, () -> "value" + loads.incrementAndGet()));
    assertEquals(1, loads.get());
  }

  @Test
  public void testGetOrLoadDoesNotKeepNull() throws Exception {
    HMSQueryCache cache = new HMSQueryCache();
    AtomicInteger loads = new AtomicInteger();
    CacheKey key = new CacheKey(KeyType.CONFIG_VALUE, "name", null);

    assertNull(cache.getOrLoad("getConfigValue", key, () -> {
      loads.incrementAndGet();
      return null;
    }));
    assertEquals("value", cache.getOrLoad("getConfigValue", key, () -> {
      loads.incrementAndGet();
      return "value";
    }));
    assertEquals(2, loads.get());
  }

  @Test
  public void testGetTableSetsCachedIdAndCachesByName() throws Exception {
    HMSQueryCache cache = new HMSQueryCache();
    Table table = table(42L);
    AtomicInteger loads = new AtomicInteger();

    GetTableRequest first = new GetTableRequest("db", "t");
    assertSame(table, cache.getTable(first, "hive", () -> {
      loads.incrementAndGet();
      return table;
    }));
    assertEquals(42L, first.getId());

    GetTableRequest second = new GetTableRequest("db", "t");
    assertSame(table, cache.getTable(second, "hive", () -> {
      loads.incrementAndGet();
      return table;
    }));
    assertEquals(42L, second.getId());
    assertEquals(1, loads.get());

    assertSame(table, cache.getTableByName("hive", "DB", "T", () -> {
      throw new AssertionError("the table is cached by name");
    }));
  }

  @Test
  public void testGetTableNotFoundIsCachedUntilInvalidated() throws Exception {
    HMSQueryCache cache = new HMSQueryCache();
    AtomicInteger loads = new AtomicInteger();
    Supplier<Table, TException> missing = () -> {
      loads.incrementAndGet();
      throw new NoSuchObjectException("db.t not found");
    };

    for (int i = 0; i < 2; i++) {
      try {
        cache.getTable(new GetTableRequest("db", "t"), "hive", missing);
        fail("expected NoSuchObjectException");
      } catch (NoSuchObjectException e) {
        assertEquals("db.t not found", e.getMessage());
      }
    }
    assertEquals(1, loads.get());

    cache.invalidateTableNotFound("hive", "DB", "T");
    assertEquals(1L, cache.getTable(new GetTableRequest("db", "t"), "hive", () -> table(1L)).getId());
  }

  @Test
  public void testDropTablesNotFound() throws Exception {
    HMSQueryCache cache = new HMSQueryCache();
    try {
      cache.getTable(new GetTableRequest("db", "t"), "hive", () -> {
        throw new NoSuchObjectException("db.t not found");
      });
      fail("expected NoSuchObjectException");
    } catch (NoSuchObjectException expected) {
      // cached as not found
    }
    CacheKey other = new CacheKey(KeyType.CONFIG_VALUE, "name", "default");
    cache.put(other, "value");

    cache.dropTablesNotFound();

    assertEquals(42L, cache.getTable(new GetTableRequest("db", "t"), "hive", () -> table(42L)).getId());
    assertEquals("value", cache.get(other));
  }

  @Test
  public void testGetOrLoadDoesNotCacheFailure() throws Exception {
    HMSQueryCache cache = new HMSQueryCache();
    CacheKey key = new CacheKey(KeyType.CONFIG_VALUE, "name", "default");
    try {
      cache.getOrLoad("getConfigValue", key, () -> {
        throw new MetaException("HMS down");
      });
      fail("expected MetaException");
    } catch (MetaException expected) {
      // propagated to the caller
    }
    assertEquals("value", cache.getOrLoad("getConfigValue", key, () -> "value"));
  }

  @Test
  public void testGetTableKeysOnCopyOfRequest() throws Exception {
    HMSQueryCache cache = new HMSQueryCache();
    AtomicInteger loads = new AtomicInteger();

    GetTableRequest first = new GetTableRequest("db", "t");
    cache.getTable(first, "hive", counting(loads, table(42L)));
    // the caller owns the request and may change it afterwards
    first.setTblName("other");
    cache.getTable(new GetTableRequest("db", "t"), "hive", counting(loads, table(42L)));

    assertEquals(1, loads.get());
  }

  @Test
  public void testGetTableOtherRequestOfSameTable() throws Exception {
    HMSQueryCache cache = new HMSQueryCache();
    AtomicInteger loads = new AtomicInteger();
    Table plain = table(42L);
    Table withStats = table(42L);

    cache.getTable(new GetTableRequest("db", "t"), "hive", counting(loads, plain));
    GetTableRequest statsReq = new GetTableRequest("db", "t");
    statsReq.setGetColumnStats(true);
    assertSame(withStats, cache.getTable(statsReq, "hive", counting(loads, withStats)));
    assertEquals(42L, statsReq.getId());
    assertEquals(2, loads.get());

    // the table by name keeps the first table cached for the name
    assertSame(plain, cache.getTableByName("hive", "db", "t", counting(loads, table(1L))));
    assertEquals(2, loads.get());
  }

  @Test
  public void testGetTableIdSharedAcrossDefaultCatalogSpellings() throws Exception {
    HMSQueryCache cache = new HMSQueryCache();
    AtomicInteger loads = new AtomicInteger();

    // no catalog on the request: the caller passes the default one
    cache.getTable(new GetTableRequest("db", "t"), "hive", counting(loads, table(42L)));
    GetTableRequest explicit = new GetTableRequest("db", "t");
    explicit.setCatName("hive");
    // a different request, so it loads, but it gets the id cached by the first one
    explicit.setGetColumnStats(true);
    cache.getTable(explicit, "hive", () -> {
      assertEquals(42L, explicit.getId());
      loads.incrementAndGet();
      return table(42L);
    });

    assertEquals(2, loads.get());
  }

  @Test
  public void testGetTableIdIgnoresCaseOfNames() throws Exception {
    HMSQueryCache cache = new HMSQueryCache();
    AtomicInteger loads = new AtomicInteger();

    cache.getTable(new GetTableRequest("db", "t"), "hive", counting(loads, table(42L)));
    // a different request, so it loads, but it gets the id cached by the first one
    GetTableRequest upper = new GetTableRequest("DB", "T");
    cache.getTable(upper, "hive", () -> {
      assertEquals(42L, upper.getId());
      loads.incrementAndGet();
      return table(42L);
    });

    assertEquals(2, loads.get());
  }

  @Test
  public void testGetTableNotFoundIsPerTableAndCatalog() throws Exception {
    HMSQueryCache cache = new HMSQueryCache();
    AtomicInteger loads = new AtomicInteger();

    assertNotFound(cache, new GetTableRequest("db", "t"), "hive", notFound(loads));
    // the not-found key ignores the case of the names
    assertNotFound(cache, new GetTableRequest("DB", "T"), "hive", notFound(loads));
    assertEquals(1, loads.get());

    assertEquals(7L, cache.getTable(new GetTableRequest("db", "t2"), "hive", counting(loads, table(7L))).getId());
    assertEquals(8L, cache.getTable(new GetTableRequest("db", "t"), "other", counting(loads, table(8L))).getId());
    assertEquals(3, loads.get());
  }

  @Test
  public void testGetTableOtherFailureIsNotCachedAsNotFound() throws Exception {
    HMSQueryCache cache = new HMSQueryCache();
    try {
      cache.getTable(new GetTableRequest("db", "t"), "hive", () -> {
        throw new MetaException("HMS down");
      });
      fail("expected MetaException");
    } catch (MetaException expected) {
      // propagated to the caller
    }
    assertEquals(42L, cache.getTable(new GetTableRequest("db", "t"), "hive", () -> table(42L)).getId());
  }

  @Test
  public void testGetTableByNameLoadsOnMiss() throws Exception {
    HMSQueryCache cache = new HMSQueryCache();
    AtomicInteger loads = new AtomicInteger();
    Table table = table(42L);

    assertSame(table, cache.getTableByName("hive", "db", "t", counting(loads, table)));
    assertSame(table, cache.getTableByName("hive", "DB", "T", counting(loads, table(1L))));
    assertEquals(1, loads.get());
  }

  @Test
  public void testGetValidWriteIdsLoadsOnlyMissingTables() throws Exception {
    HMSQueryCache cache = new HMSQueryCache();
    List<List<String>> loaded = new ArrayList<>();
    HMSQueryCache.ValidWriteIdsLoader loader = missing -> {
      loaded.add(missing);
      return writeIds(missing);
    };

    assertEquals(List.of("db.b"), names(cache.getValidWriteIds(validWriteIdsRequest("txns", "db.b"), loader)));
    assertEquals(List.of("db.a", "db.b", "db.c"),
        names(cache.getValidWriteIds(validWriteIdsRequest("txns", "db.a", "db.b", "db.c"), loader)));
    assertEquals(List.of("db.c", "db.a"),
        names(cache.getValidWriteIds(validWriteIdsRequest("txns", "db.c", "db.a"), loader)));

    assertEquals(List.of(List.of("db.b"), List.of("db.a", "db.c")), loaded);
  }

  @Test
  public void testGetValidWriteIdsKeyedByValidTxnList() throws Exception {
    HMSQueryCache cache = new HMSQueryCache();
    List<List<String>> loaded = new ArrayList<>();
    HMSQueryCache.ValidWriteIdsLoader loader = missing -> {
      loaded.add(missing);
      return writeIds(missing);
    };

    cache.getValidWriteIds(validWriteIdsRequest("txns1", "db.a"), loader);
    cache.getValidWriteIds(validWriteIdsRequest("txns2", "db.a"), loader);

    assertEquals(2, loaded.size());
  }
}
