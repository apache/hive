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

package org.apache.hadoop.hive.ql.metadata.events;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.security.authorization.plugin.AuthorizationMetaStoreFilterHook;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.benmanes.caffeine.cache.Ticker;

public class TestTableVersionRegistry {
  private static final Function<String, String> NO_METASTORE_CONF = key -> "";

  private final TableVersionRegistry registry = TableVersionRegistry.get();
  private final AtomicLong nanos = new AtomicLong();

  @Before
  public void setUp() {
    TableVersionRegistry.ticker = nanos::get;
    assertTrue(registry.enable(new HiveConf(), NO_METASTORE_CONF));
  }

  @After
  public void tearDown() {
    registry.disable();
    TableVersionRegistry.ticker = Ticker.systemTicker();
    TableVersionRegistry.syncer = NotificationEventPoll::syncToLatest;
  }

  @Test
  public void testVersionStableUntilInvalidated() {
    long v1 = registry.getVersion("hive", "db", "t");
    assertEquals(v1, registry.getVersion(null, "DB", "T"));

    registry.invalidateTable("hive", "db", "other");
    assertEquals(v1, registry.getVersion("hive", "db", "t"));

    registry.invalidateTable(null, "Db", "t");
    long v2 = registry.getVersion("hive", "db", "t");
    assertTrue(v2 > v1);
  }

  @Test
  public void testDatabaseAndAllInvalidation() {
    long v1 = registry.getVersion("hive", "db", "t");
    registry.invalidateDatabase("hive", "db");
    long v2 = registry.getVersion("hive", "db", "t");
    assertTrue(v2 > v1);

    registry.invalidateAll();
    long v3 = registry.getVersion("hive", "db", "t");
    assertTrue(v3 > v2);
  }

  @Test
  public void testTablesNotAskedForAreNotTracked() {
    registry.invalidateTable("hive", "db", "never_asked");
    assertEquals(0, registry.tableCount());
  }

  @Test
  public void testMaxStaleness() {
    // the default of 5 minutes
    long v1 = registry.getVersion("hive", "db", "t");
    nanos.addAndGet(TimeUnit.MINUTES.toNanos(4));
    assertEquals("reading a version does not extend it", v1, registry.getVersion("hive", "db", "t"));
    nanos.addAndGet(TimeUnit.MINUTES.toNanos(2));
    long v2 = registry.getVersion("hive", "db", "t");
    assertTrue(v2 > v1);

    registry.disable();
    HiveConf conf = new HiveConf();
    conf.setTimeVar(HiveConf.ConfVars.HIVE_METADATA_CACHE_NONTRANSACTIONAL_MAX_STALENESS, 0, TimeUnit.SECONDS);
    assertTrue(registry.enable(conf, NO_METASTORE_CONF));
    long v3 = registry.getVersion("hive", "db", "t");
    nanos.addAndGet(TimeUnit.DAYS.toNanos(1));
    assertEquals("no limit", v3, registry.getVersion("hive", "db", "t"));
  }

  @Test
  public void testTableCountBounded() {
    long last = 0;
    for (int i = 0; i <= TableVersionRegistry.MAX_TABLES; i++) {
      last = Math.max(last, registry.getVersion("hive", "db", "t" + i));
    }
    assertTrue(registry.tableCount() <= TableVersionRegistry.MAX_TABLES);
    // a name that was evicted gets a version newer than any handed out before, so its entries are not found
    boolean newVersion = false;
    for (int i = 0; i <= TableVersionRegistry.MAX_TABLES && !newVersion; i++) {
      newVersion = registry.getVersion("hive", "db", "t" + i) > last;
    }
    assertTrue(newVersion);
  }

  @Test
  public void testFilterHookDisables() {
    registry.disable();
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTORE_FILTER_HOOK, "org.example.FilterHook");
    assertFalse(registry.enable(conf, NO_METASTORE_CONF));
    assertFalse(registry.isEnabled());

    // set by HS2 for the V2 authorizers; it filters nothing the cache holds
    conf.setVar(HiveConf.ConfVars.METASTORE_FILTER_HOOK, AuthorizationMetaStoreFilterHook.class.getName());
    assertTrue(registry.enable(conf, NO_METASTORE_CONF));
  }

  @Test
  public void testMetastoreAuthorizationDisablesWithDoAs() {
    String serverFilter = MetastoreConf.ConfVars.METASTORE_SERVER_FILTER_ENABLED.getVarname();
    String serverFilterHook = MetastoreConf.ConfVars.FILTER_HOOK.getVarname();
    String preEventListeners = MetastoreConf.ConfVars.PRE_EVENT_LISTENERS.getVarname();

    assertEnabled(true, true, Map.of(serverFilter, "true"));
    assertEnabled(false, true, Map.of(serverFilter, "true", serverFilterHook, "org.example.FilterHook"));
    assertEnabled(true, true, Map.of(serverFilter, "false", serverFilterHook, "org.example.FilterHook"));
    assertEnabled(true, true,
        Map.of(serverFilter, "true", serverFilterHook, AuthorizationMetaStoreFilterHook.class.getName()));
    assertEnabled(false, true, Map.of(preEventListeners, "org.example.Authorizer"));
    // HMS sees HS2 itself without doAs, so whatever it authorizes is the same for every session
    assertEnabled(true, false, Map.of(serverFilter, "true", serverFilterHook, "org.example.FilterHook",
        preEventListeners, "org.example.Authorizer"));

    registry.disable();
    assertFalse("the metastore could not be asked", registry.enable(new HiveConf(), key -> {
      throw new IllegalStateException("unreachable");
    }));
  }

  private void assertEnabled(boolean expected, boolean doAs, Map<String, String> metastoreConf) {
    registry.disable();
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, doAs);
    assertEquals(metastoreConf + ", doAs=" + doAs, expected,
        registry.enable(conf, key -> metastoreConf.getOrDefault(key, "")));
  }

  @Test
  public void testStartQuery() {
    HiveConf conf = new HiveConf();
    Map<Object, Object> queryCache = new HashMap<>();
    TableVersionRegistry.startQuery(conf, queryCache);
    assertNull("not opted in", queryCache.get(TableVersionRegistry.QUERY_SETTINGS_KEY));

    conf.setVar(HiveConf.ConfVars.HIVE_METADATA_CACHE_NONTRANSACTIONAL_MODE, "strict");
    TableVersionRegistry.startQuery(conf, queryCache);
    assertNull("no poller running", queryCache.get(TableVersionRegistry.QUERY_SETTINGS_KEY));

    TableVersionRegistry.syncer = timeoutMs -> false;
    TableVersionRegistry.startQuery(conf, queryCache);
    assertNull("the events could not be applied", queryCache.get(TableVersionRegistry.QUERY_SETTINGS_KEY));

    TableVersionRegistry.syncer = timeoutMs -> true;
    TableVersionRegistry.startQuery(conf, queryCache);
    assertTrue(queryCache.get(TableVersionRegistry.QUERY_SETTINGS_KEY) instanceof TableVersionRegistry.QuerySettings);
  }

  @Test
  public void testModes() {
    assertNotEquals(null, HiveConf.ConfVars.HIVE_METADATA_CACHE_NONTRANSACTIONAL_MODE.validate("bounded"));
    assertNull(HiveConf.ConfVars.HIVE_METADATA_CACHE_NONTRANSACTIONAL_MODE.validate("strict"));
    assertNull(HiveConf.ConfVars.HIVE_METADATA_CACHE_NONTRANSACTIONAL_MODE.validate("off"));
  }

  @Test
  public void testCanCache() {
    HiveConf conf = new HiveConf();
    TableVersionRegistry.QuerySettings settings = new TableVersionRegistry.QuerySettings(conf);

    assertTrue(settings.canCache(table("t", TableType.EXTERNAL_TABLE)));
    Table disabled = table("t", TableType.MANAGED_TABLE);
    disabled.getParameters().put(TableVersionRegistry.TABLE_CACHE_DISABLED_PROPERTY, "true");
    assertFalse(settings.canCache(disabled));

    conf.setBoolVar(HiveConf.ConfVars.HIVE_DISABLE_UNSAFE_EXTERNALTABLE_OPERATIONS, true);
    settings = new TableVersionRegistry.QuerySettings(conf);
    assertFalse(settings.canCache(table("t", TableType.EXTERNAL_TABLE)));
    assertTrue(settings.canCache(table("t", TableType.MANAGED_TABLE)));
  }

  private static Table table(String name, TableType type) {
    Table table = new Table();
    table.setDbName("db");
    table.setTableName(name);
    table.setTableType(type.name());
    table.setParameters(new HashMap<>());
    return table;
  }
}
