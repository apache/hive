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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestTableVersionRegistry {
  private final TableVersionRegistry registry = TableVersionRegistry.get();

  @Before
  public void setUp() {
    assertTrue(registry.enable(new HiveConf()));
  }

  @After
  public void tearDown() {
    registry.disable();
  }

  @Test
  public void testVersionStableUntilInvalidated() {
    long v1 = registry.getVersion("hive", "db", "t", 0);
    assertEquals(v1, registry.getVersion(null, "DB", "T", 0));

    registry.invalidateTable("hive", "db", "other");
    assertEquals(v1, registry.getVersion("hive", "db", "t", 0));

    registry.invalidateTable(null, "Db", "t");
    long v2 = registry.getVersion("hive", "db", "t", 0);
    assertTrue(v2 > v1);
  }

  @Test
  public void testDatabaseAndAllInvalidation() {
    long v1 = registry.getVersion("hive", "db", "t", 0);
    registry.invalidateDatabase("hive", "db");
    long v2 = registry.getVersion("hive", "db", "t", 0);
    assertTrue(v2 > v1);

    registry.invalidateAll();
    long v3 = registry.getVersion("hive", "db", "t", 0);
    assertTrue(v3 > v2);
  }

  @Test
  public void testTablesNotAskedForAreNotTracked() {
    registry.invalidateTable("hive", "db", "never_asked");
    assertEquals(0, registry.tableCount());
  }

  @Test
  public void testMaxStaleness() throws Exception {
    long v1 = registry.getVersion("hive", "db", "t", 60_000);
    assertEquals(v1, registry.getVersion("hive", "db", "t", 60_000));
    Thread.sleep(5);
    assertNotEquals(v1, registry.getVersion("hive", "db", "t", 1));
  }

  @Test
  public void testFilterHookDisables() {
    registry.disable();
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTORE_FILTER_HOOK, "org.example.FilterHook");
    assertFalse(registry.enable(conf));
    assertFalse(registry.isEnabled());
  }

  @Test
  public void testStartQuery() {
    HiveConf conf = new HiveConf();
    Map<Object, Object> queryCache = new HashMap<>();
    TableVersionRegistry.startQuery(conf, queryCache);
    assertNull("not opted in", queryCache.get(TableVersionRegistry.QUERY_SETTINGS_KEY));

    conf.setVar(HiveConf.ConfVars.HIVE_METADATA_CACHE_NONTRANSACTIONAL_MODE, "strict");
    TableVersionRegistry.startQuery(conf, queryCache);
    assertNull("strict mode with no poller running", queryCache.get(TableVersionRegistry.QUERY_SETTINGS_KEY));

    conf.setVar(HiveConf.ConfVars.HIVE_METADATA_CACHE_NONTRANSACTIONAL_MODE, "bounded");
    TableVersionRegistry.startQuery(conf, queryCache);
    assertTrue(queryCache.get(TableVersionRegistry.QUERY_SETTINGS_KEY) instanceof TableVersionRegistry.QuerySettings);
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
