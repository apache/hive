/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest;

import org.apache.hadoop.hive.metastore.ServletSecurity.AuthType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.rest.extension.HiveRESTCatalogServerExtension;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the two-level table caching behaviour of {@link HMSCachingCatalog}: L2 (Caffeine) hits
 * return the same instance, explicit invalidation and drop evict the cache, the L1 recency guard
 * can be disabled, and a table dropped underneath the cache reports not-found rather than serving
 * a stale instance.
 */
@Category(MetastoreCheckinTest.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestHMSCachingCatalogCache {

  private static final long CACHE_EXPIRY_MS = 5 * 60 * 1_000L;
  private static final String NS = "cache_test_ns";
  private static final Namespace NAMESPACE = Namespace.of(NS);
  private static final String TABLE = "cache_test_table";
  private static final TableIdentifier TABLE_ID = TableIdentifier.of(NAMESPACE, TABLE);
  private static final Schema SCHEMA = new Schema();

  @RegisterExtension
  private static final HiveRESTCatalogServerExtension SERVER =
      HiveRESTCatalogServerExtension.builder(AuthType.NONE)
          .configure(MetastoreConf.ConfVars.ICEBERG_CATALOG_CACHE_EXPIRY.getVarname(),
              String.valueOf(CACHE_EXPIRY_MS))
          .configure("hive.in.test", "true")
          .build();

  private HiveCatalog hiveCatalog;
  private HMSCachingCatalog catalog;

  @BeforeAll
  void setupAll() {
    hiveCatalog = SERVER.newServerCatalog();
  }

  @BeforeEach
  void setupEach() {
    catalog = new HMSCachingCatalog(hiveCatalog, CACHE_EXPIRY_MS);
    hiveCatalog.createNamespace(NAMESPACE);
  }

  @AfterEach
  void cleanup() {
    try { hiveCatalog.dropTable(TABLE_ID, false); } catch (Exception ignored) {}
    try { hiveCatalog.dropNamespace(NAMESPACE); } catch (Exception ignored) {}
    // Do not call catalog.close() — it would unregister the JMX MBean that other test
    // classes (e.g. TestHMSCachingCatalogStats) rely on via the server-side catalog.
  }

  @Test
  void testL2CacheReturnsSameTableInstance() {
    hiveCatalog.createTable(TABLE_ID, SCHEMA);

    Table first = catalog.loadTable(TABLE_ID);
    Table second = catalog.loadTable(TABLE_ID);

    // Caffeine stores object references; a cache hit returns the identical instance.
    assertThat(first).isSameAs(second);
  }

  @Test
  void testInvalidateTableForcesReload() {
    hiveCatalog.createTable(TABLE_ID, SCHEMA);

    Table before = catalog.loadTable(TABLE_ID);
    assertThat(before.currentSnapshot()).isNull(); // fresh table, no snapshot yet

    // Advance the underlying table's metadata location by committing a new snapshot.
    Table raw = hiveCatalog.loadTable(TABLE_ID);
    DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(raw.location() + "/data/file-0.parquet")
        .withFileSizeInBytes(1024).withRecordCount(1).build();
    raw.newAppend().appendFile(file).commit();

    // Explicit invalidation evicts both the L2 and L1 caches.
    catalog.invalidateTable(TABLE_ID);

    Table after = catalog.loadTable(TABLE_ID);
    assertThat(after).isNotSameAs(before);
    assertThat(after.currentSnapshot()).isNotNull();
    assertThat(after.currentSnapshot().snapshotId()).isEqualTo(raw.currentSnapshot().snapshotId());
  }

  @Test
  void testDropTableEvictsCache() {
    // Create a table with a snapshot so the cached version has observable state.
    hiveCatalog.createTable(TABLE_ID, SCHEMA);
    Table raw = hiveCatalog.loadTable(TABLE_ID);
    DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(raw.location() + "/data/file-0.parquet")
        .withFileSizeInBytes(1024).withRecordCount(1).build();
    raw.newAppend().appendFile(file).commit();

    Table cached = catalog.loadTable(TABLE_ID);
    assertThat(cached.currentSnapshot()).isNotNull();

    // Drop clears both the L2 and L1 caches via invalidateTable.
    catalog.dropTable(TABLE_ID);

    // Recreate a fresh empty table (no snapshot) and reload.
    hiveCatalog.createTable(TABLE_ID, SCHEMA);

    Table reloaded = catalog.loadTable(TABLE_ID);
    assertThat(reloaded).isNotSameAs(cached);
    assertThat(reloaded.currentSnapshot()).isNull();
  }

  @Test
  void testLoadTableWithL1CacheDisabled() {
    Table created = hiveCatalog.createTable(TABLE_ID, SCHEMA);

    // Disable the L1 recency guard; its backing map is then an immutable empty map, so any write
    // to it would throw. The second load exercises the L2-hit path that records L1 freshness.
    var conf = hiveCatalog.getConf();
    int prevSize = conf.getInt("hms.caching.catalog.l1.cache.size", 32);
    int prevTtl = conf.getInt("hms.caching.catalog.l1.cache.ttl", 3_000);
    conf.setInt("hms.caching.catalog.l1.cache.size", 0);
    try {
      HMSCachingCatalog noL1 = new HMSCachingCatalog(hiveCatalog, CACHE_EXPIRY_MS);
      Table first = noL1.loadTable(TABLE_ID);
      Table second = noL1.loadTable(TABLE_ID);
      assertThat(first.location()).isEqualTo(created.location());
      assertThat(second.location()).isEqualTo(created.location());
    } finally {
      conf.setInt("hms.caching.catalog.l1.cache.size", prevSize);
      conf.setInt("hms.caching.catalog.l1.cache.ttl", prevTtl);
    }
  }

  @Test
  void testReloadOfDroppedTableThrowsNoSuchTable() {
    hiveCatalog.createTable(TABLE_ID, SCHEMA);

    // Disable the L1 recency guard so the second load re-checks the HMS location instead of
    // short-circuiting on L1 freshness.
    var conf = hiveCatalog.getConf();
    int prevSize = conf.getInt("hms.caching.catalog.l1.cache.size", 32);
    conf.setInt("hms.caching.catalog.l1.cache.size", 0);
    try {
      HMSCachingCatalog noL1 = new HMSCachingCatalog(hiveCatalog, CACHE_EXPIRY_MS);

      // Warm the L2 cache with the table.
      Table loaded = noL1.loadTable(TABLE_ID);
      assertThat(loaded).isNotNull();

      // Drop the table straight through the underlying catalog so noL1's L2 entry is left stale.
      hiveCatalog.dropTable(TABLE_ID, false);

      // Reloading must not serve the ghost: the null HMS location evicts the entry and signals
      // not-found.
      assertThatThrownBy(() -> noL1.loadTable(TABLE_ID))
          .isInstanceOf(NoSuchTableException.class);
    } finally {
      conf.setInt("hms.caching.catalog.l1.cache.size", prevSize);
    }
  }
}
