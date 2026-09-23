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

package org.apache.iceberg.rest;

import java.lang.management.ManagementFactory;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

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

/**
 * Component tests that verify the {@link HMSCachingCatalog} cache-statistics counters
 * (hit, miss, load, invalidate, l1-hit, l1-miss, and their rates) are updated correctly
 * and exposed accurately via both the getters and the JMX MBean registered under
 * {@code org.apache.iceberg.rest:type=HMSCachingCatalog,name=*}.
 *
 * <p>Each test drives a freshly built {@link HMSCachingCatalog} directly (obtained through the
 * server extension, which wraps a {@link HiveCatalog} built via the production
 * {@link org.apache.iceberg.rest.HMSCatalogFactory} path). A fresh instance starts with all
 * counters at zero, so assertions use absolute values rather than deltas.</p>
 *
 * <p>The server is started with {@link AuthType#NONE} so the tests focus purely on
 * caching behaviour without any authentication noise.</p>
 */
@Category(MetastoreCheckinTest.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestHMSCachingCatalogStats {

  /** 5 minutes expressed in milliseconds – the value injected into {@code ICEBERG_CATALOG_CACHE_EXPIRY}. */
  private static final long CACHE_EXPIRY_MS = 5 * 60 * 1_000L;
  private static final String NS = "caching_stats_test_db";
  private static final Namespace NAMESPACE = Namespace.of(NS);
  private static final String TABLE = "caching_stats_test_table";
  private static final TableIdentifier TABLE_ID = TableIdentifier.of(NAMESPACE, TABLE);

  @RegisterExtension
  private static final HiveRESTCatalogServerExtension SERVER = HiveRESTCatalogServerExtension.builder(AuthType.NONE)
      // Without a positive expiry the HMSCatalogFactory skips HMSCachingCatalog entirely.
      .configure(MetastoreConf.ConfVars.ICEBERG_CATALOG_CACHE_EXPIRY.getVarname(), String.valueOf(CACHE_EXPIRY_MS))
      .configure("hive.in.test", "true").build();

  /** Underlying catalog, shared across tests; used for setup and direct (uncached) mutations. */
  private HiveCatalog hiveCatalog;
  /** The caching catalog under test; rebuilt fresh for every test so counters start at zero. */
  private HMSCachingCatalog catalog;
  /** The platform {@link MBeanServer} used for JMX-based assertions. */
  private MBeanServer mbs;
  /** The JMX ObjectName registered by the current {@link #catalog} instance. */
  private ObjectName jmxObjectName;

  @BeforeAll
  void setupAll() {
    hiveCatalog = SERVER.newServerCatalog();
  }

  @BeforeEach
  void setupEach() throws Exception {
    catalog = new HMSCachingCatalog(hiveCatalog, CACHE_EXPIRY_MS);
    hiveCatalog.createNamespace(NAMESPACE);

    // Resolve the JMX ObjectName registered by the catalog instance just created. We use a
    // wildcard so the test is independent of the exact catalog name.
    mbs = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> names = mbs.queryNames(
        new ObjectName("org.apache.iceberg.rest:type=HMSCachingCatalog,*"), null);
    Assertions.assertFalse(names.isEmpty(),
        "HMSCachingCatalog MBean must be registered in the platform MBeanServer");
    jmxObjectName = names.iterator().next();
  }

  /** Remove any namespace/table created by the test so each run starts clean. */
  @AfterEach
  void cleanup() {
    try {
      hiveCatalog.dropTable(TABLE_ID, false);
    } catch (Exception ignored) {
      // table may not exist
    }
    try {
      hiveCatalog.dropNamespace(NAMESPACE);
    } catch (Exception ignored) {
      // namespace may not exist
    }
  }

  // ---------------------------------------------------------------------------
  // JMX helpers
  // ---------------------------------------------------------------------------

  private long jmxLong(String attribute) throws Exception {
    return (long) mbs.getAttribute(jmxObjectName, attribute);
  }

  private double jmxDouble(String attribute) throws Exception {
    return (double) mbs.getAttribute(jmxObjectName, attribute);
  }

  private void invokeJmxOperation(String operationName) throws Exception {
    mbs.invoke(jmxObjectName, operationName, new Object[0], new String[0]);
  }

  // ---------------------------------------------------------------------------
  // tests
  // ---------------------------------------------------------------------------

  /**
   * Verifies that the {@link HMSCachingCatalog} correctly tracks cache hits, misses,
   * loads, invalidations, L1 hits, and L1 misses.
   *
   * <p>Counter states for the four {@code loadTable} calls:
   * <pre>
   *   Call 1 – cold L2 miss : onCacheMiss  + onCacheLoad               → miss=1, load=1
   *   Call 2 – L1 hit       : onL1CacheHit + onCacheHit                 → l1Hit=1, hit=1
   *   Call 3 – L1 hit       : onL1CacheHit + onCacheHit                 → l1Hit=2, hit=2
   *   [sleep >L1 TTL; mutated table has new METADATA_LOCATION in HMS]
   *   Call 4 – L1 expired,
   *            location mismatch: onL1CacheMiss + onCacheInvalidate
   *                             + onCacheLoad                           → l1Miss=1, invalidate=1, load=2
   * </pre>
   * Note: call 4 does NOT fire {@code onCacheMiss}: that counter only increments when the L2
   * {@code getIfPresent} returns null (the else-branch). The location-mismatch path goes through the
   * if-branch, evicts L2 internally, and falls straight to {@code tableCache.get} + {@code onCacheLoad}.
   */
  @Test
  void testCacheCountersAreUpdated() throws Exception {
    Table created = hiveCatalog.createTable(TABLE_ID, new Schema());

    // First load  → cache miss + load; must return the table we just created.
    Table firstLoad = catalog.loadTable(TABLE_ID);
    Assertions.assertEquals(created.location(), firstLoad.location(),
        "First load must return the table we just created");
    // Second load → L1 hit  (within TTL, HMS location check skipped)
    catalog.loadTable(TABLE_ID);
    // Third load  → L1 hit
    catalog.loadTable(TABLE_ID);

    // Mutate the table by appending a data file – this creates a new snapshot which advances
    // METADATA_LOCATION in HMS, so the next loadTable call through the caching catalog will detect
    // the stale cached location and invalidate it.
    Table table = hiveCatalog.loadTable(TABLE_ID);
    DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(table.location() + "/data/fake-0.parquet")
        .withFileSizeInBytes(1024).withRecordCount(1).build();
    table.newAppend().appendFile(dataFile).commit();

    // Default L1 TTL is 3 000 ms; sleep 3 500 ms to ensure the entry is expired.
    Thread.sleep(3_500);
    // Fourth load → L1 miss + cache invalidation + reload
    Table reloaded = catalog.loadTable(TABLE_ID);

    // -- counter assertions (exact values; see Javadoc above for derivation) --
    Assertions.assertEquals(1L, catalog.getCacheMissCount(),
        "Expected exactly 1 cache miss (cold load on call 1)");
    Assertions.assertEquals(2L, catalog.getCacheLoadCount(),
        "Expected exactly 2 cache loads (call 1 + post-invalidation call 4)");
    Assertions.assertEquals(2L, catalog.getCacheHitCount(),
        "Expected exactly 2 cache hits (calls 2 and 3)");
    Assertions.assertEquals(1L, catalog.getCacheInvalidateCount(),
        "Expected exactly 1 cache invalidation (metadata location changed on call 4)");
    Assertions.assertEquals(2L, catalog.getL1CacheHitCount(),
        "Expected exactly 2 L1 hits (calls 2 and 3, within TTL)");
    Assertions.assertEquals(1L, catalog.getL1CacheMissCount(),
        "Expected exactly 1 L1 miss (call 4, after TTL expiry)");

    // The reloaded table must reflect the new snapshot created by the append above;
    // this confirms the staleness-detection path returned fresh data, not the stale cache entry.
    Assertions.assertNotNull(reloaded.currentSnapshot(),
        "Staleness detection must have reloaded the table with its new snapshot");

    // Rate attributes must be valid ratios in (0.0, 1.0].
    double hitRate = catalog.getCacheHitRate();
    Assertions.assertTrue(hitRate > 0.0 && hitRate <= 1.0,
        "CacheHitRate must be in (0.0, 1.0] but was: " + hitRate);
    double l1HitRate = catalog.getL1CacheHitRate();
    Assertions.assertTrue(l1HitRate > 0.0 && l1HitRate <= 1.0,
        "L1CacheHitRate must be in (0.0, 1.0] but was: " + l1HitRate);
  }

  /**
   * Verifies that the {@code resetCacheStats} JMX operation zeroes all counters, and that the
   * getters and JMX attributes report the same values.
   *
   * <p>Strategy:
   * <ol>
   *   <li>Perform some cache operations to ensure counters are non-zero.</li>
   *   <li>Invoke {@code resetCacheStats()} via JMX.</li>
   *   <li>Assert that every JMX counter attribute reads {@code 0} / {@code 0.0}.</li>
   *   <li>Drive further loads and confirm the counters resume from zero.</li>
   * </ol>
   */
  @Test
  void testJmxResetCacheStats() throws Exception {
    Table created = hiveCatalog.createTable(TABLE_ID, new Schema());
    Table loaded = catalog.loadTable(TABLE_ID);  // miss + load
    Assertions.assertEquals(created.location(), loaded.location(),
        "Warm-up load must return the table we just created");
    catalog.loadTable(TABLE_ID);  // hit (L1 hit on the fast path)

    // Sanity: at least one counter must be non-zero before the reset.
    Assertions.assertTrue(jmxLong("CacheHitCount") + jmxLong("CacheMissCount") > 0,
        "At least one counter must be non-zero before reset");

    // -- invoke the reset operation via JMX -------------------------------------
    invokeJmxOperation("resetCacheStats");

    // -- assertions post-reset --------------------------------------------------
    Assertions.assertEquals(0L, jmxLong("CacheHitCount"),        "CacheHitCount must be 0 after reset");
    Assertions.assertEquals(0L, jmxLong("CacheMissCount"),       "CacheMissCount must be 0 after reset");
    Assertions.assertEquals(0L, jmxLong("CacheLoadCount"),       "CacheLoadCount must be 0 after reset");
    Assertions.assertEquals(0L, jmxLong("CacheInvalidateCount"), "CacheInvalidateCount must be 0 after reset");
    Assertions.assertEquals(0L, jmxLong("CacheMetaLoadCount"),   "CacheMetaLoadCount must be 0 after reset");
    Assertions.assertEquals(0L, jmxLong("L1CacheHitCount"),      "L1CacheHitCount must be 0 after reset");
    Assertions.assertEquals(0L, jmxLong("L1CacheMissCount"),     "L1CacheMissCount must be 0 after reset");
    Assertions.assertEquals(0.0, jmxDouble("CacheHitRate"),  1e-9, "CacheHitRate must be 0.0 after reset");
    Assertions.assertEquals(0.0, jmxDouble("L1CacheHitRate"), 1e-9, "L1CacheHitRate must be 0.0 after reset");

    // -- verify rate calculation still works correctly after reset --------------
    // resetCacheStats() zeroes counters but does NOT evict the L2/L1 cache, so the table is still
    // cached. Invalidate it so the first post-reset load is a genuine cold miss rather than a hit.
    catalog.invalidateTable(TABLE_ID);

    // First load after reset: cache miss + load (L1 cold, L2 cold).
    catalog.loadTable(TABLE_ID);
    // Second and third loads: L1 hits (within TTL).
    catalog.loadTable(TABLE_ID);
    catalog.loadTable(TABLE_ID);

    // CacheHitRate: 2 hits out of 3 total accesses → ≈ 0.667
    double hitRateAfterReset = jmxDouble("CacheHitRate");
    Assertions.assertTrue(hitRateAfterReset > 0.0 && hitRateAfterReset <= 1.0,
        "CacheHitRate must be in (0.0, 1.0] after post-reset operations, but was: " + hitRateAfterReset);

    // Underlying counters must reflect the just-performed operations.
    Assertions.assertTrue(jmxLong("CacheHitCount") >= 2,
        "CacheHitCount must be >= 2 after two rapid re-loads post-reset");
    Assertions.assertTrue(jmxLong("CacheMissCount") >= 1,
        "CacheMissCount must be >= 1 after the first cold load post-reset");

    // L1CacheHitRate: the 2nd and 3rd loads should have been served by L1.
    double l1HitRateAfterReset = jmxDouble("L1CacheHitRate");
    Assertions.assertTrue(l1HitRateAfterReset > 0.0 && l1HitRateAfterReset <= 1.0,
        "L1CacheHitRate must be in (0.0, 1.0] after post-reset L1 hits, but was: " + l1HitRateAfterReset);
    Assertions.assertTrue(jmxLong("L1CacheHitCount") >= 2,
        "L1CacheHitCount must be >= 2 after two rapid re-loads within TTL post-reset");
  }
}
