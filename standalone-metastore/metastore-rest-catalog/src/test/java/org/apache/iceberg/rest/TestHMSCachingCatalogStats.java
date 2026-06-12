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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Integration tests that verify the {@link HMSCachingCatalog} cache-statistics counters
 * (hit, miss, load, invalidate, l1-hit, l1-miss, and their rates) are updated correctly
 * and exposed accurately via the JMX MBean registered under
 * {@code org.apache.iceberg.rest:type=HMSCachingCatalog,name=*}.
 *
 * <p>The server is started with {@link AuthType#NONE} so the tests focus purely on
 * caching behaviour without any authentication noise.
 */
@Category(MetastoreCheckinTest.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestHMSCachingCatalogStats {

  /** 5 minutes expressed in milliseconds – the value injected into {@code ICEBERG_CATALOG_CACHE_EXPIRY}. */
  private static final long CACHE_EXPIRY_MS = 5 * 60 * 1_000L;

  @RegisterExtension
  private static final HiveRESTCatalogServerExtension REST_CATALOG_EXTENSION = HiveRESTCatalogServerExtension.builder(AuthType.NONE)
      // Without a positive expiry the HMSCatalogFactory skips HMSCachingCatalog entirely.
      .configure(MetastoreConf.ConfVars.ICEBERG_CATALOG_CACHE_EXPIRY.getVarname(), String.valueOf(CACHE_EXPIRY_MS))
      .configure("hive.in.test", "true").build();

  private RESTCatalog catalog;
  private HiveCatalog serverCatalog;
  /** The server-side {@link HMSCachingCatalog} instance; used to invalidate entries directly. */
  private HMSCachingCatalog serverCachingCatalog;
  /** The platform {@link MBeanServer} used for all JMX-based assertions. */
  private MBeanServer mbs;
  /** Resolved once in {@link #setupAll()} and reused across every test. */
  private ObjectName jmxObjectName;

  @BeforeAll
  void setupAll() throws Exception {
    catalog = RCKUtils.initCatalogClient(java.util.Map.of("uri", REST_CATALOG_EXTENSION.getRestEndpoint()));
    serverCachingCatalog = HMSCachingCatalog.getLatestCache(null);
    Assertions.assertNotNull(serverCachingCatalog, "Expected HMSCachingCatalog to be initialized");
    serverCatalog = serverCachingCatalog.getCatalog();

    // Resolve the JMX ObjectName registered by HMSCachingCatalog. We use a wildcard
    // so the test is independent of the exact catalog name.
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
    RCKUtils.purgeCatalogTestEntries(catalog);
  }

  // ---------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------

  /**
   * Reads a single JMX attribute from the {@link HMSCachingCatalogMXBean}.
   *
   * @param attribute the attribute name as declared in {@link HMSCachingCatalogMXBean}
   *                  (e.g. {@code "CacheHitCount"})
   * @return the attribute value
   */
  private Object getJmxAttribute(String attribute) throws Exception {
    return mbs.getAttribute(jmxObjectName, attribute);
  }

  /**
   * Convenience wrapper that reads a {@code long} JMX attribute.
   */
  private long jmxLong(String attribute) throws Exception {
    return (long) getJmxAttribute(attribute);
  }

  /**
   * Convenience wrapper that reads a {@code double} JMX attribute.
   */
  private double jmxDouble(String attribute) throws Exception {
    return (double) getJmxAttribute(attribute);
  }

  /**
   * Invokes a void JMX operation on the {@link HMSCachingCatalogMXBean}.
   *
   * @param operationName the operation name (e.g. {@code "resetCacheStats"})
   */
  private void invokeJmxOperation(String operationName) throws Exception {
    mbs.invoke(jmxObjectName, operationName, new Object[0], new String[0]);
  }

  // ---------------------------------------------------------------------------
  // tests
  // ---------------------------------------------------------------------------

  /**
   * Verifies that the {@link HMSCachingCatalog} correctly tracks cache hits, misses,
   * loads, invalidations, L1 hits, and L1 misses via JMX.
   *
   * <p>Strategy:
   * <ol>
   *   <li>Snapshot JMX baseline counters before any operations so the test is isolated
   *       from cumulative state left by previous tests.</li>
   *   <li>Create a namespace and a table.</li>
   *   <li>First {@code loadTable} call → cache miss + actual load.</li>
   *   <li>Second and third rapid {@code loadTable} calls → L1 cache hits (TTL still valid).</li>
   *   <li>Mutate the table to advance its metadata location in HMS.</li>
   *   <li>Wait for the L1 TTL to expire, then reload → L1 miss + invalidation + reload.</li>
   *   <li>Assert JMX counter deltas match expectations.</li>
   * </ol>
   */
  @Test
  void testCacheCountersAreUpdated() throws Exception {
    // -- JMX baseline -----------------------------------------------------------
    long baseHit = jmxLong("CacheHitCount");
    long baseMiss = jmxLong("CacheMissCount");
    long baseLoad = jmxLong("CacheLoadCount");
    long baseL1Hit = jmxLong("L1CacheHitCount");

    // -- exercise the cache -----------------------------------------------------
    var db = Namespace.of("caching_stats_test_db");
    var tableId = TableIdentifier.of(db, "caching_stats_test_table");

    catalog.createNamespace(db);
    catalog.createTable(tableId, new Schema());

    // First load  → cache miss + load
    catalog.loadTable(tableId);
    // Second load → L1 hit  (within TTL, HMS location check skipped)
    catalog.loadTable(tableId);
    // Third load  → L1 hit
    catalog.loadTable(tableId);

    // Mutate the table by appending a data file – this creates a new snapshot
    // which advances METADATA_LOCATION in HMS, so the next loadTable call through
    // the caching catalog will detect the stale cached location and invalidate it.
    Table table = serverCatalog.loadTable(tableId);
    DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(table.location() + "/data/fake-0.parquet")
        .withFileSizeInBytes(1024).withRecordCount(1).build();
    table.newAppend().appendFile(dataFile).commit();

    long baseInvalidate = jmxLong("CacheInvalidateCount");
    // The L1 cache has a 3-second default TTL; wait for entries to expire.
    Thread.sleep(3_000);
    // Fourth load → L1 miss + cache invalidation + reload
    catalog.loadTable(tableId);

    // -- JMX assertions ---------------------------------------------------------
    long deltaHit = jmxLong("CacheHitCount") - baseHit;
    long deltaMiss = jmxLong("CacheMissCount") - baseMiss;
    long deltaLoad = jmxLong("CacheLoadCount") - baseLoad;
    long deltaInvalidate = jmxLong("CacheInvalidateCount") - baseInvalidate;
    long deltaL1Hit = jmxLong("L1CacheHitCount") - baseL1Hit;
    long deltaL1Miss = jmxLong("L1CacheMissCount");   // absolute value is fine for L1 miss

    Assertions.assertTrue(deltaMiss >= 1,
        "Expected at least 1 cache miss (first loadTable), but delta was: " + deltaMiss);
    Assertions.assertTrue(deltaLoad >= 2,
        "Expected at least 2 cache loads (initial + post-invalidation reload), but delta was: " + deltaLoad);
    Assertions.assertTrue(deltaHit >= 2,
        "Expected at least 2 cache hits (second + third loadTable), but delta was: " + deltaHit);
    Assertions.assertTrue(deltaInvalidate >= 1,
        "Expected at least 1 cache invalidation (metadata location changed), but delta was: " + deltaInvalidate);

    // L1 hits: the 2nd and 3rd loadTable calls should have been served by L1.
    Assertions.assertTrue(deltaL1Hit >= 2,
        "Expected at least 2 L1 cache hits (rapid successive loads within TTL), but delta was: " + deltaL1Hit);
    // L1 miss: at least the fourth load (after TTL expiry) must have missed L1.
    Assertions.assertTrue(deltaL1Miss >= 1,
        "Expected at least 1 L1 cache miss (after TTL expiry), but was: " + deltaL1Miss);

    // Rate attributes must be valid ratios in [0.0, 1.0].
    double hitRate = jmxDouble("CacheHitRate");
    Assertions.assertTrue(hitRate > 0.0 && hitRate <= 1.0,
        "CacheHitRate must be in (0.0, 1.0] but was: " + hitRate);

    double l1HitRate = jmxDouble("L1CacheHitRate");
    Assertions.assertTrue(l1HitRate > 0.0 && l1HitRate <= 1.0,
        "L1CacheHitRate must be in (0.0, 1.0] but was: " + l1HitRate);
  }

  /**
   * Verifies that the {@code resetCacheStats} JMX operation zeroes all counters.
   *
   * <p>Strategy:
   * <ol>
   *   <li>Perform some cache operations to ensure all counters are non-zero.</li>
   *   <li>Invoke {@code resetCacheStats()} via JMX.</li>
   *   <li>Assert that every JMX counter attribute reads {@code 0} / {@code 0.0}.</li>
   * </ol>
   */
  @Test
  void testJmxResetCacheStats() throws Exception {
    // -- warm up counters -------------------------------------------------------
    var db = Namespace.of("jmx_reset_test_db");
    var tableId = TableIdentifier.of(db, "jmx_reset_test_table");
    catalog.createNamespace(db);
    catalog.createTable(tableId, new Schema());
    catalog.loadTable(tableId);  // miss + load
    catalog.loadTable(tableId);  // hit (L1 hit on the fast path)

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
    // resetCacheStats() zeroes counters but does NOT evict the L2/L1 cache, so the
    // table is still cached. Invalidate it directly on the server-side HMSCachingCatalog
    // so the first post-reset load is a genuine cold miss rather than an L1/L2 hit.
    // NOTE: catalog.invalidateTable() only clears the REST *client* state and does not
    // reach the server-side cache.
    serverCachingCatalog.invalidateTable(tableId);

    // First load after reset: cache miss + load (L1 cold, L2 cold).
    catalog.loadTable(tableId);
    // Second load: L1 hit (within TTL).
    catalog.loadTable(tableId);
    // Third load: L1 hit (within TTL).
    catalog.loadTable(tableId);

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
