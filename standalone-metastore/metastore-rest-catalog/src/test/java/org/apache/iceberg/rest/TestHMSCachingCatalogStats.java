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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import org.apache.hadoop.hive.metastore.ServletSecurity.AuthType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.rest.extension.HiveRESTCatalogServerExtension;
import org.apache.iceberg.rest.responses.HMSCacheStatsResponse;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Integration tests that verify the {@link HMSCachingCatalog} cache-statistics counters
 * (hit, miss, load, hit-rate) are updated correctly and exposed accurately via the
 * {@code GET v1/cache/stats} REST endpoint.
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
  private static final HiveRESTCatalogServerExtension REST_CATALOG_EXTENSION =
      HiveRESTCatalogServerExtension.builder(AuthType.NONE)
          // Without a positive expiry the HMSCatalogFactory skips HMSCachingCatalog entirely.
          .configure(
              MetastoreConf.ConfVars.ICEBERG_CATALOG_CACHE_EXPIRY.getVarname(),
              String.valueOf(CACHE_EXPIRY_MS))
          .configure("hive.in.test", "true")
          .build();

  private RESTCatalog catalog;
  private HiveCatalog serverCatalog;

  @BeforeAll
  void setupAll() {
    catalog = RCKUtils.initCatalogClient(clientConfig());
    serverCatalog = HMSCachingCatalog.getLatestCache(HMSCachingCatalog::getCatalog);
  }

  /** Remove any namespace/table created by the test so each run starts clean. */
  @AfterEach
  void cleanup() {
    RCKUtils.purgeCatalogTestEntries(catalog);
  }

  // ---------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------

  private java.util.Map<String, String> clientConfig() {
    return java.util.Map.of("uri", REST_CATALOG_EXTENSION.getRestEndpoint());
  }

  /**
   * Calls the {@code GET v1/cache/stats} endpoint directly over HTTP and returns
   * the deserialised {@link HMSCacheStatsResponse}.
   */
  private HMSCacheStatsResponse fetchCacheStats() throws Exception {
    String statsUrl = REST_CATALOG_EXTENSION.getRestEndpoint() + "/v1/cache/stats";
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(statsUrl))
        .GET()
        .build();
    HttpResponse<String> response;
    try (HttpClient client = HttpClient.newHttpClient()) {
      response = client.send(request, HttpResponse.BodyHandlers.ofString());
    }
    Assertions.assertEquals(200, response.statusCode(),
        "Expected HTTP 200 from cache stats endpoint, got: " + response.statusCode());
    return new ObjectMapper().readValue(response.body(), HMSCacheStatsResponse.class);
  }


  /**
   * Verifies that the {@link HMSCachingCatalog} correctly tracks cache hits, misses and
   * loads, and that those counters are accurately returned via the REST endpoint.
   *
   * <p>Strategy:
   * <ol>
   *   <li>Snapshot baseline stats before any operations so the test is isolated from
   *       cumulative counters left by previous tests.</li>
   *   <li>Create a namespace and a table (bypasses the cache – done via
   *       {@link org.apache.iceberg.hive.HiveCatalog} directly).</li>
   *   <li>First {@code loadTable} call → cache miss + actual load.</li>
   *   <li>Second and third {@code loadTable} calls → cache hits (metadata location
   *       has not changed, so the cached entry is still valid).</li>
   *   <li>Fetch stats again and assert the deltas against the baseline.</li>
   * </ol>
   */
  @Test
  void testCacheCountersAreUpdated() throws Exception {
    // -- baseline ---------------------------------------------------------------
    Map<String, Number> baseline = fetchCacheStats().stats();
    long baseHit  = baseline.getOrDefault("hit",  0L).longValue();
    long baseMiss = baseline.getOrDefault("miss", 0L).longValue();
    long baseLoad = baseline.getOrDefault("load", 0L).longValue();

    // -- exercise the cache -----------------------------------------------------
    var db      = Namespace.of("caching_stats_test_db");
    var tableId = TableIdentifier.of(db, "caching_stats_test_table");

    catalog.createNamespace(db);
    catalog.createTable(tableId, new Schema());

    // First load  → cache miss + load
    catalog.loadTable(tableId);
    // Second load → cache hit  (metadata location unchanged)
    catalog.loadTable(tableId);
    // Third load  → cache hit
    catalog.loadTable(tableId);

    // Mutate the table by appending a data file – this creates a new snapshot
    // which advances METADATA_LOCATION in HMS, so the next loadTable call through
    // the caching catalog will detect the stale cached location and invalidate it.
    Table table = serverCatalog.loadTable(tableId);
    DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(table.location() + "/data/fake-0.parquet")
        .withFileSizeInBytes(1024)
        .withRecordCount(1)
        .build();
    table.newAppend()
        .appendFile(dataFile)
        .commit();

    long baseInvalidate = fetchCacheStats().stats().getOrDefault("invalidate", 0L).longValue();
    // the L1 cache has a 3 seconds default delay before it considers entries stale
    Thread.sleep(3_000);
    // Fourth load → cache invalidation + load (cached location != HMS location)
    catalog.loadTable(tableId);

    // -- fetch updated stats via the REST endpoint ------------------------------
    Map<String, Number> after = fetchCacheStats().stats();
    long deltaHit        = after.getOrDefault("hit",        0L).longValue() - baseHit;
    long deltaMiss       = after.getOrDefault("miss",       0L).longValue() - baseMiss;
    long deltaLoad       = after.getOrDefault("load",       0L).longValue() - baseLoad;
    long deltaInvalidate = after.getOrDefault("invalidate", 0L).longValue() - baseInvalidate;

    // -- assertions -------------------------------------------------------------
    Assertions.assertTrue(deltaMiss >= 1,
        "Expected at least 1 cache miss (first loadTable), but delta was: " + deltaMiss);
    Assertions.assertTrue(deltaLoad >= 2,
        "Expected at least 2 cache loads (initial load + post-invalidation reload), but delta was: " + deltaLoad);
    Assertions.assertTrue(deltaHit >= 2,
        "Expected at least 2 cache hits (second + third loadTable), but delta was: " + deltaHit);
    Assertions.assertTrue(deltaInvalidate >= 1,
        "Expected at least 1 cache invalidation (metadata location changed after table update), but delta was: " + deltaInvalidate);

    // hit-rate must be a valid ratio in [0.0, 1.0]
    double hitRate = after.getOrDefault("hit-rate", 0.0).doubleValue();
    Assertions.assertTrue(hitRate > 0.0 && hitRate <= 1.0,
        "hit-rate must be in [0.0, 1.0] but was: " + hitRate);
  }
}

