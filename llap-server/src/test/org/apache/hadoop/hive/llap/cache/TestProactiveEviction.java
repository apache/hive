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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.cache;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.ProactiveEviction.Request;
import org.apache.hadoop.hive.llap.ProactiveEviction.Request.Builder;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.EvictEntityRequestProto;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.metastore.Warehouse;

import com.google.common.annotations.VisibleForTesting;

import org.junit.Test;

import static org.apache.hadoop.hive.llap.cache.LlapCacheableBuffer.INVALIDATE_OK;
import static org.apache.hadoop.hive.llap.cache.TestCacheContentsTracker.cacheTagBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test cases for proactive LLAP cache eviction.
 */
public class TestProactiveEviction {

  private static final CacheTag[] TEST_TAGS = new CacheTag[] {
    cacheTagBuilder("fx.rates", "from=USD", "to=HUF"),
    cacheTagBuilder("fx.rates", "from=USD", "to=EUR"),
    cacheTagBuilder("fx.rates", "from=USD", "to=EUR"),
    cacheTagBuilder("fx.rates", "from=USD", "to=EUR"),
    cacheTagBuilder("fx.rates", "from=EUR", "to=HUF"),
    cacheTagBuilder("fx.futures", "ccy=EUR"),
    cacheTagBuilder("fx.futures", "ccy=JPY"),
    cacheTagBuilder("fx.futures", "ccy=JPY"),
    cacheTagBuilder("fx.futures", "ccy=USD"),
    cacheTagBuilder("fx.centralbanks"),
    cacheTagBuilder("fx.centralbanks"),
    cacheTagBuilder("fx.centralbanks"),
    cacheTagBuilder("equity.prices", "ex=NYSE"),
    cacheTagBuilder("equity.prices", "ex=NYSE"),
    cacheTagBuilder("equity.prices", "ex=NASDAQ"),
    cacheTagBuilder("fixedincome.bonds"),
    cacheTagBuilder("fixedincome.bonds"),
    cacheTagBuilder("fixedincome.yieldcurves")
  };

  @Test
  public void testCachetagAndRequestMatching() throws Exception {
    assertMatchOnTags(Builder.create().addDb("fx"), "111111111111000000");
    assertMatchOnTags(Builder.create().addTable("fx", "futures"), "000001111000000000");
    assertMatchOnTags(Builder.create().addPartitionOfATable("fx", "futures", buildParts("ccy", "JPY")),
    "000000110000000000");
    assertMatchOnTags(Builder.create().addPartitionOfATable("equity", "prices", buildParts("ex", "NYSE"))
        .addPartitionOfATable("equity", "prices", buildParts("ex", "NYSE")),"000000000000110000");
    assertMatchOnTags(Builder.create().addTable("fx", "rates").addTable("fx", "futures"),
        "111111111000000000");
    assertMatchOnTags(Builder.create().addPartitionOfATable("fx", "rates", buildParts("from", "PLN")),
        "000000000000000000");
    assertMatchOnTags(Builder.create().addTable("fixedincome", "bonds"), "000000000000000110");
    assertMatchOnTags(Builder.create().addPartitionOfATable("fx", "rates", buildParts("from", "EUR", "to", "HUF")),
        "000010000000000000");
  }

  private static LinkedHashMap buildParts(String... vals) {
    LinkedHashMap<String, String> ret = new LinkedHashMap<>();
    for (int i = 0; i < vals.length; i+=2) {
      ret.put(vals[i], vals[i+1]);
    }
    return ret;
  }

  private static void assertMatchOnTags(Builder requestBuilder, String expected) {
    assert expected.length() == TEST_TAGS.length;
    // Marshal + unmarshal
    Request request = Builder.create().fromProtoRequest(requestBuilder.build().toProtoRequests().get(0)).build();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < TEST_TAGS.length; ++i) {
      sb.append(request.isTagMatch(TEST_TAGS[i]) ? '1' : '0');
    }
    assertEquals(expected, sb.toString());
  }

  /**
   * Verifies that passing an explicit catalog produces correct matching via isTagMatch.
   * TEST_TAGS all belong to the default catalog, so requests for a different catalog must not match.
   */
  @Test
  public void testCatalogAwareCacheTagAndRequestMatching() {
    // Default catalog matches as expected.
    assertMatchOnTags(Builder.create().addDb("fx"), "111111111111000000");
    assertMatchOnTags(Builder.create().addTable("fx", "futures"), "000001111000000000");
    assertMatchOnTags(Builder.create().addPartitionOfATable("fx", "futures",
            buildParts("ccy", "JPY")), "000000110000000000");
    assertMatchOnTags(Builder.create().addTable("fixedincome", "bonds"), "000000000000000110");
    assertMatchOnTags(Builder.create().addPartitionOfATable("fx", "rates",
            buildParts("from", "EUR", "to", "HUF")), "000010000000000000");

    // Non-default catalog: CacheTag now carries catalog info, so none of the TEST_TAGS
    // (all default-catalog) should match requests targeting a different catalog.
    assertMatchOnTags(Builder.create().addDb("custom_catalog", "fx"), "000000000000000000");
    assertMatchOnTags(Builder.create().addTable("custom_catalog", "equity", "prices"),
        "000000000000000000");
    assertMatchOnTags(Builder.create().addPartitionOfATable(
        "custom_catalog", "equity", "prices", buildParts("ex", "NYSE")),
        "000000000000000000");
  }

  /**
   * Verifies that catalog_name is serialized into the proto and correctly restored via fromProtoRequest.
   */
  @Test
  public void testProtoRoundTripPreservesCatalog() {
    // Default catalog is always serialized into the proto.
    Request defaultCatRequest = Builder.create().addDb("testdb").build();
    List<EvictEntityRequestProto> protos = defaultCatRequest.toProtoRequests();
    assertEquals(1, protos.size());
    EvictEntityRequestProto proto = protos.get(0);
    assertEquals(Warehouse.DEFAULT_CATALOG_NAME, proto.getCatalogName());
    assertEquals("testdb", proto.getDbName());

    Request roundTripped = Builder.create().fromProtoRequest(proto).build();
    assertTrue(roundTripped.hasDatabaseName(Warehouse.DEFAULT_CATALOG_NAME, "testdb"));

    // Custom catalog is also preserved.
    Request customCatRequest = Builder.create().addTable("spark_catalog", "salesdb", "orders").build();
    protos = customCatRequest.toProtoRequests();
    assertEquals(1, protos.size());
    proto = protos.get(0);
    assertEquals("spark_catalog", proto.getCatalogName());
    assertEquals("salesdb", proto.getDbName());

    roundTripped = Builder.create().fromProtoRequest(proto).build();
    assertTrue(roundTripped.hasDatabaseName("spark_catalog", "salesdb"));
  }

  /**
   * Verifies that entities in different catalogs are independently scoped even when they share
   * the same DB name, and that getSingleCatalogName/getSingleDbName return null when multiple
   * catalog-DB pairs are present.
   */
  @Test
  public void testMultiCatalogBuilderScoping() {
    // Two different catalogs, each with the same DB name but different tables.
    Request request = Builder.create()
        .addTable("catalog_a", "shared_db", "table_a")
        .addTable("catalog_b", "shared_db", "table_b")
        .build();

    assertEquals(2, request.getEntities().size());
    assertTrue(request.getEntities().containsKey(new Request.CatalogDb("catalog_a", "shared_db")));
    assertTrue(request.getEntities().containsKey(new Request.CatalogDb("catalog_b", "shared_db")));

    // catalog_a only knows about table_a.
    assertTrue(request.getEntities().get(new Request.CatalogDb("catalog_a", "shared_db")).containsKey("table_a"));
    assertFalse(request.getEntities().get(new Request.CatalogDb("catalog_a", "shared_db")).containsKey("table_b"));

    // catalog_b only knows about table_b.
    assertTrue(request.getEntities().get(new Request.CatalogDb("catalog_b", "shared_db")).containsKey("table_b"));
    assertFalse(request.getEntities().get(new Request.CatalogDb("catalog_b", "shared_db")).containsKey("table_a"));
  }

  /**
   * Verifies that multiple tables and partitions added to the same catalog+DB are merged
   * into a single catalog entry (no duplication).
   */
  @Test
  public void testSameCatalogMultipleEntitiesMergedCorrectly() {
    Request request = Builder.create()
        .addTable("mydb", "table1")
        .addTable("mydb", "table2")
        .addPartitionOfATable("mydb", "table3", buildParts("dt", "2024-01-01"))
        .addPartitionOfATable("mydb", "table3", buildParts("dt", "2024-01-02"))
        .build();

    assertTrue(request.hasDatabaseName(Warehouse.DEFAULT_CATALOG_NAME, "mydb"));
    // One catalog, one DB, three tables.
    assertEquals(1, request.getEntities().size());
    assertEquals(3, request.getEntities()
        .get(new Request.CatalogDb(Warehouse.DEFAULT_CATALOG_NAME, "mydb")).size());
    // table3 has two partition specs.
    assertEquals(2, request.getEntities()
        .get(new Request.CatalogDb(Warehouse.DEFAULT_CATALOG_NAME, "mydb")).get("table3").size());
  }

  /**
   * Verifies that CacheTag catalog information is correctly used to isolate eviction between catalogs.
   * A request targeting catalog A must not evict buffers that belong to catalog B, even when the
   * DB and table names are identical.
   */
  @Test
  public void testCatalogIsolationInIsTagMatch() {
    CacheTag defaultCatalogTag = cacheTagBuilder("fx.rates", "from=USD", "to=HUF");
    CacheTag otherCatalogTag = cacheTagBuilder("other_catalog.fx.rates", "from=USD", "to=HUF");

    // Request for the default catalog's "fx" DB matches only default-catalog tags.
    Request defaultCatalogRequest = Builder.create()
        .fromProtoRequest(Builder.create()
            .addDb("fx")
            .build().toProtoRequests().get(0))
        .build();
    assertTrue(defaultCatalogRequest.isTagMatch(defaultCatalogTag));
    assertFalse("Must not evict buffers belonging to other_catalog",
        defaultCatalogRequest.isTagMatch(otherCatalogTag));

    // Request for a different catalog matches only tags from that catalog.
    Request otherCatalogRequest = Builder.create()
        .fromProtoRequest(Builder.create()
            .addDb("other_catalog", "fx")
            .build().toProtoRequests().get(0))
        .build();
    assertTrue(otherCatalogRequest.isTagMatch(otherCatalogTag));
    assertFalse("Must not evict buffers belonging to the default catalog",
        otherCatalogRequest.isTagMatch(defaultCatalogTag));

    // A request for a DB that doesn't exist in the tags must not match, regardless of catalog.
    Request noMatchRequest = Builder.create()
        .fromProtoRequest(Builder.create()
            .addDb("any_catalog", "nonexistent_db")
            .build().toProtoRequests().get(0))
        .build();
    assertFalse(noMatchRequest.isTagMatch(defaultCatalogTag));
    assertFalse(noMatchRequest.isTagMatch(otherCatalogTag));
  }

  @Test
  public void testProactiveSweep() throws Exception {
    closeSweeperExecutorForTest();

    // Test that proactive sweeper thread does not get created if we turn the feature off
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.LLAP_IO_PROACTIVE_EVICTION_ENABLED, false);
    new DummyPolicy(conf);
    assertFalse(isProactiveEvictionSweeperThreadStarted());

    // Below here - testing with the feature turned on
    conf = new HiveConf();
    // NOTE: Choosing a too small value (<10ms) here can make this test case flaky
    long sweepIntervalInMs = 200;
    conf.setTimeVar(HiveConf.ConfVars.LLAP_IO_PROACTIVE_EVICTION_SWEEP_INTERVAL, sweepIntervalInMs,
        TimeUnit.MILLISECONDS);

    TestLowLevelLrfuCachePolicy.EvictionTracker evictionListener = new TestLowLevelLrfuCachePolicy.EvictionTracker();
    DummyPolicy policy = new DummyPolicy(conf);
    policy.setEvictionListener(evictionListener);
    LowLevelCacheMemoryManager mm = new LowLevelCacheMemoryManager(1024L, policy,
        LlapDaemonCacheMetrics.create("test", "1"));
    assertTrue(isProactiveEvictionSweeperThreadStarted());


    // Create buffers and insert them into dummy (but proactive eviction supporting) policy.
    LlapDataBuffer[] buffs = IntStream.range(0, 10).
        mapToObj(i -> LowLevelCacheImpl.allocateFake()).toArray(LlapDataBuffer[]::new);
    Arrays.stream(buffs).forEach(b -> policy.cache(b, null));

    // Marking buffers 0,1 in two mark events, but should be swept up together later
    buffs[0].markForEviction();
    mm.notifyProactiveEvictionMark();
    buffs[1].markForEviction();
    mm.notifyProactiveEvictionMark();

    // No buffers should be evicted right away just yet
    IntStream.range(0, 10).forEach(i -> assertBufferEvicted(false, false, buffs[i], evictionListener));

    // Buffers 0 and 1 should be invalidated after sweep interval time + some margin
    Thread.sleep(sweepIntervalInMs * 2);
    IntStream.range(0, 2).forEach(i -> assertBufferEvicted(true, true, buffs[i], evictionListener));
    IntStream.range(2, 10).forEach(i -> assertBufferEvicted(false, false, buffs[i], evictionListener));

    // Marking buffers 5,6,7,8,9 in one mark event
    IntStream.range(5, 10).forEach(i -> buffs[i].markForEviction());
    mm.notifyProactiveEvictionMark();
    Thread.sleep(sweepIntervalInMs * 2);

    // Buffers 2,3,4 should remain in cache policy
    IntStream.range(0, 2).forEach(i -> assertBufferEvicted(true, true, buffs[i], evictionListener));
    IntStream.range(2, 5).forEach(i -> assertBufferEvicted(false, false, buffs[i], evictionListener));
    IntStream.range(5, 10).forEach(i -> assertBufferEvicted(true, true, buffs[i], evictionListener));

    assertEquals(3, policy.purge());
    // Should have seen normal eviction for buffers 2,3,4 after purge
    IntStream.range(2, 5).forEach(i -> assertBufferEvicted(true, false, buffs[i], evictionListener));

    // Check that no unnecessary work was put on the policy - first 2 marks were swept together then the 3rd one later
    assertEquals(2, policy.proactiveEvictionSweepCount);
  }

  public static void closeSweeperExecutorForTest() throws Exception {
    ScheduledExecutorService service = retrieveSweeperExecutor();
    if (service != null) {
      service.shutdownNow();
    }
  }

  private static boolean isProactiveEvictionSweeperThreadStarted() throws Exception {
    ScheduledExecutorService service = retrieveSweeperExecutor();
    if (service == null) {
      return false;
    }
    return !service.isShutdown();
  }

  private static ScheduledExecutorService retrieveSweeperExecutor() throws Exception {
    Field sweeperExecutorField = ProactiveEvictingCachePolicy.Impl.class
        .getDeclaredField("PROACTIVE_EVICTION_SWEEPER_EXECUTOR");
    sweeperExecutorField.setAccessible(true);
    return (ScheduledExecutorService) sweeperExecutorField.get(null);
  }

  private static void assertBufferEvicted(boolean expectingEvicted, boolean wasProactive, LlapDataBuffer buffer,
      TestLowLevelLrfuCachePolicy.EvictionTracker evictionListener ) {
    assertEquals(expectingEvicted, buffer.isInvalid());
    assertEquals(expectingEvicted && !wasProactive, evictionListener.evicted.contains(buffer));
    assertEquals(expectingEvicted && wasProactive, evictionListener.proactivelyEvicted.contains(buffer));
  }

  class DummyPolicy extends ProactiveEvictingCachePolicy.Impl implements LowLevelCachePolicy {

    EvictionListener evictionListener = null;
    private Set<LlapCacheableBuffer> buffers = new HashSet<>();

    @VisibleForTesting
    public int proactiveEvictionSweepCount = 0;

    protected DummyPolicy(Configuration conf) {
      super(conf);
    }

    @Override
    public void cache(LlapCacheableBuffer buffer, LowLevelCache.Priority priority) {
      buffers.add(buffer);
    }

    @Override
    public void notifyLock(LlapCacheableBuffer buffer) {
    }

    @Override
    public void notifyUnlock(LlapCacheableBuffer buffer) {
    }

    @Override
    public long evictSomeBlocks(long memoryToReserve) {
      return 0;
    }

    @Override
    public void setEvictionListener(EvictionListener listener) {
      this.evictionListener = listener;
    }

    @Override
    public long purge() {
      return evictOrPurge(true);
    }

    @Override
    public void debugDumpShort(StringBuilder sb) {
    }

    @Override
    public void evictProactively() {
      ++proactiveEvictionSweepCount;
      evictOrPurge(false);
    }

    private long evictOrPurge(boolean isPurge) {
      long evictedBytes = 0;
      Iterator<LlapCacheableBuffer> it = buffers.iterator();
      while (it.hasNext()) {
        LlapCacheableBuffer buffer = it.next();
        if (isPurge || (!isPurge && buffer.isMarkedForEviction())) {
          if (INVALIDATE_OK == buffer.invalidate()) {
            evictedBytes += buffer.getMemoryUsage();
            if (!isPurge) {
              evictionListener.notifyProactivelyEvicted(buffer);
            } else {
              evictionListener.notifyEvicted(buffer);
            }
            it.remove();
          }
        }
      }
      return evictedBytes;
    }
  }

}
