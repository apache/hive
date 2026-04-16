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

package org.apache.hadoop.hive.ql;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.testutils.HiveTestEnvSetup;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCachedResults {

  private static final Logger LOG = LoggerFactory.getLogger(TestCachedResults.class);
  private static final long MAX_ALLOWED_CACHE_SIZE = 1_000_000L;

  private static final String Q_WINDOW =
      "SELECT t1.id, "
          + "SUM(t2.id)   OVER (PARTITION BY t1.id % 10 ORDER BY t1.id) AS running_sum, "
          + "AVG(t2.id)   OVER (PARTITION BY t1.id % 10)                AS window_avg, "
          + "COUNT(t2.id) OVER (PARTITION BY t1.id % 10 ORDER BY t1.id) AS running_cnt "
          + "FROM tab t1 "
          + "JOIN tab t2 ON t1.id % 10 = t2.id % 10 "
          + "WHERE t1.id <= 300";

  private static final String Q_JOIN =
      "SELECT base.id, base.bucket, agg.bucket_avg "
          + "FROM ( SELECT id, id % 10 AS bucket FROM tab WHERE id <= 500 ) base "
          + "JOIN ( "
          + "  SELECT id % 10 AS bucket, AVG(id) AS bucket_avg, COUNT(*) AS cnt "
          + "  FROM tab GROUP BY id % 10 "
          + ") agg ON base.bucket = agg.bucket "
          + "ORDER BY base.id";

  private static final String Q_CTE =
      "WITH base AS ( "
          + "  SELECT id, id % 2 AS is_even, id % 5 AS mod5, id % 10 AS mod10 FROM tab "
          + "), joined AS ( "
          + "  SELECT a.id AS a_id, b.id AS b_id, a.mod5, a.mod10, (a.id * b.id) AS product "
          + "  FROM base a JOIN base b ON a.mod5 = b.mod5 AND a.is_even = b.is_even "
          + "  WHERE a.id <= 200 AND b.id <= 200 "
          + ") "
          + "SELECT mod5, mod10, COUNT(*) AS cnt, SUM(product) AS total_product, "
          + "MAX(product) AS max_product, MIN(a_id) AS min_a "
          + "FROM joined GROUP BY mod5, mod10 ORDER BY mod5, mod10";

  @ClassRule
  public static HiveTestEnvSetup envSetup = new HiveTestEnvSetup();

  private static HiveConf conf;

  private ScheduledExecutorService scheduler;
  private volatile long maxCacheSize;

  @BeforeClass
  public static void setUp() throws Exception {
    conf = envSetup.getTestCtx().hiveConf;
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_ENABLED, true);
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_DIRECTORY, "/tmp/hive/cache");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_NONTRANSACTIONAL_TABLES_ENABLED, true);
    HiveConf.setLongVar(conf, HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_MAX_SIZE, MAX_ALLOWED_CACHE_SIZE);
    LOG.info("max allowed cache size: {}", conf.getLongVar(HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_MAX_SIZE));
    SessionState.start(conf);
    createAndPopulateTables();
  }

  private static void createAndPopulateTables() throws Exception {
    IDriver driver = DriverFactory.newDriver(conf);
    driver.run("DROP TABLE IF EXISTS tab");
    driver.run("CREATE TABLE tab (id INT)");
    driver.run(
        "INSERT INTO TABLE tab SELECT pos + 1 AS id FROM ( "
            + "SELECT posexplode(split(space(999), ' ')) AS (pos, val) ) t");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    DriverFactory.newDriver(conf).run("DROP TABLE IF EXISTS tab");
  }

  @Before
  public void beforeEach() {
    scheduler = Executors.newSingleThreadScheduledExecutor();
    maxCacheSize = 0;
  }

  @After
  public void afterEach() throws Exception {
    QueryResultsCache.cleanupInstance();
    Path cachePath = new Path(conf.getVar(HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_DIRECTORY));
    try {
      FileSystem fs = cachePath.getFileSystem(conf);
      fs.delete(cachePath, true);
    } catch (IOException e) {
      LOG.warn("Failed to clean up cache directory: {}", cachePath, e);
    }
    scheduler.shutdownNow();
    scheduler.awaitTermination(1, TimeUnit.SECONDS);
  }

  private void executeQueries(IDriver driver) throws Exception {
    driver.run(Q_WINDOW);
    driver.run(Q_JOIN);
    driver.run(Q_CTE);
  }

  @Test
  public void testSafeCacheWrite() throws Exception {
    runCacheScenario(true);
    LOG.info("Maximum cache size in safe mode: {}", maxCacheSize);
    Assert.assertFalse("max cache size should stay within limit", maxCacheSize > MAX_ALLOWED_CACHE_SIZE);
  }

  @Test
  public void testUnsafeCacheWrite() throws Exception {
    runCacheScenario(false);
    LOG.info("Maximum cache size in non-safe mode: {}", maxCacheSize);
    Assert.assertFalse("max cache size should exceed limit when unsafe", maxCacheSize < MAX_ALLOWED_CACHE_SIZE);
  }

  private void runCacheScenario(boolean safeCacheWrite) throws Exception {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_QUERY_RESULTS_SAFE_CACHE_WRITE_ENABLED, safeCacheWrite);
    startCacheMonitor(1);
    executeQueries(DriverFactory.newDriver(conf));
    stopCacheMonitor();
    Assert.assertNotEquals("cache size should have grown", 0, maxCacheSize);
  }

  private void startCacheMonitor(long intervalMs) {
    Path cachePath = new Path(conf.getVar(HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_DIRECTORY));
    scheduler.scheduleAtFixedRate(() -> {
      try {
        long size = cachePath.getFileSystem(conf).getContentSummary(cachePath).getLength();
        maxCacheSize = Math.max(maxCacheSize, size);
      } catch (IOException e) {
        LOG.debug("cache path not readable yet: {}", cachePath, e);
      }
    }, 0, intervalMs, TimeUnit.MILLISECONDS);
  }

  private void stopCacheMonitor() throws InterruptedException {
    scheduler.shutdown();
    scheduler.awaitTermination(2, TimeUnit.SECONDS);
  }
}
