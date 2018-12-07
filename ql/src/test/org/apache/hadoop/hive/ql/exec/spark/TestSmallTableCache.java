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

package org.apache.hadoop.hive.ql.exec.spark;

import com.google.common.testing.FakeTicker;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * Test the two level cache.
 */
public class TestSmallTableCache {
  private static final String KEY = "Test";
  private static final String TEST_VALUE_1 = "TestValue1";
  private static final String TEST_VALUE_2 = "TestValue2";
  private SmallTableCache.SmallTableLocalCache<String, String> cache;
  private AtomicInteger counter;

  @Before
  public void setUp() {
    this.cache = new SmallTableCache.SmallTableLocalCache<>();
    this.counter = new AtomicInteger(0);
  }

  @Test
  public void testEmptyCache() throws ExecutionException {

    String res = cache.get(KEY, () -> {
      counter.incrementAndGet();
      return new String(TEST_VALUE_1);
    });

    assertEquals(TEST_VALUE_1, res);
    assertEquals(1, counter.get());
    assertEquals(1, cache.size());
  }

  @Test
  public void testL1Hit() throws ExecutionException {
    cache.get(KEY, () -> {
      counter.incrementAndGet();
      return new String(TEST_VALUE_1);
    });

    String res = cache.get(KEY, () -> {
      counter.incrementAndGet();
      return new String(TEST_VALUE_2);
    });

    assertEquals(TEST_VALUE_1, res);
    assertEquals(1, counter.get());
    assertEquals(1, cache.size());
  }

  @Test
  public void testL2Hit() throws ExecutionException {

    FakeTicker ticker = new FakeTicker();
    cache = new SmallTableCache.SmallTableLocalCache<>(ticker);

    cache.get(KEY, () -> {
      counter.incrementAndGet();
      return new String(TEST_VALUE_1);
    });

    ticker.advance(60, TimeUnit.SECONDS);

    String res = cache.get(KEY, () -> {
      counter.incrementAndGet();
      return new String(TEST_VALUE_2);
    });

    assertEquals(TEST_VALUE_1, res);
    assertEquals(1, counter.get());
    assertEquals(1, cache.size());
  }

  @Test
  public void testL2Miss() throws ExecutionException {

    FakeTicker ticker = new FakeTicker();
    cache = new SmallTableCache.SmallTableLocalCache<>(ticker);

    cache.get(KEY, () -> {
      counter.incrementAndGet();
      return new String(TEST_VALUE_1);
    });

    ticker.advance(60, TimeUnit.SECONDS);
    cache.cleanup();
    forceOOMToClearSoftValues();

    String res = cache.get(KEY, () -> {
      counter.incrementAndGet();
      return new String(TEST_VALUE_2);
    });

    assertEquals(TEST_VALUE_2, res);
    assertEquals(2, counter.get());
    assertEquals(1, cache.size());
  }

  @Test
  public void testL2IsNotClearedIfTheItemIsInL1() throws ExecutionException {

    FakeTicker ticker = new FakeTicker();
    cache = new SmallTableCache.SmallTableLocalCache<>(ticker);

    cache.get(KEY, () -> {
      counter.incrementAndGet();
      return new String(TEST_VALUE_1);
    });

    forceOOMToClearSoftValues();
    ticker.advance(60, TimeUnit.SECONDS);
    cache.cleanup();

    String res = cache.get(KEY, () -> {
      counter.incrementAndGet();
      return new String(TEST_VALUE_2);
    });

    assertEquals(TEST_VALUE_1, res);
    assertEquals(1, counter.get());
    assertEquals(1, cache.size());
  }

  @Test
  public void testClear() throws ExecutionException {
    cache.get(KEY, () -> {
      counter.incrementAndGet();
      return new String(TEST_VALUE_1);
    });
    cache.clear((k, v) -> {
    });

    assertEquals(1, counter.get());
    assertEquals(0, cache.size());
  }

  @Test
  public void testPutL1() throws ExecutionException {
    cache.put(KEY, new String(TEST_VALUE_1));
    String res = cache.get(KEY, () -> {
      counter.incrementAndGet();
      return new String(TEST_VALUE_2);
    });

    assertEquals(TEST_VALUE_1, res);
    assertEquals(0, counter.get());
  }

  @Test
  public void testPutL2() throws ExecutionException {

    FakeTicker ticker = new FakeTicker();
    cache = new SmallTableCache.SmallTableLocalCache<>(ticker);

    cache.put(KEY, new String(TEST_VALUE_1));
    String res = cache.get(KEY, () -> {
      counter.incrementAndGet();
      return new String(TEST_VALUE_2);
    });

    ticker.advance(60, TimeUnit.SECONDS);
    cache.cleanup();

    assertEquals(TEST_VALUE_1, res);
    assertEquals(0, counter.get());
  }

  private void forceOOMToClearSoftValues() {
    try {
      while (true) {
        Object[] ignored = new Object[Integer.MAX_VALUE / 2];
      }
    } catch (OutOfMemoryError e) {
    }
  }
}
