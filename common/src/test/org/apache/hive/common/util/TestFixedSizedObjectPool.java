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
package org.apache.hive.common.util;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.hive.common.util.FixedSizedObjectPool;
import org.apache.hadoop.hive.common.Pool;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFixedSizedObjectPool {

  private static abstract class PoolRunnable implements Runnable {
    protected final FixedSizedObjectPool<Object> pool;
    private final CountDownLatch cdlIn;
    private final CountDownLatch cdlOut;
    public final List<Object> objects = new ArrayList<>();
    private final int count;

    PoolRunnable(FixedSizedObjectPool<Object> pool,
        CountDownLatch cdlIn, CountDownLatch cdlOut, int count) {
      this.pool = pool;
      this.cdlIn = cdlIn;
      this.cdlOut = cdlOut;
      this.count = count;
    }

    @Override
    public void run() {
      syncThreadStart(cdlIn, cdlOut);
      for (int i = 0; i < count; ++i) {
        doOneOp();
      }
    }

    protected abstract void doOneOp();
  }

  private static final class OfferRunnable extends PoolRunnable {
    OfferRunnable(FixedSizedObjectPool<Object> pool,
        CountDownLatch cdlIn, CountDownLatch cdlOut, int count) {
      super(pool, cdlIn, cdlOut, count);
    }

    @Override
    protected void doOneOp() {
      Object o = new Object();
      if (pool.tryOffer(o)) {
        objects.add(o);
      }
    }
  }

  private static final class TakeRunnable extends PoolRunnable {
    TakeRunnable(FixedSizedObjectPool<Object> pool,
        CountDownLatch cdlIn, CountDownLatch cdlOut, int count) {
      super(pool, cdlIn, cdlOut, count);
    }

    @Override
    protected void doOneOp() {
      Object o = pool.take();
      if (o != OneObjHelper.THE_OBJECT) {
        objects.add(o);
      }
    }
  }

  private static class DummyHelper implements Pool.PoolObjectHelper<Object> {
    @Override
    public Object create() {
      return new Object();
    }

    @Override
    public void resetBeforeOffer(Object t) {
    }
  }

  private static class OneObjHelper implements Pool.PoolObjectHelper<Object> {
    public static final Object THE_OBJECT = new Object();
    @Override
    public Object create() {
      return THE_OBJECT;
    }

    @Override
    public void resetBeforeOffer(Object t) {
    }
  }

  @Test
  public void testFullEmpty() {
    final int SIZE = 8;
    HashSet<Object> offered = new HashSet<>();
    FixedSizedObjectPool<Object> pool = new FixedSizedObjectPool<>(SIZE, new DummyHelper(), true);
    Object newObj = pool.take();
    for (int i = 0; i < SIZE; ++i) {
      Object obj = new Object();
      offered.add(obj);
      assertTrue(pool.tryOffer(obj));
    }
    assertFalse(pool.tryOffer(newObj));
    for (int i = 0; i < SIZE; ++i) {
      Object obj = pool.take();
      assertTrue(offered.remove(obj));
    }
    assertTrue(offered.isEmpty());
    Object newObj2 = pool.take();
    assertNotSame(newObj, newObj2);
  }

  public static final Logger LOG = LoggerFactory.getLogger(TestFixedSizedObjectPool.class);

  @Test
  public void testMTT1() {
    testMTTImpl(1, 3, 3);
  }

  @Test
  public void testMTT8() {
    testMTTImpl(8, 3, 3);
  }

  @Test
  public void testMTT4096() {
    testMTTImpl(4096, 3, 3);
  }

  @Test
  public void testMTT4096_1() {
    testMTTImpl(4096, 1, 1);
  }

  @Test
  public void testMTT20000() {
    testMTTImpl(20000, 3, 3);
  }

  @Test
  public void testMTT4096_10() {
    testMTTImpl(4096, 10, 10);
  }

  public void testMTTImpl(int size, int takerCount, int giverCount) {
    final int TASK_COUNT = takerCount + giverCount, GIVECOUNT = 15000, TAKECOUNT = 15000;
    ExecutorService executor = Executors.newFixedThreadPool(TASK_COUNT);
    final CountDownLatch cdlIn = new CountDownLatch(TASK_COUNT), cdlOut = new CountDownLatch(1);
    final FixedSizedObjectPool<Object> pool =
        new FixedSizedObjectPool<>(size, new OneObjHelper(), true);
    // Pre-fill the pool halfway.
    HashSet<Object> allGiven = new HashSet<>();
    for (int i = 0; i < (size >> 1); ++i) {
      Object o = new Object();
      allGiven.add(o);
      assertTrue(pool.tryOffer(o));
    }
    @SuppressWarnings("unchecked")
    FutureTask<Object>[] tasks = new FutureTask[TASK_COUNT];
    TakeRunnable[] takers = new TakeRunnable[takerCount];
    OfferRunnable[] givers = new OfferRunnable[giverCount];
    int ti = 0;
    for (int i = 0; i < takerCount; ++i, ++ti) {
      takers[i] = new TakeRunnable(pool, cdlIn, cdlOut, TAKECOUNT);
      tasks[ti] = new FutureTask<Object>(takers[i], null);
      executor.execute(tasks[ti]);
    }
    for (int i = 0; i < giverCount; ++i, ++ti) {
      givers[i] = new OfferRunnable(pool, cdlIn, cdlOut, GIVECOUNT);
      tasks[ti] = new FutureTask<Object>(givers[i], null);
      executor.execute(tasks[ti]);
    }
    long time = 0;
    try {
      cdlIn.await(); // Wait for all threads to be ready.
      time = System.nanoTime();
      cdlOut.countDown(); // Release them at the same time.
      for (int i = 0; i < TASK_COUNT; ++i) {
        tasks[i].get();
      }
      time = (System.nanoTime() - time);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
    int given = allGiven.size(), takenOld = 0;
    for (OfferRunnable g : givers) {
      for (Object o : g.objects) {
        assertTrue(allGiven.add(o));
        ++given;
      }
    }
    for (TakeRunnable t : takers) {
      for (Object o : t.objects) {
        assertTrue(allGiven.remove(o));
        ++takenOld;
      }
    }
    LOG.info("MTT test - size " + size + ", takers/givers "
        + takerCount + "/" + giverCount + "; offered " + (given - (size >> 1)) + " (attempted "
        + (GIVECOUNT * giverCount) + "); reused " + takenOld + ", allocated "
        + ((TAKECOUNT * takerCount) - takenOld) + " (took " + time/1000000L
        + "ms including thread sync)");
    // Most of the above will be failed offers and takes (due to speed of the thing).
    // Verify that we can drain the pool, then cycle it, i.e. the state is not corrupted.
    while (pool.take() != OneObjHelper.THE_OBJECT);
    for (int i = 0; i < size; ++i) {
      assertTrue(pool.tryOffer(new Object()));
    }
    assertFalse(pool.tryOffer(new Object()));
    for (int i = 0; i < size; ++i) {
      assertTrue(OneObjHelper.THE_OBJECT != pool.take());
    }
    assertTrue(OneObjHelper.THE_OBJECT == pool.take());
  }

  private static void syncThreadStart(final CountDownLatch cdlIn, final CountDownLatch cdlOut) {
    cdlIn.countDown();
    try {
      cdlOut.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
