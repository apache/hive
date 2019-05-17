/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.metrics;

import static java.lang.Math.max;
import static java.lang.System.nanoTime;
import static org.apache.hadoop.hive.llap.metrics.ReadWriteLockMetrics.LockMetricInfo.ReadLockCount;
import static org.apache.hadoop.hive.llap.metrics.ReadWriteLockMetrics.LockMetricInfo.ReadLockWaitTimeMax;
import static org.apache.hadoop.hive.llap.metrics.ReadWriteLockMetrics.LockMetricInfo.ReadLockWaitTimeTotal;
import static org.apache.hadoop.hive.llap.metrics.ReadWriteLockMetrics.LockMetricInfo.WriteLockCount;
import static org.apache.hadoop.hive.llap.metrics.ReadWriteLockMetrics.LockMetricInfo.WriteLockWaitTimeMax;
import static org.apache.hadoop.hive.llap.metrics.ReadWriteLockMetrics.LockMetricInfo.WriteLockWaitTimeTotal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.metrics2.MetricsSource;
import org.junit.Ignore;
import org.junit.Test;

/**
 * JUnit test suite for the <code>ReadWriteLockMetrics</code> class.
 * The test uses a background thread and has some hard coded thread execution
 * times. It should normally not take more than 2 threads and 400ms execution
 * time.
 */
public class TestReadWriteLockMetrics {
  /**
   * Thread which performs locks in loop, holding the lock for 5ms.
   */
  private static class LockHolder extends Thread {
    public static final long LOCK_HOLD_TIME = 5;   ///< lock hold time in ms

    private final Lock targetLock;                 ///< the lock to hold
    private long lockCount;                        ///< loop coun
    private long lockWaitSum;                      ///< total lock wait time
    private long lockWaitMax;                      ///< highest lock wait time
    private long endTime;                          ///< runtime for the thread

    /**
     * Create a new lock holding thread.
     * The so created thread start immediately.
     *
     * @param l The lock to lock/unlock in loop
     * @param ttl The expected thread run time in ms
     */
    public LockHolder(Lock l, long ttl) {
      targetLock = l;

      lockCount = 0;
      lockWaitSum = 0;
      lockWaitMax = 0;
      endTime = ttl;

      setName(getClass().getSimpleName());
      setDaemon(true);
      start();
    }

    /**
     * Returns the number of counted locks.
     * @return The total lock loop execution coun
     */
    public long getLockCount() {
      return lockCount;
    }

    /**
     * Returns the accumulated nano seconds for locks.
     * @return The aggregated time, the thread was waiting on locks (in nanos)
     */
    public long getLockSum() {
      return lockWaitSum;
    }

    /**
     * Returns the highest lock time in nano seconds.
     * @return The highest (single) lock wait time (in nanos)
     */
    public long getLockMax() {
      return lockWaitMax;
    }

    @Override
    public void run() {
      endTime = nanoTime() + toNano(endTime);  // ttl was in ms

      // loop for specified amount of time
      while (nanoTime() <= endTime && !isInterrupted()) {
        try {
          long start = nanoTime();
          targetLock.lock();
          ++lockCount;
          long diff = nanoTime() - start;
          lockWaitSum += diff;
          lockWaitMax = max(diff, lockWaitMax);

          while (nanoTime() <= (start + toNano(LOCK_HOLD_TIME))) {
            // spin for LOCK_HOLD_TIME ms (under lock)
          }
        } finally {
          targetLock.unlock();
        }
      }
    }
  }

  /**
   * Helper to verify the actual value by comparing it with a +/- tolerance of
   * 10% with the expected value.
   *
   * @param txt Assertion message
   * @param expected The expected value (tolerance will be applied)
   * @param actual Actual test outcome
   */
  private void assertWithTolerance(String txt, long expected, long actual) {
    long lowExpected = expected - (expected / 10L);
    long highExpected = expected + (expected / 10L);

    StringBuffer msg = new StringBuffer(txt);
    msg.append(" (expected ");
    msg.append(lowExpected);
    msg.append(" <= x <= ");
    msg.append(highExpected);
    msg.append(" but actual = ");
    msg.append(actual);
    msg.append(")");

    assertTrue(msg.toString(), actual >= lowExpected && actual <= highExpected);
  }

  /**
   * Helper to convert milliseconds to nanoseconds.
   *
   * @param ms Millisecond inpu
   * @return Value in nanoseconds
   */
  private static long toNano(long ms) {
    return ms * 1000000;
  }

  /**
   * Helper to produce <code>ReadWriteLockMetrics</code> instances.
   * The wrapping of lock instances is configuration dependent. This helper ensures that the
   * configuration creates wrapped lock instances.
   *
   * @param lock The lock to wrap
   * @param ms The metrics source, storing the lock measurements
   * @return The wrapped lock
   */
  private ReadWriteLockMetrics create(ReadWriteLock lock, MetricsSource ms) {
    Configuration dummyConf = new Configuration();

    HiveConf.setBoolVar(dummyConf,
        HiveConf.ConfVars.LLAP_COLLECT_LOCK_METRICS, true);
    return (ReadWriteLockMetrics)ReadWriteLockMetrics.wrap(dummyConf, lock, ms);
  }

  /**
   * Runs a simple test where a thread is running in a loop, getting read locks w/o having to
   * deal with any contention. The test shows that the locks are received rather quick and tha
   * all metrics for write locks remain zero.
   */
  @Ignore("Test requires available CPU resources for background threads")
  @Test
  public void testWithoutContention() throws Exception {
    final long execTime = 100;

    MetricsSource ms  = ReadWriteLockMetrics.createLockMetricsSource("test1");
    ReadWriteLock rwl = create(new ReentrantReadWriteLock(), ms);
    LockHolder    lhR = new LockHolder(rwl.readLock(), execTime);

    // wait for the thread to do its locks and waits (for 100ms)
    lhR.join();

    // get the reported metrics
    MockMetricsCollector tmc = new MockMetricsCollector();
    ms.getMetrics(tmc, true);

    List<MockMetricsCollector.MockRecord> result = tmc.getRecords();
    assertEquals("Unexpected amount of metrics", 1, result.size());
    MockMetricsCollector.MockRecord rec = result.get(0);

    // verify label and context (context is hard coded)
    assertEquals("Invalid record label", "test1", rec.getLabel());
    assertEquals("Invalid record context", "Locking", rec.getContext());

    // we expect around exectome / thread loop time executions
    assertWithTolerance("Unexpected count of lock executions (reader)",
        execTime / LockHolder.LOCK_HOLD_TIME,  lhR.getLockCount());
    assertEquals("Counting the locks failed",
                 lhR.getLockCount(), rec.getMetrics().get(ReadLockCount));

    // sanity check in read lock metrics
    assertNotEquals("Local thread should have lock time", lhR.getLockSum(), 0);
    assertNotEquals("Accounted lock time zero",
                    rec.getMetrics().get(ReadLockWaitTimeTotal), 0);
    assertTrue("Local measurement larger (overhead)",
               rec.getMetrics().get(ReadLockWaitTimeTotal).longValue()
                                    < lhR.getLockSum());

    assertNotEquals("Local thread should have max lock time",
                    lhR.getLockMax(), 0);
    assertNotEquals("Accounted lock max time zero",
                    rec.getMetrics().get(ReadLockWaitTimeMax), 0);

    assertTrue("Local max larger (overhead)",
               rec.getMetrics().get(ReadLockWaitTimeMax).longValue()
                                    < lhR.getLockMax());

    assertTrue("Max greater or equal to avergae lock time",
               (rec.getMetrics().get(ReadLockWaitTimeTotal).longValue()
                / rec.getMetrics().get(ReadLockCount).longValue())
                  <= rec.getMetrics().get(ReadLockWaitTimeMax).longValue());

    assertTrue("Lock time less than 1% (no contention)",
               rec.getMetrics().get(ReadLockWaitTimeTotal).longValue()
               < toNano(execTime / 100L));

    // sanity check on write lock metrics (should be all zero)
    assertEquals("No writer lock activity expected (total)",
                 rec.getMetrics().get(WriteLockWaitTimeTotal), 0L);
    assertEquals("No writer lock activity expected (max)",
                 rec.getMetrics().get(WriteLockWaitTimeMax), 0L);
    assertEquals("No writer lock activity expected (count)",
                 rec.getMetrics().get(WriteLockCount), 0L);
  }

  /**
   * Test where read/write lock contention is tested.
   * This test has a background thread that tries to get read locks within a
   * loop while the main thread holds a write lock for half of the tex
   * execution time. The test verifies that the reported metric for read lock
   * wait time reflects that the thread was blocked until the write lock was
   * released. It also performs basic sanity checks on the read and write lock
   * metrics.
   */
  @Ignore("Test requires available CPU resources for background threads")
  @Test
  public void testWithContention() throws Exception {
    final long execTime = 200;

    MetricsSource ms  = ReadWriteLockMetrics.createLockMetricsSource("test1");
    ReadWriteLock rwl = create(new ReentrantReadWriteLock(), ms);
    LockHolder    lhR = new LockHolder(rwl.readLock(), execTime);

    // get a write lock for half of the execution time
    try {
      long endOfLock = nanoTime() + toNano(execTime / 2);
      rwl.writeLock().lock();

      while (nanoTime() < endOfLock) {
        // spin until end time is reached
      }
    } finally {
      rwl.writeLock().unlock();
    }

    // wait for the thread to do its locks and waits (for 100ms)
    lhR.join();

    MockMetricsCollector tmc = new MockMetricsCollector();
    ms.getMetrics(tmc, true);

    List<MockMetricsCollector.MockRecord> result = tmc.getRecords();
    assertEquals("Unexpected amount of metrics", 1, result.size());
    MockMetricsCollector.MockRecord rec = result.get(0);

    // sanity checks for read lock values
    assertEquals("Verifying the loop count (read lock)",
                 lhR.getLockCount(),
                 rec.getMetrics().get(ReadLockCount).longValue());

    assertWithTolerance("Only half of possible read locks expected",
                        (execTime / LockHolder.LOCK_HOLD_TIME) / 2,
                        rec.getMetrics().get(ReadLockCount).longValue());

    assertWithTolerance("Max read lock wait time close to write lock hold",
                        toNano(execTime / 2),
                        rec.getMetrics().get(ReadLockWaitTimeMax).longValue());

    assertTrue("Total read lock wait time larger than max",
               rec.getMetrics().get(ReadLockWaitTimeMax).longValue()
               < rec.getMetrics().get(ReadLockWaitTimeTotal).longValue());

    // sanity check for write locks
    assertEquals("Write lock count supposed to be one",
                 1, rec.getMetrics().get(WriteLockCount).longValue());

    assertTrue("Write lock wait time non zero",
               0L < rec.getMetrics().get(WriteLockWaitTimeTotal).longValue());
    assertEquals("With one lock, total should me max",
                 rec.getMetrics().get(WriteLockWaitTimeTotal),
                 rec.getMetrics().get(WriteLockWaitTimeMax));
  }

  /**
   * Testing the <code>wrap</code> function for different configuration
   * combinations.
   */
  @Test
  public void testWrap() throws Exception {
    Configuration testConf = new Configuration();
    MetricsSource ms = ReadWriteLockMetrics.createLockMetricsSource("testConf");

    // default = passthrough
    ReadWriteLock rwlDef =
        ReadWriteLockMetrics.wrap(testConf, new ReentrantReadWriteLock(), ms);
    assertTrue("Basic ReentrantReadWriteLock expected",
        rwlDef instanceof ReentrantReadWriteLock);
    assertFalse("Basic ReentrantReadWriteLock expected",
        rwlDef instanceof ReadWriteLockMetrics);

    // false = pass through
    HiveConf.setBoolVar(testConf,
        HiveConf.ConfVars.LLAP_COLLECT_LOCK_METRICS, false);
    ReadWriteLock rwlBasic =
        ReadWriteLockMetrics.wrap(testConf, new ReentrantReadWriteLock(), ms);
    assertTrue("Basic ReentrantReadWriteLock expected",
               rwlBasic instanceof ReentrantReadWriteLock);
    assertFalse("Basic ReentrantReadWriteLock expected",
               rwlBasic instanceof ReadWriteLockMetrics);

    // true = wrap
    HiveConf.setBoolVar(testConf,
                        HiveConf.ConfVars.LLAP_COLLECT_LOCK_METRICS, true);
    ReadWriteLock rwlWrap =
        ReadWriteLockMetrics.wrap(testConf, new ReentrantReadWriteLock(), ms);
    assertTrue("Wrapped lock expected",
               rwlWrap instanceof ReadWriteLockMetrics);

    // null = passthrough
    ReadWriteLock rwlNoConf =
        ReadWriteLockMetrics.wrap(null, new ReentrantReadWriteLock(), null);
    assertTrue("Basic ReentrantReadWriteLock expected",
               rwlNoConf instanceof ReentrantReadWriteLock);
    assertFalse("Basic ReentrantReadWriteLock expected",
                rwlNoConf instanceof ReadWriteLockMetrics);
  }
}
