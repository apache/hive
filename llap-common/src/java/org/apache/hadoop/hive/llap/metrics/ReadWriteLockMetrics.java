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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * Wrapper around a read/write lock to collect the lock wait times.
 * Instances of this wrapper class can be used to collect/accumulate the wai
 * times around R/W locks. This is helpful if the source of a performance issue
 * might be related to lock contention and you need to identify the actual
 * locks. Instances of this class can be wrapped around any <code>ReadWriteLock
 * </code> implementation.
 */
public class ReadWriteLockMetrics implements ReadWriteLock {
  private LockWrapper readLock;         ///< wrapper around original read lock
  private LockWrapper writeLock;        ///< wrapper around original write lock

  /**
   * Helper class to compare two <code>LockMetricSource</code> instances.
   * This <code>Comparator</code> class can be used to sort a list of <code>
   * LockMetricSource</code> instances in descending order by their total lock
   * wait time.
   */
  public static class MetricsComparator implements Comparator<MetricsSource>, Serializable {
    private static final long serialVersionUID = -1;

    @Override
    public int compare(MetricsSource o1, MetricsSource o2) {
      if (o1 != null && o2 != null
          && o1 instanceof LockMetricSource && o2 instanceof LockMetricSource) {
        LockMetricSource lms1 = (LockMetricSource)o1;
        LockMetricSource lms2 = (LockMetricSource)o2;

        long totalMs1 = (lms1.readLockWaitTimeTotal.value() / 1000000L)
                        + (lms1.writeLockWaitTimeTotal.value() / 1000000L);
        long totalMs2 = (lms2.readLockWaitTimeTotal.value() / 1000000L)
                        + (lms2.writeLockWaitTimeTotal.value() / 1000000L);

        // sort descending by total lock time
        if (totalMs1 < totalMs2) {
          return 1;
        }

        if (totalMs1 > totalMs2) {
          return -1;
        }

        // sort by label (ascending) if lock time is the same
        return lms1.lockLabel.compareTo(lms2.lockLabel);
      }

      return 0;
    }
  }

  /**
   * Wraps a <code>ReadWriteLock</code> into a monitored lock if required by
   * configuration. This helper is checking the <code>
   * hive.llap.lockmetrics.collect</code> configuration option and wraps the
   * passed in <code>ReadWriteLock</code> into a monitoring container if the
   * option is set to <code>true</code>. Otherwise, the original (passed in)
   * lock instance is returned unmodified.
   *
   * @param conf Configuration instance to check for LLAP conf options
   * @param lock The <code>ReadWriteLock</code> to wrap for monitoring
   * @param metrics The target container for locking metrics
   * @see #createLockMetricsSource
   */
  public static ReadWriteLock wrap(Configuration conf, ReadWriteLock lock,
                                   MetricsSource metrics) {
    Preconditions.checkNotNull(lock, "Caller has to provide valid input lock");
    boolean needsWrap = false;

    if (null != conf) {
      needsWrap =
        HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_COLLECT_LOCK_METRICS);
    }

    if (false == needsWrap) {
      return lock;
    }

    Preconditions.checkNotNull(metrics,
        "Caller has to procide group specific metrics source");
    return new ReadWriteLockMetrics(lock, metrics);
  }

  /**
   * Factory method for new metric collections.
   * You can create and use a single <code>MetricsSource</code> collection for
   * multiple R/W locks. This makes sense if several locks belong to a single
   * group and you're then interested in the accumulated values for the whole
   * group, rather than the single lock instance. The passed in label is
   * supposed to identify the group uniquely.
   *
   * @param label The group identifier for lock statistics
   */
  public static MetricsSource createLockMetricsSource(String label) {
    Preconditions.checkNotNull(label);
    Preconditions.checkArgument(!label.contains("\""),
        "Label can't contain quote (\")");
    return new LockMetricSource(label);
  }

  /**
   * Returns a list with all created <code>MetricsSource</code> instances for
   * the R/W lock metrics. The returned list contains the instances that were
   * previously created via the <code>createLockMetricsSource</code> function.
   *
   * @return A list of all R/W lock based metrics sources
   */
  public static List<MetricsSource> getAllMetricsSources() {
    ArrayList<MetricsSource> ret = null;

    synchronized (LockMetricSource.allInstances) {
      ret = new ArrayList<>(LockMetricSource.allInstances);
    }

    return ret;
  }

  /// Enumeration of metric info names and descriptions
  @VisibleForTesting
  public enum LockMetricInfo implements MetricsInfo {
    ReadLockWaitTimeTotal("The total wait time for read locks in nanoseconds"),
    ReadLockWaitTimeMax("The maximum wait time for a read lock in nanoseconds"),
    ReadLockCount("Total amount of read lock requests"),
    WriteLockWaitTimeTotal(
        "The total wait time for write locks in nanoseconds"),
    WriteLockWaitTimeMax(
        "The maximum wait time for a write lock in nanoseconds"),
    WriteLockCount("Total amount of write lock requests");

    private final String description;   ///< metric description

    /**
     * Creates a new <code>MetricsInfo</code> with the given description.
     *
     * @param desc The description of the info
     */
    private LockMetricInfo(String desc) {
      description = desc;
    }

    @Override
    public String description() {
      return this.description;
    }
  }

  /**
   * Source of the accumulated lock times and counts.
   * Instances of this <code>MetricSource</code> can be created via the static
   * factory method <code>createLockMetricsSource</code> and shared across
   * multiple instances of the outer <code>ReadWriteLockMetric</code> class.
   */
  @Metrics(about = "Lock Metrics", context = "locking")
  private static class LockMetricSource implements MetricsSource {
    private static final ArrayList<MetricsSource> allInstances = new ArrayList<>();

    private final String lockLabel;   ///< identifier for the group of locks

    /// accumulated wait time for read locks
    @Metric
    MutableCounterLong readLockWaitTimeTotal;

    /// highest wait time for read locks
    @Metric
    MutableCounterLong readLockWaitTimeMax;

    /// total number of read lock calls
    @Metric
    MutableCounterLong readLockCounts;

    /// accumulated wait time for write locks
    @Metric
    MutableCounterLong writeLockWaitTimeTotal;

    /// highest wait time for write locks
    @Metric
    MutableCounterLong writeLockWaitTimeMax;

    /// total number of write lock calls
    @Metric
    MutableCounterLong writeLockCounts;

    /**
     * Creates a new metrics collection instance.
     * Several locks can share a single <code>MetricsSource</code> instances
     * where all of them  increment the metrics counts together. This can be
     * interesting to have a single instance for a group of related locks. The
     * group should then be identified by the label.
     *
     * @param label The identifier of the metrics collection
     */
    private LockMetricSource(String label) {
      lockLabel = label;
      readLockWaitTimeTotal
          = new MutableCounterLong(LockMetricInfo.ReadLockWaitTimeTotal, 0);
      readLockWaitTimeMax
          = new MutableCounterLong(LockMetricInfo.ReadLockWaitTimeMax, 0);
      readLockCounts
          = new MutableCounterLong(LockMetricInfo.ReadLockCount, 0);
      writeLockWaitTimeTotal
          = new MutableCounterLong(LockMetricInfo.WriteLockWaitTimeTotal, 0);
      writeLockWaitTimeMax
          = new MutableCounterLong(LockMetricInfo.WriteLockWaitTimeMax, 0);
      writeLockCounts
          = new MutableCounterLong(LockMetricInfo.WriteLockCount, 0);

      synchronized (allInstances) {
        allInstances.add(this);
      }
    }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
      collector.addRecord(this.lockLabel)
               .setContext("Locking")
               .addCounter(LockMetricInfo.ReadLockWaitTimeTotal,
                           readLockWaitTimeTotal.value())
               .addCounter(LockMetricInfo.ReadLockWaitTimeMax,
                           readLockWaitTimeMax.value())
               .addCounter(LockMetricInfo.ReadLockCount,
                           readLockCounts.value())
               .addCounter(LockMetricInfo.WriteLockWaitTimeTotal,
                           writeLockWaitTimeTotal.value())
               .addCounter(LockMetricInfo.WriteLockWaitTimeMax,
                           writeLockWaitTimeMax.value())
               .addCounter(LockMetricInfo.WriteLockCount,
                           writeLockCounts.value());
    }

    @Override
    public String toString() {
      long avgRead     = 0L;
      long avgWrite    = 0L;
      long totalMillis = 0L;

      if (0 < readLockCounts.value()) {
        avgRead = readLockWaitTimeTotal.value() / readLockCounts.value();
      }

      if (0 < writeLockCounts.value()) {
        avgWrite = writeLockWaitTimeTotal.value() / writeLockCounts.value();
      }

      totalMillis = (readLockWaitTimeTotal.value() / 1000000L)
                    + (writeLockWaitTimeTotal.value() / 1000000L);

      StringBuffer sb = new StringBuffer();
      sb.append("{ \"type\" : \"R/W Lock Stats\", \"label\" : \"");
      sb.append(lockLabel);
      sb.append("\", \"totalLockWaitTimeMillis\" : ");
      sb.append(totalMillis);
      sb.append(", \"readLock\" : { \"count\" : ");
      sb.append(readLockCounts.value());
      sb.append(", \"avgWaitTimeNanos\" : ");
      sb.append(avgRead);
      sb.append(", \"maxWaitTimeNanos\" : ");
      sb.append(readLockWaitTimeMax.value());
      sb.append(" }, \"writeLock\" : { \"count\" : ");
      sb.append(writeLockCounts.value());
      sb.append(", \"avgWaitTimeNanos\" : ");
      sb.append(avgWrite);
      sb.append(", \"maxWaitTimeNanos\" : ");
      sb.append(writeLockWaitTimeMax.value());
      sb.append(" } }");

      return sb.toString();
    }
  }

  /**
   * Inner helper class to wrap the original lock with a monitored one.
   * This inner class is delegating all actual locking operations to the wrapped
   * lock, while itself is only responsible to measure the time that it took to
   * acquire a specific lock.
   */
  private static class LockWrapper implements Lock {
    /// the lock to delegate the work to
    private final Lock wrappedLock;
    /// total lock wait time in nanos
    private final MutableCounterLong lockWaitTotal;
    /// highest lock wait time (max)
    private final MutableCounterLong lockWaitMax;
    /// number of lock counts
    private final MutableCounterLong lockWaitCount;

    /**
     * Creates a new wrapper around an existing lock.
     *
     * @param original The original lock to wrap by this monitoring lock
     * @param total The (atomic) counter to increment for total lock wait time
     * @param max The (atomic) counter to adjust to the maximum wait time
     * @param cnt The (atomic) counter to increment with each lock call
     */
    LockWrapper(Lock original, MutableCounterLong total,
                MutableCounterLong max, MutableCounterLong cnt) {
      wrappedLock = original;
      this.lockWaitTotal = total;
      this.lockWaitMax = max;
      this.lockWaitCount = cnt;
    }

    @Override
    public void lock() {
      long start = System.nanoTime();
      wrappedLock.lock();
      incrementBy(System.nanoTime() - start);
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      long start = System.nanoTime();
      wrappedLock.lockInterruptibly();
      incrementBy(System.nanoTime() - start);
    }

    @Override
    public boolean tryLock() {
      return wrappedLock.tryLock();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit)
                   throws InterruptedException {
      long start = System.nanoTime();
      boolean ret = wrappedLock.tryLock(time, unit);
      incrementBy(System.nanoTime() - start);
      return ret;
    }

    @Override
    public void unlock() {
      wrappedLock.unlock();
    }

    @Override
    public Condition newCondition() {
      return wrappedLock.newCondition();
    }

    /**
     * Helper to increment the monitoring counters.
     * Called from the lock implementations to increment the total/max/coun
     * values of the monitoring counters.
     *
     * @param waitTime The actual wait time (in nanos) for the lock operation
     */
    private void incrementBy(long waitTime) {
      this.lockWaitTotal.incr(waitTime);
      this.lockWaitCount.incr();

      if (waitTime > this.lockWaitMax.value()) {
        this.lockWaitMax.incr(waitTime - this.lockWaitMax.value());
      }
    }
  }

  /**
   * Creates a new monitoring wrapper around a R/W lock.
   * The so created wrapper instance can be used instead of the original R/W
   * lock, which then automatically updates the monitoring values in the <code>
   * MetricsSource</code>. This allows easy "slide in" of lock monitoring where
   * originally only a standard R/W lock was used.
   *
   * @param lock The original R/W lock to wrap for monitoring
   * @param metrics The target for lock monitoring
   */
  private ReadWriteLockMetrics(ReadWriteLock lock, MetricsSource metrics) {
    Preconditions.checkNotNull(lock);
    Preconditions.checkArgument(metrics instanceof LockMetricSource,
        "Invalid MetricsSource");

    LockMetricSource lms = (LockMetricSource)metrics;
    readLock = new LockWrapper(lock.readLock(), lms.readLockWaitTimeTotal,
                               lms.readLockWaitTimeMax, lms.readLockCounts);
    writeLock = new LockWrapper(lock.writeLock(), lms.writeLockWaitTimeTotal,
                                lms.writeLockWaitTimeMax, lms.writeLockCounts);
  }

  @Override
  public Lock readLock() {
    return readLock;
  }

  @Override
  public Lock writeLock() {
    return writeLock;
  }
}
