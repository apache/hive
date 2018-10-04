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

package org.apache.hadoop.hive.ql.lock;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Compile Lock Factory.
 */
public final class CompileLockFactory {

  private static final ReentrantLock SERIALIZABLE_COMPILE_LOCK = new ReentrantLock();

  private CompileLockFactory() {
  }

  public static CompileLock newInstance(HiveConf conf, String command) {
    Lock underlying = SERIALIZABLE_COMPILE_LOCK;

    boolean isParallelEnabled = (conf != null)
        && HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_PARALLEL_COMPILATION);

    if (isParallelEnabled) {
      int compileQuota = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_SERVER2_PARALLEL_COMPILATION_LIMIT);

      underlying = (compileQuota > 0) ?
          SessionWithQuotaCompileLock.instance : SessionState.get().getCompileLock();
    }

    long timeout = HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.HIVE_SERVER2_COMPILE_LOCK_TIMEOUT, TimeUnit.SECONDS);

    return new CompileLock(underlying, timeout, command);
  }

  /**
   * Combination of global semaphore and session reentrant lock.
   */
  private enum SessionWithQuotaCompileLock implements Lock {

    instance(SessionState.getSessionConf()
        .getIntVar(HiveConf.ConfVars.HIVE_SERVER2_PARALLEL_COMPILATION_LIMIT));

    private final Semaphore globalCompileQuotas;

    SessionWithQuotaCompileLock(int compilePoolSize) {
      globalCompileQuotas = new Semaphore(compilePoolSize);
    }

    @Override
    public void lock() {
      getSessionLock().lock();
      globalCompileQuotas.acquireUninterruptibly();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
      boolean result = false;
      long startTime = System.nanoTime();

      try {
        result = getSessionLock().tryLock(time, unit)
            && globalCompileQuotas.tryAcquire(
                getRemainingTime(startTime, unit.toNanos(time)), TimeUnit.NANOSECONDS);
      } finally {
        if (!result && getSessionLock().isHeldByCurrentThread()) {
          getSessionLock().unlock();
        }
      }
      return result;
    }

    @Override
    public void unlock() {
      getSessionLock().unlock();
      globalCompileQuotas.release();
    }

    private ReentrantLock getSessionLock() {
      return SessionState.get().getCompileLock();
    }

    private long getRemainingTime(long startTime, long time) {
      long timeout = time - (System.nanoTime() - startTime);
      return (timeout < 0) ? 0 : timeout;
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Condition newCondition() {
      throw new UnsupportedOperationException();
    }
  }

}

