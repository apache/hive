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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.hadoop.hive.ql.ErrorMsg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates HS2 compile lock logic.
 */
public final class CompileLock implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(CompileLock.class);

  private static final String LOCK_ACQUIRED_MSG = "Acquired the compile lock.";
  private static final String WAIT_LOCK_ACQUIRE_MSG = "Waiting to acquire compile lock: ";

  private final Lock underlying;
  private final long defaultTimeout;

  private final String command;
  private boolean isLocked = false;

  CompileLock(Lock underlying, long timeout, String command) {
    this.underlying = underlying;
    this.command = command;
    this.defaultTimeout = timeout;
  }

  /**
   * Acquires the compile lock. If the compile lock wait timeout is configured,
   * it will acquire the lock if it is not held by another thread within the given
   * waiting time.
   *
   * @return {@code true} if the lock was successfully acquired,
   * or {@code false} if compile lock wait timeout is configured and
   * either the waiting time elapsed before the lock could be acquired
   * or the current thread was interrupted.
   */
  public boolean tryAcquire() {
    return tryAcquire(defaultTimeout, TimeUnit.SECONDS);
  }

  private boolean tryAcquire(long timeout, TimeUnit unit) {
    // First shot without waiting.
    try {
      if (underlying.tryLock(0, unit)) {
        LOG.debug(LOCK_ACQUIRED_MSG);
        return acquired();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.debug("Interrupted Exception ignored", e);
      return failedToAcquire();
    }

    // If the first shot fails, then we log the waiting messages.
    if (LOG.isDebugEnabled()) {
      LOG.debug(WAIT_LOCK_ACQUIRE_MSG + command);
    }

    if (timeout > 0) {
      try {
        if (!underlying.tryLock(timeout, unit)) {
          LOG.error(ErrorMsg.COMPILE_LOCK_TIMED_OUT.getErrorCodedMsg() + ": " + command);
          return failedToAcquire();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.debug("Interrupted Exception ignored", e);
        return failedToAcquire();
      }
    } else {
      underlying.lock();
    }

    LOG.debug(LOCK_ACQUIRED_MSG);
    return acquired();
  }

  private boolean acquired() {
    return locked(true);
  }

  private boolean failedToAcquire() {
    return locked(false);
  }

  private boolean locked(boolean isLocked) {
    this.isLocked  = isLocked;
    return isLocked;
  }

  /**
   * Releases the compile lock.
   *
   * An underlying {@code Lock} implementation will usually impose restrictions on which
   * thread can release a lock (typically only the holder of the lock can release it)
   * and may throw an (unchecked) exception if the restriction is violated.
   */
  public void release() {
    underlying.unlock();
    isLocked = false;
  }

  @Override
  public void close() {
    if (isLocked) {
      release();
    }
  }

}
