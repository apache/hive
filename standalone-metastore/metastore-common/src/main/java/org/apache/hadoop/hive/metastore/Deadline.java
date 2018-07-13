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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * This class is used to monitor long running methods in a thread.
 * It is recommended to use it as a ThreadLocal variable.
 */
public class Deadline {
  private static final Logger LOG = LoggerFactory.getLogger(Deadline.class.getName());

  /**
   * its value is init from conf, and could be reset from client.
   */
  private long timeoutNanos;

  /**
   * it is reset before executing a method
   */
  private long startTime = NO_DEADLINE;

  /**
   * The name of public methods in HMSHandler
   */
  private String method;

  private Deadline(long timeoutMs) {
    this.timeoutNanos = timeoutMs * 1000000L;
  }

  /**
   * Deadline object per thread.
   */
  private static final ThreadLocal<Deadline> DEADLINE_THREAD_LOCAL = new ThreadLocal<Deadline>() {
        @Override
        protected Deadline initialValue() {
          return null;
        }
      };

  private static void setCurrentDeadline(Deadline deadline) {
    DEADLINE_THREAD_LOCAL.set(deadline);
  }

  static Deadline getCurrentDeadline() {
    return DEADLINE_THREAD_LOCAL.get();
  }

  private static void removeCurrentDeadline() {
    DEADLINE_THREAD_LOCAL.remove();
  }

  /**
   * register a Deadline threadlocal object to current thread.
   * @param timeout
   */
  public static void registerIfNot(long timeout) {
    if (getCurrentDeadline() == null) {
      setCurrentDeadline(new Deadline(timeout));
    }
  }

  /**
   * reset the timeout value of this timer.
   * @param timeoutMs
   */
  public static void resetTimeout(long timeoutMs) throws MetaException {
    if (timeoutMs <= 0) {
      throw MetaStoreUtils.newMetaException(new DeadlineException("The reset timeout value should be " +
          "larger than 0: " + timeoutMs));
    }
    Deadline deadline = getCurrentDeadline();
    if (deadline != null) {
      deadline.timeoutNanos = timeoutMs * 1000000L;
    } else {
      throw MetaStoreUtils.newMetaException(new DeadlineException("The threadlocal Deadline is null," +
          " please register it first."));
    }
  }

  /**
   * start the timer before a method is invoked.
   * @param method method to be invoked
   */
  public static boolean startTimer(String method) throws MetaException {
    Deadline deadline = getCurrentDeadline();
    if (deadline == null) {
      throw MetaStoreUtils.newMetaException(new DeadlineException("The threadlocal Deadline is null," +
          " please register it first."));
    }
    if (deadline.startTime != NO_DEADLINE) return false;
    deadline.method = method;
    do {
      deadline.startTime = System.nanoTime();
    } while (deadline.startTime == NO_DEADLINE);
    return true;
  }

  /**
   * end the time after a method is done.
   */
  public static void stopTimer() throws MetaException {
    Deadline deadline = getCurrentDeadline();
    if (deadline != null) {
      deadline.startTime = NO_DEADLINE;
      deadline.method = null;
    } else {
      throw MetaStoreUtils.newMetaException(new DeadlineException("The threadlocal Deadline is null," +
          " please register it first."));
    }
  }

  /**
   * remove the registered Deadline threadlocal object from current thread.
   */
  public static void clear() {
    removeCurrentDeadline();
  }

  /**
   * Check whether the long running method timeout.
   * @throws MetaException when the method timeout
   */
  public static void checkTimeout() throws MetaException {
    Deadline deadline = getCurrentDeadline();
    if (deadline != null) {
      deadline.check();
    } else {
      throw MetaStoreUtils.newMetaException(new DeadlineException("The threadlocal Deadline is null," +
          " please register it first."));
    }
  }

  private static final long NO_DEADLINE = Long.MIN_VALUE;

  private void check() throws MetaException{
    try {
      if (startTime == NO_DEADLINE) {
        throw new DeadlineException("Should execute startTimer() method before " +
            "checkTimeout. Error happens in method: " + method);
      }
      long elapsedTime = System.nanoTime() - startTime;
      if (elapsedTime > timeoutNanos) {
        throw new DeadlineException("Timeout when executing method: " + method + "; "
            + (elapsedTime / 1000000L) + "ms exceeds " + (timeoutNanos / 1000000L)  + "ms");
      }
    } catch (DeadlineException e) {
      throw MetaStoreUtils.newMetaException(e);
    }
  }
}
