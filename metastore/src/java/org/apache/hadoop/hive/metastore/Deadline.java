/**
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
  private long timeout;

  /**
   * it is reset before executing a method
   */
  private long startTime = -1;

  /**
   * The name of public methods in HMSHandler
   */
  private String method;

  private Deadline(long timeout) {
    this.timeout = timeout;
  }

  /**
   * Deadline object per thread.
   */
  private static final ThreadLocal<Deadline> DEADLINE_THREAD_LOCAL = new
      ThreadLocal<Deadline>() {
        @Override
        protected Deadline initialValue() {
          return null;
        }
      };

  static void setCurrentDeadline(Deadline deadline) {
    DEADLINE_THREAD_LOCAL.set(deadline);
  }

  static Deadline getCurrentDeadline() {
    return DEADLINE_THREAD_LOCAL.get();
  }

  static void removeCurrentDeadline() {
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
   * @param timeout
   */
  public static void resetTimeout(long timeout) throws MetaException {
    if (timeout <= 0) {
      throw newMetaException(new DeadlineException("The reset timeout value should be " +
          "larger than 0: " + timeout));
    }
    Deadline deadline = getCurrentDeadline();
    if (deadline != null) {
      deadline.timeout = timeout;
    } else {
      throw newMetaException(new DeadlineException("The threadlocal Deadline is null," +
          " please register it firstly."));
    }
  }

  /**
   * Check whether the timer is started.
   * @return
   * @throws MetaException
   */
  public static boolean isStarted() throws MetaException {
    Deadline deadline = getCurrentDeadline();
    if (deadline != null) {
      return deadline.startTime >= 0;
    } else {
      throw newMetaException(new DeadlineException("The threadlocal Deadline is null," +
          " please register it firstly."));
    }
  }

  /**
   * start the timer before a method is invoked.
   * @param method
   */
  public static void startTimer(String method) throws MetaException {
    Deadline deadline = getCurrentDeadline();
    if (deadline != null) {
      deadline.startTime = System.currentTimeMillis();
      deadline.method = method;
    } else {
      throw newMetaException(new DeadlineException("The threadlocal Deadline is null," +
          " please register it firstly."));
    }
  }

  /**
   * end the time after a method is done.
   */
  public static void stopTimer() throws MetaException {
    Deadline deadline = getCurrentDeadline();
    if (deadline != null) {
      deadline.startTime = -1;
      deadline.method = null;
    } else {
      throw newMetaException(new DeadlineException("The threadlocal Deadline is null," +
          " please register it firstly."));
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
   * @throws DeadlineException when the method timeout
   */
  public static void checkTimeout() throws MetaException {
    Deadline deadline = getCurrentDeadline();
    if (deadline != null) {
      deadline.check();
    } else {
      throw newMetaException(new DeadlineException("The threadlocal Deadline is null," +
          " please register it first."));
    }
  }

  private void check() throws MetaException{
    try {
      if (startTime < 0) {
        throw new DeadlineException("Should execute startTimer() method before " +
            "checkTimeout. Error happens in method: " + method);
      }
      if (startTime + timeout < System.currentTimeMillis()) {
        throw new DeadlineException("Timeout when executing method: " + method);
      }
    } catch (DeadlineException e) {
      throw newMetaException(e);
    }
  }

  /**
   * convert DeadlineException to MetaException
   * @param e
   * @return
   */
  private static MetaException newMetaException(DeadlineException e) {
    MetaException metaException = new MetaException(e.getMessage());
    metaException.initCause(e);
    return metaException;
  }
}
