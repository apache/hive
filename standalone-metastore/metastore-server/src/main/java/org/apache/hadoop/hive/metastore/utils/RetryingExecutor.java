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

package org.apache.hadoop.hive.metastore.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryingExecutor<T> {
  private static final Logger LOG = LoggerFactory.getLogger(RetryingExecutor.class);

  private final int maxRetries;
  private long sleepInterval = 1000;
  private final Callable<T> command;
  private Predicate<Throwable> retryPolicy;
  private int currentRetries = 0;
  private String commandName;
  private Function<Long, Long> sleepIntervalFunc;

  public RetryingExecutor(int maxRetries, Callable<T> command) {
    this.maxRetries = maxRetries;
    this.command = command;
    // default commandName unless specified
    this.commandName = StackWalker.getInstance()
        .walk(frames -> frames
            .skip(1)
            .findFirst()
            .map(StackWalker.StackFrame::getMethodName)).get();
  }

  public RetryingExecutor<T> onRetry(Predicate<Throwable> retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  public RetryingExecutor<T> commandName(String name) {
    this.commandName = name;
    return this;
  }

  public RetryingExecutor<T> sleepInterval(long sleepInterval) {
    return sleepInterval(sleepInterval, null);
  }

  public RetryingExecutor<T> sleepInterval(long sleepInterval,
      Function<Long, Long> sleepIntervalFunc) {
    this.sleepInterval = sleepInterval;
    this.sleepIntervalFunc = sleepIntervalFunc;
    return this;
  }

  public T run() throws MetaException {
    while (true) {
      try {
        return command.call();
      } catch (Exception e) {
        Throwable t = checkException(e);
        LOG.info("Attempting to retry the command:{} in {} out of {} retries, error message: {}",
            commandName, currentRetries, maxRetries, t.getMessage());
        if (currentRetries >= maxRetries) {
          String message = "Couldn't finish the command: " + commandName +
              " because we reached the maximum of retries: " + maxRetries;
          LOG.error(message, e);
          throw new MetaException(message + " :: " + e.getMessage());
        }
        currentRetries++;
        try {
          Thread.sleep(getSleepInterval());
        } catch (InterruptedException e1) {
          String msg = "Couldn't run the command: " + commandName + " in " + currentRetries +
              " retry, because the following error: ";
          LOG.error(msg, e1);
          throw new MetaException(msg + e1.getMessage());
        }
        LOG.debug("Exception occurred in running: {}", commandName, t);
      }
    }
  }

  private Throwable checkException(Throwable e) throws MetaException {
    Throwable cause = e;
    if (e instanceof InvocationTargetException ||
        e instanceof UndeclaredThrowableException) {
      cause = e.getCause();
    }
    if (retryPolicy != null && !retryPolicy.test(cause)) {
      String message = "See a fatal exception, avoid to retry the command:" + commandName;
      LOG.info(message, cause);
      String errorMessage = ExceptionUtils.getMessage(cause);
      Throwable rootCause = ExceptionUtils.getRootCause(e);
      errorMessage += (rootCause == null ? "" : ("\nRoot cause: " + rootCause));
      throw new MetaException(message + " :: " + errorMessage);
    }
    return cause;
  }

  public static class RetryException extends Exception {
    private static final long serialVersionUID = 1L;

    public RetryException(Exception ex) {
      super(ex);
    }

    public RetryException(String msg) {
      super(msg);
    }
  }

  public long getSleepInterval() {
    if (sleepIntervalFunc != null) {
      this.sleepInterval = sleepIntervalFunc.apply(sleepInterval);
    }
    return sleepInterval;
  }
}
