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

package org.apache.hadoop.hive.ql.exec.util;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Class to implement any retry logic in case of exceptions.
 */
public class Retryable {
  private static final long MINIMUM_DELAY_IN_SEC = 60;

  private long totalDurationInSeconds;
  private Set<Class<? extends Exception>> retryOn;
  private Set<Class<? extends Exception>> failOn;
  private Set<Class<? extends Exception>> failOnParentExceptions;
  private long initialDelayInSeconds;
  private long maxRetryDelayInSeconds;
  private double backOff;
  private int maxJitterInSeconds;

  private Retryable() {
    this.retryOn = new HashSet<>();
    this.failOn = new HashSet<>();
    this.failOnParentExceptions = new HashSet<>();
    this.initialDelayInSeconds = HiveConf.toTime(HiveConf.ConfVars.REPL_RETRY_INTIAL_DELAY.defaultStrVal,
      HiveConf.getDefaultTimeUnit(HiveConf.ConfVars.REPL_RETRY_INTIAL_DELAY), TimeUnit.SECONDS);
    this.maxRetryDelayInSeconds = HiveConf.toTime(HiveConf.ConfVars.REPL_RETRY_MAX_DELAY_BETWEEN_RETRIES.defaultStrVal,
      HiveConf.getDefaultTimeUnit(HiveConf.ConfVars.REPL_RETRY_MAX_DELAY_BETWEEN_RETRIES), TimeUnit.SECONDS);
    this.backOff = HiveConf.ConfVars.REPL_RETRY_BACKOFF_COEFFICIENT.defaultFloatVal;
    this.maxJitterInSeconds = (int) HiveConf.toTime(HiveConf.ConfVars.REPL_RETRY_JITTER.defaultStrVal,
      HiveConf.getDefaultTimeUnit(HiveConf.ConfVars.REPL_RETRY_JITTER), TimeUnit.SECONDS);
    this.totalDurationInSeconds = HiveConf.toTime(HiveConf.ConfVars.REPL_RETRY_TOTAL_DURATION.defaultStrVal,
      HiveConf.getDefaultTimeUnit(HiveConf.ConfVars.REPL_RETRY_TOTAL_DURATION), TimeUnit.SECONDS);;
  }

  public static Builder builder() {
    return new Builder();
  }

  public <T> T executeCallable(Callable<T> callable) throws Exception {
    long startTime = System.currentTimeMillis();
    long delay = this.initialDelayInSeconds;
    Exception previousException = null;
    while(true) {
      try {
        if (UserGroupInformation.isSecurityEnabled()) {
          SecurityUtils.reloginExpiringKeytabUser();
          return UserGroupInformation.getLoginUser().doAs((PrivilegedExceptionAction<T>) () -> callable.call());
        } else {
          return callable.call();
        }
      } catch (Exception e) {
        if (this.failOnParentExceptions.stream().noneMatch(k -> k.isAssignableFrom(e.getClass()))
          && this.failOn.stream().noneMatch(k -> e.getClass().equals(k))
          && this.retryOn.stream().anyMatch(k -> e.getClass().isAssignableFrom(k))) {
          if (elapsedTimeInSeconds(startTime) + delay > this.totalDurationInSeconds) {
            // case where waiting would go beyond max duration. So throw exception and return
            throw e;
          }
          sleep(delay);
          //retry case. compute next sleep time
          delay = getNextDelay(delay, previousException, e);
          // reset current captured exception.
          previousException = e;
        } else {
          // Exception cannot be retried on. Throw exception and return
          throw e;
        }
      }
    }
  }

  private void sleep(long seconds) {
    try {
      Thread.sleep(seconds * 1000);
    } catch (InterruptedException e) {
      // no-op.. just proceed
    }
  }

  private long getNextDelay(long currentDelay, final Exception previousException, final Exception currentException) {
    if (previousException != null && !previousException.getClass().equals(currentException.getClass())) {
      // New exception encountered. Returning initial delay for next retry.
      return this.initialDelayInSeconds;
    }
    if (currentDelay <= 0) { // in case initial delay was set to 0.
      currentDelay = MINIMUM_DELAY_IN_SEC;
    }
    currentDelay *= this.backOff;
    if (this.maxJitterInSeconds > 0) {
      currentDelay += new Random().nextInt(this.maxJitterInSeconds);
    }
    if (currentDelay > this.maxRetryDelayInSeconds) {
      currentDelay = this.maxRetryDelayInSeconds;
    }
    return  currentDelay;
  }

  private long elapsedTimeInSeconds(long fromTimeMillis) {
    return (System.currentTimeMillis() - fromTimeMillis)/ 1000;
  }

  public static class Builder {
    private final Retryable runnable = new Retryable();
    public Builder() {
    }

    public Builder withHiveConf(HiveConf conf) {
      runnable.totalDurationInSeconds = conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_TOTAL_DURATION, TimeUnit.SECONDS);
      runnable.initialDelayInSeconds = conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_INTIAL_DELAY, TimeUnit.SECONDS);
      runnable.maxRetryDelayInSeconds = conf.getTimeVar(HiveConf.ConfVars
        .REPL_RETRY_MAX_DELAY_BETWEEN_RETRIES, TimeUnit.SECONDS);
      runnable.backOff = conf.getFloatVar(HiveConf.ConfVars.REPL_RETRY_BACKOFF_COEFFICIENT);
      runnable.maxJitterInSeconds = (int) conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_JITTER, TimeUnit.SECONDS);
      return this;
    }

    public Retryable build() {
      return runnable;
    }

    public Builder withTotalDuration(long maxDuration) {
      runnable.totalDurationInSeconds = maxDuration;
      return this;
    }

    // making this thread safe as it appends to list
    public synchronized Builder withRetryOnException(final Class<? extends Exception> exceptionClass) {
      if (exceptionClass != null) {
        runnable.retryOn.add(exceptionClass);
      }
      return this;
    }

    public synchronized Builder withRetryOnExceptionList(final List<Class<? extends Exception>> exceptionClassList) {
      for (final Class<? extends Exception> exceptionClass : exceptionClassList) {
        if (exceptionClass != null) {
          runnable.retryOn.add(exceptionClass);
        }
      }
      return this;
    }

    public synchronized Builder withFailOnParentException(final Class<? extends Exception> exceptionClass) {
      if (exceptionClass != null) {
        runnable.failOnParentExceptions.add(exceptionClass);
      }
      return this;
    }

    public synchronized Builder withFailOnParentExceptionList(final List<Class<?
            extends Exception>> exceptionClassList) {
      for (final Class<? extends Exception> exceptionClass : exceptionClassList) {
        if (exceptionClass != null) {
          runnable.failOnParentExceptions.add(exceptionClass);
        }
      }
      return this;
    }

    public synchronized Builder withFailOnException(final Class<? extends Exception> exceptionClass) {
      if (exceptionClass != null) {
        runnable.failOn.add(exceptionClass);
      }
      return this;
    }

    public synchronized Builder withFailOnExceptionList(final List<Class<?
      extends Exception>> exceptionClassList) {
      for (final Class<? extends Exception> exceptionClass : exceptionClassList) {
        if (exceptionClass != null) {
          runnable.failOn.add(exceptionClass);
        }
      }
      return this;
    }

    public Builder withInitialDelay(long initialDelayInSeconds) {
      runnable.initialDelayInSeconds = initialDelayInSeconds;
      return this;
    }

    public Builder withMaxRetryDelay(long maxRetryDelayInSeconds) {
      runnable.maxRetryDelayInSeconds = maxRetryDelayInSeconds;
      return this;
    }

    public Builder withBackoff(double backoff) {
      runnable.backOff = backoff;
      return this;
    }

    public Builder withMaxJitterValue(int maxJitterInSeconds) {
      runnable.maxJitterInSeconds = maxJitterInSeconds;
      return this;
    }
  }
}
