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
package org.apache.hadoop.hive.ql.util;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

public class PeriodicLoggerWithStopwatch {
  protected final Logger LOG;

  private Stopwatch stopwatch = Stopwatch.createUnstarted();
  private int period = 0; // 0: logarithmic, 0<: fix period
  private long trigger = 1; // log if we reached this number
  private long count = 0;
  private long min = 0; // don't log below this value
  private Supplier<String> supplier = null;

  public PeriodicLoggerWithStopwatch() {
    this("PeriodicLoggerWithStopwatch");
  }

  public PeriodicLoggerWithStopwatch(String loggerClass) {
    LOG = LoggerFactory.getLogger(loggerClass);
  }

  public PeriodicLoggerWithStopwatch(Object caller) {
    LOG = LoggerFactory.getLogger(caller.getClass());
  }

  public PeriodicLoggerWithStopwatch logMessageSupplier(Supplier<String> supplier){
    this.supplier = supplier;
    return this;
  }

  public PeriodicLoggerWithStopwatch fixed(int period) {
    this.period = period;
    return this;
  }

  public PeriodicLoggerWithStopwatch logarithmic() {
    this.period = 0;
    return this;
  }

  public PeriodicLoggerWithStopwatch min(int min) {
    this.min = min;
    return this;
  }

  public void increment() {
    count += 1;
    checkAndLog();
  }

  public void increment(long num) {
    count += num;
    checkAndLog();
  }

  public long getCount() {
    return count;
  }

  private void checkAndLog() {
    while (count >= trigger && count >= min && LOG.isInfoEnabled()) {
      trigger = period == 0 ? trigger * 10 : count + period;
      if (trigger < 0 || count < 0) {
        trigger = 0;
        count = 1;
      }
      log();
    }
  }

  /**
   * This can be called to log the current status at any time.
   */
  public void log() {
    if (supplier == null) {
      long elapsedSeconds = getElapsed(TimeUnit.SECONDS);
      LOG.info("{} in {}s, {}/s", count, elapsedSeconds,
          elapsedSeconds == 0 ? 0 : (double) count / elapsedSeconds);
    } else {
      LOG.info(supplier.get());
    }
  }

  public PeriodicLoggerWithStopwatch stop() {
    stopwatch.stop();
    return this;
  }

  public PeriodicLoggerWithStopwatch start() {
    stopwatch.start();
    return this;
  }

  public void reset() {
    stopwatch.reset();
    count = 0;
    trigger = 1;
  }

  private long getElapsed(TimeUnit unit) {
    return stopwatch.elapsed(unit);
  }
}
