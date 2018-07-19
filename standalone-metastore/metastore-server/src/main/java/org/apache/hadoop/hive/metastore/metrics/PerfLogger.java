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

package org.apache.hadoop.hive.metastore.metrics;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * PerfLogger.
 *
 * Can be used to measure and log the time spent by a piece of code.
 */
public class PerfLogger {
  protected final Map<String, Long> startTimes = new HashMap<>();
  protected final Map<String, Long> endTimes = new HashMap<>();

  static final private Logger LOG = LoggerFactory.getLogger(PerfLogger.class.getName());
  protected static final ThreadLocal<PerfLogger> perfLogger = new ThreadLocal<>();


  private PerfLogger() {
    // Use getPerfLogger to get an instance of PerfLogger
  }

  /**
   * Get the singleton PerfLogger instance.
   * @param resetPerfLogger if false, get the current PerfLogger, or create a new one if a
   *                        current one does not exist.  If true, a new instance of PerfLogger
   *                        will be returned rather than the existing one.  Note that the
   *                        existing PerfLogger is not shutdown and any object which already has a
   *                        reference to it may continue to use it. But all future calls to this
   *                        method with this set to false will get the new PerfLogger instance.
   * @return a PerfLogger
   */
  public static PerfLogger getPerfLogger(boolean resetPerfLogger) {
    PerfLogger result = perfLogger.get();
    if (resetPerfLogger || result == null) {
      result = new PerfLogger();
      perfLogger.set(result);
    }
    return result;
  }

  public static void setPerfLogger(PerfLogger resetPerfLogger) {
    perfLogger.set(resetPerfLogger);
  }

  /**
   * Call this function when you start to measure time spent by a piece of code.
   * @param callerName the logging object to be used.
   * @param method method or ID that identifies this perf log element.
   */
  public void PerfLogBegin(String callerName, String method) {
    long startTime = System.currentTimeMillis();
    startTimes.put(method, new Long(startTime));
    if (LOG.isDebugEnabled()) {
      LOG.debug("<PERFLOG method=" + method + " from=" + callerName + ">");
    }
    beginMetrics(method);
  }
  /**
   * Call this function in correspondence of PerfLogBegin to mark the end of the measurement.
   * @param callerName
   * @param method
   * @return long duration  the difference between now and startTime, or -1 if startTime is null
   */
  public long PerfLogEnd(String callerName, String method) {
    return PerfLogEnd(callerName, method, null);
  }

  /**
   * Call this function in correspondence of PerfLogBegin to mark the end of the measurement.
   * @param callerName
   * @param method
   * @return long duration  the difference between now and startTime, or -1 if startTime is null
   */
  public long PerfLogEnd(String callerName, String method, String additionalInfo) {
    Long startTime = startTimes.get(method);
    long endTime = System.currentTimeMillis();
    endTimes.put(method, new Long(endTime));
    long duration = startTime == null ? -1 : endTime - startTime.longValue();

    if (LOG.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder("</PERFLOG method=").append(method);
      if (startTime != null) {
        sb.append(" start=").append(startTime);
      }
      sb.append(" end=").append(endTime);
      if (startTime != null) {
        sb.append(" duration=").append(duration);
      }
      sb.append(" from=").append(callerName);
      if (additionalInfo != null) {
        sb.append(" ").append(additionalInfo);
      }
      sb.append(">");
      LOG.debug(sb.toString());
    }
    endMetrics(method);
    return duration;
  }

  public Long getStartTime(String method) {
    long startTime = 0L;

    if (startTimes.containsKey(method)) {
      startTime = startTimes.get(method);
    }
    return startTime;
  }

  public Long getEndTime(String method) {
    long endTime = 0L;

    if (endTimes.containsKey(method)) {
      endTime = endTimes.get(method);
    }
    return endTime;
  }

  public boolean startTimeHasMethod(String method) {
    return startTimes.containsKey(method);
  }

  public boolean endTimeHasMethod(String method) {
    return endTimes.containsKey(method);
  }

  public Long getDuration(String method) {
    long duration = 0;
    if (startTimes.containsKey(method) && endTimes.containsKey(method)) {
      duration = endTimes.get(method) - startTimes.get(method);
    }
    return duration;
  }


  public ImmutableMap<String, Long> getStartTimes() {
    return ImmutableMap.copyOf(startTimes);
  }

  public ImmutableMap<String, Long> getEndTimes() {
    return ImmutableMap.copyOf(endTimes);
  }

  //Methods for metrics integration.  Each thread-local PerfLogger will open/close scope during each perf-log method.
  protected transient Map<String, Timer.Context> timerContexts = new HashMap<>();

  private void beginMetrics(String method) {
    Timer timer = Metrics.getOrCreateTimer(MetricsConstants.API_PREFIX + method);
    if (timer != null) {
      timerContexts.put(method, timer.time());
    }

  }

  private void endMetrics(String method) {
    Timer.Context context = timerContexts.remove(method);
    if (context != null) {
      context.close();
    }
  }

  /**
   * Cleans up any dangling perfLog metric call scopes.
   */
  public void cleanupPerfLogMetrics() {
    for (Timer.Context context : timerContexts.values()) {
      context.close();
    }
    timerContexts.clear();
  }
}
