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

package org.apache.hadoop.hive.ql.log;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * PerfLogger.
 *
 * Can be used to measure and log the time spent by a piece of code.
 */
public class PerfLogger {
  public static final String ACQUIRE_READ_WRITE_LOCKS = "acquireReadWriteLocks";
  public static final String COMPILE = "compile";
  public static final String DO_AUTHORIZATION = "doAuthorization";
  public static final String DRIVER_EXECUTE = "Driver.execute";
  public static final String RELEASE_LOCKS = "releaseLocks";
  public static final String PRUNE_LISTING = "prune-listing";
  public static final String PARTITION_RETRIEVING = "partition-retrieving";
  public static final String PRE_HOOK = "PreHook.";
  public static final String POST_HOOK = "PostHook.";
  public static final String FAILURE_HOOK = "FailureHook.";
  public static final String DRIVER_RUN = "Driver.run";
  public static final String TIME_TO_SUBMIT = "TimeToSubmit";

  protected static final ThreadLocal<PerfLogger> perfLogger = new ThreadLocal<PerfLogger>();

  protected final Map<String, Long> startTimes = new HashMap<String, Long>();
  protected final Map<String, Long> endTimes = new HashMap<String, Long>();

  static final private Log LOG = LogFactory.getLog(PerfLogger.class.getName());

  protected PerfLogger() {
    // Use getPerfLogger to get an instance of PerfLogger
  }

  public static PerfLogger getPerfLogger() {
    return getPerfLogger(false);
  }

  /**
   * Call this function to get an instance of PerfLogger.
   *
   * Use resetPerfLogger to require a new instance.  Useful at the beginning of execution.
   *
   * @return  Tries to return an instance of the class whose name is configured in
   *          hive.exec.perf.logger, but if it can't it just returns an instance of
   *          the base PerfLogger class
   */
  public static PerfLogger getPerfLogger(boolean resetPerfLogger) {
    if (perfLogger.get() == null || resetPerfLogger) {
      if (SessionState.get() == null) {
        perfLogger.set(new PerfLogger());
      } else {
        HiveConf conf = SessionState.get().getConf();
        try {
          perfLogger.set((PerfLogger) ReflectionUtils.newInstance(conf.getClassByName(
              conf.getVar(ConfVars.HIVE_PERF_LOGGER)), conf));
        } catch (ClassNotFoundException e) {
          LOG.error("Performance Logger Class not found:" + e.getMessage());
          perfLogger.set(new PerfLogger());
        }
      }
    }
    return perfLogger.get();
  }

  /**
   * Call this function when you start to measure time spent by a piece of code.
   * @param _log the logging object to be used.
   * @param method method or ID that identifies this perf log element.
   */
  public void PerfLogBegin(Log _log, String method) {
    long startTime = System.currentTimeMillis();
    _log.info("<PERFLOG method=" + method + ">");
    startTimes.put(method, new Long(startTime));
  }

  /**
   * Call this function in correspondence of PerfLogBegin to mark the end of the measurement.
   * @param _log
   * @param method
   * @return long duration  the difference between now and startTime, or -1 if startTime is null
   */
  public long PerfLogEnd(Log _log, String method) {
    Long startTime = startTimes.get(method);
    long endTime = System.currentTimeMillis();
    long duration = -1;

    endTimes.put(method, new Long(endTime));

    StringBuilder sb = new StringBuilder("</PERFLOG method=").append(method);
    if (startTime != null) {
      sb.append(" start=").append(startTime);
    }
    sb.append(" end=").append(endTime);
    if (startTime != null) {
      duration = endTime - startTime.longValue();
      sb.append(" duration=").append(duration);
    }
    sb.append(">");
    _log.info(sb);

    return duration;
  }

  /**
   * Call this function at the end of processing a query (any time after the last call to PerfLogEnd
   * for a given query) to run any cleanup/final steps that need to be run
   * @param _log
   */
  public void close(Log _log, QueryPlan queryPlan) {

  }

  public Long getStartTime(String method) {
    return startTimes.get(method);
  }

  public Long getEndTime(String method) {
    return endTimes.get(method);
  }
}
