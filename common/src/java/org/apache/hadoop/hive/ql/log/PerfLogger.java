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

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.common.MetricsScope;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.ReflectionUtils;
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
  public static final String ACQUIRE_READ_WRITE_LOCKS = "acquireReadWriteLocks";
  public static final String COMPILE = "compile";
  public static final String PARSE = "parse";
  public static final String ANALYZE = "semanticAnalyze";
  public static final String OPTIMIZER = "optimizer";
  public static final String DO_AUTHORIZATION = "doAuthorization";
  public static final String DRIVER_EXECUTE = "Driver.execute";
  public static final String INPUT_SUMMARY = "getInputSummary";
  public static final String GET_SPLITS = "getSplits";
  public static final String RUN_TASKS = "runTasks";
  public static final String SERIALIZE_PLAN = "serializePlan";
  public static final String DESERIALIZE_PLAN = "deserializePlan";
  public static final String CLONE_PLAN = "clonePlan";
  public static final String TASK = "task.";
  public static final String RELEASE_LOCKS = "releaseLocks";
  public static final String PRUNE_LISTING = "prune-listing";
  public static final String PARTITION_RETRIEVING = "partition-retrieving";
  public static final String PRE_HOOK = "PreHook.";
  public static final String POST_HOOK = "PostHook.";
  public static final String FAILURE_HOOK = "FailureHook.";
  public static final String DRIVER_RUN = "Driver.run";
  public static final String TEZ_COMPILER = "TezCompiler";
  public static final String TEZ_SUBMIT_TO_RUNNING = "TezSubmitToRunningDag";
  public static final String TEZ_BUILD_DAG = "TezBuildDag";
  public static final String TEZ_SUBMIT_DAG = "TezSubmitDag";
  public static final String TEZ_RUN_DAG = "TezRunDag";
  public static final String TEZ_CREATE_VERTEX = "TezCreateVertex.";
  public static final String TEZ_RUN_VERTEX = "TezRunVertex.";
  public static final String TEZ_INITIALIZE_PROCESSOR = "TezInitializeProcessor";
  public static final String TEZ_RUN_PROCESSOR = "TezRunProcessor";
  public static final String TEZ_INIT_OPERATORS = "TezInitializeOperators";
  public static final String LOAD_HASHTABLE = "LoadHashtable";

  public static final String SPARK_SUBMIT_TO_RUNNING = "SparkSubmitToRunning";
  public static final String SPARK_BUILD_PLAN = "SparkBuildPlan";
  public static final String SPARK_BUILD_RDD_GRAPH = "SparkBuildRDDGraph";
  public static final String SPARK_SUBMIT_JOB = "SparkSubmitJob";
  public static final String SPARK_RUN_JOB = "SparkRunJob";
  public static final String SPARK_CREATE_TRAN = "SparkCreateTran.";
  public static final String SPARK_RUN_STAGE = "SparkRunStage.";
  public static final String SPARK_INIT_OPERATORS = "SparkInitializeOperators";
  public static final String SPARK_GENERATE_TASK_TREE = "SparkGenerateTaskTree";
  public static final String SPARK_OPTIMIZE_OPERATOR_TREE = "SparkOptimizeOperatorTree";
  public static final String SPARK_OPTIMIZE_TASK_TREE = "SparkOptimizeTaskTree";
  public static final String SPARK_FLUSH_HASHTABLE = "SparkFlushHashTable.";

  protected final Map<String, Long> startTimes = new HashMap<String, Long>();
  protected final Map<String, Long> endTimes = new HashMap<String, Long>();

  static final private Logger LOG = LoggerFactory.getLogger(PerfLogger.class.getName());
  protected static final ThreadLocal<PerfLogger> perfLogger = new ThreadLocal<PerfLogger>();


  private PerfLogger() {
    // Use getPerfLogger to get an instance of PerfLogger
  }

  public static PerfLogger getPerfLogger(HiveConf conf, boolean resetPerfLogger) {
    PerfLogger result = perfLogger.get();
    if (resetPerfLogger || result == null) {
      if (conf == null) {
        result = new PerfLogger();
      } else {
        try {
          result = (PerfLogger) ReflectionUtils.newInstance(conf.getClassByName(
            conf.getVar(HiveConf.ConfVars.HIVE_PERF_LOGGER)), conf);
        } catch (ClassNotFoundException e) {
          LOG.error("Performance Logger Class not found:" + e.getMessage());
          result = new PerfLogger();
        }
      }
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
  transient Map<String, MetricsScope> openScopes = new HashMap<String, MetricsScope>();

  private void beginMetrics(String method) {
    Metrics metrics = MetricsFactory.getInstance();
    if (metrics != null) {
      MetricsScope scope = metrics.createScope(MetricsConstant.API_PREFIX + method);
      openScopes.put(method, scope);
    }

  }

  private void endMetrics(String method) {
    Metrics metrics = MetricsFactory.getInstance();
    if (metrics != null) {
      MetricsScope scope = openScopes.remove(method);
      if (scope != null) {
        metrics.endScope(scope);
      }
    }
  }

  /**
   * Cleans up any dangling perfLog metric call scopes.
   */
  public void cleanupPerfLogMetrics() {
    Metrics metrics = MetricsFactory.getInstance();
    if (metrics != null) {
      for (MetricsScope openScope : openScopes.values()) {
        metrics.endScope(openScope);
      }
    }
    openScopes.clear();
  }
}
