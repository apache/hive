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
package org.apache.hadoop.hive.ql.scheduled;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

/**
 *  Scheduled query executor operational context.
 *
 *  Contains all the information classes which are needed to service scheduled queries.
 */
public class ScheduledQueryExecutionContext {

  public final ExecutorService executor;
  public final IScheduledQueryMaintenanceService schedulerService;
  public final HiveConf conf;
  public final String executorHostName;

  public ScheduledQueryExecutionContext(
      ExecutorService executor,
      HiveConf conf,
      IScheduledQueryMaintenanceService service) {
    this.executor = executor;
    this.conf = conf;
    this.schedulerService = service;
    try {
      this.executorHostName = InetAddress.getLocalHost().getHostName();
      if (executorHostName == null) {
        throw new RuntimeException("Hostname is null; Can't function without a valid hostname!");
      }
    } catch (UnknownHostException e) {
      throw new RuntimeException("Can't function without a valid hostname!", e);
    }
  }

  /**
   * @return time in milliseconds
   */
  public long getIdleSleepTime() {
    return conf.getTimeVar(ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_IDLE_SLEEP_TIME, TimeUnit.MILLISECONDS);
  }

  // Interval
  public long getProgressReporterSleepTime() {
    return conf.getTimeVar(ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_PROGRESS_REPORT_INTERVAL, TimeUnit.MILLISECONDS);
  }

  public int getNumberOfExecutors() {
    return conf.getIntVar(ConfVars.HIVE_SCHEDULED_QUERIES_MAX_EXECUTORS);
  }

}
