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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * Utility singleton class to manage all the scheduler threads.
 */
public class SchedulerThreadPool {

  private static volatile ScheduledExecutorService pool;

  /**
   * Initialize the thread pool with configuration.
   */
  public static void initialize(Configuration conf) {
    if (pool == null) {
      synchronized (SchedulerThreadPool.class) {
        if (pool == null) {
          ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
              .setNameFormat("Hive-exec Scheduled Worker %d").build();
          pool = Executors.newScheduledThreadPool(HiveConf.getIntVar(conf,
              HiveConf.ConfVars.HIVE_EXEC_SCHEDULED_POOL_NUM_THREADS), threadFactory);
        }
      }
    }
  }

  public static ScheduledExecutorService getInstance() {
    if (pool == null) {
      // allow initialization with default pool size because of arbitrary hive-exec.jar usages
      initialize(new HiveConf());
    }
    return pool;
  }

  public static void shutdown() {
    if (pool != null) {
      synchronized (SchedulerThreadPool.class) {
        if (pool != null) {
          pool.shutdown();
          pool = null;
        }
      }
    }
  }
}
