/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Utility singleton class to manage all the threads.
 */
public class ThreadPool {

  static final private Logger LOG = LoggerFactory.getLogger(ThreadPool.class);
  private static ThreadPool self = null;
  private static ScheduledExecutorService pool;

  public static synchronized ThreadPool initialize(Configuration conf) {
    if (self == null) {
      self = new ThreadPool(conf);
      LOG.debug("ThreadPool initialized");
    }
    return self;
  }

  private ThreadPool(Configuration conf) {
    pool = Executors.newScheduledThreadPool(MetastoreConf.getIntVar(conf,
        MetastoreConf.ConfVars.THREAD_POOL_SIZE));
  }

  public static ScheduledExecutorService getPool() {
    if (self == null) {
      throw new RuntimeException("ThreadPool accessed before initialized");
    }
    return pool;
  }

  public static synchronized void shutdown() {
    if (self != null) {
      pool.shutdown();
      self = null;
    }
  }
}
