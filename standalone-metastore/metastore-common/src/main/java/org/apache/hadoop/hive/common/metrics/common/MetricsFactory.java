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
package org.apache.hadoop.hive.common.metrics.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import java.lang.reflect.Constructor;

/**
 * Class that manages a static Metric instance for this process.
 */
public class MetricsFactory {

  //Volatile ensures that static access returns Metrics instance in fully-initialized state.
  //Alternative is to synchronize static access, which has performance penalties.
  private volatile static Metrics metrics;

  /**
   * Initializes static Metrics instance.
   */
  public synchronized static void init(Configuration conf) throws Exception {
    if (metrics == null) {
      Class metricsClass = conf.getClassByName(
          MetastoreConf.getVar(conf, MetastoreConf.ConfVars.METRICS_CLASS));
      Constructor constructor = metricsClass.getConstructor(Configuration.class);
      metrics = (Metrics) constructor.newInstance(conf);
    }
  }

  /**
   * Returns static Metrics instance, null if not initialized or closed.
   */
  public static Metrics getInstance() {
    return metrics;
  }

  /**
   * Closes and removes static Metrics instance.
   */
  public synchronized static void close() throws Exception {
    if (metrics != null) {
      metrics.close();
      metrics = null;
    }
  }
}
