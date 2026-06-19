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
package org.apache.hadoop.hive.llap.cache;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.common.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface to manage the cache content info, so that it can be saved on shutdown and loaded on startup.
 */
public interface LlapCacheHydration extends Configurable {

  Logger LOG = LoggerFactory.getLogger(LlapCacheHydration.class);

  /**
   * Save the state of the cache.
   */
  void save();

  /**
   * Pre load the content into the cache. Will be invoked on startup.
   */
  void load();

  /**
   * Initialize the strategy.
   */
  void init();

  static void setupAndStartIfEnabled(Configuration conf) {
    String clazz = HiveConf.getVar(conf, ConfVars.LLAP_CACHE_HYDRATION_STRATEGY_CLASS);
    if (!StringUtils.isEmpty(clazz)) {
      try {
        LlapCacheHydration strategy =
            ReflectionUtil.newInstance(Class.forName(clazz).asSubclass(LlapCacheHydration.class), conf);
        strategy.init();

        Runner runner = new Runner(strategy);
        Thread t = new Thread(runner, Runner.THREAD_NAME);
        t.start();
      } catch (Exception ex) {
        LOG.warn("Llap cache hydration error.", ex);
      }
    }
  }

  final class Runner implements Runnable {

    private static final String THREAD_NAME = "LlapCacheHydrationRunner";

    private final LlapCacheHydration strategy;

    public Runner(LlapCacheHydration strategy) {
      this.strategy = strategy;
    }

    @Override
    public void run() {
      strategy.load();
    }
  }
}
