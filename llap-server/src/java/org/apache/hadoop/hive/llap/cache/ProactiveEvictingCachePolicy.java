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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.common.util.ShutdownHookManager;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Interface for classes required to be able to receive notifyProactiveEvictionMark event. Had to be this way due to
 * how CacheContentsTracker is designed to wrap around actual cache policies.
 *
 * Implementing this interface is required but not sufficient for a cache policy to be proactive eviction supporting.
 * Such policies also need to extend the implementation below, so that the asynchronous sweeping is taken care of,
 * and they need to implement evictProactively() method.
 */
public interface ProactiveEvictingCachePolicy {

  /**
   * Invoking this signals that new buffers have been marked for proactive eviction. Cache policies implementing this
   * need this information to correctly time sweep runs.
   */
  void notifyProactiveEvictionMark();

  abstract class Impl implements ProactiveEvictingCachePolicy {
    protected final boolean proactiveEvictionEnabled;
    protected final boolean instantProactiveEviction;
    private final long proactiveEvictionSweepIntervalInMs;
    private static ScheduledExecutorService PROACTIVE_EVICTION_SWEEPER_EXECUTOR = null;

    // Last sweep starts higher, so we don't trigger sweeps before the first mark.
    AtomicLong lastMarkTime = new AtomicLong(0L);
    AtomicLong lastSweepTime = new AtomicLong(1L);

    static {
      ShutdownHookManager.addShutdownHook(new Runnable() {
        @Override
        public void run() {
          if (PROACTIVE_EVICTION_SWEEPER_EXECUTOR != null) {
            PROACTIVE_EVICTION_SWEEPER_EXECUTOR.shutdownNow();
          }
        }
      });
    }

    protected Impl(Configuration conf) {
      proactiveEvictionEnabled = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_IO_PROACTIVE_EVICTION_ENABLED);
      instantProactiveEviction = proactiveEvictionEnabled &&
          HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_IO_PROACTIVE_EVICTION_INSTANT_DEALLOC);
      proactiveEvictionSweepIntervalInMs = HiveConf.getTimeVar(conf,
          HiveConf.ConfVars.LLAP_IO_PROACTIVE_EVICTION_SWEEP_INTERVAL, TimeUnit.MILLISECONDS);

      if (proactiveEvictionEnabled) {
        PROACTIVE_EVICTION_SWEEPER_EXECUTOR = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
            .setNameFormat("Proactive-Eviction-Sweeper").setDaemon(true).build());
        PROACTIVE_EVICTION_SWEEPER_EXECUTOR.scheduleWithFixedDelay(new Runnable() {
          @Override
          public void run() {
            long sweepTime = lastSweepTime.get();
            long markTime = lastMarkTime.get();
            if (markTime >= sweepTime) {
              lastSweepTime.set(System.currentTimeMillis());
              evictProactively();
            }
          }
        }, 0, proactiveEvictionSweepIntervalInMs, TimeUnit.MILLISECONDS);
      }
    }

    @Override
    public void notifyProactiveEvictionMark() {
      lastMarkTime.set(System.currentTimeMillis());
    }

    /**
     * An implementing cache policy should evict all buffers from its data structures that are found to be marked.
     */
    public abstract void evictProactively();
  }
}
