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

package org.apache.hadoop.hive.common;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;

import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.ListenerNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that monitors memory usage and notifies the listeners when a certain of threshold of memory is used
 * after GC (collection usage).
 */
public class HeapMemoryMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(HeapMemoryMonitor.class.getName());
  // notifies when memory usage is 70% after GC
  private static final double DEFAULT_THRESHOLD = 0.7d;
  private static final MemoryPoolMXBean tenuredGenPool = getTenuredGenPool();

  private final double threshold;
  private List<Listener> listeners = new ArrayList<>();
  private NotificationListener notificationListener;

  public interface Listener {
    void memoryUsageAboveThreshold(long usedMemory, long maxMemory);
  }

  public HeapMemoryMonitor(double threshold) {
    this.threshold = threshold <= 0.0d || threshold > 1.0d ? DEFAULT_THRESHOLD : threshold;
    setupTenuredGenPoolThreshold(tenuredGenPool);
  }

  private void setupTenuredGenPoolThreshold(final MemoryPoolMXBean tenuredGenPool) {
    if (tenuredGenPool == null) {
      return;
    }
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      final long memoryThreshold = (int) Math.floor(pool.getUsage().getMax() * threshold);
      final boolean isTenured = isTenured(pool);
      if (!isTenured) {
        continue;
      }
      // set memory threshold on memory used after GC
      final boolean isCollectionUsageThresholdSupported = pool.isCollectionUsageThresholdSupported();
      if (isCollectionUsageThresholdSupported) {
        LOG.info("Setting collection usage threshold to {}", memoryThreshold);
        pool.setCollectionUsageThreshold(memoryThreshold);
        return;
      } else {
        // if collection usage threshold is not support, worst case set memory threshold on memory usage (before GC)
        final boolean isUsageThresholdSupported = pool.isUsageThresholdSupported();
        if (isUsageThresholdSupported) {
          LOG.info("Setting usage threshold to {}", memoryThreshold);
          pool.setUsageThreshold(memoryThreshold);
          return;
        }
      }
    }
  }

  private static MemoryPoolMXBean getTenuredGenPool() {
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      final String vendor = System.getProperty("java.vendor");
      final boolean isTenured = isTenured(pool);
      if (!isTenured) {
        continue;
      }
      final boolean isCollectionUsageThresholdSupported = pool.isCollectionUsageThresholdSupported();
      if (isCollectionUsageThresholdSupported) {
        return pool;
      } else {
        final boolean isUsageThresholdSupported = pool.isUsageThresholdSupported();
        if (isUsageThresholdSupported) {
          return pool;
        } else {
          LOG.error("{} vendor does not support isCollectionUsageThresholdSupported() and isUsageThresholdSupported()" +
            " for tenured memory pool '{}'.", vendor, pool.getName());
        }
      }
    }
    return null;
  }


  private static boolean isTenured(MemoryPoolMXBean memoryPoolMXBean) {
    if (memoryPoolMXBean.getType() != MemoryType.HEAP) {
      return false;
    }

    String name = memoryPoolMXBean.getName();
    return name.equals("CMS Old Gen") // CMS
      || name.equals("PS Old Gen") // Parallel GC
      || name.equals("G1 Old Gen") // G1GC
      // other vendors like IBM, Azul etc. use different names
      || name.equals("Old Space")
      || name.equals("Tenured Gen")
      || name.equals("Java heap")
      || name.equals("GenPauseless Old Gen");
  }

  public void registerListener(final Listener listener) {
    listeners.add(listener);
  }

  public MemoryUsage getTenuredGenMemoryUsage() {
    if (tenuredGenPool == null) {
      return null;
    }
    return tenuredGenPool.getUsage();
  }

  public void start() {
    // unsupported if null
    if (tenuredGenPool == null) {
      return;
    }
    MemoryMXBean mxBean = ManagementFactory.getMemoryMXBean();
    NotificationEmitter emitter = (NotificationEmitter) mxBean;
    notificationListener = (n, hb) -> {
      if (n.getType().equals(
        MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED)) {
        long maxMemory = tenuredGenPool.getUsage().getMax();
        long usedMemory = tenuredGenPool.getUsage().getUsed();
        for (Listener listener : listeners) {
          listener.memoryUsageAboveThreshold(usedMemory, maxMemory);
        }
      }
    };
    emitter.addNotificationListener(notificationListener, null, null);
  }

  public void close() {
    if(notificationListener != null) {
      MemoryMXBean mxBean = ManagementFactory.getMemoryMXBean();
      NotificationEmitter emitter = (NotificationEmitter) mxBean;
      try {
        emitter.removeNotificationListener(notificationListener);
      } catch(ListenerNotFoundException e) {
        LOG.warn("Failed to remove HeapMemoryMonitor notification listener from MemoryMXBean", e);
      }
    }
  }
}
