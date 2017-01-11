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

package org.apache.hadoop.hive.schshim;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationFileLoaderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class FairSchedulerQueueAllocator implements QueueAllocator {
  private static final Logger LOG = LoggerFactory.getLogger(FairSchedulerQueueAllocator.class);
  private static final String YARN_SCHEDULER_FILE_PROPERTY = "yarn.scheduler.fair.allocation.file";

  private String currentlyWatching;
  private AllocationFileLoaderService loaderService;
  private final AtomicReference<AllocationConfiguration> allocationConfiguration
    = new AtomicReference<AllocationConfiguration>();

  /**
   * Generates a Yarn FairScheduler queue resolver based on 'fair-scheduler.xml'.
   * @param config The HiveConf configuration.
   * @param username      The user to configure the job for.
   * @return Returns a configured allocation resolver.
   * @throws IOException
   */
  public synchronized AtomicReference<AllocationConfiguration> makeConfigurationFor(Configuration config, String username) throws IOException {
    updateWatcher(config);

    return allocationConfiguration;
  }

  public synchronized void refresh(Configuration config) {
    updateWatcher(config);
  }

  @VisibleForTesting
  public String getCurrentlyWatchingFile() {
    return this.currentlyWatching;
  }

  private void updateWatcher(Configuration config) {
    if (this.loaderService != null && StringUtils.equals(currentlyWatching, config.get(YARN_SCHEDULER_FILE_PROPERTY))) return;

    this.currentlyWatching = config.get(YARN_SCHEDULER_FILE_PROPERTY);

    if (this.loaderService != null) {
      this.loaderService.stop();
    }

    this.loaderService = new AllocationFileLoaderService();
    this.loaderService.init(config);
    this.loaderService.setReloadListener(new AllocationFileLoaderService.Listener() {
      @Override
      public void onReload(AllocationConfiguration allocs) {
        allocationConfiguration.set(allocs);
      }
    });

    try {
      this.loaderService.reloadAllocations();
    } catch (Exception ex) {
      LOG.error("Failed to load queue allocations", ex);
    }

    if (allocationConfiguration.get() == null) {
      allocationConfiguration.set(new AllocationConfiguration(config));
    }

    this.loaderService.start();
  }
}
