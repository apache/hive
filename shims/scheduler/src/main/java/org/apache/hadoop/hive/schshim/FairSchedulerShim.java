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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.SchedulerShim;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationFileLoaderService;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueuePlacementPolicy;

public class FairSchedulerShim implements SchedulerShim {
  private static final Logger LOG = LoggerFactory.getLogger(FairSchedulerShim.class);
  private static final String MR2_JOB_QUEUE_PROPERTY = "mapreduce.job.queuename";

  @Override
  public void refreshDefaultQueue(Configuration conf, String userName)
      throws IOException {
    String requestedQueue = YarnConfiguration.DEFAULT_QUEUE_NAME;
    final AtomicReference<AllocationConfiguration> allocConf = new AtomicReference<AllocationConfiguration>();

    AllocationFileLoaderService allocsLoader = new AllocationFileLoaderService();
    allocsLoader.init(conf);
    allocsLoader.setReloadListener(new AllocationFileLoaderService.Listener() {
      @Override
      public void onReload(AllocationConfiguration allocs) {
        allocConf.set(allocs);
      }
    });
    try {
      allocsLoader.reloadAllocations();
    } catch (Exception ex) {
      throw new IOException("Failed to load queue allocations", ex);
    }
    if (allocConf.get() == null) {
      allocConf.set(new AllocationConfiguration(conf));
    }
    QueuePlacementPolicy queuePolicy = allocConf.get().getPlacementPolicy();
    if (queuePolicy != null) {
      requestedQueue = queuePolicy.assignAppToQueue(requestedQueue, userName);
      if (StringUtils.isNotBlank(requestedQueue)) {
        LOG.debug("Setting queue name to " + requestedQueue + " for user "
            + userName);
        conf.set(MR2_JOB_QUEUE_PROPERTY, requestedQueue);
      }
    }
  }

}
