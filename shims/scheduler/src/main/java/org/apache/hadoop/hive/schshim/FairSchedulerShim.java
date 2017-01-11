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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.SchedulerShim;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueuePlacementPolicy;

import java.io.IOException;

/*
 * FairSchedulerShim monitors changes in fair-scheduler.xml (if it exists) to allow for dynamic
 * reloading and queue resolution. When changes to the fair-scheduler.xml file are detected, the
 * cached queue resolution policies for each user are cleared, and then re-cached/validated on job-submit.
 */

public class FairSchedulerShim implements SchedulerShim {
  private static final Logger LOG = LoggerFactory.getLogger(FairSchedulerShim.class);
  private static final String MR2_JOB_QUEUE_PROPERTY = "mapreduce.job.queuename";

  private final QueueAllocator queueAllocator;

  @VisibleForTesting
  public FairSchedulerShim(QueueAllocator queueAllocator) {
    this.queueAllocator = queueAllocator;
  }

  public FairSchedulerShim() {
    this(new FairSchedulerQueueAllocator());
  }

  /**
   * Applies the default YARN fair scheduler queue for a user.
   * @param conf - the current HiveConf configuration.
   * @param forUser - the user to configure the default queue for.
   * @throws IOException
   */
  @Override
  public synchronized void refreshDefaultQueue(Configuration conf, String forUser)
    throws IOException {
    setJobQueueForUserInternal(conf, YarnConfiguration.DEFAULT_QUEUE_NAME, forUser);
  }

  /**
   * Validates the YARN fair scheduler queue configuration.
   * @param conf - the current HiveConf configuration.
   * @param forUser - the user to configure the default queue for.
   * @throws IOException
   */
  @Override
  public synchronized void validateQueueConfiguration(Configuration conf, String forUser) throws IOException {
    // Currently, "validation" is just to ensure that the client can still set the same queue that they
    // could previously. In almost all situations, this should be essentially a no-op (unless the fair-scheduler.xml
    // file changes in such a way as this is disallowed). Currently this implementation is just inteded to allow us
    // to validate that the user's configuration is at least reasonable on a per-request basis beyond from the already-
    // occurring per session setup.

    // TODO: Build out ACL enforcement.

    String currentJobQueue = conf.get(MR2_JOB_QUEUE_PROPERTY);
    if (currentJobQueue != null && !currentJobQueue.isEmpty()) {
      setJobQueueForUserInternal(conf, currentJobQueue, forUser);
    } else {
      refreshDefaultQueue(conf, forUser);
    }
  }

  public QueueAllocator getQueueAllocator() {
    return this.queueAllocator;
  }

  private void setJobQueueForUserInternal(Configuration conf, String queueName, String forUser) throws IOException {
    QueuePlacementPolicy queuePolicy = queueAllocator.makeConfigurationFor(conf, forUser).get().getPlacementPolicy();

    if (queuePolicy != null) {
      String requestedQueue = queuePolicy.assignAppToQueue(queueName, forUser);
      if (StringUtils.isNotBlank(requestedQueue)) {
        LOG.info("Setting queue name to: '{}' for user '{}'", requestedQueue, forUser);
        conf.set(MR2_JOB_QUEUE_PROPERTY, requestedQueue);
      } else {
        LOG.warn("Unable to set queue: {} for user: {}, resetting to user's default queue.", requestedQueue, forUser);
        conf.set(MR2_JOB_QUEUE_PROPERTY, queuePolicy.assignAppToQueue(YarnConfiguration.DEFAULT_QUEUE_NAME, forUser));
      }
    }
  }
}