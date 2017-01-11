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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.AllocationConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.QueuePlacementPolicy;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFairSchedulerQueueAllocator {
  private static final String EMPTY = "";
  private static final int USERNAME_ARGUMENT_INDEX = 1;
  private static final String YARN_SCHEDULER_FILE_PROPERTY = "yarn.scheduler.fair.allocation.file";
  private static final String MR2_JOB_QUEUE_PROPERTY = "mapreduce.job.queuename";

  @Test
  public void testChangingLastUsedHiveConfigurationStringDirectly() throws Exception {
    Configuration configuration = new Configuration();
    FairSchedulerShim shim = new FairSchedulerShim();
    FairSchedulerQueueAllocator allocator = (FairSchedulerQueueAllocator) shim.getQueueAllocator();

    // On initialization should be uncached.
    assertNull(allocator.getCurrentlyWatchingFile());

    // Per job submission the location of fair-scheduler should be updated.
    for (String location : new String[] { "/first", "/second", "third/fourth" }){
      for (String user : new String[] { "firstUser", "secondUser", "thirdUser" }) {
        configuration.set(YARN_SCHEDULER_FILE_PROPERTY, location);
        shim.refreshDefaultQueue(configuration, user);
        assertEquals(allocator.getCurrentlyWatchingFile(), location);
      }
    }
  }

  @Test
  public void testNeverBeforeSeenUsersEffectOnLastUsedHiveConfigurationString() throws Exception {
    final Configuration configuration = new Configuration();
    FairSchedulerShim shim = new FairSchedulerShim();
    FairSchedulerQueueAllocator allocator = (FairSchedulerQueueAllocator) shim.getQueueAllocator();

    // Per job submission the location of fair-scheduler should be updated.
    configuration.set(YARN_SCHEDULER_FILE_PROPERTY, "/some/unchanging/location");
    for (String user : new String[] { "first", "second", "third", "fourth", "fifth" }) {
      shim.refreshDefaultQueue(configuration, user);
      assertEquals(allocator.getCurrentlyWatchingFile(), "/some/unchanging/location");
    }
  }

  @Test
  public void testQueueAllocation() throws Exception {
    Configuration configuration = new Configuration();
    QueueAllocator allocator = mock(QueueAllocator.class);

    when(allocator.makeConfigurationFor(any(Configuration.class), any(String.class)))
      .thenAnswer(new Answer<AtomicReference<AllocationConfiguration>>() {
        @Override
        public AtomicReference<AllocationConfiguration> answer(InvocationOnMock invocationOnMock) throws Throwable {
          // Capture which user is causing the reset for verification purposes.
          final String username = (String) invocationOnMock.getArguments()[USERNAME_ARGUMENT_INDEX];

          AllocationConfiguration allocationConfiguration = mock(AllocationConfiguration.class);
          when(allocationConfiguration.getPlacementPolicy())
            .thenAnswer(new Answer<QueuePlacementPolicy>() {
              @Override
              public QueuePlacementPolicy answer(InvocationOnMock invocationOnMock) throws Throwable {
                QueuePlacementPolicy placementPolicy = mock(QueuePlacementPolicy.class);
                when(placementPolicy.assignAppToQueue(any(String.class), any(String.class)))
                  .thenAnswer(new Answer<String>() {
                    @Override
                    public String answer(InvocationOnMock invocationOnMock) throws Throwable {
                      return String.format("queue.for.%s", username);
                    }
                  });

                return placementPolicy;
              }
            });

          return new AtomicReference<>(allocationConfiguration);
        }
      });

    FairSchedulerShim shim = new FairSchedulerShim(allocator);

    // Per job submission the location of fair-scheduler should be updated.
    configuration.set(YARN_SCHEDULER_FILE_PROPERTY, "/some/file/location");
    for (String user : new String[] { "first", "second", "third", "fourth", "fifth" }) {
      shim.refreshDefaultQueue(configuration, user);

      String queueName = String.format("queue.for.%s", user);
      assertEquals(configuration.get(MR2_JOB_QUEUE_PROPERTY), queueName);
    }
  }
}