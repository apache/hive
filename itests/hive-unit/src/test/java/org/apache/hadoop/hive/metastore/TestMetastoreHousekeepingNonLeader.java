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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.ql.txn.compactor.Worker;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Test for specifying HMS leader other than the current one.
 */
public class TestMetastoreHousekeepingNonLeader extends MetastoreHousekeepingLeaderTestBase {
  private static final Logger LOG =
          LoggerFactory.getLogger(TestMetastoreHousekeepingLeaderEmptyConfig.class);

  @Before
  public void setUp() throws Exception {
    // Empty string for leader indicates that the HMS is leader.
    internalSetup("some_non_leader_host.domain1.domain", true);
  }

  @Test
  public void testHouseKeepingThreadExistence() throws Exception {
    searchHousekeepingThreads();

    // Verify existence of threads
    for (Map.Entry<String, Boolean> entry : threadNames.entrySet()) {
      if (!entry.getValue()) {
        LOG.info("No thread found with name " + entry.getKey());
      }
      Assert.assertFalse("Thread with name " + entry.getKey() + " found.", entry.getValue());
    }

    for (Map.Entry<Class<? extends Thread>, Boolean> entry : threadClasses.entrySet()) {
      // A non-leader HMS will still run the configured number of Compaction worker threads.
      if (entry.getKey() == Worker.class) {
        if (entry.getValue()) {
          LOG.info("Thread found for " + entry.getKey().getSimpleName());
        }
      } else {
        if (!entry.getValue()) {
          LOG.info("No thread found for " + entry.getKey().getSimpleName());
        }
        Assert.assertFalse("Thread found for class " + entry.getKey().getSimpleName(),
                entry.getValue());
      }
    }
  }
}
