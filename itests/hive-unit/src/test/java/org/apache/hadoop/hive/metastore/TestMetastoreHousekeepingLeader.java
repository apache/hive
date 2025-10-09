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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Test for specifying a valid hostname as HMS leader.
 */
public class TestMetastoreHousekeepingLeader extends MetastoreHousekeepingLeaderTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestMetastoreHousekeepingLeader.class);

  @Before
  public void setUp() throws Exception {
    internalSetup("localhost", true);
  }

  @Test
  public void testHouseKeepingThreadExistence() throws Exception {
    searchHousekeepingThreads();

    // Verify existence of threads
    for (Map.Entry<String, Boolean> entry : threadNames.entrySet()) {
      if (entry.getValue()) {
        LOG.info("Found thread with name " + entry.getKey());
      }
      Assert.assertTrue("No thread with name " + entry.getKey() + " found.", entry.getValue());
    }

    for (Map.Entry<Class<? extends Thread>, Boolean> entry : threadClasses.entrySet()) {
      if (entry.getValue()) {
        LOG.info("Found thread for " + entry.getKey().getSimpleName());
      }
      Assert.assertTrue("No thread found for class " + entry.getKey().getSimpleName(),
              entry.getValue());
    }
  }
}

