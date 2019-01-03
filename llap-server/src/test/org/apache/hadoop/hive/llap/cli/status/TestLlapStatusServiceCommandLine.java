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

package org.apache.hadoop.hive.llap.cli.status;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static junit.framework.TestCase.assertEquals;

import java.util.Properties;

/**
 * Tests for LlapStatusServiceCommandLine.
 */
public class TestLlapStatusServiceCommandLine {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testArgumentParsingDefault() throws Exception {
    LlapStatusServiceCommandLine cl = new LlapStatusServiceCommandLine(new String[] {});

    assertEquals("findAppTimeout should be the default value if not specified otherwise",
        cl.getFindAppTimeoutMs(), LlapStatusServiceCommandLine.DEFAULT_FIND_YARN_APP_TIMEOUT_MS);

    assertEquals("refreshInterval should be the default value if not specified otherwise",
        cl.getRefreshIntervalMs(), LlapStatusServiceCommandLine.DEFAULT_STATUS_REFRESH_INTERVAL_MS);

    assertEquals("watchTimeout should be the default value if not specified otherwise",
        cl.getWatchTimeoutMs(), LlapStatusServiceCommandLine.DEFAULT_WATCH_MODE_TIMEOUT_MS);

    assertEquals("runningNodesThreshold should be the default value if not specified otherwise",
        cl.getRunningNodesThreshold(), LlapStatusServiceCommandLine.DEFAULT_RUNNING_NODES_THRESHOLD);

    assertEquals("hiveConf should be empty properties if not specified otherwise", cl.getHiveConf(),
        new Properties());

    assertEquals("isLaunched should be the true if not specified otherwise", cl.isLaunched(), true);

    assertEquals("watchMode should be the false if not specified otherwise", cl.isWatchMode(), false);
  }

  @Test
  public void testNegativeRefreshInterval() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Refresh interval should be >0");

    new LlapStatusServiceCommandLine(new String[] {"--refreshInterval", "-1"});
  }

  @Test
  public void testNegativeWatchTimeout() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Watch timeout should be >0");

    new LlapStatusServiceCommandLine(new String[] {"--watchTimeout", "-1"});
  }

  @Test
  public void testNegativeRunningNodesThreshold() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Running nodes threshold value should be between 0.0 and 1.0 (inclusive)");

    new LlapStatusServiceCommandLine(new String[] {"--runningNodesThreshold", "-1"});
  }

  @Test
  public void testRunningNodesThresholdOverOne() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Running nodes threshold value should be between 0.0 and 1.0 (inclusive)");

    new LlapStatusServiceCommandLine(new String[] {"--runningNodesThreshold", "1.1"});
  }
}
