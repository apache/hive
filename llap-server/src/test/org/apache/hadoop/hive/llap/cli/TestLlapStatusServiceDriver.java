/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.llap.cli;

import org.apache.hadoop.hive.llap.cli.LlapStatusOptionsProcessor.LlapStatusOptions;
import org.apache.hadoop.hive.llap.cli.LlapStatusServiceDriver.ExitCode;
import org.apache.hadoop.hive.llap.cli.LlapStatusServiceDriver.LlapStatusCliException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static junit.framework.TestCase.assertEquals;

import java.util.Properties;

// TODO: write unit tests for the main logic of this class - needs refactoring first, current design isn't testable.
public class TestLlapStatusServiceDriver {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testArgumentParsingDefault() throws LlapStatusCliException {
    LlapStatusServiceDriver driver = new LlapStatusServiceDriver();
    LlapStatusOptions parseOptions = driver.parseOptions(new String[] {});

    assertEquals("findAppTimeout should be the default value if not specified otherwise",
        parseOptions.getFindAppTimeoutMs(), LlapStatusOptionsProcessor.FIND_YARN_APP_TIMEOUT_MS);

    assertEquals("refreshInterval should be the default value if not specified otherwise",
        parseOptions.getRefreshIntervalMs(), LlapStatusOptionsProcessor.DEFAULT_STATUS_REFRESH_INTERVAL_MS);

    assertEquals("watchTimeout should be the default value if not specified otherwise",
        parseOptions.getWatchTimeoutMs(), LlapStatusOptionsProcessor.DEFAULT_WATCH_MODE_TIMEOUT_MS);

    assertEquals("runningNodesThreshold should be the default value if not specified otherwise",
        parseOptions.getRunningNodesThreshold(), LlapStatusOptionsProcessor.DEFAULT_RUNNING_NODES_THRESHOLD);

    assertEquals("hiveConf should be empty properties if not specified otherwise", parseOptions.getConf(),
        new Properties());

    assertEquals("isLaunched should be the true if not specified otherwise", parseOptions.isLaunched(), true);

    assertEquals("watchMode should be the false if not specified otherwise", parseOptions.isWatchMode(), false);
  }

  @Test
  public void testNegativeRefreshInterval() throws LlapStatusCliException {
    thrown.expect(LlapStatusCliException.class);
    thrown.expectMessage(ExitCode.INCORRECT_USAGE.getInt() + ": Incorrect usage");

    LlapStatusServiceDriver driver = new LlapStatusServiceDriver();
    driver.parseOptions(new String[] {"--refreshInterval -1"});
  }

  @Test
  public void testNegativeWatchTimeout() throws LlapStatusCliException {
    thrown.expect(LlapStatusCliException.class);
    thrown.expectMessage(ExitCode.INCORRECT_USAGE.getInt() + ": Incorrect usage");

    LlapStatusServiceDriver driver = new LlapStatusServiceDriver();
    driver.parseOptions(new String[] {"--watchTimeout -1"});
  }

  @Test
  public void testNegativeRunningNodesThreshold() throws LlapStatusCliException {
    thrown.expect(LlapStatusCliException.class);
    thrown.expectMessage(ExitCode.INCORRECT_USAGE.getInt() + ": Incorrect usage");

    LlapStatusServiceDriver driver = new LlapStatusServiceDriver();
    driver.parseOptions(new String[] {"--runningNodesThreshold -1"});
  }

  @Test
  public void testRunningNodesThresholdOverOne() throws LlapStatusCliException {
    thrown.expect(LlapStatusCliException.class);
    thrown.expectMessage(ExitCode.INCORRECT_USAGE.getInt() + ": Incorrect usage");

    LlapStatusServiceDriver driver = new LlapStatusServiceDriver();
    driver.parseOptions(new String[] {"--runningNodesThreshold 1.1"});
  }
}
