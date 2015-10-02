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
package org.apache.hive.ptest.execution;

import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class TestTestCheckPhase extends AbstractTestPhase {
  private TestCheckPhase phase;

  @Before
  public void setup() throws Exception {
    initialize(getClass().getSimpleName());
    createHostExecutor();
  }
  @Test
  public void testNoTests() throws Exception {
    URL url = this.getClass().getResource("/HIVE-9377.1.patch");
    File patchFile = new File(url.getFile());
    Set<String> addedTests = new HashSet<String>();
    phase = new TestCheckPhase(hostExecutors, localCommandFactory,
      templateDefaults, patchFile, logger, addedTests);
    phase.execute();

    Assert.assertEquals(addedTests.size(), 0);
  }


  @Test
  public void testJavaTests() throws Exception {
    URL url = this.getClass().getResource("/HIVE-10761.6.patch");
    File patchFile = new File(url.getFile());
    Set<String> addedTests = new HashSet<String>();
    phase = new TestCheckPhase(hostExecutors, localCommandFactory,
      templateDefaults, patchFile, logger, addedTests);
    phase.execute();

    Assert.assertEquals(addedTests.size(), 3);
    Assert.assertTrue(addedTests.contains("TestCodahaleMetrics.java"));
    Assert.assertTrue(addedTests.contains("TestMetaStoreMetrics.java"));
    Assert.assertTrue(addedTests.contains("TestLegacyMetrics.java"));
  }

  @Test
  public void testQTests() throws Exception {
    URL url = this.getClass().getResource("/HIVE-11271.4.patch");
    File patchFile = new File(url.getFile());
    Set<String> addedTests = new HashSet<String>();
    phase = new TestCheckPhase(hostExecutors, localCommandFactory,
      templateDefaults, patchFile, logger, addedTests);
    phase.execute();

    Assert.assertEquals(addedTests.size(), 1);
    Assert.assertTrue(addedTests.contains("unionall_unbalancedppd.q"));
  }

  @Test
  public void testRemoveTest() throws Exception {
    URL url = this.getClass().getResource("/remove-test.patch");
    File patchFile = new File(url.getFile());
    Set<String> addedTests = new HashSet<String>();
    phase = new TestCheckPhase(hostExecutors, localCommandFactory,
      templateDefaults, patchFile, logger, addedTests);
    phase.execute();

    Assert.assertEquals(addedTests.size(), 0);
  }
}
