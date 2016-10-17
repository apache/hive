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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.hive.ptest.execution.conf.QFileTestBatch;
import org.apache.hive.ptest.execution.conf.TestBatch;
import org.apache.hive.ptest.execution.conf.UnitTestBatch;
import org.approvaltests.Approvals;
import org.approvaltests.reporters.JunitReporter;
import org.approvaltests.reporters.UseReporter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Suppliers;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

@UseReporter(JunitReporter.class)
public class TestExecutionPhase extends AbstractTestPhase {

  private static final String DRIVER = "driver";
  private static final String QFILENAME = "sometest";
  private ExecutionPhase phase;
  private File testDir;
  private Set<String> executedTests;
  private Set<String> failedTests;
  private List<TestBatch> testBatches;
  private TestBatch testBatch;

  @Before
  public void setup() throws Exception {
    initialize(getClass().getSimpleName());
    executedTests = Sets.newHashSet();
    failedTests = Sets.newHashSet();
  }
  private ExecutionPhase getPhase() throws IOException {
    createHostExecutor();
    phase = new ExecutionPhase(hostExecutors, executionContext, hostExecutorBuilder, 
        localCommandFactory, templateDefaults, succeededLogDir, failedLogDir,
        Suppliers.ofInstance(testBatches), executedTests, failedTests, logger);
    return phase;
  }
  private void setupQFile(boolean isParallel) throws Exception {
    testDir = Dirs.create( new File(baseDir, "test"));
    Assert.assertTrue(new File(testDir, QFILENAME).createNewFile());
    testBatch =
        new QFileTestBatch(new AtomicInteger(1), "testcase", DRIVER, "qfile", Sets.newHashSet(QFILENAME), isParallel,
            "testModule");
    testBatches = Collections.singletonList(testBatch);
  }
  private void setupUnitTest() throws Exception {
    testBatch = new UnitTestBatch(new AtomicInteger(1), "testcase", Arrays.asList(DRIVER), "fakemodule", false);
    testBatches = Collections.singletonList(testBatch);
  }
  private void setupUnitTest(int nTests) throws Exception {
    List<String> testList = new ArrayList<>();
    for (int i = 0 ; i < nTests ; i++) {
      testList.add("TestClass-" + i);
    }
    testBatch = new UnitTestBatch(new AtomicInteger(1), "testcase", testList, "fakemodule", false);
    testBatches = Collections.singletonList(testBatch);
  }
  private void copyTestOutput(String resource, File directory, String name) throws Exception {
    String junitOutput = Templates.readResource(resource);
    File junitOutputFile = new File(Dirs.create(
        new File(directory, name)), "TEST-SomeTest.xml");
    Files.write(junitOutput.getBytes(Charsets.UTF_8), junitOutputFile);
  }

  private void copyTestOutput(String resource, File directory, String batchName,
                              String outputName) throws Exception {
    String junitOutput = Templates.readResource(resource);
    File junitOutputFile = new File(Dirs.create(
        new File(directory, batchName)), outputName);
    Files.write(junitOutput.getBytes(Charsets.UTF_8), junitOutputFile);
  }
  @After
  public void teardown() {
    FileUtils.deleteQuietly(baseDir);
  }
  @Test
  public void testPassingQFileTest() throws Throwable {
    setupQFile(true);
    copyTestOutput("SomeTest-success.xml", succeededLogDir, testBatch.getName());
    getPhase().execute();
    Approvals.verify(getExecutedCommands());
    Assert.assertEquals(Sets.newHashSet("SomeTest." + QFILENAME), executedTests);
    Assert.assertEquals(Sets.newHashSet(), failedTests);
  }
  @Test
  public void testFailingQFile() throws Throwable {
    setupQFile(true);
    sshCommandExecutor.putFailure("bash " + LOCAL_DIR + "/" + HOST + "-" + USER +
        "-0/scratch/hiveptest-" + "1-" + DRIVER + "-" + QFILENAME + ".sh", 1);
    copyTestOutput("SomeTest-failure.xml", failedLogDir, testBatch.getName());
    getPhase().execute();
    Assert.assertEquals(1, sshCommandExecutor.getMatchCount());
    Approvals.verify(getExecutedCommands());
    Assert.assertEquals(Sets.newHashSet("SomeTest." + QFILENAME), executedTests);
    Assert.assertEquals(Sets.newHashSet("SomeTest." + QFILENAME + " (batchId=1)"), failedTests);
  }
  @Test
  public void testPassingUnitTest() throws Throwable {
    setupUnitTest();
    copyTestOutput("SomeTest-success.xml", succeededLogDir, testBatch.getName());
    getPhase().execute();
    Approvals.verify(getExecutedCommands());
    Assert.assertEquals(Sets.newHashSet("SomeTest." + QFILENAME), executedTests);
    Assert.assertEquals(Sets.newHashSet(), failedTests);
  }
  @Test
  public void testFailingUnitTest() throws Throwable {
    setupUnitTest();
    sshCommandExecutor.putFailure("bash " + LOCAL_DIR + "/" + HOST + "-" + USER +
        "-0/scratch/hiveptest-" + testBatch.getBatchId() + "_" + DRIVER + ".sh", 1);
    copyTestOutput("SomeTest-failure.xml", failedLogDir, testBatch.getName());
    getPhase().execute();
    Assert.assertEquals(1, sshCommandExecutor.getMatchCount());
    Approvals.verify(getExecutedCommands());
    Assert.assertEquals(Sets.newHashSet("SomeTest." + QFILENAME), executedTests);
    Assert.assertEquals(Sets.newHashSet("SomeTest." + QFILENAME + " (batchId=1)"), failedTests);
  }

  @Test(timeout = 20000)
  public void testTimedOutUnitTest() throws Throwable {
    setupUnitTest(3);
    copyTestOutput("SomeTest-success.xml", succeededLogDir, testBatch.getName(), "TEST-TestClass-0.xml");
    copyTestOutput("SomeTest-success.xml", succeededLogDir, testBatch.getName(), "TEST-TestClass-1.xml");
    getPhase().execute();
    Approvals.verify(getExecutedCommands());
    Assert.assertEquals(1, failedTests.size());
    Assert.assertEquals("TestClass-2 - did not produce a TEST-*.xml file (likely timed out) (batchId=1)", failedTests.iterator().next());
  }
}