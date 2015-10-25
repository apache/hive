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

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.io.Resources;
import junit.framework.Assert;

import org.apache.hive.ptest.execution.JIRAService.BuildInfo;
import org.apache.hive.ptest.execution.conf.TestConfiguration;
import org.approvaltests.Approvals;
import org.approvaltests.reporters.JunitReporter;
import org.approvaltests.reporters.UseReporter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@UseReporter(JunitReporter.class)
public class TestJIRAService extends AbstractTestPhase {

  TestConfiguration conf;
  JIRAService jiraService;

  @Before
  public void setup() throws Exception {
    initialize(getClass().getSimpleName());
    conf = TestConfiguration.fromInputStream(
      Resources.getResource("test-configuration.properties").openStream(), logger);
    conf.setPatch("https://HIVE-10000.patch");
    jiraService = new JIRAService(logger, conf, "tag-10");
  }


  @Test
  public void testFormatBuildTagPositive() throws Throwable {
    BuildInfo buildInfo = JIRAService.formatBuildTag("abc-123");
    Assert.assertEquals("abc/123", buildInfo.getFormattedBuildTag());
    Assert.assertEquals("abc", buildInfo.getBuildName());
    buildInfo = JIRAService.formatBuildTag("PreCommit-HIVE-TRUNK-Build-1115");
    Assert.assertEquals("PreCommit-HIVE-TRUNK-Build/1115", buildInfo.getFormattedBuildTag());
    Assert.assertEquals("PreCommit-HIVE-TRUNK-Build", buildInfo.getBuildName());
  }
  @Test(expected=IllegalArgumentException.class)
  public void testFormatBuildTagNoDashSlash() throws Throwable {
    JIRAService.formatBuildTag("abc/123");
  }
  @Test(expected=IllegalArgumentException.class)
  public void testFormatBuildTagNoDashSpace() throws Throwable {
    JIRAService.formatBuildTag("abc 123");
  }
  @Test(expected=IllegalArgumentException.class)
  public void testFormatBuildTagNoDashNone() throws Throwable {
    JIRAService.formatBuildTag("abc123");
  }
  @Test
  public void testTrimMesssagesBoundry() {
    List<String> messages = Lists.newArrayList();
    Assert.assertEquals(messages, JIRAService.trimMessages(messages));
    messages.clear();
    for (int i = 0; i < JIRAService.MAX_MESSAGES; i++) {
      messages.add(String.valueOf(i));
    }
    Assert.assertEquals(messages, JIRAService.trimMessages(messages));
  }
  @Test
  public void testTrimMesssagesNotTrimmed() {
    List<String> messages = Lists.newArrayList("a", "b", "c");
    Assert.assertEquals(messages, JIRAService.trimMessages(messages));
  }
  @Test
  public void testTrimMesssagesTrimmed() {
    List<String> messages = Lists.newArrayList();
    for (int i = 0; i < JIRAService.MAX_MESSAGES + 1; i++) {
      messages.add(String.valueOf(i));
    }
    List<String> expected = Lists.newArrayList(messages);
    expected.remove(0);
    expected.add(0, JIRAService.TRIMMED_MESSAGE);
    Assert.assertEquals(expected, JIRAService.trimMessages(messages));
  }

  @Test
  public void testErrorWithMessages() throws Exception {
    SortedSet<String> failedTests = new TreeSet<String>();
    List<String> messages = new ArrayList<String>();
    messages.add("Error message 1");
    messages.add("Error message 2");
    Set<String> addedTests = new HashSet<String>();
    Approvals.verify(jiraService.generateComments(true, 0, failedTests, messages, addedTests));
  }

  @Test
  public void testErrorWithoutMessages() throws Exception {
    SortedSet<String> failedTests = new TreeSet<String>();
    List<String> messages = new ArrayList<String>();
    Set<String> addedTests = new HashSet<String>();
    Approvals.verify(jiraService.generateComments(true, 0, failedTests, messages, addedTests));
  }

  @Test
  public void testFailNoAdd() throws Exception {
    SortedSet<String> failedTests = new TreeSet<String>();
    failedTests.add("FailedTest1");
    failedTests.add("FailedTest2");
    List<String> messages = new ArrayList<String>();
    Set<String> addedTests = new HashSet<String>();
    Approvals.verify(jiraService.generateComments(false, 5, failedTests, messages, addedTests));
  }

  @Test
  public void testFailAdd() throws Exception {
    SortedSet<String> failedTests = new TreeSet<String>();
    failedTests.add("FailedTest1");
    failedTests.add("FailedTest2");
    List<String> messages = new ArrayList<String>();
    Set<String> addedTests = new HashSet<String>();
    addedTests.add("AddedTest1");
    addedTests.add("AddedTest2");
    Approvals.verify(jiraService.generateComments(false, 5, failedTests, messages, addedTests));
  }

  @Test
  public void testSuccessNoAdd() throws Exception {
    SortedSet<String> failedTests = new TreeSet<String>();
    List<String> messages = new ArrayList<String>();
    Set<String> addedTests = new HashSet<String>();
    Approvals.verify(jiraService.generateComments(false, 5, failedTests, messages, addedTests));
  }

  @Test
  public void testSuccessAdd() throws Exception {
    SortedSet<String> failedTests = new TreeSet<String>();
    List<String> messages = new ArrayList<String>();
    Set<String> addedTests = new HashSet<String>();
    addedTests.add("AddedTest1");
    addedTests.add("AddedTest2");
    Approvals.verify(jiraService.generateComments(false, 5, failedTests, messages, addedTests));
  }
}