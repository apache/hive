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
package org.apache.hive.ptest.api.server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.hive.ptest.api.Status;
import org.apache.hive.ptest.api.request.TestStartRequest;
import org.apache.hive.ptest.execution.LocalCommandFactory;
import org.apache.hive.ptest.execution.PTest;
import org.apache.hive.ptest.execution.conf.ExecutionContextConfiguration;
import org.apache.hive.ptest.execution.conf.Host;
import org.apache.hive.ptest.execution.conf.TestConfiguration;
import org.apache.hive.ptest.execution.context.ExecutionContext;
import org.apache.hive.ptest.execution.context.ExecutionContextProvider;
import org.apache.hive.ptest.execution.ssh.RSyncCommandExecutor;
import org.apache.hive.ptest.execution.ssh.SSHCommandExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;

import com.google.common.collect.Sets;
import com.google.common.io.Resources;
public class TestTestExecutor {

  private static final String TEST_HANDLE = "myhandle";
  private static final String PRIVATE_KEY = "mykey";
  private static final String PROFILE = "myprole";
  @Rule
  public TemporaryFolder baseDir = new TemporaryFolder();
  private TestExecutor testExecutor;
  private BlockingQueue<Test> testQueue;
  private ExecutionContextConfiguration executionContextConfiguration;
  private ExecutionContextProvider executionContextProvider;
  private ExecutionContext executionContext;
  private PTest.Builder ptestBuilder;
  private PTest ptest;
  private Test test;
  private TestStartRequest startRequest;
  private File profileProperties;


  @Before
  public void setup() throws Exception {
    startRequest = new TestStartRequest();
    startRequest.setProfile(PROFILE);
    startRequest.setTestHandle(TEST_HANDLE);
    test = new Test(startRequest, Status.pending(), System.currentTimeMillis());
    testQueue = new ArrayBlockingQueue<Test>(1);
    executionContextConfiguration = mock(ExecutionContextConfiguration.class);
    executionContextProvider = mock(ExecutionContextProvider.class);
    ptest = mock(PTest.class);
    Set<Host> hosts = Sets.newHashSet();
    String baseDirPath = baseDir.getRoot().getAbsolutePath();
    executionContext = new ExecutionContext(executionContextProvider, hosts, baseDirPath, PRIVATE_KEY);
    profileProperties = new File(baseDirPath, PROFILE + ".properties");
    when(executionContextConfiguration.getProfileDirectory()).thenReturn(baseDirPath);
    when(executionContextConfiguration.getGlobalLogDirectory()).thenReturn(baseDirPath);
    when(executionContextProvider.createExecutionContext()).thenReturn(executionContext);
    Assert.assertTrue(profileProperties.toString(), profileProperties.createNewFile());
    OutputStream profilePropertiesOutputStream = new FileOutputStream(profileProperties);
    Resources.copy(Resources.getResource("test-configuration.properties"), profilePropertiesOutputStream);
    profilePropertiesOutputStream.close();
    ptestBuilder = new PTest.Builder() {
      @Override
      public PTest build(TestConfiguration configuration, ExecutionContext executionContext,
          String buildTag, File logDir, LocalCommandFactory localCommandFactory, SSHCommandExecutor sshCommandExecutor,
          RSyncCommandExecutor rsyncCommandExecutor, Logger logger) throws Exception {
        return ptest;
      }
    };
    testExecutor = new TestExecutor(executionContextConfiguration, executionContextProvider,
        testQueue, ptestBuilder);
    testExecutor.setDaemon(true);
    testExecutor.start();
    TimeUnit.MILLISECONDS.sleep(100);
  }
  @After
  public void teardown() {
    if(testExecutor != null) {
      testExecutor.shutdown();
    }
  }
  @org.junit.Test
  public void testSuccess() throws Exception {
    Assert.assertTrue(String.valueOf(test.getStatus()),
        Status.isPending(test.getStatus()));
    testQueue.add(test);
    TimeUnit.SECONDS.sleep(1);
    Assert.assertTrue(String.valueOf(test.getStatus()),
        Status.isOK(test.getStatus()));
  }
  @org.junit.Test
  public void testTestFailure() throws Exception {
    Assert.assertTrue(String.valueOf(test.getStatus()),
        Status.isPending(test.getStatus()));
    when(ptest.run()).thenReturn(1);
    testQueue.add(test);
    TimeUnit.SECONDS.sleep(1);
    Assert.assertTrue(String.valueOf(test.getStatus()),
        Status.isFailed(test.getStatus()));
  }
  @org.junit.Test
  public void testNoProfileProperties() throws Exception {
    Assert.assertTrue(String.valueOf(test.getStatus()),
        Status.isPending(test.getStatus()));
    Assert.assertTrue(profileProperties.toString(), profileProperties.delete());
    testQueue.add(test);
    TimeUnit.SECONDS.sleep(1);
    Assert.assertTrue(String.valueOf(test.getStatus()),
        Status.isIllegalArgument(test.getStatus()));
  }
}
