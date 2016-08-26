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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.hive.ptest.api.Status;
import org.apache.hive.ptest.api.request.TestListRequest;
import org.apache.hive.ptest.api.request.TestLogRequest;
import org.apache.hive.ptest.api.request.TestStartRequest;
import org.apache.hive.ptest.api.request.TestStopRequest;
import org.apache.hive.ptest.api.response.TestListResponse;
import org.apache.hive.ptest.api.response.TestLogResponse;
import org.apache.hive.ptest.api.response.TestStartResponse;
import org.apache.hive.ptest.api.response.TestStatus;
import org.apache.hive.ptest.api.response.TestStatusResponse;
import org.apache.hive.ptest.api.response.TestStopResponse;
import org.apache.hive.ptest.execution.PTest;
import org.apache.hive.ptest.execution.conf.ExecutionContextConfiguration;
import org.apache.hive.ptest.execution.context.ExecutionContextProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;




/**
 * Server interface of the ptest environment. Each request
 * is converted from JSON and each response is returned in JSON.
 */
@Controller
@RequestMapping(value = "/api/v1")
public class ExecutionController {
  private static final long MAX_READ_SIZE = 1024L * 1024L;
  private static final String CONF_PROPERTY = "hive.ptest.execution.context.conf";

  private static final Logger LOG = LoggerFactory
      .getLogger(ExecutionController.class);
  private final ExecutionContextConfiguration mExecutionContextConfiguration;
  private final ExecutionContextProvider mExecutionContextProvider;
  private final Map<String, Test> mTests;
  private final BlockingQueue<Test> mTestQueue;
  private final TestExecutor mTestExecutor;
  private final File mGlobalLogDir;
  public ExecutionController()
      throws IOException {
    String executionContextConfigurationFile = System.getProperty(CONF_PROPERTY, "").trim();
    Preconditions.checkArgument(!executionContextConfigurationFile.isEmpty(), CONF_PROPERTY + " is required");
    LOG.info("Reading configuration from file: " + executionContextConfigurationFile);
    mExecutionContextConfiguration = ExecutionContextConfiguration.fromFile(executionContextConfigurationFile);
    LOG.info("ExecutionContext is [{}]", mExecutionContextConfiguration);
    mExecutionContextProvider = mExecutionContextConfiguration.getExecutionContextProvider();
    mTests = Collections.synchronizedMap(new LinkedHashMap<String, Test>() {
      private static final long serialVersionUID = 1L;
      @Override
      public boolean removeEldestEntry(Map.Entry<String, Test> entry) {
        Test testExecution = entry.getValue();
        File testOutputFile = testExecution.getOutputFile();
        return size() > 30 || (testOutputFile != null && !testOutputFile.isFile());
      }
    });
    mTestQueue = new ArrayBlockingQueue<Test>(5);
    mGlobalLogDir = new File(mExecutionContextConfiguration.getGlobalLogDirectory());
    mTestExecutor = new TestExecutor(mExecutionContextConfiguration, mExecutionContextProvider,
        mTestQueue, new PTest.Builder());
    mTestExecutor.setName("TestExecutor");
    mTestExecutor.setDaemon(true);
    mTestExecutor.start();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.info("Shutdown hook called");
        try {
          mTestExecutor.shutdown();
        } catch (Exception e) {
          LOG.error("Error shutting down TestExecutor", e);
        }
        try {
          mExecutionContextProvider.close();
        } catch (Exception e) {
          LOG.error("Error shutting down ExecutionContextProvider", e);
        }
      }
    });
  }

  @RequestMapping(value="/testStart", method = RequestMethod.POST)
  public @ResponseBody TestStartResponse testStart(@RequestBody TestStartRequest startRequest,
      BindingResult result) {
    LOG.info("startRequest " + startRequest.toString());
    TestStartResponse startResponse = doStartTest(startRequest, result);
    LOG.info("startResponse " + startResponse.toString());
    return startResponse;
  }

  private TestStartResponse doStartTest(TestStartRequest startRequest, BindingResult result) {
    if(result.hasErrors() ||
        Strings.nullToEmpty(startRequest.getProfile()).trim().isEmpty() ||
        Strings.nullToEmpty(startRequest.getTestHandle()).trim().isEmpty() ||
        startRequest.getProfile().contains("/")) {
      return new TestStartResponse(Status.illegalArgument());
    }
    if(!assertTestHandleIsAvailable(startRequest.getTestHandle())) {
      return new TestStartResponse(Status.illegalArgument("Test handle " + startRequest.getTestHandle() + " already used"));
    }
    Test test = new Test(startRequest,
        Status.pending(), System.currentTimeMillis());
    if(mTestQueue.offer(test)) {
      mTests.put(startRequest.getTestHandle(), test);
      return new TestStartResponse(Status.ok());
    } else {
      return new TestStartResponse(Status.queueFull());
    }
  }

  @RequestMapping(value="/testStop", method = RequestMethod.POST)
  public @ResponseBody TestStopResponse testStop(@RequestBody TestStopRequest stopRequest,
      BindingResult result) {
    String testHandle = stopRequest.getTestHandle();
    Test test = mTests.get(testHandle);
    if(result.hasErrors() ||
        Strings.nullToEmpty(stopRequest.getTestHandle()).trim().isEmpty() ||
        test == null) {
      return new TestStopResponse(Status.illegalArgument());
    }
    test.setStopRequested(true);
    return new TestStopResponse(Status.ok());
  }

  @RequestMapping(value="/testStatus", method = RequestMethod.POST)
  public @ResponseBody TestStatusResponse testStatus(@RequestBody TestStopRequest stopRequest,
      BindingResult result) {
    String testHandle = stopRequest.getTestHandle();
    Test test = mTests.get(testHandle);
    if(result.hasErrors() ||
        Strings.nullToEmpty(stopRequest.getTestHandle()).trim().isEmpty() ||
        test == null) {
      return new TestStatusResponse(Status.illegalArgument());
    }
    return new TestStatusResponse(Status.ok(), test.toTestStatus());
  }

  @RequestMapping(value="/testLog", method = RequestMethod.POST)
  public @ResponseBody TestLogResponse testLog(@RequestBody TestLogRequest logsRequest,
      BindingResult result) {
    String testHandle = logsRequest.getTestHandle();
    Test testExecution = mTests.get(testHandle);
    if(result.hasErrors() ||
        Strings.nullToEmpty(logsRequest.getTestHandle()).trim().isEmpty() ||
        testExecution == null ||
        logsRequest.getLength() > MAX_READ_SIZE) {
      return new TestLogResponse(Status.illegalArgument());
    }
    File outputFile = testExecution.getOutputFile();
    if(outputFile == null ||
        logsRequest.getOffset() > outputFile.length()) {
      return new TestLogResponse(Status.illegalArgument());
    }
    RandomAccessFile fileHandle = null;
    try {
      fileHandle = new RandomAccessFile(outputFile, "r");
      long offset = logsRequest.getOffset();
      fileHandle.seek(offset);
      int readLength = 0;
      if(offset < fileHandle.length()) {
        readLength = (int)Math.min(fileHandle.length() - offset, logsRequest.getLength());
      }
      byte[] buffer = new byte[readLength];
      fileHandle.readFully(buffer);
      offset += readLength;
      return new TestLogResponse(Status.ok(), offset,
          new String(buffer, Charsets.UTF_8));
    } catch (IOException e) {
      LOG.info("Unexpected IO error reading " + testExecution.getOutputFile() , e);
      return new TestLogResponse(Status.internalError(e.getMessage()));
    } finally {
      if(fileHandle != null) {
        try {
          fileHandle.close();
        } catch (IOException e) {
          LOG.warn("Error closing " + outputFile, e);
        }
      }
    }
  }

  @RequestMapping(value="/testList", method = RequestMethod.POST)
  public @ResponseBody TestListResponse testList(@RequestBody TestListRequest request) {
    List<TestStatus> entries = Lists.newArrayList();
    synchronized (mTests) {
      for(String testHandle : mTests.keySet()) {
        Test test = mTests.get(testHandle);
        entries.add(test.toTestStatus());
      }
    }
    return new TestListResponse(Status.ok(), entries);
  }
  private synchronized boolean assertTestHandleIsAvailable(final String testHandle) {
    File testOutputDir = new File(mGlobalLogDir, testHandle);
    Preconditions.checkState(!testOutputDir.isFile(), "Output directory " + testOutputDir + " is file");
    return testOutputDir.mkdir();
  }
}
