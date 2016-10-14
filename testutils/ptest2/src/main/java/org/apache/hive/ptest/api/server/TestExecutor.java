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
import java.io.PrintStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hive.ptest.api.Status;
import org.apache.hive.ptest.api.request.TestStartRequest;
import org.apache.hive.ptest.execution.Constants;
import org.apache.hive.ptest.execution.Dirs;
import org.apache.hive.ptest.execution.LocalCommandFactory;
import org.apache.hive.ptest.execution.LogDirectoryCleaner;
import org.apache.hive.ptest.execution.PTest;
import org.apache.hive.ptest.execution.conf.ExecutionContextConfiguration;
import org.apache.hive.ptest.execution.conf.TestConfiguration;
import org.apache.hive.ptest.execution.context.CreateHostsFailedException;
import org.apache.hive.ptest.execution.context.ExecutionContext;
import org.apache.hive.ptest.execution.context.ExecutionContextProvider;
import org.apache.hive.ptest.execution.context.ServiceNotAvailableException;
import org.apache.hive.ptest.execution.ssh.RSyncCommandExecutor;
import org.apache.hive.ptest.execution.ssh.SSHCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes parallel test in a single thread since the slaves
 * will be fully utilized by the test environment.
 */
public class TestExecutor extends Thread {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestExecutor.class);
  private final ExecutionContextConfiguration mExecutionContextConfiguration;
  private final ExecutionContextProvider mExecutionContextProvider;
  private final BlockingQueue<Test> mTestQueue;
  private final PTest.Builder mPTestBuilder;
  private ExecutionContext mExecutionContext;
  private boolean execute;

  public TestExecutor(ExecutionContextConfiguration executionContextConfiguration,
      ExecutionContextProvider executionContextProvider,
      BlockingQueue<Test> testQueue, PTest.Builder pTestBuilder) {
    mExecutionContextConfiguration = executionContextConfiguration;
    mExecutionContextProvider = executionContextProvider;
    mTestQueue = testQueue;
    mPTestBuilder = pTestBuilder;
    execute = true;
  }

  @Override
  public void run() {
    while(execute) {
      Test test = null;
      PrintStream logStream = null;
      Logger logger = null;
      try {
        // start a log cleaner at the start of each test
        LogDirectoryCleaner cleaner = new LogDirectoryCleaner(new File(mExecutionContextConfiguration.
            getGlobalLogDirectory()), mExecutionContextConfiguration.getMaxLogDirectoriesPerProfile());
        cleaner.setName("LogCleaner-" + mExecutionContextConfiguration.
            getGlobalLogDirectory());
        cleaner.setDaemon(true);
        cleaner.start();
        test = mTestQueue.poll(30, TimeUnit.MINUTES);
        if(!execute) {
          terminateExecutionContext();
          break;
        }
        if(test == null) {
          terminateExecutionContext();
        } else {
          test.setStatus(Status.inProgress());
          test.setDequeueTime(System.currentTimeMillis());
          if(mExecutionContext == null) {
            mExecutionContext = createExceutionContext();
          }
          test.setExecutionStartTime(System.currentTimeMillis());
          TestStartRequest startRequest = test.getStartRequest();
          String profile = startRequest.getProfile();
          File profileConfFile = new File(mExecutionContextConfiguration.getProfileDirectory(),
              String.format("%s.properties", profile));
          LOG.info("Attempting to run using profile file: {}", profileConfFile);
          if(!profileConfFile.isFile()) {
            test.setStatus(Status.illegalArgument(
                "Profile " + profile + " not found in directory " +
                    mExecutionContextConfiguration.getProfileDirectory()));
            test.setExecutionFinishTime(System.currentTimeMillis());
          } else {
            File logDir = Dirs.create(new File(mExecutionContextConfiguration.
                getGlobalLogDirectory(), test.getStartRequest().getTestHandle()));
            File logFile = new File(logDir, "execution.txt");
            test.setOutputFile(logFile);
            logStream = new PrintStream(logFile);
            logger = new TestLogger(logStream, TestLogger.LEVEL.DEBUG);
            TestConfiguration testConfiguration = TestConfiguration.fromFile(profileConfFile, logger);
            testConfiguration.setPatch(startRequest.getPatchURL());
            testConfiguration.setJiraName(startRequest.getJiraName());
            testConfiguration.setClearLibraryCache(startRequest.isClearLibraryCache());
            LocalCommandFactory localCommandFactory = new LocalCommandFactory(logger);
            PTest ptest = mPTestBuilder.build(testConfiguration, mExecutionContext,
                test.getStartRequest().getTestHandle(), logDir,
                localCommandFactory, new SSHCommandExecutor(logger),
                new RSyncCommandExecutor(logger, mExecutionContextConfiguration.getMaxRsyncThreads(),
                  localCommandFactory), logger);
            int result = ptest.run();
            if(result == Constants.EXIT_CODE_SUCCESS) {
              test.setStatus(Status.ok());
            } else {
              test.setStatus(Status.failed("Tests failed with exit code " + result));
            }
            logStream.flush();
            // if all drones where abandoned on a host, try replacing them.
            mExecutionContext.replaceBadHosts();
          }
        }
      } catch (Exception e) {
        LOG.error("Unxpected Error", e);
        if(test != null) {
          test.setStatus(Status.failed("Tests failed with exception " +
              e.getClass().getName() + ": " + e.getMessage()));
          if(logger != null) {
            String msg = "Error executing " + test.getStartRequest().getTestHandle();
            logger.error(msg, e);
          }
        }
        // if we died for any reason lets get a new set of hosts
        terminateExecutionContext();
      } finally {
        if(test != null) {
          test.setExecutionFinishTime(System.currentTimeMillis());
        }
        if(logStream != null) {
          logStream.flush();
          logStream.close();
        }
      }
    }
  }

  private void terminateExecutionContext() {
    if(mExecutionContext != null) {
      mExecutionContext.terminate();
      mExecutionContext = null;
    }
  }
  private ExecutionContext createExceutionContext()
      throws ServiceNotAvailableException, InterruptedException, CreateHostsFailedException {
    long start = System.currentTimeMillis();
    LOG.info("Attempting to create a new execution context");
    ExecutionContext result = mExecutionContextProvider.createExecutionContext();
    long elapsedTime = System.currentTimeMillis() - start;
    LOG.info("Context Creation time: " + TimeUnit.SECONDS.
        convert(elapsedTime, TimeUnit.MILLISECONDS) + " seconds");
    return result;
  }

  public void shutdown() {
    execute = false;
    this.interrupt();
  }
}
