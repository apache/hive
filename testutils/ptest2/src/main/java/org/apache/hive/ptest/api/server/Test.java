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

import org.apache.hive.ptest.api.Status;
import org.apache.hive.ptest.api.request.TestStartRequest;
import org.apache.hive.ptest.api.response.TestStatus;

/**
 * Represents a test run in memory on the server.
 */
class Test {
  private TestStartRequest startRequest;
  private Status status;
  private long enqueueTime;
  private long dequeueTime;
  private long executionStartTime;
  private long executionFinishTime;
  private File outputFile;
  private boolean stopRequested;

  Test(TestStartRequest startRequest,
      Status status, long enqueueTime) {
    super();
    this.startRequest = startRequest;
    this.status = status;
    this.enqueueTime = enqueueTime;
  }
  boolean isStopRequested() {
    return stopRequested;
  }
  void setStopRequested(boolean stopRequested) {
    this.stopRequested = stopRequested;
  }
  TestStartRequest getStartRequest() {
    return startRequest;
  }
  void setStartRequest(TestStartRequest startRequest) {
    this.startRequest = startRequest;
  }
  Status getStatus() {
    return status;
  }
  void setStatus(Status status) {
    this.status = status;
  }
  long getEnqueueTime() {
    return enqueueTime;
  }
  void setEnqueueTime(long enqueueTime) {
    this.enqueueTime = enqueueTime;
  }
  long getExecutionStartTime() {
    return executionStartTime;
  }
  void setExecutionStartTime(long executionStartTime) {
    this.executionStartTime = executionStartTime;
  }
  long getExecutionFinishTime() {
    return executionFinishTime;
  }
  void setExecutionFinishTime(long executionFinishTime) {
    this.executionFinishTime = executionFinishTime;
  }
  File getOutputFile() {
    return outputFile;
  }
  void setOutputFile(File outputFile) {
    this.outputFile = outputFile;
  }
  long getDequeueTime() {
    return dequeueTime;
  }
  void setDequeueTime(long dequeueTime) {
    this.dequeueTime = dequeueTime;
  }
  TestStatus toTestStatus() {
    long elapsedQueueTime;
    if(getDequeueTime() > 0) {
      elapsedQueueTime = getDequeueTime() - getEnqueueTime();
    } else {
      elapsedQueueTime = System.currentTimeMillis() - getEnqueueTime();
    }
    long elapsedExecutionTime = 0;
    if(getExecutionStartTime() > 0) {
      if(getExecutionFinishTime() > 0) {
        elapsedExecutionTime = getExecutionFinishTime() - getExecutionStartTime();
      } else {
        elapsedExecutionTime = System.currentTimeMillis() - getExecutionStartTime();
      }
    }
    long logFileLength = 0;
    if(getOutputFile() != null) {
      logFileLength = getOutputFile().length();
    }
    return new TestStatus(startRequest.getTestHandle(), getStatus(),
        elapsedQueueTime, elapsedExecutionTime, logFileLength);
  }
}
