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
package org.apache.hive.ptest.api.response;

import java.util.concurrent.TimeUnit;

import org.apache.hive.ptest.api.Status;

public class TestStatus {
  private String testHandle;
  private Status status;
  private long elapsedQueueTime;
  private long elapsedExecutionTime;
  private long logFileLength;
  public TestStatus(String testHandle, Status status, long elapsedQueueTime,
      long elapsedExecutionTime, long logFileLength) {
    super();
    this.testHandle = testHandle;
    this.status = status;
    this.elapsedQueueTime = elapsedQueueTime;
    this.elapsedExecutionTime = elapsedExecutionTime;
    this.logFileLength = logFileLength;
  }
  public TestStatus() {

  }
  public String getTestHandle() {
    return testHandle;
  }
  public void setTestHandle(String testHandle) {
    this.testHandle = testHandle;
  }
  public Status getStatus() {
    return status;
  }
  public void setStatus(Status status) {
    this.status = status;
  }
  public long getElapsedQueueTime() {
    return elapsedQueueTime;
  }
  public void setElapsedQueueTime(long elapsedQueueTime) {
    this.elapsedQueueTime = elapsedQueueTime;
  }
  public long getElapsedExecutionTime() {
    return elapsedExecutionTime;
  }
  public void setElapsedExecutionTime(long elapsedExecutionTime) {
    this.elapsedExecutionTime = elapsedExecutionTime;
  }
  public long getLogFileLength() {
    return logFileLength;
  }
  public void setLogFileLength(long logFileLength) {
    this.logFileLength = logFileLength;
  }
  @Override
  public String toString() {
    return "TestStatus [testHandle=" + testHandle + ", status=" + status
        + ", elapsedQueueTime=" + toMinutes(elapsedQueueTime) + " minute(s), elapsedExecutionTime="
        + toMinutes(elapsedExecutionTime) + " minute(s), logFileLength=" + logFileLength + "]";
  }
  private long toMinutes(long value) {
    return TimeUnit.MINUTES.convert(value, TimeUnit.MILLISECONDS);
  }
}
