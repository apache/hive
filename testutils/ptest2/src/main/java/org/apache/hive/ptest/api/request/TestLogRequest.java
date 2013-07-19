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
package org.apache.hive.ptest.api.request;

public class TestLogRequest {
  private String testHandle;
  private long offset;
  private long length;

  public TestLogRequest() {

  }
  public TestLogRequest(String testHandle, long offset, long length) {
    super();
    this.testHandle = testHandle;
    this.offset = offset;
    this.length = length;
  }
  public long getOffset() {
    return offset;
  }
  public void setOffset(long offset) {
    this.offset = offset;
  }
  public long getLength() {
    return length;
  }
  public void setLength(long length) {
    this.length = length;
  }
  public String getTestHandle() {
    return testHandle;
  }
  public void setTestHandle(String testHandle) {
    this.testHandle = testHandle;
  }
}
