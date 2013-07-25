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

import java.util.Collections;
import java.util.List;

import org.apache.hive.ptest.api.Status;

public class TestListResponse implements GenericResponse {
  private Status status;
  private List<TestStatus> entries;

  public TestListResponse() {
    this(null);
  }
  public TestListResponse(Status status) {
    this(status, Collections.<TestStatus>emptyList());
  }
  public TestListResponse(Status status, List<TestStatus> entries) {
    this.status = status;
    this.entries = entries;
  }
  @Override
  public Status getStatus() {
    return status;
  }
  public void setStatus(Status status) {
    this.status = status;
  }
  public List<TestStatus> getEntries() {
    return entries;
  }
  public void setEntries(List<TestStatus> entries) {
    this.entries = entries;
  }
}
