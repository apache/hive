/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.service.cli.operation;

import org.apache.hive.service.cli.OperationState;

/**
 * Used to display some info in the HS2 WebUI.
 */
public class SQLOperationInfo {
  public String userName;
  public String queryStr;
  public String executionEngine;
  public OperationState endState; //state before CLOSED (one of CANCELLED, FINISHED, ERROR)
  public int elapsedTime;
  public long endTime;

  public SQLOperationInfo(
    String userName,
    String queryStr,
    String executionEngine,
    OperationState endState,
    int elapsedTime,
    long endTime
  ) {
    this.userName = userName;
    this.queryStr = queryStr;
    this.executionEngine = executionEngine;
    this.endState = endState;
    this.elapsedTime = elapsedTime;
    this.endTime = endTime;
  }
}
