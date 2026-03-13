/*
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

package org.apache.hadoop.hive.ql.ddl.misc.msck;

import java.util.ArrayList;
import java.util.List;

/**
 * Generic result of a table repair operation.
 * Contains the number of issues found/fixed and a detailed log message.
 */
public record MsckResult(int numIssues, String message, List<String> details) {

  /**
   * Creates a result with issue count and log message.
   *
   * @param numIssues number of issues found or fixed
   * @param message log message describing the operation result
   */
  public MsckResult(int numIssues, String message) {
    this(numIssues, message, new ArrayList<>());
  }

  /**
   * Creates a result with issue count, log message, and detail items.
   *
   * @param numIssues number of issues found or fixed
   * @param message log message describing the operation result
   * @param details list of detail strings (e.g., file paths)
   */
  public MsckResult(int numIssues, String message, List<String> details) {
    this.numIssues = numIssues;
    this.message = message;
    this.details = details != null ? details : new ArrayList<>();
  }

}
