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

package org.apache.hadoop.hive.ql.processors;

import org.apache.hadoop.hive.metastore.api.Schema;

/**
 * Encapsulates the basic response info returned by classes the implement the
 * <code>CommandProcessor</code> interface. Typically <code>errorMessage</code>
 * and <code>SQLState</code> will only be set if the <code>responseCode</code>
 * is not 0.
 */
public class CommandProcessorResponse {
  private final int responseCode;
  private final String errorMessage;
  private final String SQLState;
  private final Schema resSchema;

  private final Throwable exception;

  public CommandProcessorResponse(int responseCode) {
    this(responseCode, null, null, null, null);
  }

  public CommandProcessorResponse(int responseCode, String errorMessage, String SQLState) {
    this(responseCode, errorMessage, SQLState, null, null);
  }

  public CommandProcessorResponse(int responseCode, String errorMessage, String SQLState, Throwable exception) {
    this(responseCode, errorMessage, SQLState, null, exception);
  }

  public CommandProcessorResponse(int responseCode, String errorMessage, String SQLState, Schema schema) {
    this(responseCode, errorMessage, SQLState, schema, null);
  }

  /**
   * Create CommandProcessorResponse object indicating an error.
   * Creates new CommandProcessorResponse with responseCode=1, and sets message
   * from exception argument
   *
   * @param e
   * @return
   */
  public static CommandProcessorResponse create(Exception e) {
    return new CommandProcessorResponse(1, e.getMessage(), null);
  }

  public CommandProcessorResponse(int responseCode, String errorMessage, String SQLState,
      Schema schema, Throwable exception) {
    this.responseCode = responseCode;
    this.errorMessage = errorMessage;
    this.SQLState = SQLState;
    this.resSchema = schema;
    this.exception = exception;
  }

  public int getResponseCode() { return responseCode; }
  public String getErrorMessage() { return errorMessage; }
  public String getSQLState() { return SQLState; }
  public Schema getSchema() { return resSchema; }
  public Throwable getException() { return exception; }
  public String toString() {
    return "(" + responseCode + "," + errorMessage + "," + SQLState + 
      (resSchema == null ? "" : ",") +
      (exception == null ? "" : exception.getMessage()) + ")";
  }
}
