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

package org.apache.hadoop.hive.ql.processors;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.ErrorMsg;

/**
 * Encapsulates the basic response info returned by classes the implement the
 * <code>CommandProcessor</code> interface. Typically <code>errorMessage</code>
 * and <code>SQLState</code> will only be set if the <code>responseCode</code>
 * is not 0.  Note that often {@code responseCode} ends up the exit value of
 * command shell process so should keep it to < 127.
 */
public class CommandProcessorResponse extends Exception {
  private final int responseCode;
  private final String errorMessage;
  private final int hiveErrorCode;
  private final String SQLState;
  private final Schema resSchema;

  private final Throwable exception;
  private final List<String> consoleMessages;

  public CommandProcessorResponse(int responseCode) {
    this(responseCode, null, null, null, null);
  }

  public CommandProcessorResponse(int responseCode, String errorMessage, String SQLState) {
    this(responseCode, errorMessage, SQLState, null, null);
  }

  public CommandProcessorResponse(int responseCode, List<String> consoleMessages) {
    this(responseCode, null, null, null, null, -1, consoleMessages);
  }

  public CommandProcessorResponse(int responseCode, String errorMessage, String SQLState, Throwable exception) {
    this(responseCode, errorMessage, SQLState, null, exception);
  }

  public CommandProcessorResponse(int responseCode, String errorMessage, String SQLState, Schema schema) {
    this(responseCode, errorMessage, SQLState, schema, null);
  }
  public CommandProcessorResponse(int responseCode, ErrorMsg canonicalErrMsg, Throwable t, String ... msgArgs) {
    this(responseCode, canonicalErrMsg.format(msgArgs),
      canonicalErrMsg.getSQLState(), null, t, canonicalErrMsg.getErrorCode(), null);
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
    this(responseCode, errorMessage, SQLState, schema, exception, -1, null);
  }
  public CommandProcessorResponse(int responseCode, String errorMessage, String SQLState,
      Schema schema, Throwable exception, int hiveErrorCode, List<String> consoleMessages) {
    this.responseCode = responseCode;
    this.errorMessage = errorMessage;
    this.SQLState = SQLState;
    this.resSchema = schema;
    this.exception = exception;
    this.hiveErrorCode = hiveErrorCode;
    this.consoleMessages = consoleMessages;
  }

  public int getResponseCode() { return responseCode; }
  public String getErrorMessage() { return errorMessage; }
  public String getSQLState() { return SQLState; }
  public Schema getSchema() { return resSchema; }
  public Throwable getException() { return exception; }

  public List<String> getConsoleMessages() {
    return consoleMessages;
  }
  public int getErrorCode() { return hiveErrorCode; }
  @Override
  public String toString() {
    return "(responseCode = " + responseCode + ", errorMessage = " + errorMessage + ", " +
      (hiveErrorCode > 0 ? "hiveErrorCode = " + hiveErrorCode + ", " : "" ) +
      "SQLState = " + SQLState +
      (resSchema == null ? "" : ", resSchema = " + resSchema) +
      (exception == null ? "" : ", exception = " + exception.getMessage()) + ")";
  }

  public boolean failed() {
    return responseCode != 0;
  }
}
