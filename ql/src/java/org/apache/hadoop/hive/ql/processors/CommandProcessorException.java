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

/**
 * Exception thrown during command processing class.
 */
public class CommandProcessorException extends Exception {
  private static final long serialVersionUID = 1L;

  private final int responseCode;
  private final int hiveErrorCode;
  private final String sqlState;

  public CommandProcessorException(int responseCode) {
    this(responseCode, -1, null, null, null);
  }

  public CommandProcessorException(String errorMessage) {
    this(errorMessage, null);
  }

  public CommandProcessorException(Throwable exception) {
    this(exception.getMessage(), exception);
  }

  public CommandProcessorException(String errorMessage, Throwable exception) {
    this(1, -1, errorMessage, null, exception);
  }

  public CommandProcessorException(int responseCode, int hiveErrorCode, String errorMessage, String sqlState,
      Throwable exception) {
    super(errorMessage, exception);
    this.responseCode = responseCode;
    this.hiveErrorCode = hiveErrorCode;
    this.sqlState = sqlState;
  }

  public int getResponseCode() {
    return responseCode;
  }
  
  public String getCauseMessage() {
    return getCause() == null ? "" : getCause().getMessage();
  }

  public int getErrorCode() {
    return hiveErrorCode;
  }

  public String getSqlState() {
    return sqlState;
  }

  @Override
  public String toString() {
    return "(responseCode = " + responseCode + ", errorMessage = " + getMessage() + ", "
        + (hiveErrorCode > 0 ? "hiveErrorCode = " + hiveErrorCode + ", " : "") + "SQLState = " + sqlState
        + (getCause() == null ? "" : ", exception = " + getCause().getMessage()) + ")";
  }

}
