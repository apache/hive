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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.common;

/**
 * Enum type representing the various errors throws by HCat.
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.common.ErrorType} instead
 */
public enum ErrorType {

  /* HCat Input Format related errors 1000 - 1999 */
  ERROR_DB_INIT                       (1000, "Error initializing database session"),
  ERROR_EXCEED_MAXPART                (1001, "Query result exceeded maximum number of partitions allowed"),

  ERROR_SET_INPUT                    (1002, "Error setting input information"),

  /* HCat Output Format related errors 2000 - 2999 */
  ERROR_INVALID_TABLE                 (2000, "Table specified does not exist"),
  ERROR_SET_OUTPUT                    (2001, "Error setting output information"),
  ERROR_DUPLICATE_PARTITION           (2002, "Partition already present with given partition key values"),
  ERROR_NON_EMPTY_TABLE               (2003, "Non-partitioned table already contains data"),
  ERROR_NOT_INITIALIZED               (2004, "HCatOutputFormat not initialized, setOutput has to be called"),
  ERROR_INIT_STORAGE_HANDLER          (2005, "Error initializing storage handler instance"),
  ERROR_PUBLISHING_PARTITION          (2006, "Error adding partition to metastore"),
  ERROR_SCHEMA_COLUMN_MISMATCH        (2007, "Invalid column position in partition schema"),
  ERROR_SCHEMA_PARTITION_KEY          (2008, "Partition key cannot be present in the partition data"),
  ERROR_SCHEMA_TYPE_MISMATCH          (2009, "Invalid column type in partition schema"),
  ERROR_INVALID_PARTITION_VALUES      (2010, "Invalid partition values specified"),
  ERROR_MISSING_PARTITION_KEY         (2011, "Partition key value not provided for publish"),
  ERROR_MOVE_FAILED                   (2012, "Moving of data failed during commit"),
  ERROR_TOO_MANY_DYNAMIC_PTNS         (2013, "Attempt to create too many dynamic partitions"),
  ERROR_INIT_LOADER                   (2014,  "Error initializing Pig loader"),
  ERROR_INIT_STORER                   (2015,  "Error initializing Pig storer"),
  ERROR_NOT_SUPPORTED                 (2016,  "Error operation not supported"),

  /* Authorization Errors 3000 - 3999 */
  ERROR_ACCESS_CONTROL           (3000, "Permission denied"),

  /* Miscellaneous errors, range 9000 - 9998 */
  ERROR_UNIMPLEMENTED                 (9000, "Functionality currently unimplemented"),
  ERROR_INTERNAL_EXCEPTION            (9001, "Exception occurred while processing HCat request");

  /** The error code. */
  private int errorCode;

  /** The error message. */
  private String errorMessage;

  /** Should the causal exception message be appended to the error message, yes by default*/
  private boolean appendCauseMessage = true;

  /** Is this a retriable error, no by default. */
  private boolean isRetriable = false;

  /**
   * Instantiates a new error type.
   * @param errorCode the error code
   * @param errorMessage the error message
   */
  private ErrorType(int errorCode, String errorMessage) {
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
  }

  /**
   * Instantiates a new error type.
   * @param errorCode the error code
   * @param errorMessage the error message
   * @param appendCauseMessage should causal exception message be appended to error message
   */
  private ErrorType(int errorCode, String errorMessage, boolean appendCauseMessage) {
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
    this.appendCauseMessage = appendCauseMessage;
  }

  /**
   * Instantiates a new error type.
   * @param errorCode the error code
   * @param errorMessage the error message
   * @param appendCauseMessage should causal exception message be appended to error message
   * @param isRetriable is this a retriable error
   */
  private ErrorType(int errorCode, String errorMessage, boolean appendCauseMessage, boolean isRetriable) {
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
    this.appendCauseMessage = appendCauseMessage;
    this.isRetriable = isRetriable;
  }

  /**
   * Gets the error code.
   * @return the error code
   */
  public int getErrorCode() {
    return errorCode;
  }

  /**
   * Gets the error message.
   * @return the error message
   */
  public String getErrorMessage() {
    return errorMessage;
  }

  /**
   * Checks if this is a retriable error.
   * @return true, if is a retriable error, false otherwise
   */
  public boolean isRetriable() {
    return isRetriable;
  }

  /**
   * Whether the cause of the exception should be added to the error message.
   * @return true, if the cause should be added to the message, false otherwise
   */
  public boolean appendCauseMessage() {
    return appendCauseMessage;
  }
}
