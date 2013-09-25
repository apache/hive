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
package org.apache.hive.hcatalog.common;

import java.io.IOException;

/**
 * Class representing exceptions thrown by HCat.
 */
public class HCatException extends IOException {

  private static final long serialVersionUID = 1L;

  /** The error type enum for this exception. */
  private final ErrorType errorType;

  /**
   * Instantiates a new hcat exception.
   * @param errorType the error type
   */
  public HCatException(ErrorType errorType) {
    this(errorType, null, null);
  }


  /**
   * Instantiates a new hcat exception.
   * @param errorType the error type
   * @param cause the cause
   */
  public HCatException(ErrorType errorType, Throwable cause) {
    this(errorType, null, cause);
  }

  /**
   * Instantiates a new hcat exception.
   * @param errorType the error type
   * @param extraMessage extra messages to add to the message string
   */
  public HCatException(ErrorType errorType, String extraMessage) {
    this(errorType, extraMessage, null);
  }

  /**
   * Instantiates a new hcat exception.
   * @param errorType the error type
   * @param extraMessage extra messages to add to the message string
   * @param cause the cause
   */
  public HCatException(ErrorType errorType, String extraMessage, Throwable cause) {
    super(buildErrorMessage(
      errorType,
      extraMessage,
      cause), cause);
    this.errorType = errorType;
  }


  //TODO : remove default error type constructors after all exceptions
  //are changed to use error types

  /**
   * Instantiates a new hcat exception.
   * @param message the error message
   */
  public HCatException(String message) {
    this(ErrorType.ERROR_INTERNAL_EXCEPTION, message, null);
  }

  /**
   * Instantiates a new hcat exception.
   * @param message the error message
   * @param cause the cause
   */
  public HCatException(String message, Throwable cause) {
    this(ErrorType.ERROR_INTERNAL_EXCEPTION, message, cause);
  }


  /**
   * Builds the error message string. The error type message is appended with the extra message. If appendCause
   * is true for the error type, then the message of the cause also is added to the message.
   * @param type the error type
   * @param extraMessage the extra message string
   * @param cause the cause for the exception
   * @return the exception message string
   */
  public static String buildErrorMessage(ErrorType type, String extraMessage, Throwable cause) {

    //Initial message is just the error type message
    StringBuffer message = new StringBuffer(HCatException.class.getName());
    message.append(" : " + type.getErrorCode());
    message.append(" : " + type.getErrorMessage());

    if (extraMessage != null) {
      //Add the extra message value to buffer
      message.append(" : " + extraMessage);
    }

    if (type.appendCauseMessage()) {
      if (cause != null) {
        //Add the cause message to buffer
        message.append(". Cause : " + cause.toString());
      }
    }

    return message.toString();
  }


  /**
   * Is this a retriable error.
   * @return is it retriable
   */
  public boolean isRetriable() {
    return errorType.isRetriable();
  }

  /**
   * Gets the error type.
   * @return the error type enum
   */
  public ErrorType getErrorType() {
    return errorType;
  }

  /**
   * Gets the error code.
   * @return the error code
   */
  public int getErrorCode() {
    return errorType.getErrorCode();
  }

  /* (non-Javadoc)
  * @see java.lang.Throwable#toString()
  */
  @Override
  public String toString() {
    return getMessage();
  }

}
