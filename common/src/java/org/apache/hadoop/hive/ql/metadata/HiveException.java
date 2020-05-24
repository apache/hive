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

package org.apache.hadoop.hive.ql.metadata;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.ql.ErrorMsg;

/**
 * Generic exception class for Hive.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class HiveException extends Exception {

  /**
   * Standard predefined message with error code and possibly SQL State, etc.
   */
  private ErrorMsg canonicalErrorMsg = ErrorMsg.GENERIC_ERROR;

  /**
   * Error Messages returned from remote exception (eg. hadoop error)
   */
  private String remoteErrorMsg;

  public HiveException() {
    super();
  }

  public HiveException(String message) {
    super(message);
  }

  public HiveException(Throwable cause) {
    super(cause);
  }

  public HiveException(String message, Throwable cause) {
    super(message, cause);
  }

  public HiveException(ErrorMsg message, String... msgArgs) {
    this(null, null, message, msgArgs);
  }

  public HiveException(Throwable cause, ErrorMsg errorMsg, String... msgArgs) {
    this(cause, null, errorMsg, msgArgs);
  }

  public HiveException(Throwable cause, ErrorMsg errorMsg) {
    this(cause, null, errorMsg, new String[0]);
  }

  public HiveException(ErrorMsg errorMsg) {
    this(null, null, errorMsg, new String[0]);
  }

  /**
   * This is the recommended constructor to use since it helps use
   * canonical messages throughout and propagate remote errors.
   *
   * @param errorMsg Canonical error message
   * @param msgArgs message arguments if message is parametrized; must be {@code null} is message takes no arguments
   */
  public HiveException(Throwable cause, String remErrMsg, ErrorMsg errorMsg, String... msgArgs) {
    super(errorMsg.format(msgArgs), cause);
    canonicalErrorMsg = errorMsg;
    remoteErrorMsg = remErrMsg;
  }

  /**
   * @return {@link ErrorMsg#GENERIC_ERROR} by default
   */
  public ErrorMsg getCanonicalErrorMsg() {
    return canonicalErrorMsg;
  }

  public String getRemoteErrorMsg() { return remoteErrorMsg; }
}
