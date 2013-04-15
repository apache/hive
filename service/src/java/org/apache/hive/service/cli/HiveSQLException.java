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

package org.apache.hive.service.cli;

import java.sql.SQLException;

import org.apache.hive.service.cli.thrift.TStatus;
import org.apache.hive.service.cli.thrift.TStatusCode;

/**
 * HiveSQLException.
 *
 */
public class HiveSQLException extends SQLException {

  /**
   *
   */
  private static final long serialVersionUID = -6095254671958748094L;

  /**
   *
   */
  public HiveSQLException() {
    super();
  }

  /**
   * @param reason
   */
  public HiveSQLException(String reason) {
    super(reason);
  }

  /**
   * @param cause
   */
  public HiveSQLException(Throwable cause) {
    super(cause);
  }

  /**
   * @param reason
   * @param sqlState
   */
  public HiveSQLException(String reason, String sqlState) {
    super(reason, sqlState);
  }

  /**
   * @param reason
   * @param cause
   */
  public HiveSQLException(String reason, Throwable cause) {
    super(reason, cause);
  }

  /**
   * @param reason
   * @param sqlState
   * @param vendorCode
   */
  public HiveSQLException(String reason, String sqlState, int vendorCode) {
    super(reason, sqlState, vendorCode);
  }

  /**
   * @param reason
   * @param sqlState
   * @param cause
   */
  public HiveSQLException(String reason, String sqlState, Throwable cause) {
    super(reason, sqlState, cause);
  }

  /**
   * @param reason
   * @param sqlState
   * @param vendorCode
   * @param cause
   */
  public HiveSQLException(String reason, String sqlState, int vendorCode, Throwable cause) {
    super(reason, sqlState, vendorCode, cause);
  }

  public HiveSQLException(TStatus status) {
    // TODO: set correct vendorCode field
    super(status.getErrorMessage(), status.getSqlState(), 1);
  }

  public TStatus toTStatus() {
    // TODO: convert sqlState, etc.
    TStatus tStatus = new TStatus(TStatusCode.ERROR_STATUS);
    tStatus.setSqlState(getSQLState());
    tStatus.setErrorCode(getErrorCode());
    tStatus.setErrorMessage(getMessage());
    return tStatus;
  }

  public static TStatus toTStatus(Exception e) {
    if (e instanceof HiveSQLException) {
      return ((HiveSQLException)e).toTStatus();
    }
    TStatus tStatus = new TStatus(TStatusCode.ERROR_STATUS);
    return tStatus;
  }

}
