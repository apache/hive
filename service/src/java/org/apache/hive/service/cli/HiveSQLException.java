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

package org.apache.hive.service.cli;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.ServiceContext;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;

import com.google.common.annotations.VisibleForTesting;

/**
 * An exception that provides information on a Hive access error or other
 * errors.
 */
public class HiveSQLException extends SQLException {

  public static final String QUERY_ID = "Query ID";
  private static final long serialVersionUID = -6095254671958748095L;
  private String queryId;

  @VisibleForTesting
  public static final List<String> DEFAULT_INFO =
      Collections.singletonList("Server-side error; please check HS2 logs.");

  /**
   * Constructor.
   * @deprecated Use constructor with queryId
   */
  @Deprecated
  public HiveSQLException() {
    super();
  }

  /**
   * Constructs a SQLException object with a given reason. The SQLState is
   * initialized to null and the vendor code is initialized to 0. The cause is
   * not initialized, and may subsequently be initialized by a call to the
   * Throwable.initCause(java.lang.Throwable) method.
   *
   * @param reason a description of the exception
   * @deprecated Use constructor with queryId
   */
  @Deprecated
  public HiveSQLException(String reason) {
    super(reason);
  }

  /**
   * Constructs a SQLException object with a given cause. The SQLState is
   * initialized to null and the vendor code is initialized to 0. The reason is
   * initialized to null if cause==null or to cause.toString() if cause!=null.
   *
   * @param cause the underlying reason for this SQLException - may be null
   *          indicating the cause is non-existent or unknown
   * @deprecated Use constructor with queryId
   */
  @Deprecated
  public HiveSQLException(Throwable cause) {
    super(cause);
  }

  public HiveSQLException(Throwable cause, String queryId) {
    super(cause);
    this.queryId = queryId;
  }

  /**
   * @param reason
   * @param sqlState
   * @deprecated Use constructor with queryId
   */
  @Deprecated
  public HiveSQLException(String reason, String sqlState) {
    super(reason, sqlState);
  }

  /**
   * @param reason
   * @param sqlState
   */
  public HiveSQLException(String reason, String sqlState, String queryId) {
    super(reason, sqlState);
    this.queryId = queryId;
  }

  /**
   * @param reason
   * @param cause
   * @deprecated Use constructor with queryId
   */
  @Deprecated
  public HiveSQLException(String reason, Throwable cause) {
    super(reason, cause);
  }

  /**
   * @param reason
   * @param cause
   */
  public HiveSQLException(String reason, Throwable cause, String queryId) {
    super(reason, cause);
    this.queryId = queryId;
  }

  /**
   * @param reason
   * @param sqlState
   * @param vendorCode
   * @deprecated Use constructor with queryId
   */
  @Deprecated
  public HiveSQLException(String reason, String sqlState, int vendorCode) {
    super(reason, sqlState, vendorCode);
  }

  /**
   * @param reason
   * @param sqlState
   * @param vendorCode
   * @param queryId
   */
  public HiveSQLException(String reason, String sqlState, int vendorCode, String queryId) {
    super(reason, sqlState, vendorCode);
    this.queryId = queryId;
  }

  /**
   * @param reason
   * @param sqlState
   * @param cause
   * @deprecated Use constructor with queryId
   */
  @Deprecated
  public HiveSQLException(String reason, String sqlState, Throwable cause) {
    super(reason, sqlState, cause);
  }

  public HiveSQLException(String reason, String sqlState, Throwable cause, String queryId) {
    super(reason, sqlState, cause);
    this.queryId = queryId;
  }

  /**
   * @param reason
   * @param sqlState
   * @param vendorCode
   * @param cause
   * @deprecated Use constructor with queryId
   */
  @Deprecated
  public HiveSQLException(String reason, String sqlState, int vendorCode, Throwable cause) {
    super(reason, sqlState, vendorCode, cause);
  }

  /**
   * @deprecated Use constructor with queryId
   */
  @Deprecated
  public HiveSQLException(TStatus status) {
    super(status.getErrorMessage(), status.getSqlState(), status.getErrorCode());
  }

  /**
   * Converts current object to a {@link TStatus} object.
   *
   * @return a {@link TStatus} object
   */
  public TStatus toTStatus() {
    // TODO: convert sqlState, etc.
    TStatus tStatus = new TStatus(TStatusCode.ERROR_STATUS);
    tStatus.setSqlState(getSQLState());
    tStatus.setErrorCode(getErrorCode());
    tStatus.setErrorMessage(getMessage());
    tStatus.setInfoMessages(DEFAULT_INFO);
    return tStatus;
  }

  /**
   * Converts the specified {@link Exception} object into a {@link TStatus}
   * object.
   *
   * @param e a {@link Exception} object
   * @return a {@link TStatus} object
   */
  public static TStatus toTStatus(Exception e) {
    if (e instanceof HiveSQLException) {
      return ((HiveSQLException) e).toTStatus();
    }
    TStatus tStatus = new TStatus(TStatusCode.ERROR_STATUS);
    tStatus.setErrorMessage(e.getMessage());
    tStatus.setInfoMessages(DEFAULT_INFO);
    return tStatus;
  }

  @Override
  public String getMessage(){
    String errorMsg = super.getMessage() == null ? "Error" : super.getMessage();
    if (!errorMsg.contains(QUERY_ID) && queryId != null) {
      return String.format("%s; Query ID: %s", errorMsg, queryId);
    } else {
      return errorMsg;
    }
  }
}
