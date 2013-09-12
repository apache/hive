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

import org.apache.hive.service.cli.thrift.TOperationState;

/**
 * OperationState.
 *
 */
public enum OperationState {
  INITIALIZED(TOperationState.INITIALIZED_STATE),
  RUNNING(TOperationState.RUNNING_STATE),
  FINISHED(TOperationState.FINISHED_STATE),
  CANCELED(TOperationState.CANCELED_STATE),
  CLOSED(TOperationState.CLOSED_STATE),
  ERROR(TOperationState.ERROR_STATE),
  UNKNOWN(TOperationState.UKNOWN_STATE),
  PENDING(TOperationState.PENDING_STATE);

  private final TOperationState tOperationState;

  OperationState(TOperationState tOperationState) {
    this.tOperationState = tOperationState;
  }


  public static OperationState getOperationState(TOperationState tOperationState) {
    // TODO: replace this with a Map?
    for (OperationState opState : values()) {
      if (tOperationState.equals(opState.tOperationState)) {
        return opState;
      }
    }
    return OperationState.UNKNOWN;
  }

  public static void validateTransition(OperationState oldState, OperationState newState)
      throws HiveSQLException {
    switch (oldState) {
    case INITIALIZED:
      switch (newState) {
      case PENDING:
      case RUNNING:
      case CLOSED:
        return;
      }
      break;
    case PENDING:
      switch (newState) {
      case RUNNING:
      case FINISHED:
      case CANCELED:
      case ERROR:
      case CLOSED:
        return;
      }
      break;
    case RUNNING:
      switch (newState) {
      case FINISHED:
      case CANCELED:
      case ERROR:
      case CLOSED:
        return;
      }
      break;
    case FINISHED:
    case CANCELED:
    case ERROR:
      if (OperationState.CLOSED.equals(newState)) {
        return;
      }
    default:
      // fall-through
    }
    throw new HiveSQLException("Illegal Operation state transition");
  }

  public void validateTransition(OperationState newState)
  throws HiveSQLException {
    validateTransition(this, newState);
  }

  public TOperationState toTOperationState() {
    return tOperationState;
  }
}
