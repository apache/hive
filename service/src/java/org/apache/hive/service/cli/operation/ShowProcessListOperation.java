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
package org.apache.hive.service.cli.operation;

import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.ShowProcessListProcessor;
import org.apache.hadoop.hive.ql.session.ProcessListInfo;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.session.SessionManager;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ShowProcessListOperation extends HiveCommandOperation {

  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

  protected ShowProcessListOperation(HiveSession parentSession, String statement,
      CommandProcessor commandProcessor, Map<String, String> confOverlay) {
    super(parentSession, statement, commandProcessor, confOverlay);
  }

  @Override
  public void runInternal() throws HiveSQLException {
    // For ShowProcessListProcessor , session and operation level details  are fetched from SessionManager.
    List<ProcessListInfo> liveQueries = getLiveQueryInfos(parentSession);
    ShowProcessListProcessor showProcesslistProcessor = (ShowProcessListProcessor) commandProcessor;
    if (liveQueries != null) {
      showProcesslistProcessor.setup(liveQueries);
    }
    super.runInternal();
  }

  private List<ProcessListInfo> getLiveQueryInfos(HiveSession parentSession) {
    SessionManager sessionManager = parentSession.getSessionManager();
    if (sessionManager == null) {
      return null;
    }
    long currentTime = System.currentTimeMillis();
    Collection<Operation> operations = sessionManager.getOperations();
    return operations.stream()
        .filter(op -> op instanceof SQLOperation) // Filter for SQLOperation instances
        .map(op -> {
          HiveSession session = op.getParentSession();
          QueryInfo query = sessionManager.getOperationManager()
              .getQueryInfo(op.getHandle().getHandleIdentifier().toString());

          LocalDateTime beginTime = LocalDateTime.ofInstant(
              Instant.ofEpochMilli(query.getBeginTime()), ZoneId.systemDefault()
          );
          long txnId = 0;
          if (op.queryState != null && op.queryState.getTxnManager() != null) {
            txnId = op.queryState.getTxnManager().getCurrentTxnId();
          }
          return new ProcessListInfo.Builder()
              .setUserName(session.getUserName())
              .setIpAddr(session.getIpAddress())
              .setSessionId(session.getSessionHandle().getHandleIdentifier().toString())
              .setSessionActiveTime((currentTime - session.getCreationTime()) / 1000)
              .setSessionIdleTime((currentTime - session.getLastAccessTime()) / 1000)
              .setQueryId(op.getQueryId())
              .setExecutionEngine(query.getExecutionEngine())
              .setBeginTime(beginTime.format(FORMATTER))
              .setRuntime(query.getRuntime() == null ? "Not finished" : String.valueOf(query.getRuntime() / 1000))
              .setElapsedTime(query.getElapsedTime() / 1000)
              .setState(query.getState())
              .setTxnId(txnId)
              .build();
        })
        .collect(Collectors.toList());
  }
}
