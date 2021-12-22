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

package org.apache.hadoop.hive.ql.hooks;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PrivateHookContext extends HookContext {

  private final Context ctx;

  public PrivateHookContext(QueryPlan queryPlan, QueryState queryState,
      Map<String, ContentSummary> inputPathToContentSummary, String userName, String ipAddress,
      String hiveInstanceAddress, String operationId, String sessionId, String threadId, boolean isHiveServerQuery,
      PerfLogger perfLogger, QueryInfo queryInfo, Context ctx) throws Exception {
    super(queryPlan, queryState, inputPathToContentSummary, userName, ipAddress, hiveInstanceAddress, operationId,
        sessionId, threadId, isHiveServerQuery, perfLogger, queryInfo);
    this.ctx = ctx;
  }

  public PrivateHookContext(DriverContext driverContext, Context context) throws Exception {
    this(driverContext.getPlan(), driverContext.getQueryState(),
        context.getPathToCS(), SessionState.get().getUserName(), SessionState.get().getUserIpAddress(),
        InetAddress.getLocalHost().getHostAddress(), driverContext.getOperationId(),
        SessionState.get().getSessionId(), Thread.currentThread().getName(), SessionState.get().isHiveServerQuery(),
        SessionState.getPerfLogger(), driverContext.getQueryInfo(), context);
  }

  public Context getContext() {
    return ctx;
  }
}
