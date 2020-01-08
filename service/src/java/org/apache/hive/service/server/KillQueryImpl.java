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

package org.apache.hive.service.server;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.session.KillQuery;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ApplicationsRequestScope;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.operation.Operation;
import org.apache.hive.service.cli.operation.OperationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class KillQueryImpl implements KillQuery {
  private final static Logger LOG = LoggerFactory.getLogger(KillQueryImpl.class);

  private final OperationManager operationManager;
  private enum TagOrId {TAG, ID, UNKNOWN};

  public KillQueryImpl(OperationManager operationManager) {
    this.operationManager = operationManager;
  }

  public static Set<ApplicationId> getChildYarnJobs(Configuration conf, String tag) throws IOException, YarnException {
    Set<ApplicationId> childYarnJobs = new HashSet<ApplicationId>();
    GetApplicationsRequest gar = GetApplicationsRequest.newInstance();
    gar.setScope(ApplicationsRequestScope.OWN);
    gar.setApplicationTags(Collections.singleton(tag));

    ApplicationClientProtocol proxy = ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class);
    GetApplicationsResponse apps = proxy.getApplications(gar);
    List<ApplicationReport> appsList = apps.getApplicationList();
    for(ApplicationReport appReport : appsList) {
      if (isAdmin() || appReport.getApplicationTags().contains(QueryState.USERID_TAG + "=" + SessionState.get()
              .getUserName())) {
        childYarnJobs.add(appReport.getApplicationId());
      }
    }

    if (childYarnJobs.isEmpty()) {
      LOG.info("No child applications found");
    } else {
      LOG.info("Found child YARN applications: " + StringUtils.join(childYarnJobs, ","));
    }

    return childYarnJobs;
  }

  public static void killChildYarnJobs(Configuration conf, String tag) {
    try {
      if (tag == null) {
        return;
      }
      LOG.info("Killing yarn jobs using query tag:" + tag);
      Set<ApplicationId> childYarnJobs = getChildYarnJobs(conf, tag);
      if (!childYarnJobs.isEmpty()) {
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        for (ApplicationId app : childYarnJobs) {
          yarnClient.killApplication(app);
        }
      }
    } catch (IOException | YarnException ye) {
      LOG.warn("Exception occurred while killing child job({})", ye);
    }
  }

  private static boolean isAdmin() {
    boolean isAdmin = false;
    if (SessionState.get().getAuthorizerV2() != null) {
      try {
        SessionState.get().getAuthorizerV2().checkPrivileges(HiveOperationType.KILL_QUERY,
                new ArrayList<HivePrivilegeObject>(), new ArrayList<HivePrivilegeObject>(),
                new HiveAuthzContext.Builder().build());
        isAdmin = true;
      } catch (Exception e) {
      }
    }
    return isAdmin;
  }

  private boolean cancelOperation(Operation operation, boolean isAdmin, String errMsg) throws
          HiveSQLException {
    if (isAdmin || operation.getParentSession().getUserName().equals(SessionState.get()
            .getAuthenticator().getUserName())) {
      OperationHandle handle = operation.getHandle();
      operationManager.cancelOperation(handle, errMsg);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void killQuery(String queryIdOrTag, String errMsg, HiveConf conf) throws HiveException {
    try {
      TagOrId tagOrId = TagOrId.UNKNOWN;
      Set<Operation> operationsToKill = new HashSet<Operation>();
      if (operationManager.getOperationByQueryId(queryIdOrTag) != null) {
        operationsToKill.add(operationManager.getOperationByQueryId(queryIdOrTag));
        tagOrId = TagOrId.ID;
      } else {
        operationsToKill.addAll(operationManager.getOperationsByQueryTag(queryIdOrTag));
        if (!operationsToKill.isEmpty()) {
          tagOrId = TagOrId.TAG;
        }
      }
      if (operationsToKill.isEmpty()) {
        LOG.info("Query not found: " + queryIdOrTag);
      }
      boolean admin = isAdmin();
      switch(tagOrId) {
        case ID:
          Operation operation = operationsToKill.iterator().next();
          boolean canceled = cancelOperation(operation, admin, errMsg);
          if (canceled) {
            String queryTag = operation.getQueryTag();
            if (queryTag == null) {
              queryTag = queryIdOrTag;
            }
            killChildYarnJobs(conf, queryTag);
          } else {
            // no privilege to cancel
            throw new HiveSQLException("No privilege to kill query id");
          }
          break;
        case TAG:
          int numCanceled = 0;
          for (Operation operationToKill : operationsToKill) {
            if (cancelOperation(operationToKill, admin, errMsg)) {
              numCanceled++;
            }
          }
          killChildYarnJobs(conf, queryIdOrTag);
          if (numCanceled == 0) {
            throw new HiveSQLException("No privilege to kill query tag");
          }
          break;
        case UNKNOWN:
          killChildYarnJobs(conf, queryIdOrTag);
          break;
      }
    } catch (HiveSQLException e) {
      LOG.error("Kill query failed for query " + queryIdOrTag, e);
      throw new HiveException(e.getMessage(), e);
    }
  }
}
