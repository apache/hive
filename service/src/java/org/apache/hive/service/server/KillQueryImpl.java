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

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.process.kill.KillQueriesOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.session.KillQuery;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
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
  private final KillQueryZookeeperManager killQueryZookeeperManager;

  private enum TagOrId {TAG, ID, UNKNOWN}

  public KillQueryImpl(OperationManager operationManager, KillQueryZookeeperManager killQueryZookeeperManager) {
    this.operationManager = operationManager;
    this.killQueryZookeeperManager = killQueryZookeeperManager;
  }

  public static Set<ApplicationId> getChildYarnJobs(Configuration conf, String tag, String doAs, boolean doAsAdmin)
      throws IOException, YarnException {
    Set<ApplicationId> childYarnJobs = new HashSet<>();
    GetApplicationsRequest gar = GetApplicationsRequest.newInstance();
    gar.setScope(ApplicationsRequestScope.OWN);
    gar.setApplicationTags(Collections.singleton(tag));

    ApplicationClientProtocol proxy = ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class);
    GetApplicationsResponse apps = proxy.getApplications(gar);
    List<ApplicationReport> appsList = apps.getApplicationList();
    for (ApplicationReport appReport : appsList) {
      if (doAsAdmin) {
        childYarnJobs.add(appReport.getApplicationId());
      } else if (StringUtils.isNotBlank(doAs)) {
        if (appReport.getApplicationTags().contains(QueryState.USERID_TAG + "=" + doAs)) {
          childYarnJobs.add(appReport.getApplicationId());
        }
      }
    }

    if (childYarnJobs.isEmpty()) {
      LOG.info("No child applications found");
    } else {
      LOG.info("Found child YARN applications: " + StringUtils.join(childYarnJobs, ","));
    }

    return childYarnJobs;
  }

  public static void killChildYarnJobs(Configuration conf, String tag, String doAs, boolean doAsAdmin) {
    try {
      if (tag == null) {
        return;
      }
      LOG.info("Killing yarn jobs using query tag:" + tag);
      Set<ApplicationId> childYarnJobs = getChildYarnJobs(conf, tag, doAs, doAsAdmin);
      if (!childYarnJobs.isEmpty()) {
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        for (ApplicationId app : childYarnJobs) {
          yarnClient.killApplication(app);
        }
      }
    } catch (IOException | YarnException ye) {
      LOG.warn("Exception occurred while killing child job({})", tag, ye);
    }
  }

  private static boolean isAdmin() {
    boolean isAdmin = false;
    // RANGER-1851
    HivePrivilegeObject serviceNameObj = new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.SERVICE_NAME, null, "hiveservice");
    SessionState ss = SessionState.get();
    if (!HiveConf.getBoolVar(ss.getConf(), HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)) {
      // If authorization is disabled, hs2 process owner should have kill privileges
      try {
        return StringUtils.equals(ss.getUserName(), UserGroupInformation.getCurrentUser().getShortUserName());
      } catch (IOException e) {
        LOG.warn("Unable to check for admin privileges", e);
        return false;
      }
    }
    if (ss.getAuthorizerV2() != null) {
      try {
        ss.getAuthorizerV2().checkPrivileges(HiveOperationType.KILL_QUERY, Arrays.asList(serviceNameObj), new ArrayList<HivePrivilegeObject>(),
            new HiveAuthzContext.Builder().build());
        isAdmin = true;
      } catch (Exception e) {
        LOG.warn("Error while checking privileges", e);
      }
    }
    return isAdmin;
  }

  private boolean cancelOperation(Operation operation, String doAs, boolean doAsAdmin, String errMsg)
      throws HiveSQLException {
    if (doAsAdmin || (!StringUtils.isBlank(doAs) && operation.getParentSession().getUserName().equals(doAs))) {
      OperationHandle handle = operation.getHandle();
      operationManager.cancelOperation(handle, errMsg);
      return true;
    }
    return false;
  }

  public boolean isLocalQuery(String queryIdOrTag) {
    TagOrId tagOrId = TagOrId.UNKNOWN;
    if (operationManager.getOperationByQueryId(queryIdOrTag) != null) {
      tagOrId = TagOrId.ID;
    } else if (!operationManager.getOperationsByQueryTag(queryIdOrTag).isEmpty()) {
      tagOrId = TagOrId.TAG;
    }
    return tagOrId != TagOrId.UNKNOWN;
  }

  @Override
  public void killQuery(String queryIdOrTag, String errMsg, HiveConf conf) throws HiveException {
    killQuery(queryIdOrTag, errMsg, conf, false, SessionState.get().getUserName(), isAdmin());
  }

  public void killLocalQuery(String queryIdOrTag, HiveConf conf, String doAs, boolean doAsAdmin) throws HiveException {
    killQuery(queryIdOrTag, null, conf, true, doAs, doAsAdmin);
  }

  private void killQuery(String queryIdOrTag, String errMsg, HiveConf conf, boolean onlyLocal, String doAs,
      boolean doAsAdmin) throws HiveException {
    errMsg = StringUtils.defaultString(errMsg, KillQueriesOperation.KILL_QUERY_MESSAGE);
    TagOrId tagOrId = TagOrId.UNKNOWN;
    Set<Operation> operationsToKill = new HashSet<>();
    if (operationManager.getOperationByQueryId(queryIdOrTag) != null) {
      operationsToKill.add(operationManager.getOperationByQueryId(queryIdOrTag));
      tagOrId = TagOrId.ID;
      LOG.debug("Query found with id: {}", queryIdOrTag);
    } else {
      operationsToKill.addAll(operationManager.getOperationsByQueryTag(queryIdOrTag));
      if (!operationsToKill.isEmpty()) {
        tagOrId = TagOrId.TAG;
        LOG.debug("Query found with tag: {}", queryIdOrTag);
      }
    }
    if (!operationsToKill.isEmpty()) {
      killOperations(queryIdOrTag, errMsg, conf, tagOrId, operationsToKill, doAs, doAsAdmin);
    } else {
      LOG.debug("Query not found with tag/id: {}", queryIdOrTag);
      if (!onlyLocal && killQueryZookeeperManager != null &&
          conf.getBoolVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_KILLQUERY_ENABLE)) {
        try {
          LOG.debug("Killing query with zookeeper coordination: " + queryIdOrTag);
          killQueryZookeeperManager
              .killQuery(queryIdOrTag, SessionState.get().getAuthenticator().getUserName(), isAdmin());
        } catch (IOException e) {
          LOG.error("Kill query failed for queryId: " + queryIdOrTag, e);
          throw new HiveException("Unable to kill query locally or on remote servers.", e);
        }
      } else {
        LOG.warn("Unable to kill query with id {}", queryIdOrTag);
      }
    }
  }

  private void killOperations(String queryIdOrTag, String errMsg, HiveConf conf, TagOrId tagOrId,
      Set<Operation> operationsToKill, String doAs, boolean doAsAdmin) throws HiveException {
    try {
      switch (tagOrId) {
      case ID:
        Operation operation = operationsToKill.iterator().next();
        boolean canceled = cancelOperation(operation, doAs, doAsAdmin, errMsg);
        if (canceled) {
          String queryTag = operation.getQueryTag();
          if (queryTag == null) {
            queryTag = queryIdOrTag;
          }
          killChildYarnJobs(conf, queryTag, doAs, doAsAdmin);
        } else {
          // no privilege to cancel
          throw new HiveSQLException("No privilege to kill query id");
        }
        break;
      case TAG:
        int numCanceled = 0;
        for (Operation operationToKill : operationsToKill) {
          if (cancelOperation(operationToKill, doAs, doAsAdmin, errMsg)) {
            numCanceled++;
          }
        }
        if (numCanceled == 0) {
          throw new HiveSQLException("No privilege to kill query tag");
        } else {
          killChildYarnJobs(conf, queryIdOrTag, doAs, doAsAdmin);
        }
        break;
      case UNKNOWN:
      default:
        break;
      }
    } catch (HiveSQLException e) {
      LOG.error("Kill query failed for query " + queryIdOrTag, e);
      throw new HiveException(e.getMessage(), e);
    }
  }
}
