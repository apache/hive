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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.KillQuery;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class KillQueryImpl implements KillQuery {
  private final static Logger LOG = LoggerFactory.getLogger(KillQueryImpl.class);

  private final OperationManager operationManager;

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
      childYarnJobs.add(appReport.getApplicationId());
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
      throw new RuntimeException("Exception occurred while killing child job(s)", ye);
    }
  }

  @Override
  public void killQuery(String queryId, String errMsg, HiveConf conf) throws HiveException {
    try {
      String queryTag = null;

      Operation operation = operationManager.getOperationByQueryId(queryId);
      if (operation == null) {
        // Check if user has passed the query tag to kill the operation. This is possible if the application
        // restarts and it does not have the proper query id. The tag can be used in that case to kill the query.
        operation = operationManager.getOperationByQueryTag(queryId);
        if (operation == null) {
          LOG.info("Query not found: " + queryId);
        }
      } else {
        // This is the normal flow, where the query is tagged and user wants to kill the query using the query id.
        queryTag = operation.getQueryTag();
      }

      if (queryTag == null) {
        //use query id as tag if user wanted to kill only the yarn jobs after hive server restart. The yarn jobs are
        //tagged with query id by default. This will cover the case where the application after restarts wants to kill
        //the yarn jobs with query tag. The query tag can be passed as query id.
        queryTag = queryId;
      }

      LOG.info("Killing yarn jobs for query id : " + queryId + " using tag :" + queryTag);
      killChildYarnJobs(conf, queryTag);

      if (operation != null) {
        OperationHandle handle = operation.getHandle();
        operationManager.cancelOperation(handle, errMsg);
      }
    } catch (HiveSQLException e) {
      LOG.error("Kill query failed for query " + queryId, e);
      throw new HiveException(e.getMessage(), e);
    }
  }
}
