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
package org.apache.hadoop.hive.ql.exec.repl;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerRestClient;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerRestClientImpl;
import org.apache.hadoop.hive.ql.exec.repl.ranger.NoOpRangerRestClient;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerPolicy;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerExportPolicyList;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.load.log.RangerLoadLogger;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_AUTHORIZATION_PROVIDER_SERVICE_ENDPOINT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_RANGER_ADD_DENY_POLICY_TARGET;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_RANGER_SERVICE_NAME;

/**
 * RangerLoadTask.
 *
 * Rask to import Ranger authorization policies.
 **/
public class RangerLoadTask extends Task<RangerLoadWork> implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(RangerLoadTask.class);

  private transient RangerRestClient rangerRestClient;

  private transient ReplLogger replLogger;

  public RangerLoadTask() {
    super();
  }

  @VisibleForTesting
  RangerLoadTask(final RangerRestClient rangerRestClient, final HiveConf conf, final RangerLoadWork work) {
    this.conf = conf;
    this.work = work;
    this.rangerRestClient = rangerRestClient;
  }

  @Override
  public String getName() {
    return "RANGER_LOAD";
  }

  @Override
  public int execute() {
    try {
      LOG.info("Importing Ranger Metadata");
      RangerExportPolicyList rangerExportPolicyList = null;
      List<RangerPolicy> rangerPolicies = null;
      if (rangerRestClient == null) {
        rangerRestClient = getRangerRestClient();
      }
      String rangerEndpoint = conf.getVar(REPL_AUTHORIZATION_PROVIDER_SERVICE_ENDPOINT);
      if (StringUtils.isEmpty(rangerEndpoint) || !rangerRestClient.checkConnection(rangerEndpoint)) {
        throw new Exception("Ranger endpoint is not valid. "
                + "Please pass a valid config hive.repl.authorization.provider.service.endpoint");
      }
      if (work.getCurrentDumpPath() != null) {
        LOG.info("Importing Ranger Metadata from {} ", work.getCurrentDumpPath());
        rangerExportPolicyList = rangerRestClient.readRangerPoliciesFromJsonFile(new Path(work.getCurrentDumpPath(),
                ReplUtils.HIVE_RANGER_POLICIES_FILE_NAME), conf);
        int expectedPolicyCount = rangerExportPolicyList == null ? 0 : rangerExportPolicyList.getListSize();
        replLogger = new RangerLoadLogger(work.getSourceDbName(), work.getTargetDbName(),
          work.getCurrentDumpPath().toString(), expectedPolicyCount);
        replLogger.startLog();
        if (rangerExportPolicyList != null && !CollectionUtils.isEmpty(rangerExportPolicyList.getPolicies())) {
          rangerPolicies = rangerExportPolicyList.getPolicies();
        }
      }

      if (CollectionUtils.isEmpty(rangerPolicies)) {
        LOG.info("There are no ranger policies to import");
        rangerPolicies = new ArrayList<>();
      }
      List<RangerPolicy> rangerPoliciesWithDenyPolicy = rangerPolicies;
      if (conf.getBoolVar(REPL_RANGER_ADD_DENY_POLICY_TARGET)) {
        rangerPoliciesWithDenyPolicy = rangerRestClient.addDenyPolicies(rangerPolicies,
          conf.getVar(REPL_RANGER_SERVICE_NAME), work.getSourceDbName(), work.getTargetDbName());
      }

      List<RangerPolicy> updatedRangerPolicies = rangerRestClient.changeDataSet(rangerPoliciesWithDenyPolicy,
          work.getSourceDbName(), work.getTargetDbName());

      long importCount = 0;
      if (!CollectionUtils.isEmpty(updatedRangerPolicies)) {
        if (rangerExportPolicyList == null) {
          rangerExportPolicyList = new RangerExportPolicyList();
        }
        rangerExportPolicyList.setPolicies(updatedRangerPolicies);
        rangerRestClient.importRangerPolicies(rangerExportPolicyList, work.getTargetDbName(), rangerEndpoint,
                conf.getVar(REPL_RANGER_SERVICE_NAME));
        LOG.info("Number of ranger policies imported {}", rangerExportPolicyList.getListSize());
        importCount = rangerExportPolicyList.getListSize();
        replLogger.endLog(importCount);
        LOG.info("Ranger policy import finished {} ", importCount);
      }
      return 0;
    } catch (Exception e) {
      LOG.error("Failed", e);
      setException(e);
      return ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
    }
  }

  private RangerRestClient getRangerRestClient() {
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST_REPL) || conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)) {
      return new NoOpRangerRestClient();
    }
    return new RangerRestClientImpl();
  }

  @Override
  public StageType getType() {
    return StageType.RANGER_LOAD;
  }

  @Override
  public boolean canExecuteInParallel() {
    return false;
  }
}
