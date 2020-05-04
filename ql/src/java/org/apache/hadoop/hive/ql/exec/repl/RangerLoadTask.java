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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerRestClient;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerRestClientImpl;
import org.apache.hadoop.hive.ql.exec.repl.ranger.TestRangerRestClient;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerPolicy;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerExportPolicyList;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_AUTHORIZATION_PROVIDER_SERVICE_ENDPOINT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_RANGER_SERVICE_NAME;

public class RangerLoadTask extends Task<RangerLoadWork> implements Serializable {
  private static final long serialVersionUID = 1L;

  private Logger LOG = LoggerFactory.getLogger(RangerLoadTask.class);

  @Override
  public String getName() {
    return "RANGER_LOAD";
  }

  @Override
  public int execute() {
    try {
      RangerRestClient rangerRestClient = getRangerRestClient();
      RangerExportPolicyList rangerExportPolicyList = null;
      List<RangerPolicy> rangerPolicies = null;
      String rangerEndpoint = conf.getVar(REPL_AUTHORIZATION_PROVIDER_SERVICE_ENDPOINT);
      if (StringUtils.isEmpty(rangerEndpoint)) {
        throw new Exception("Ranger endpoint is not valid. "
                + "Please pass a valid config hive.repl.authorization.provider.service.endpoint");
      }
      if (work.getCurrentDumpPath() != null) {
        rangerExportPolicyList = rangerRestClient.readRangerPoliciesFromJsonFile(new Path(work.getCurrentDumpPath(),
                ReplUtils.HIVE_RANGER_POLICIES_FILE_NAME + ".json"));
        if (rangerExportPolicyList != null && !CollectionUtils.isEmpty(rangerExportPolicyList.getPolicies())) {
          rangerPolicies = rangerExportPolicyList.getPolicies();
        }
      }

      if (CollectionUtils.isEmpty(rangerPolicies)) {
        rangerPolicies = new ArrayList<>();
      }
      List<RangerPolicy> updatedRangerPolicies = rangerRestClient.changeDataSet(rangerPolicies, work.getSourceDbName(),
              work.getTargetDbName());
      int importCount = 0;
      if (!CollectionUtils.isEmpty(updatedRangerPolicies)) {
        if (rangerExportPolicyList == null) {
          rangerExportPolicyList = new RangerExportPolicyList();
        }
        rangerExportPolicyList.setPolicies(updatedRangerPolicies);
        rangerRestClient.importRangerPolicies(rangerExportPolicyList, work.getTargetDbName(), rangerEndpoint,
                conf.getVar(REPL_RANGER_SERVICE_NAME));
        LOG.info("Number of ranger policies imported {}", rangerExportPolicyList.getListSize());
        importCount = rangerExportPolicyList.getListSize();
        LOG.info("Ranger policy import finished {} ", importCount);
      }
      //Create the ack file to notify ranger dump is finished.
      Path loadAckFile = new Path(work.getCurrentDumpPath(), ReplAck.RANGER_LOAD_ACKNOWLEDGEMENT.toString());
      Utils.create(loadAckFile, conf);
      return 0;
    } catch (Exception e) {
      LOG.error("failed", e);
      setException(e);
      return ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
    }
  }

  private RangerRestClient getRangerRestClient() {
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST_REPL) || conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST)) {
      return new TestRangerRestClient();
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
