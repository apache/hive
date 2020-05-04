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
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerExportPolicyList;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerPolicy;
import org.apache.hadoop.hive.ql.exec.repl.ranger.TestRangerRestClient;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerRestClientImpl;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_AUTHORIZATION_PROVIDER_SERVICE_ENDPOINT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_RANGER_SERVICE_NAME;

public class RangerDumpTask extends Task<RangerDumpWork> implements Serializable {
  private static final long serialVersionUID = 1L;

  private Logger LOG = LoggerFactory.getLogger(RangerDumpTask.class);

  @Override
  public String getName() {
    return "RANGER_DUMP";
  }

  @Override
  public int execute() {
    try {
      int exportCount = 0;
      Path filePath = null;
      RangerRestClient rangerRestClient = getRangerRestClient();
      LOG.info("Ranger policy export started");
      String rangerEndpoint = conf.getVar(REPL_AUTHORIZATION_PROVIDER_SERVICE_ENDPOINT);
      if (StringUtils.isEmpty(rangerEndpoint)) {
        throw new Exception("Ranger endpoint is not valid. "
                + "Please pass a valid config hive.repl.authorization.provider.service.endpoint");
      }
      String rangerHiveServiceName = conf.getVar(REPL_RANGER_SERVICE_NAME);
      RangerExportPolicyList rangerExportPolicyList = rangerRestClient.exportRangerPolicies(rangerEndpoint,
              work.getDbName(), rangerHiveServiceName);
      List<RangerPolicy> rangerPolicies = rangerExportPolicyList.getPolicies();
      if (rangerPolicies.isEmpty()) {
        LOG.info("Ranger policy export request returned empty list or failed, Please refer Ranger admin logs.");
        rangerExportPolicyList = new RangerExportPolicyList();
      } else {
        rangerPolicies = rangerRestClient.removeMultiResourcePolicies(rangerPolicies);
      }
      if (!CollectionUtils.isEmpty(rangerPolicies)) {
        rangerExportPolicyList.setPolicies(rangerPolicies);
        String fileName = ReplUtils.HIVE_RANGER_POLICIES_FILE_NAME + ".json";
        filePath = rangerRestClient.saveRangerPoliciesToFile(rangerExportPolicyList,
                work.getCurrentDumpPath(), fileName);
        if (filePath != null) {
          LOG.info("Ranger policy export finished successfully");
          exportCount = rangerExportPolicyList.getListSize();
        }
      }
      //Create the ack file to notify ranger dump is finished.
      Path dumpAckFile = new Path(work.getCurrentDumpPath(), ReplAck.RANGER_DUMP_ACKNOWLEDGEMENT.toString());
      Utils.create(dumpAckFile, conf);
      LOG.debug("Ranger policy export filePath:" + filePath);
      LOG.info("Number of ranger policies exported {}", exportCount);
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
    return StageType.RANGER_DUMP;
  }

  @Override
  public boolean canExecuteInParallel() {
    return true;
  }
}
