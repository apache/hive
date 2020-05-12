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
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerExportPolicyList;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerPolicy;
import org.apache.hadoop.hive.ql.exec.repl.ranger.NoOpRangerRestClient;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerRestClientImpl;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_AUTHORIZATION_PROVIDER_SERVICE_ENDPOINT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_RANGER_SERVICE_NAME;

/**
 * RangerDumpTask.
 *
 * Exports the Ranger security policies to staging directory.
 **/
public class RangerDumpTask extends Task<RangerDumpWork> implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(RangerDumpTask.class);

  private transient RangerRestClient rangerRestClient;

  public RangerDumpTask() {
    super();
  }

  @VisibleForTesting
  RangerDumpTask(final RangerRestClient rangerRestClient, final HiveConf conf, final RangerDumpWork work) {
    this.conf = conf;
    this.work = work;
    this.rangerRestClient = rangerRestClient;
  }

  @Override
  public String getName() {
    return "RANGER_DUMP";
  }

  @Override
  public int execute() {
    try {
      int exportCount = 0;
      Path filePath = null;
      LOG.info("Exporting Ranger Metadata");
      if (rangerRestClient == null) {
        rangerRestClient = getRangerRestClient();
      }
      String rangerEndpoint = conf.getVar(REPL_AUTHORIZATION_PROVIDER_SERVICE_ENDPOINT);
      if (StringUtils.isEmpty(rangerEndpoint) || !rangerRestClient.checkConnection(rangerEndpoint)) {
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
        filePath = rangerRestClient.saveRangerPoliciesToFile(rangerExportPolicyList,
                work.getCurrentDumpPath(), ReplUtils.HIVE_RANGER_POLICIES_FILE_NAME, conf);
        if (filePath != null) {
          LOG.info("Ranger policy export finished successfully");
          exportCount = rangerExportPolicyList.getListSize();
        }
      }
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
      return new NoOpRangerRestClient();
    }
    return new RangerRestClientImpl();
  }

  @Override
  public StageType getType() {
    return StageType.RANGER_DUMP;
  }

  @Override
  public boolean canExecuteInParallel() {
    return false;
  }
}
