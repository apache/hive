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
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerRestClient;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerRestClientImpl;
import org.apache.hadoop.hive.ql.exec.repl.ranger.NoOpRangerRestClient;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerPolicy;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerExportPolicyList;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.ReplLogger;
import org.apache.hadoop.hive.ql.parse.repl.load.log.RangerLoadLogger;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Status;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
      SecurityUtils.reloginExpiringKeytabUser();
      if (rangerRestClient == null) {
        rangerRestClient = getRangerRestClient();
      }
      URL url = work.getRangerConfigResource();
      if (url == null) {
        throw new SemanticException(ErrorMsg.REPL_INVALID_CONFIG_FOR_SERVICE
          .format("Ranger configuration is not valid "
            + ReplUtils.RANGER_CONFIGURATION_RESOURCE_NAME, ReplUtils.REPL_RANGER_SERVICE));
      }
      conf.addResource(url);
      String rangerHiveServiceName = conf.get(ReplUtils.RANGER_HIVE_SERVICE_NAME);
      String rangerEndpoint = conf.get(ReplUtils.RANGER_REST_URL);
      if (StringUtils.isEmpty(rangerEndpoint)) {
        throw new SemanticException(ErrorMsg.REPL_INVALID_CONFIG_FOR_SERVICE
          .format("Ranger endpoint is not valid "
            + rangerEndpoint, ReplUtils.REPL_RANGER_SERVICE));
      }
      if (!rangerRestClient.checkConnection(rangerEndpoint, conf)) {
        throw new SemanticException(ErrorMsg.REPL_EXTERNAL_SERVICE_CONNECTION_ERROR.format(ReplUtils
            .REPL_RANGER_SERVICE,
          "Ranger endpoint is not valid " + rangerEndpoint));
      }
      if (work.getCurrentDumpPath() != null) {
        LOG.info("Importing Ranger Metadata from {} ", work.getCurrentDumpPath());
        rangerExportPolicyList = rangerRestClient.readRangerPoliciesFromJsonFile(new Path(work.getCurrentDumpPath(),
                ReplUtils.HIVE_RANGER_POLICIES_FILE_NAME), conf);
        int expectedPolicyCount = rangerExportPolicyList == null ? 0 : rangerExportPolicyList.getListSize();
        replLogger = new RangerLoadLogger(work.getSourceDbName(), work.getTargetDbName(),
          work.getCurrentDumpPath().toString(), expectedPolicyCount);
        replLogger.startLog();
        Map<String, Long> metricMap = new HashMap<>();
        metricMap.put(ReplUtils.MetricName.POLICIES.name(), (long) expectedPolicyCount);
        work.getMetricCollector().reportStageStart(getName(), metricMap);
        if (rangerExportPolicyList != null && !CollectionUtils.isEmpty(rangerExportPolicyList.getPolicies())) {
          rangerPolicies = rangerExportPolicyList.getPolicies();
        }
      }

      if (CollectionUtils.isEmpty(rangerPolicies)) {
        LOG.info("There are no ranger policies to import");
        rangerPolicies = new ArrayList<>();
      }

      List<RangerPolicy> updatedRangerPolicies = rangerRestClient.changeDataSet(rangerPolicies,
          work.getSourceDbName(), work.getTargetDbName());

      long importCount = 0;
      if (!CollectionUtils.isEmpty(updatedRangerPolicies)) {
        if (rangerExportPolicyList == null) {
          rangerExportPolicyList = new RangerExportPolicyList();
        }
        rangerExportPolicyList.setPolicies(updatedRangerPolicies);
        rangerRestClient.importRangerPolicies(rangerExportPolicyList, work.getTargetDbName(), rangerEndpoint,
                rangerHiveServiceName, conf);
        LOG.info("Number of ranger policies imported {}", rangerExportPolicyList.getListSize());
        importCount = rangerExportPolicyList.getListSize();
        work.getMetricCollector().reportStageProgress(getName(), ReplUtils.MetricName.POLICIES.name(), importCount);
        replLogger.endLog(importCount);
        LOG.info("Ranger policy import finished {} ", importCount);
      }
      work.getMetricCollector().reportStageEnd(getName(), Status.SUCCESS);
      return 0;
    } catch (RuntimeException e) {
      LOG.error("Runtime Excepton during RangerLoad", e);
      setException(e);
      try{
        ReplUtils.handleException(true, e, work.getCurrentDumpPath().getParent().toString(), work.getMetricCollector(),
                getName(), conf);
      } catch (Exception ex){
        LOG.error("Failed to collect replication metrics: ", ex);
      }
      throw e;
    } catch (Exception e) {
      LOG.error("RangerLoad Failed", e);
      int errorCode = ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
      setException(e);
      try{
        return ReplUtils.handleException(true, e, work.getCurrentDumpPath().getParent().toString(), work.getMetricCollector(),
                getName(), conf);
      } catch (Exception ex){
        LOG.error("Failed to collect replication metrics: ", ex);
        return errorCode;
      }
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
