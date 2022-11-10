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
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Status;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * RangerDenyTask.
 *
 * Task to add Ranger Deny Policy
 **/
public class RangerDenyTask extends Task<RangerDenyWork> implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RangerDenyTask.class);

    private transient RangerRestClient rangerRestClient;

    public RangerDenyTask() {
        super();
    }

    @VisibleForTesting
    RangerDenyTask(final RangerRestClient rangerRestClient, final HiveConf conf, final RangerDenyWork work) {
        this.conf = conf;
        this.work = work;
        this.rangerRestClient = rangerRestClient;
    }

    @Override
    public String getName() {
        return "RANGER_DENY";
    }

    @Override
    public int execute() {
        try {
            LOG.info("Checking Ranger Deny Policy for {}", work.getTargetDbName());
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
                throw new SemanticException(ErrorMsg.REPL_EXTERNAL_SERVICE_CONNECTION_ERROR.format(
                        ReplUtils.REPL_RANGER_SERVICE, "Ranger endpoint is not valid " + rangerEndpoint));
            }
            Map<String, Long> metricMap = new HashMap<>();
            metricMap.put(ReplUtils.MetricName.POLICIES.name(), 1L);
            work.getMetricCollector().reportStageStart(getName(), metricMap);
            if (conf.getBoolVar(HiveConf.ConfVars.REPL_RANGER_ADD_DENY_POLICY_TARGET)) {
                RangerPolicy rangerDenyPolicy = rangerRestClient.getDenyPolicyForReplicatedDb(rangerHiveServiceName,
                        work.getSourceDbName(), work.getTargetDbName());

                RangerExportPolicyList rangerExportPolicyList = new RangerExportPolicyList();
                rangerExportPolicyList.setPolicies(new ArrayList<RangerPolicy>() {{add(rangerDenyPolicy);}});
                rangerRestClient.importRangerPolicies(rangerExportPolicyList, work.getTargetDbName(), rangerEndpoint,
                        rangerHiveServiceName, conf);
                work.getMetricCollector().reportStageProgress(getName(), ReplUtils.MetricName.POLICIES.name(), 1);
                LOG.info("Created Ranger Deny policy for {}", work.getTargetDbName());
            } else {
                String policyName = work.getSourceDbName() + "_replication deny policy for " + work.getTargetDbName();
                rangerRestClient.deleteRangerPolicy(policyName, rangerEndpoint, rangerHiveServiceName, conf);
                work.getMetricCollector().reportStageProgress(getName(), ReplUtils.MetricName.POLICIES.name(), 1);
                LOG.info("Deleted Ranger Deny policy for {}", work.getTargetDbName());
            }
            work.getMetricCollector().reportStageEnd(getName(), Status.SUCCESS);
            return 0;
        } catch (RuntimeException e) {
            LOG.error("Runtime Excepton during Ranger Deny policy creation.", e);
            setException(e);
            try{
                ReplUtils.handleException(true, e, work.getCurrentDumpPath().getParent().toString(),
                        work.getMetricCollector(), getName(), conf);
            } catch (Exception ex){
                LOG.error("Failed to collect replication metrics: ", ex);
            }
            throw e;
        } catch (Exception e) {
            try {
                LOG.error("Ranger Deny policy creation Failed", e);
                setException(e);
                return ReplUtils.handleException(true, e, work.getCurrentDumpPath().getParent().toString(), work.getMetricCollector(),
                        getName(), conf);
            } catch (Exception ex) {
                LOG.error("Failed to collect replication metrics: ", ex);
                int errorCode = ErrorMsg.getErrorMsg(e.getMessage()).getErrorCode();
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
        return StageType.RANGER_DENY;
    }

    @Override
    public boolean canExecuteInParallel() {
        return false;
    }
}
