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
package org.apache.hadoop.hive.ql.scheduled;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollRequest;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryProgressInfo;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetastoreBasedScheduledQueryService implements IScheduledQueryMaintenanceService {

  private static final Logger LOG = LoggerFactory.getLogger(MetastoreBasedScheduledQueryService.class);
  private HiveConf conf;

  public MetastoreBasedScheduledQueryService(HiveConf conf) {
    this.conf = conf;
  }

  @Override
  public ScheduledQueryPollResponse scheduledQueryPoll() {
    try {
      ScheduledQueryPollRequest request = new ScheduledQueryPollRequest();
      request.setClusterNamespace(getClusterNamespace());
      ScheduledQueryPollResponse resp = getMSC().scheduledQueryPoll(request);
      return resp;
    } catch (Exception e) {
      LOG.error("Exception while polling scheduled queries", e);
      return null;
    }
  }

  @Override
  public void scheduledQueryProgress(ScheduledQueryProgressInfo info) {
    try {
      getMSC().scheduledQueryProgress(info);
    } catch (Exception e) {
      LOG.error("Exception while updating scheduled execution status of: " + info.getScheduledExecutionId(), e);
    }
  }

  private IMetaStoreClient getMSC() throws MetaException, HiveException {
    return Hive.get(conf).getMSC();
  }

  @Override
  public String getClusterNamespace() {
    return conf.getVar(ConfVars.HIVE_SCHEDULED_QUERIES_NAMESPACE);

  }

}
