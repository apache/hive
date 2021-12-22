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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Metastore task to remove old replication metrics.
 */
public class ReplicationMetricsMaintTask implements MetastoreTaskThread {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationMetricsMaintTask.class);

  private Configuration conf;

  @Override
  public long initialDelay(TimeUnit unit) {
    // no delay before the first execution;
    return 0;
  }

  @Override
  public long runFrequency(TimeUnit unit) {
    return MetastoreConf.getTimeVar(conf, ConfVars.REPL_METRICS_CLEANUP_FREQUENCY,
        unit);
  }

  @Override
  public void setConf(Configuration configuration) {
    conf = configuration;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void run() {
    try {
      if (!MetastoreConf.getBoolVar(conf, ConfVars.SCHEDULED_QUERIES_ENABLED)) {
        return;
      }
      RawStore ms = HMSHandler.getMSForConf(conf);
      int maxRetainSecs = (int) MetastoreConf.getTimeVar(conf, ConfVars.REPL_METRICS_MAX_AGE, TimeUnit.SECONDS);
      LOG.info("Cleaning up Metrics older than {} ", maxRetainSecs);
      int deleteCnt = ms.deleteReplicationMetrics(maxRetainSecs);
      if (deleteCnt > 0L) {
        LOG.info("Number of deleted entries: {} " + deleteCnt);
      }
    } catch (Exception e) {
      LOG.error("Exception while trying to delete: " + e.getMessage(), e);
    }
  }
}
