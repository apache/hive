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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.RawStore;
import java.util.concurrent.TimeUnit;

/**
 * Metastore task to handle RuntimeStat related expiration.
 */
public class RuntimeStatsCleanerTask implements MetastoreTaskThread {
  private static final Logger LOG = LoggerFactory.getLogger(RuntimeStatsCleanerTask.class);

  private Configuration conf;

  @Override
  public long runFrequency(TimeUnit unit) {
    return MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.RUNTIME_STATS_CLEAN_FREQUENCY, unit);
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
      RawStore ms = HMSHandler.getMSForConf(conf);
      int maxRetainSecs=(int) MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.RUNTIME_STATS_MAX_AGE, TimeUnit.SECONDS);
      int deleteCnt = ms.deleteRuntimeStats(maxRetainSecs);

      if (deleteCnt > 0L){
        LOG.info("Number of deleted entries: " + deleteCnt);
      }
    } catch (Exception e) {
      LOG.error("Exception while trying to delete: " + e.getMessage(), e);
    }
  }
}
